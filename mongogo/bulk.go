package mongogo

import (
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Bulk represents a series of bulk write operations for a collection.
// The Bulk type provides an mgo-compatible batched write API.
type Bulk struct {
	collection *Collection
	ordered    bool
	models     []mongo.WriteModel
}

// BulkResult holds the result of a bulk write operation.
type BulkResult struct {
	Matched   int
	Modified  int
	Inserted  int
	Upserted  int
	Deleted   int
	UpsertedIds map[int64]interface{}
}

// Unordered configures the bulk operation to run in unordered mode,
// meaning all operations are run regardless of individual failures.
func (b *Bulk) Unordered() *Bulk {
	b.ordered = false
	return b
}

// Insert queues an insert operation in the bulk write.
func (b *Bulk) Insert(docs ...interface{}) {
	for _, doc := range docs {
		b.models = append(b.models, mongo.NewInsertOneModel().SetDocument(doc))
	}
}

// Update queues a single update operation matching the selector.
func (b *Bulk) Update(pairs ...interface{}) {
	if len(pairs)%2 != 0 {
		panic("mongogo: Bulk.Update requires pairs of (selector, update)")
	}
	for i := 0; i < len(pairs); i += 2 {
		b.models = append(b.models,
			mongo.NewUpdateOneModel().
				SetFilter(pairs[i]).
				SetUpdate(pairs[i+1]))
	}
}

// UpdateAll queues update operations that update all matching documents.
func (b *Bulk) UpdateAll(pairs ...interface{}) {
	if len(pairs)%2 != 0 {
		panic("mongogo: Bulk.UpdateAll requires pairs of (selector, update)")
	}
	for i := 0; i < len(pairs); i += 2 {
		b.models = append(b.models,
			mongo.NewUpdateManyModel().
				SetFilter(pairs[i]).
				SetUpdate(pairs[i+1]))
	}
}

// Upsert queues upsert operations (update or insert).
func (b *Bulk) Upsert(pairs ...interface{}) {
	if len(pairs)%2 != 0 {
		panic("mongogo: Bulk.Upsert requires pairs of (selector, update)")
	}
	for i := 0; i < len(pairs); i += 2 {
		b.models = append(b.models,
			mongo.NewUpdateOneModel().
				SetFilter(pairs[i]).
				SetUpdate(pairs[i+1]).
				SetUpsert(true))
	}
}

// Remove queues a single remove operation.
func (b *Bulk) Remove(selectors ...interface{}) {
	for _, sel := range selectors {
		b.models = append(b.models, mongo.NewDeleteOneModel().SetFilter(sel))
	}
}

// RemoveAll queues remove-all operations.
func (b *Bulk) RemoveAll(selectors ...interface{}) {
	for _, sel := range selectors {
		b.models = append(b.models, mongo.NewDeleteManyModel().SetFilter(sel))
	}
}

// Run executes all queued bulk operations and returns the combined result.
func (b *Bulk) Run() (*BulkResult, error) {
	if len(b.models) == 0 {
		return &BulkResult{}, nil
	}

	ctx, cancel := b.collection.Database.Session.timeoutContext()
	defer cancel()

	opts := options.BulkWrite().SetOrdered(b.ordered)
	result, err := b.collection.internal().BulkWrite(ctx, b.models, opts)
	if err != nil {
		// Check for BulkWriteException to extract partial results.
		var bwe mongo.BulkWriteException
		if ok := isBulkWriteException(err, &bwe); ok {
			br := bulkResultFromDriverResult(result)
			return br, &BulkError{Cases: extractBulkCases(bwe)}
		}
		return nil, err
	}

	return bulkResultFromDriverResult(result), nil
}

func bulkResultFromDriverResult(r *mongo.BulkWriteResult) *BulkResult {
	if r == nil {
		return &BulkResult{}
	}
	br := &BulkResult{
		Matched:     int(r.MatchedCount),
		Modified:    int(r.ModifiedCount),
		Inserted:    int(r.InsertedCount),
		Upserted:    int(r.UpsertedCount),
		Deleted:     int(r.DeletedCount),
		UpsertedIds: make(map[int64]interface{}),
	}
	for k, v := range r.UpsertedIDs {
		br.UpsertedIds[k] = v
	}
	return br
}

func isBulkWriteException(err error, out *mongo.BulkWriteException) bool {
	var bwe mongo.BulkWriteException
	if e, ok := err.(mongo.BulkWriteException); ok {
		*out = e
		return true
	}
	_ = bwe
	return false
}

// BulkError represents errors from a bulk write operation.
type BulkError struct {
	Cases []BulkErrorCase
}

func (e *BulkError) Error() string {
	if len(e.Cases) == 0 {
		return "mongogo: bulk write error"
	}
	return e.Cases[0].Err.Error()
}

// BulkErrorCase represents a single error case within a bulk operation.
type BulkErrorCase struct {
	Index int
	Err   error
}

// IsDup returns true if the bulk error was caused by a duplicate key.
func (e *BulkError) IsDup() bool {
	for _, c := range e.Cases {
		if IsDup(c.Err) {
			return true
		}
	}
	return false
}

func extractBulkCases(bwe mongo.BulkWriteException) []BulkErrorCase {
	cases := make([]BulkErrorCase, 0, len(bwe.WriteErrors))
	for _, we := range bwe.WriteErrors {
		cases = append(cases, BulkErrorCase{
			Index: we.Index,
			Err:   we,
		})
	}
	return cases
}

// IsDup returns true if the error is a MongoDB duplicate key error.
func IsDup(err error) bool {
	if err == nil {
		return false
	}
	we, ok := err.(mongo.WriteException)
	if ok {
		for _, e := range we.WriteErrors {
			if e.Code == 11000 || e.Code == 11001 || e.Code == 12582 {
				return true
			}
		}
	}
	// Check for bulk write error codes
	bwe, ok2 := err.(mongo.BulkWriteException)
	if ok2 {
		for _, e := range bwe.WriteErrors {
			if e.Code == 11000 || e.Code == 11001 || e.Code == 12582 {
				return true
			}
		}
	}
	return false
}

// IsDupKey is an alias for IsDup.
var IsDupKey = IsDup

// IsErrNoDocuments returns true if the error is ErrNotFound.
func IsErrNoDocuments(err error) bool {
	return err == ErrNotFound
}

// ParseIndexKey parses an mgo-style index key spec into a bson.D.
// Fields starting with "-" are descending.
func ParseIndexKey(key []string) bson.D {
	doc := bson.D{}
	for _, k := range key {
		if len(k) > 0 && k[0] == '-' {
			doc = append(doc, bson.E{Key: k[1:], Value: -1})
		} else {
			doc = append(doc, bson.E{Key: k, Value: 1})
		}
	}
	return doc
}

