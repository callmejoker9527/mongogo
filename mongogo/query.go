package mongogo

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Query represents a MongoDB query.
type Query struct {
	session    *Session
	collection *Collection
	filter     interface{}
	sort       bson.D
	projection interface{}
	hint       interface{}
	skip       int64
	limit      int64
	prefetch   float64
	maxTime    time.Duration
	comment    string
	snapshot   bool
	batchSize  int32
	collation  *Collation
	noCursorTimeout bool
	allowDiskUse    bool
}

// Sort sets the sort order for the query.
// Use "-field" for descending order.
func (q *Query) Sort(fields ...string) *Query {
	q.sort = bson.D{}
	for _, field := range fields {
		if len(field) > 0 && field[0] == '-' {
			q.sort = append(q.sort, bson.E{Key: field[1:], Value: -1})
		} else {
			q.sort = append(q.sort, bson.E{Key: field, Value: 1})
		}
	}
	return q
}

// Select sets the projection (fields to return).
// Use bson.M{"field": 1} to include fields, bson.M{"field": 0} to exclude.
func (q *Query) Select(selector interface{}) *Query {
	q.projection = selector
	return q
}

// Skip sets the number of documents to skip.
func (q *Query) Skip(n int) *Query {
	q.skip = int64(n)
	return q
}

// Limit sets the maximum number of documents to return.
// A negative limit closes the cursor after returning the absolute value of documents.
func (q *Query) Limit(n int) *Query {
	q.limit = int64(n)
	return q
}

// Hint sets the index hint for the query.
func (q *Query) Hint(indexKey ...string) *Query {
	doc := bson.D{}
	for _, k := range indexKey {
		if len(k) > 0 && k[0] == '-' {
			doc = append(doc, bson.E{Key: k[1:], Value: -1})
		} else {
			doc = append(doc, bson.E{Key: k, Value: 1})
		}
	}
	q.hint = doc
	return q
}

// SetMaxTime sets the maximum execution time for the query.
func (q *Query) SetMaxTime(d time.Duration) *Query {
	q.maxTime = d
	return q
}

// Batch sets the batch size for the cursor.
func (q *Query) Batch(n int) *Query {
	q.batchSize = int32(n)
	return q
}

// Prefetch sets the pre-fetch ratio for the query (ignored, for API compat).
func (q *Query) Prefetch(p float64) *Query {
	q.prefetch = p
	return q
}

// Comment adds a comment to the query (for profiling/logging).
func (q *Query) Comment(comment string) *Query {
	q.comment = comment
	return q
}

// Snapshot requests the snapshot mode (deprecated in MongoDB 3.6+, no-op here).
func (q *Query) Snapshot() *Query {
	q.snapshot = true
	return q
}

// Collation sets the collation for the query.
func (q *Query) Collation(collation *Collation) *Query {
	q.collation = collation
	return q
}

// NoCursorTimeout disables the server-side idle cursor timeout.
func (q *Query) NoCursorTimeout() *Query {
	q.noCursorTimeout = true
	return q
}

// Count returns the number of documents matching the query.
func (q *Query) Count() (int, error) {
	ctx, cancel := q.session.timeoutContext()
	defer cancel()

	opts := options.Count()
	if q.hint != nil {
		opts.SetHint(q.hint)
	}

	n, err := q.collection.internal().CountDocuments(ctx, q.filter, opts)
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

// One executes the query and decodes the first result into result.
// Returns ErrNotFound if no document matches.
func (q *Query) One(result interface{}) error {
	ctx, cancel := q.session.timeoutContext()
	defer cancel()

	opts := q.buildFindOneOptions()
	err := q.collection.internal().FindOne(ctx, q.filter, opts).Decode(result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return ErrNotFound
		}
		return err
	}
	return nil
}

// All executes the query and decodes all results into result (must be a pointer to a slice).
func (q *Query) All(result interface{}) error {
	ctx, cancel := q.session.timeoutContext()
	defer cancel()

	opts := q.buildFindOptions()
	cursor, err := q.collection.internal().Find(ctx, q.filter, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx) //nolint
	return cursor.All(ctx, result)
}

// Distinct returns a list of unique values for the given field.
func (q *Query) Distinct(key string, result interface{}) error {
	ctx, cancel := q.session.timeoutContext()
	defer cancel()

	opts := options.Distinct()
	if q.collation != nil {
		opts.SetCollation(toDriverCollation(q.collation))
	}

	// In driver v2, Distinct returns *DistinctResult
	dr := q.collection.internal().Distinct(ctx, key, q.filter, opts)
	if err := dr.Err(); err != nil {
		return err
	}
	return dr.Decode(result)
}

// Iter returns a cursor iterator for the query results.
func (q *Query) Iter() *Iter {
	ctx := context.Background()
	if q.session.sockTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, q.session.sockTimeout)
		_ = cancel // stored in Iter
	}

	opts := q.buildFindOptions()
	cursor, err := q.collection.internal().Find(ctx, q.filter, opts)
	return newIter(cursor, err, q.session)
}

// Tail returns a tailable cursor (for capped collections).
func (q *Query) Tail(timeout time.Duration) *Iter {
	ctx := context.Background()
	opts := q.buildFindOptions()
	opts.SetCursorType(options.TailableAwait)
	if timeout > 0 {
		opts.SetMaxAwaitTime(timeout)
	}
	cursor, err := q.collection.internal().Find(ctx, q.filter, opts)
	return newIter(cursor, err, q.session)
}

// Apply runs a findAndModify command on the first document matched by the query.
func (q *Query) Apply(change Change, result interface{}) (*ChangeInfo, error) {
	ctx, cancel := q.session.timeoutContext()
	defer cancel()

	info := &ChangeInfo{}

	if change.Remove {
		opts := options.FindOneAndDelete()
		if q.sort != nil {
			opts.SetSort(q.sort)
		}
		if q.projection != nil {
			opts.SetProjection(q.projection)
		}
	if q.collation != nil {
		opts.SetCollation(toDriverCollation(q.collation))
	}
	err := q.collection.internal().FindOneAndDelete(ctx, q.filter, opts).Decode(result)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return info, ErrNotFound
			}
			return info, err
		}
		info.Removed = 1
		return info, nil
	}

	opts := options.FindOneAndUpdate()
	if change.Upsert {
		opts.SetUpsert(true)
	}
	if change.ReturnNew {
		opts.SetReturnDocument(options.After)
	} else {
		opts.SetReturnDocument(options.Before)
	}
	if q.sort != nil {
		opts.SetSort(q.sort)
	}
	if q.projection != nil {
		opts.SetProjection(q.projection)
	}
	if q.collation != nil {
		opts.SetCollation(toDriverCollation(q.collation))
	}

	err := q.collection.internal().FindOneAndUpdate(ctx, q.filter, change.Update, opts).Decode(result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			if change.Upsert {
				return info, nil
			}
			return info, ErrNotFound
		}
		return info, err
	}
	info.Updated = 1
	return info, nil
}

// MapReduce is provided for API compatibility.
// Note: MapReduce was deprecated in MongoDB 5.0 and removed for sharded clusters.
// Use the aggregation pipeline instead.
func (q *Query) MapReduce(job *MapReduce, result interface{}) (*MapReduceInfo, error) {
	outArg := job.Out
	if outArg == nil {
		outArg = bson.M{"inline": 1}
	}

	cmd := bson.D{
		{Key: "mapReduce", Value: q.collection.Name},
		{Key: "map", Value: job.Map},
		{Key: "reduce", Value: job.Reduce},
		{Key: "query", Value: q.filter},
		{Key: "out", Value: outArg},
	}
	if job.Finalize != "" {
		cmd = append(cmd, bson.E{Key: "finalize", Value: job.Finalize})
	}
	if job.Scope != nil {
		cmd = append(cmd, bson.E{Key: "scope", Value: job.Scope})
	}
	if job.Verbose {
		cmd = append(cmd, bson.E{Key: "verbose", Value: true})
	}
	if q.sort != nil {
		cmd = append(cmd, bson.E{Key: "sort", Value: q.sort})
	}
	if q.limit > 0 {
		cmd = append(cmd, bson.E{Key: "limit", Value: q.limit})
	}

	var raw bson.M
	if err := q.collection.Database.Run(cmd, &raw); err != nil {
		return nil, err
	}

	info := &MapReduceInfo{}
	if counts, ok := raw["counts"].(bson.M); ok {
		if n, ok := counts["input"].(int32); ok {
			info.InputCount = int(n)
		}
		if n, ok := counts["emit"].(int32); ok {
			info.EmitCount = int(n)
		}
		if n, ok := counts["output"].(int32); ok {
			info.OutputCount = int(n)
		}
	}
	return info, nil
}

// Where adds a $where JavaScript condition to the query filter.
// Note: $where uses JavaScript evaluation and may be slow.
// Consider using aggregation $expr for better performance on MongoDB 3.6+.
func (q *Query) Where(js string) *Query {
	q.filter = bson.D{
		{Key: "$and", Value: bson.A{
			q.filter,
			bson.D{{Key: "$where", Value: js}},
		}},
	}
	return q
}

// For is an alias for Where.
func (q *Query) For(js string) *Query {
	return q.Where(js)
}

// SetMaxScan limits the number of documents scanned.
// Corresponds to the $maxScan modifier (deprecated in MongoDB 4.0, removed in 4.4+).
// This is kept for API compatibility; prefer Limit() for limiting result size.
func (q *Query) SetMaxScan(n int64) *Query {
	// $maxScan was removed in MongoDB 4.4; this is a no-op for compatibility.
	_ = n
	return q
}

// AllowDiskUse allows the server to write temporary files for queries with large result sets.
// Supported in MongoDB 4.4+.
func (q *Query) AllowDiskUse() *Query {
	q.allowDiskUse = true
	return q
}

// Explain returns the query execution plan.
// verbosity can be "queryPlanner", "executionStats" (default), or "allPlansExecution".
func (q *Query) Explain(result interface{}) error {
	return q.ExplainVerbosity("executionStats", result)
}

// ExplainVerbosity returns the query execution plan with the specified verbosity level.
// verbosity: "queryPlanner" | "executionStats" | "allPlansExecution"
func (q *Query) ExplainVerbosity(verbosity string, result interface{}) error {
	if verbosity == "" {
		verbosity = "executionStats"
	}
	findCmd := bson.D{
		{Key: "find", Value: q.collection.Name},
		{Key: "filter", Value: q.filter},
	}
	if q.sort != nil {
		findCmd = append(findCmd, bson.E{Key: "sort", Value: q.sort})
	}
	if q.projection != nil {
		findCmd = append(findCmd, bson.E{Key: "projection", Value: q.projection})
	}
	if q.skip > 0 {
		findCmd = append(findCmd, bson.E{Key: "skip", Value: q.skip})
	}
	if q.limit != 0 {
		findCmd = append(findCmd, bson.E{Key: "limit", Value: q.limit})
	}
	if q.hint != nil {
		findCmd = append(findCmd, bson.E{Key: "hint", Value: q.hint})
	}
	cmd := bson.D{
		{Key: "explain", Value: findCmd},
		{Key: "verbosity", Value: verbosity},
	}
	return q.collection.Database.Run(cmd, result)
}

// buildFindOneOptions builds mongo-driver FindOneOptions from the query.
func (q *Query) buildFindOneOptions() *options.FindOneOptionsBuilder {
	opts := options.FindOne()
	if q.projection != nil {
		opts.SetProjection(q.projection)
	}
	if q.sort != nil {
		opts.SetSort(q.sort)
	}
	if q.skip > 0 {
		opts.SetSkip(q.skip)
	}
	if q.hint != nil {
		opts.SetHint(q.hint)
	}
	if q.comment != "" {
		opts.SetComment(q.comment)
	}
	if q.collation != nil {
		opts.SetCollation(toDriverCollation(q.collation))
	}
	return opts
}

// buildFindOptions builds mongo-driver FindOptions from the query.
func (q *Query) buildFindOptions() *options.FindOptionsBuilder {
	opts := options.Find()
	if q.projection != nil {
		opts.SetProjection(q.projection)
	}
	if q.sort != nil {
		opts.SetSort(q.sort)
	}
	if q.skip > 0 {
		opts.SetSkip(q.skip)
	}
	if q.limit != 0 {
		opts.SetLimit(q.limit)
	}
	if q.hint != nil {
		opts.SetHint(q.hint)
	}
	if q.batchSize > 0 {
		opts.SetBatchSize(q.batchSize)
	}
	if q.comment != "" {
		opts.SetComment(q.comment)
	}
	if q.collation != nil {
		opts.SetCollation(toDriverCollation(q.collation))
	}
	if q.noCursorTimeout {
		opts.SetNoCursorTimeout(true)
	}
	if q.allowDiskUse {
		opts.SetAllowDiskUse(true)
	}
	return opts
}

