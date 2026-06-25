package mongogo

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Collection represents a MongoDB collection.
type Collection struct {
	Database *Database
	Name     string   // e.g. "users"
	FullName string   // e.g. "mydb.users"
}

// internal returns the underlying mongo.Collection.
func (c *Collection) internal() *mongo.Collection {
	return c.Database.Session.client.Database(c.Database.Name).Collection(c.Name)
}

// With returns a copy of the collection bound to a different session.
func (c *Collection) With(s *Session) *Collection {
	return &Collection{
		Database: &Database{Session: s, Name: c.Database.Name},
		Name:     c.Name,
		FullName: c.FullName,
	}
}

// EnsureIndex creates an index if it does not already exist.
func (c *Collection) EnsureIndex(index Index) error {
	model, err := indexToModel(index)
	if err != nil {
		return err
	}
	ctx, cancel := c.Database.Session.timeoutContext()
	defer cancel()
	_, err = c.internal().Indexes().CreateOne(ctx, model)
	return err
}

// EnsureIndexKey creates an index on the given fields, creating it if necessary.
func (c *Collection) EnsureIndexKey(key ...string) error {
	return c.EnsureIndex(Index{Key: key})
}

// DropIndex removes the index with the given key.
func (c *Collection) DropIndex(key ...string) error {
	name := indexName(key)
	ctx, cancel := c.Database.Session.timeoutContext()
	defer cancel()
	return c.internal().Indexes().DropOne(ctx, name)
}

// DropIndexName removes the index with the given name.
func (c *Collection) DropIndexName(name string) error {
	ctx, cancel := c.Database.Session.timeoutContext()
	defer cancel()
	return c.internal().Indexes().DropOne(ctx, name)
}

// DropAllIndexes removes all indexes from the collection except _id.
func (c *Collection) DropAllIndexes() error {
	ctx, cancel := c.Database.Session.timeoutContext()
	defer cancel()
	return c.internal().Indexes().DropAll(ctx)
}

// Indexes returns all indexes on the collection.
func (c *Collection) Indexes() ([]Index, error) {
	ctx, cancel := c.Database.Session.timeoutContext()
	defer cancel()

	cursor, err := c.internal().Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx) //nolint

	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	indexes := make([]Index, 0, len(results))
	for _, r := range results {
		idx := bsonToIndex(r)
		indexes = append(indexes, idx)
	}
	return indexes, nil
}

// Find returns a query to select documents matching the selector.
// selector can be nil, bson.D, bson.M, or a struct.
func (c *Collection) Find(selector interface{}) *Query {
	if selector == nil {
		selector = bson.D{}
	}
	return &Query{
		session:    c.Database.Session,
		collection: c,
		filter:     selector,
		limit:      0,
		skip:       0,
		prefetch:   0.25,
	}
}

// FindId returns a query that selects the document with the given id.
func (c *Collection) FindId(id interface{}) *Query {
	return c.Find(bson.D{{Key: "_id", Value: id}})
}

// Count returns the number of documents matching the selector.
func (c *Collection) Count() (n int, err error) {
	return c.Find(nil).Count()
}

// Insert inserts one or more documents into the collection.
func (c *Collection) Insert(docs ...interface{}) error {
	ctx, cancel := c.Database.Session.timeoutContext()
	defer cancel()

	col := c.internal()
	if len(docs) == 1 {
		_, err := col.InsertOne(ctx, docs[0])
		if err != nil {
			return convertError(err)
		}
		return nil
	}
	_, err := col.InsertMany(ctx, docs)
	return convertError(err)
}

// Update modifies the first document that matches the selector.
func (c *Collection) Update(selector interface{}, update interface{}) error {
	ctx, cancel := c.Database.Session.timeoutContext()
	defer cancel()

	result, err := c.internal().UpdateOne(ctx, selector, update)
	if err != nil {
		return convertError(err)
	}
	if result.MatchedCount == 0 {
		return ErrNotFound
	}
	return nil
}

// UpdateId modifies the document with the given id.
func (c *Collection) UpdateId(id interface{}, update interface{}) error {
	return c.Update(bson.D{{Key: "_id", Value: id}}, update)
}

// UpdateAll modifies all documents that match the selector.
func (c *Collection) UpdateAll(selector interface{}, update interface{}) (*ChangeInfo, error) {
	ctx, cancel := c.Database.Session.timeoutContext()
	defer cancel()

	result, err := c.internal().UpdateMany(ctx, selector, update)
	if err != nil {
		return nil, convertError(err)
	}
	return &ChangeInfo{
		Updated: int(result.ModifiedCount),
		Matched: int(result.MatchedCount),
	}, nil
}

// Upsert inserts or updates a document matching the selector.
func (c *Collection) Upsert(selector interface{}, update interface{}) (*ChangeInfo, error) {
	ctx, cancel := c.Database.Session.timeoutContext()
	defer cancel()

	opts := options.UpdateOne().SetUpsert(true)
	result, err := c.internal().UpdateOne(ctx, selector, update, opts)
	if err != nil {
		return nil, convertError(err)
	}
	info := &ChangeInfo{
		Updated: int(result.ModifiedCount),
		Matched: int(result.MatchedCount),
	}
	if result.UpsertedID != nil {
		info.UpsertedId = result.UpsertedID
	}
	return info, nil
}

// UpsertId inserts or updates the document with the given id.
func (c *Collection) UpsertId(id interface{}, update interface{}) (*ChangeInfo, error) {
	return c.Upsert(bson.D{{Key: "_id", Value: id}}, update)
}

// Remove deletes the first document that matches the selector.
func (c *Collection) Remove(selector interface{}) error {
	ctx, cancel := c.Database.Session.timeoutContext()
	defer cancel()

	result, err := c.internal().DeleteOne(ctx, selector)
	if err != nil {
		return convertError(err)
	}
	if result.DeletedCount == 0 {
		return ErrNotFound
	}
	return nil
}

// RemoveId deletes the document with the given id.
func (c *Collection) RemoveId(id interface{}) error {
	return c.Remove(bson.D{{Key: "_id", Value: id}})
}

// RemoveAll deletes all documents that match the selector.
func (c *Collection) RemoveAll(selector interface{}) (*ChangeInfo, error) {
	ctx, cancel := c.Database.Session.timeoutContext()
	defer cancel()

	result, err := c.internal().DeleteMany(ctx, selector)
	if err != nil {
		return nil, convertError(err)
	}
	return &ChangeInfo{Removed: int(result.DeletedCount)}, nil
}

// Apply runs a findAndModify command on the collection.
// It finds the document matching query and applies the change.
// If change.ReturnNew is true, result will have the modified document.
func (c *Collection) Apply(change Change, result interface{}) (*ChangeInfo, error) {
	return c.Find(nil).Apply(change, result)
}

// DropCollection removes the collection from the database.
func (c *Collection) DropCollection() error {
	ctx, cancel := c.Database.Session.timeoutContext()
	defer cancel()
	return c.internal().Drop(ctx)
}

// Rename renames the collection.
func (c *Collection) Rename(newName string) error {
	cmd := bson.D{
		{Key: "renameCollection", Value: c.FullName},
		{Key: "to", Value: c.Database.Name + "." + newName},
	}
	return c.Database.Session.DB("admin").Run(cmd, nil)
}

// Bulk returns a new Bulk operation for this collection.
func (c *Collection) Bulk() *Bulk {
	return &Bulk{
		collection: c,
		ordered:    true,
	}
}

// Pipe creates a new Aggregation pipeline.
func (c *Collection) Pipe(pipeline interface{}) *Pipe {
	return &Pipe{
		collection: c,
		pipeline:   pipeline,
	}
}

// Stats returns statistics about the collection.
func (c *Collection) Stats() (*CollectionStats, error) {
	result := &CollectionStats{}
	cmd := bson.D{{Key: "collStats", Value: c.Name}}
	if err := c.Database.Run(cmd, result); err != nil {
		return nil, err
	}
	return result, nil
}

// CreateIndexes creates multiple indexes at once.
// This is more efficient than calling EnsureIndex multiple times.
func (c *Collection) CreateIndexes(indexes []Index) error {
	models := make([]mongo.IndexModel, 0, len(indexes))
	for _, idx := range indexes {
		model, err := indexToModel(idx)
		if err != nil {
			return err
		}
		models = append(models, model)
	}
	ctx, cancel := c.Database.Session.timeoutContext()
	defer cancel()
	_, err := c.internal().Indexes().CreateMany(ctx, models)
	return err
}

// ReIndex drops and recreates all indexes on the collection.
func (c *Collection) ReIndex() error {
	cmd := bson.D{{Key: "reIndex", Value: c.Name}}
	return c.Database.Run(cmd, nil)
}

// ParallelScan is provided for API compatibility with mgo.
// In MongoDB 4.1+ the parallelCollectionScan command was removed.
// This implementation falls back to a single regular cursor.
// For true parallel scanning of large collections, use sharding or
// split range queries manually.
func (c *Collection) ParallelScan(nCursors int) ([]*Iter, error) {
	ctx, cancel := c.Database.Session.timeoutContext()
	// We cannot cancel immediately as Iter holds the context.
	_ = cancel

	cursor, err := c.internal().Find(ctx, bson.D{})
	if err != nil {
		cancel()
		return nil, err
	}
	it := &Iter{
		cursor:  cursor,
		ctx:     ctx,
		cancel:  cancel,
		session: c.Database.Session,
	}
	return []*Iter{it}, nil
}

// FindRef returns a Query for the document referenced by ref within this collection.
func (c *Collection) FindRef(ref *DBRef) *Query {
	return c.Find(bson.D{{Key: "_id", Value: ref.Id}})
}

// EstimatedCount returns an estimate of the number of documents in the collection.
// This is faster than Count() as it uses collection metadata rather than scanning.
// Available in MongoDB 4.0+.
func (c *Collection) EstimatedCount() (int, error) {
	ctx, cancel := c.Database.Session.timeoutContext()
	defer cancel()
	n, err := c.internal().EstimatedDocumentCount(ctx)
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

// Watch opens a change stream on the collection.
// pipeline is an optional aggregation pipeline to filter/transform events.
// Returns a ChangeStream that can be used to receive change events.
// Requires MongoDB 3.6+ and a replica set or sharded cluster.
func (c *Collection) Watch(pipeline interface{}, opts ...options.Lister[options.ChangeStreamOptions]) (*ChangeStream, error) {
	if pipeline == nil {
		pipeline = bson.A{}
	}
	ctx, cancel := c.Database.Session.timeoutContext()
	defer cancel()
	cs, err := c.internal().Watch(ctx, pipeline, opts...)
	if err != nil {
		return nil, err
	}
	return newChangeStream(cs, c.Database.Session), nil
}

// --- Index helpers ---

// indexName generates a standard MongoDB index name from field keys.
func indexName(key []string) string {
	var parts []string
	for _, k := range key {
		if strings.HasPrefix(k, "-") {
			parts = append(parts, k[1:]+"_-1")
		} else if strings.HasPrefix(k, "$") {
			// Special indexes: $2d, $2dsphere, $text, etc.
			switch k {
			case "$2d":
				parts = append(parts, "2d")
			case "$2dsphere":
				parts = append(parts, "2dsphere")
			case "$text":
				parts = append(parts, "text")
			case "$hashed":
				parts = append(parts, "hashed")
			default:
				parts = append(parts, k[1:])
			}
		} else {
			parts = append(parts, k+"_1")
		}
	}
	return strings.Join(parts, "_")
}

// indexToModel converts a mongogo Index to a mongo-driver IndexModel.
func indexToModel(index Index) (mongo.IndexModel, error) {
	keyDoc := bson.D{}
	for _, k := range index.Key {
		if strings.HasPrefix(k, "-") {
			keyDoc = append(keyDoc, bson.E{Key: k[1:], Value: -1})
		} else if k == "$text" || strings.HasPrefix(k, "@") {
			field := k
			if strings.HasPrefix(k, "@") {
				field = k[1:]
			}
			keyDoc = append(keyDoc, bson.E{Key: field, Value: "text"})
		} else if k == "$2d" || strings.HasPrefix(k, "#") {
			field := k
			if strings.HasPrefix(k, "#") {
				field = k[1:]
			}
			keyDoc = append(keyDoc, bson.E{Key: field, Value: "2d"})
		} else if k == "$2dsphere" {
			keyDoc = append(keyDoc, bson.E{Key: k[1:], Value: "2dsphere"})
		} else if k == "$hashed" {
			keyDoc = append(keyDoc, bson.E{Key: k[1:], Value: "hashed"})
		} else {
			keyDoc = append(keyDoc, bson.E{Key: k, Value: 1})
		}
	}

	opts := options.Index()
	if index.Name != "" {
		opts.SetName(index.Name)
	}
	if index.Unique {
		opts.SetUnique(true)
	}
	if index.Sparse {
		opts.SetSparse(true)
	}
	// Note: Background index builds are deprecated in MongoDB 4.2+ and removed in driver v2.
	// Indexes are always built in the foreground in modern MongoDB.
	_ = index.Background // acknowledged but not applied
	if index.ExpireAfter > 0 {
		secs := int32(index.ExpireAfter / time.Second)
		opts.SetExpireAfterSeconds(secs)
	}
	if index.DefaultLanguage != "" {
		opts.SetDefaultLanguage(index.DefaultLanguage)
	}
	if index.LanguageOverride != "" {
		opts.SetLanguageOverride(index.LanguageOverride)
	}
	if index.Weights != nil {
		w := bson.D{}
		for field, weight := range index.Weights {
			w = append(w, bson.E{Key: field, Value: weight})
		}
		opts.SetWeights(w)
	}
	if index.Collation != nil {
		opts.SetCollation(toDriverCollation(index.Collation))
	}
	if index.PartialFilter != nil {
		opts.SetPartialFilterExpression(index.PartialFilter)
	}
	if index.StorageEngine != nil {
		opts.SetStorageEngine(index.StorageEngine)
	}

	return mongo.IndexModel{
		Keys:    keyDoc,
		Options: opts,
	}, nil
}

// bsonToIndex converts a bson.M from ListIndexes to an Index struct.
func bsonToIndex(m bson.M) Index {
	idx := Index{}
	if name, ok := m["name"].(string); ok {
		idx.Name = name
	}
	if key, ok := m["key"].(bson.M); ok {
		for field, val := range key {
			switch v := val.(type) {
			case int32:
				if v == -1 {
					idx.Key = append(idx.Key, "-"+field)
				} else {
					idx.Key = append(idx.Key, field)
				}
			case string:
				idx.Key = append(idx.Key, "$"+v+":"+field)
			default:
				idx.Key = append(idx.Key, field)
			}
		}
	}
	if unique, ok := m["unique"].(bool); ok {
		idx.Unique = unique
	}
	if sparse, ok := m["sparse"].(bool); ok {
		idx.Sparse = sparse
	}
	if background, ok := m["background"].(bool); ok {
		idx.Background = background
	}
	if exp, ok := m["expireAfterSeconds"].(int32); ok {
		idx.ExpireAfter = time.Duration(exp) * time.Second
	}
	return idx
}

// toDriverCollation converts a mongogo Collation to a mongo-driver Collation.
func toDriverCollation(c *Collation) *options.Collation {
	if c == nil {
		return nil
	}
	return &options.Collation{
		Locale:          c.Locale,
		CaseLevel:       c.CaseLevel,
		CaseFirst:       c.CaseFirst,
		Strength:        c.Strength,
		NumericOrdering: c.NumericOrdering,
		Alternate:       c.Alternate,
		MaxVariable:     c.MaxVariable,
		Normalization:   c.Normalization,
		Backwards:       c.Backwards,
	}
}

// convertError translates mongo-driver errors to mongogo errors.
func convertError(err error) error {
	if err == nil {
		return nil
	}
	if err == mongo.ErrNoDocuments {
		return ErrNotFound
	}
	return err
}

// Pipe represents an aggregation pipeline.
type Pipe struct {
	collection *Collection
	pipeline   interface{}
	allowDisk  bool
	batchSize  int
	maxTime    time.Duration
	collation  *Collation
}

// AllowDiskUse allows the aggregation to write to temporary files on disk.
func (p *Pipe) AllowDiskUse() *Pipe {
	p.allowDisk = true
	return p
}

// Batch sets the batch size for the aggregation cursor.
func (p *Pipe) Batch(n int) *Pipe {
	p.batchSize = n
	return p
}

// SetMaxTime sets the maximum execution time for the pipeline.
func (p *Pipe) SetMaxTime(d time.Duration) *Pipe {
	p.maxTime = d
	return p
}

// All executes the pipeline and decodes all results into result (must be a pointer to a slice).
func (p *Pipe) All(result interface{}) error {
	ctx, cancel := p.collection.Database.Session.timeoutContext()
	defer cancel()

	opts := p.buildOptions()
	cursor, err := p.collection.internal().Aggregate(ctx, p.pipeline, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx) //nolint
	return cursor.All(ctx, result)
}

// One executes the pipeline and decodes the first result into result.
func (p *Pipe) One(result interface{}) error {
	ctx, cancel := p.collection.Database.Session.timeoutContext()
	defer cancel()

	opts := p.buildOptions()
	cursor, err := p.collection.internal().Aggregate(ctx, p.pipeline, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx) //nolint

	if !cursor.Next(ctx) {
		if err := cursor.Err(); err != nil {
			return err
		}
		return ErrNotFound
	}
	return cursor.Decode(result)
}

// Iter returns an Iter for the aggregation pipeline.
func (p *Pipe) Iter() *Iter {
	ctx, cancel := p.collection.Database.Session.timeoutContext()
	defer cancel()

	opts := p.buildOptions()
	cursor, err := p.collection.internal().Aggregate(ctx, p.pipeline, opts)
	return &Iter{
		cursor:  cursor,
		err:     err,
		ctx:     ctx,
		cancel:  cancel,
		session: p.collection.Database.Session,
	}
}

// Explain returns the execution plan for the pipeline.
func (p *Pipe) Explain(result interface{}) error {
	cmd := bson.D{
		{Key: "aggregate", Value: p.collection.Name},
		{Key: "pipeline", Value: p.pipeline},
		{Key: "explain", Value: true},
		{Key: "cursor", Value: bson.D{}},
	}
	return p.collection.Database.Run(cmd, result)
}

func (p *Pipe) buildOptions() *options.AggregateOptionsBuilder {
	opts := options.Aggregate()
	if p.allowDisk {
		opts.SetAllowDiskUse(true)
	}
	if p.batchSize > 0 {
		opts.SetBatchSize(int32(p.batchSize))
	}
	if p.collation != nil {
		opts.SetCollation(toDriverCollation(p.collation))
	}
	return opts
}

// MapReduce performs a map-reduce operation on the collection.
// Note: MapReduce was removed in MongoDB 5.0 for sharded clusters and is
// deprecated. Consider using the aggregation pipeline instead.
func (c *Collection) MapReduce(job *MapReduce, result interface{}) (*MapReduceInfo, error) {
	outArg := job.Out
	if outArg == nil {
		outArg = bson.M{"inline": 1}
	}

	cmd := bson.D{
		{Key: "mapReduce", Value: c.Name},
		{Key: "map", Value: job.Map},
		{Key: "reduce", Value: job.Reduce},
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

	var raw bson.M
	if err := c.Database.Run(cmd, &raw); err != nil {
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
	if t, ok := raw["timeMillis"].(int32); ok {
		info.Time = int64(t)
	}

	// Decode inline results
	if results, ok := raw["results"]; ok && result != nil {
		data, err := bson.Marshal(bson.M{"results": results})
		if err != nil {
			return info, err
		}
		type resultWrapper struct {
			Results bson.Raw `bson:"results"`
		}
		var rw resultWrapper
		if err := bson.Unmarshal(data, &rw); err != nil {
			return info, err
		}
		if err := bson.Unmarshal(rw.Results, result); err != nil {
			return info, fmt.Errorf("mapreduce result decode: %v", err)
		}
	}

	return info, nil
}

// context returns a context with the session timeout.
func (c *Collection) context() (context.Context, context.CancelFunc) {
	return c.Database.Session.timeoutContext()
}

