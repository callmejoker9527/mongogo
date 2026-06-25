package mongogo

import (
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// Database represents a MongoDB database.
type Database struct {
	Session *Session
	Name    string
}

// C returns a collection within the database.
func (db *Database) C(name string) *Collection {
	return &Collection{
		Database: db,
		Name:     name,
		FullName: db.Name + "." + name,
	}
}

// CollectionNames returns the names of all collections in the database.
func (db *Database) CollectionNames() ([]string, error) {
	ctx, cancel := db.Session.timeoutContext()
	defer cancel()
	return db.Session.client.Database(db.Name).ListCollectionNames(ctx, bson.D{})
}

// Run executes the given command against the database.
// cmd can be a bson.D, bson.M, or any struct with bson tags.
// result is the output destination.
func (db *Database) Run(cmd interface{}, result interface{}) error {
	ctx, cancel := db.Session.timeoutContext()
	defer cancel()

	var cmdDoc interface{}
	switch v := cmd.(type) {
	case string:
		cmdDoc = bson.D{{Key: v, Value: 1}}
	default:
		cmdDoc = v
	}

	mdb := db.Session.client.Database(db.Name)
	rp := db.Session.readPref()
	return mdb.RunCommand(ctx, cmdDoc, options.RunCmd().SetReadPreference(rp)).Decode(result)
}

// UpsertId upserts a document identified by id.
func (db *Database) UpsertId(id interface{}, update interface{}) (*ChangeInfo, error) {
	return db.C("_default").UpsertId(id, update)
}

// GridFS returns a GridFS instance for the given prefix.
func (db *Database) GridFS(prefix string) *GridFS {
	return newGridFS(db, prefix)
}

// Login authenticates the database with the given credentials.
// With mongo-driver v2, authentication is handled at connection time.
// This method is provided for API compatibility.
func (db *Database) Login(user, pass string) error {
	return nil
}

// Logout logs out from the database.
// With mongo-driver v2, this is a no-op.
func (db *Database) Logout() {
}

// CreateCollection creates a new collection with the given options.
func (db *Database) CreateCollection(name string, opts ...options.Lister[options.CreateCollectionOptions]) error {
	ctx, cancel := db.Session.timeoutContext()
	defer cancel()
	return db.Session.client.Database(db.Name).CreateCollection(ctx, name, opts...)
}

// DropDatabase removes the entire database including all its collections.
func (db *Database) DropDatabase() error {
	ctx, cancel := db.Session.timeoutContext()
	defer cancel()
	return db.Session.client.Database(db.Name).Drop(ctx)
}

// With returns a copy of the database bound to a different session.
func (db *Database) With(s *Session) *Database {
	return &Database{Session: s, Name: db.Name}
}

// Session returns the session this database is associated with.
func (db *Database) GetSession() *Session {
	return db.Session
}

// internal returns the underlying mongo.Database.
func (db *Database) internal() *mongo.Database {
	return db.Session.client.Database(db.Name)
}

// FindRef resolves a DBRef and decodes the referenced document into result.
func (db *Database) FindRef(ref *DBRef, result interface{}) error {
	var c *Collection
	if ref.Database == "" {
		c = db.C(ref.Collection)
	} else {
		c = db.Session.DB(ref.Database).C(ref.Collection)
	}
	return c.FindId(ref.Id).One(result)
}

// DBRef represents a database reference.
type DBRef struct {
	Collection string      `bson:"$ref"`
	Id         interface{} `bson:"$id"`
	Database   string      `bson:"$db,omitempty"`
}

// commandResult is a generic container for RunCommand responses.
type commandResult struct {
	bson.Raw
}

// listIndexesResult is used internally to list indexes.
type listIndexesResult struct {
	V          int32  `bson:"v"`
	Key        bson.D `bson:"key"`
	Name       string `bson:"name"`
	Unique     *bool  `bson:"unique,omitempty"`
	Sparse     *bool  `bson:"sparse,omitempty"`
	Background *bool  `bson:"background,omitempty"`
}

// Stats holds collection statistics.
type CollectionStats struct {
	Ns         string `bson:"ns"`
	Count      int    `bson:"count"`
	Size       int    `bson:"size"`
	AvgObjSize int    `bson:"avgObjSize"`
	StorageSize int   `bson:"storageSize"`
	NumExtents int    `bson:"numExtents"`
	Nindexes   int    `bson:"nindexes"`
	IndexSizes bson.M `bson:"indexSizes"`
}

// Stats returns statistics about the collection.
func (db *Database) Stats() (*CollectionStats, error) {
	result := &CollectionStats{}
	err := db.Run(bson.D{{Key: "dbStats", Value: 1}}, result)
	return result, err
}

// Eval is not supported in MongoDB 4.2+ and this method returns an error.
func (db *Database) Eval(expr interface{}, args ...interface{}) error {
	return ErrEvalNotSupported
}

// ErrEvalNotSupported is returned when Eval is called.
var ErrEvalNotSupported = &evalNotSupportedError{}

type evalNotSupportedError struct{}

func (e *evalNotSupportedError) Error() string {
	return "mongogo: $eval command was removed in MongoDB 4.2; use aggregation pipeline instead"
}

// Watch opens a change stream on the database.
// pipeline is an optional aggregation pipeline to filter/transform events.
// Requires MongoDB 4.0+ with a replica set or sharded cluster.
func (db *Database) Watch(pipeline interface{}, opts ...options.Lister[options.ChangeStreamOptions]) (*ChangeStream, error) {
	if pipeline == nil {
		pipeline = bson.A{}
	}
	ctx, cancel := db.Session.timeoutContext()
	defer cancel()
	cs, err := db.internal().Watch(ctx, pipeline, opts...)
	if err != nil {
		return nil, err
	}
	return newChangeStream(cs, db.Session), nil
}

// CreateView creates a read-only view based on an existing collection or view.
// name is the view name, viewOn is the source collection/view,
// and pipeline is the aggregation pipeline to apply.
// Requires MongoDB 3.4+.
func (db *Database) CreateView(name, viewOn string, pipeline interface{}, opts ...options.Lister[options.CreateCollectionOptions]) error {
	ctx, cancel := db.Session.timeoutContext()
	defer cancel()
	viewOpts := options.CreateView()
	return db.internal().CreateView(ctx, name, viewOn, pipeline, viewOpts)
}

// CreateCollectionWithOpts creates a collection with extended options.
// Supports capped collections, time-series (MongoDB 5.0+), validation rules, etc.
func (db *Database) CreateCollectionWithOpts(name string, opts *CreateCollectionOptions) error {
	ctx, cancel := db.Session.timeoutContext()
	defer cancel()

	driverOpts := options.CreateCollection()

	if opts != nil {
		if opts.Capped && opts.MaxBytes > 0 {
			driverOpts.SetCapped(true)
			driverOpts.SetSizeInBytes(opts.MaxBytes)
			if opts.MaxDocs > 0 {
				driverOpts.SetMaxDocuments(opts.MaxDocs)
			}
		}
		if opts.TimeSeries != nil {
			ts := options.TimeSeries().SetTimeField(opts.TimeSeries.TimeField)
			if opts.TimeSeries.MetaField != "" {
				ts.SetMetaField(opts.TimeSeries.MetaField)
			}
			if opts.TimeSeries.Granularity != "" {
				ts.SetGranularity(opts.TimeSeries.Granularity)
			}
			driverOpts.SetTimeSeriesOptions(ts)
		}
		if opts.Collation != nil {
			driverOpts.SetCollation(toDriverCollation(opts.Collation))
		}
		if opts.Validator != nil {
			driverOpts.SetValidator(opts.Validator)
			if opts.ValidationLevel != "" {
				driverOpts.SetValidationLevel(opts.ValidationLevel)
			}
			if opts.ValidationAction != "" {
				driverOpts.SetValidationAction(opts.ValidationAction)
			}
		}
	}

	return db.internal().CreateCollection(ctx, name, driverOpts)
}

// AddUser creates or updates a user in the database.
// Note: This uses the createUser/updateUser commands available in MongoDB 2.6+.
// The original mgo AddUser method is deprecated; prefer direct command execution.
func (db *Database) AddUser(user, pass string, readOnly bool) error {
	// Try updateUser first, then createUser on "not found" error
	roles := []bson.M{}
	if readOnly {
		roles = []bson.M{{"role": "read", "db": db.Name}}
	} else {
		roles = []bson.M{{"role": "readWrite", "db": db.Name}}
	}
	cmd := bson.D{
		{Key: "updateUser", Value: user},
		{Key: "pwd", Value: pass},
		{Key: "roles", Value: roles},
	}
	err := db.Run(cmd, nil)
	if err != nil {
		// If user doesn't exist, create them
		cmd[0] = bson.E{Key: "createUser", Value: user}
		return db.Run(cmd, nil)
	}
	return nil
}

// RemoveUser removes a user from the database.
func (db *Database) RemoveUser(user string) error {
	cmd := bson.D{{Key: "dropUser", Value: user}}
	return db.Run(cmd, nil)
}

// Command executes the given command and returns the result via RunCommand.
// Unlike Run, this returns a *mongo.SingleResult for chaining.
func (db *Database) Command(cmd interface{}) *mongo.SingleResult {
	ctx, cancel := db.Session.timeoutContext()
	defer cancel()
	rp, _ := readpref.New(readpref.PrimaryMode)
	return db.internal().RunCommand(ctx, cmd, options.RunCmd().SetReadPreference(rp))
}

// ListCollectionSpecs returns detailed specifications for all collections.
func (db *Database) ListCollectionSpecs(filter interface{}) ([]mongo.CollectionSpecification, error) {
	if filter == nil {
		filter = bson.D{}
	}
	ctx, cancel := db.Session.timeoutContext()
	defer cancel()
	return db.internal().ListCollectionSpecifications(ctx, filter)
}

