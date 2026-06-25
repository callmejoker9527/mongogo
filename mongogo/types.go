// mongogo - MongoDB driver for Go (based on mgo API, powered by mongo-driver v2)
//
// This package provides an mgo-compatible API on top of the official
// MongoDB Go driver v2, supporting modern MongoDB versions (4.0+).
package mongogo

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Mode represents the session consistency mode.
type Mode int

const (
	// Eventual mode: reads from the nearest member, may change servers between reads.
	Eventual Mode = 0
	// Monotonic mode: reads from secondary before first write, then primary.
	Monotonic Mode = 1
	// Strong mode (Primary): all operations go to the primary.
	Strong Mode = 2
	// Primary: same as Strong.
	Primary Mode = 2
	// PrimaryPreferred: read from primary if available, otherwise secondary.
	PrimaryPreferred Mode = 3
	// Secondary: read from nearest secondary.
	Secondary Mode = 4
	// SecondaryPreferred: read from secondary if available, otherwise primary.
	SecondaryPreferred Mode = 5
	// Nearest: read from nearest member regardless of primary/secondary.
	Nearest Mode = 6
)

var (
	// ErrNotFound is returned when a query finds no results.
	ErrNotFound = errors.New("not found")
	// ErrCursor is returned when an invalid cursor is used.
	ErrCursor = errors.New("invalid cursor")
)

// D is an ordered representation of a BSON document (alias for bson.D).
type D = bson.D

// M is an unordered representation of a BSON document (alias for bson.M).
type M = bson.M

// E is a single key-value pair in a D document (alias for bson.E).
type E = bson.E

// A is a BSON array (alias for bson.A).
type A = bson.A

// ObjectId is a MongoDB ObjectID (alias for bson.ObjectID).
type ObjectId = bson.ObjectID

// NewObjectId creates a new ObjectID.
func NewObjectId() ObjectId {
	return bson.NewObjectID()
}

// ObjectIdHex creates an ObjectID from a hex string.
func ObjectIdHex(s string) ObjectId {
	oid, err := bson.ObjectIDFromHex(s)
	if err != nil {
		panic("invalid ObjectId hex: " + s)
	}
	return oid
}

// IsObjectIdHex returns true if s is a valid ObjectID hex string.
func IsObjectIdHex(s string) bool {
	_, err := bson.ObjectIDFromHex(s)
	return err == nil
}

// Safe defines the write concern settings.
type Safe struct {
	// W is the write concern (number of servers or "majority").
	W int
	// WMode is the write concern string mode (e.g. "majority").
	WMode string
	// WTimeout is the write concern timeout in milliseconds.
	WTimeout int
	// FSync if true, waits for fsync.
	FSync bool
	// J if true, waits for journaling.
	J bool
}

// DialInfo holds options for establishing a session with a MongoDB cluster.
type DialInfo struct {
	// Addrs holds the addresses for the seed servers.
	Addrs []string

	// Direct disables replica set discovery.
	Direct bool

	// Timeout is the amount of time to wait for a server to respond.
	Timeout time.Duration

	// FailFast causes failures to be reported sooner.
	FailFast bool

	// Database is the default database name.
	Database string

	// ReplicaSetName restricts the session to the named replica set.
	ReplicaSetName string

	// Source is the database used for authentication.
	Source string

	// Service is the GSSAPI service name.
	Service string

	// ServiceHost is the GSSAPI service host.
	ServiceHost string

	// Mechanism is the authentication mechanism (e.g. "SCRAM-SHA-256").
	Mechanism string

	// Username and Password for authentication.
	Username string
	Password string

	// PoolLimit defines the per-server connection pool limit. Defaults to 100.
	PoolLimit int

	// MinPoolSize defines the minimum number of connections in the pool.
	MinPoolSize int

	// MaxConnIdleTime defines how long a connection can be idle before being closed.
	MaxConnIdleTime time.Duration

	// AppName is the name of the application.
	AppName string

	// TLSConfig if set, enables TLS.
	// Use *tls.Config from crypto/tls package.
	TLSConfig interface{}
}

// Credential holds authentication info for a specific database.
type Credential struct {
	Username    string
	Password    string
	Source      string
	Service     string
	ServiceHost string
	Mechanism   string
}

// Index describes a MongoDB index.
type Index struct {
	Key        []string // Index key fields (use "-field" for descending).
	Unique     bool
	DropDups   bool // Deprecated in MongoDB 3.0+
	Background bool
	Sparse     bool

	// ExpireAfter is used for TTL indexes.
	ExpireAfter time.Duration

	// Name is the index name. If empty, an auto-generated name is used.
	Name string

	// Min and Max are used for 2d indexes.
	Min, Max int

	// Minf and Maxf are float versions for 2d indexes.
	Minf, Maxf float64

	// BucketSize is used for geoHaystack indexes.
	BucketSize float64

	// Bits is the precision for 2d indexes.
	Bits int

	// DefaultLanguage sets the language for text indexes.
	DefaultLanguage string

	// LanguageOverride sets the field that overrides the default language.
	LanguageOverride string

	// TextIndexVersion and Weights are for text indexes.
	TextIndexVersion int
	Weights          map[string]int

	// Collation specifies the collation for the index.
	Collation *Collation

	// PartialFilter is a filter expression for partial indexes.
	PartialFilter interface{}

	// WildcardProjection is for wildcard indexes.
	WildcardProjection interface{}

	// StorageEngine specifies storage engine options.
	StorageEngine interface{}
}

// Collation specifies language-specific rules for string comparison.
type Collation struct {
	Locale          string `bson:"locale"`
	CaseLevel       bool   `bson:"caseLevel,omitempty"`
	CaseFirst       string `bson:"caseFirst,omitempty"`
	Strength        int    `bson:"strength,omitempty"`
	NumericOrdering bool   `bson:"numericOrdering,omitempty"`
	Alternate       string `bson:"alternate,omitempty"`
	MaxVariable     string `bson:"maxVariable,omitempty"`
	Normalization   bool   `bson:"normalization,omitempty"`
	Backwards       bool   `bson:"backwards,omitempty"`
}

// ChangeInfo holds information about the result of an update or remove.
type ChangeInfo struct {
	// Updated is the number of existing documents updated.
	Updated int
	// Removed is the number of documents removed.
	Removed int
	// Matched is the number of documents matched (before update).
	Matched int
	// UpsertedId is the _id of the upserted document (if applicable).
	UpsertedId interface{}
}

// MapReduce holds the parameters for a map/reduce job.
type MapReduce struct {
	Map      string // JavaScript map function
	Reduce   string // JavaScript reduce function
	Finalize string // Optional JavaScript finalize function
	Out      interface{}
	Scope    interface{}
	Verbose  bool
}

// MapReduceInfo holds the result info for a map/reduce operation.
type MapReduceInfo struct {
	InputCount  int
	EmitCount   int
	OutputCount int
	Database    string
	Collection  string
	Time        int64
	VerboseTime *MapReduceTime
}

// MapReduceTime breaks down the time spent in a map/reduce operation.
type MapReduceTime struct {
	Total    int64
	Map      int64
	EmitLoop int64
	EmitWait int64
	Reduce   int64
	Mode     int64
	Output   int64
}

// Change holds the parameters for a findAndModify operation.
type Change struct {
	Update    interface{} // Update document
	Upsert    bool        // Insert if not found
	Remove    bool        // Remove the found document
	ReturnNew bool        // Return the new document instead of the old
}

// BuildInfo holds version information about the MongoDB server.
type BuildInfo struct {
	Version        string
	VersionArray   []int  `bson:"versionArray"`
	GitVersion     string `bson:"gitVersion"`
	OpenSSLVersion string `bson:"OpenSSLVersion"`
	SysInfo        string `bson:"sysInfo"`
	Bits           int
	Debug          bool
	MaxObjectSize  int `bson:"maxBsonObjectSize"`
}

// GetLastError is the result of a getLastError command.
type GetLastError struct {
	Err             string `bson:"err"`
	Code            int
	ConnectionID    int32 `bson:"connectionId"`
	LastOp          int64 `bson:"lastOp"`
	N               int
	Shards          interface{} `bson:"singleShard"`
	UpdatedExisting bool        `bson:"updatedExisting"`
	Upserted        interface{}
	Ok              bool
}

func (e *GetLastError) Error() string {
	return e.Err
}

// Raw is an alias for bson.Raw, representing a raw BSON document.
// This can be used for delayed decoding or to inspect raw BSON data.
type Raw = bson.Raw

// RawD is an alias for bson.D (ordered raw document).
type RawD = bson.D

// ----- ObjectId utilities -----

// NewObjectIdWithTime creates a new ObjectID from a given time.
// The returned ObjectID will have its time component set to t, while the
// remaining components will be derived from the machine, PID, and a counter.
func NewObjectIdWithTime(t time.Time) ObjectId {
	return bson.NewObjectIDFromTimestamp(t)
}

// ObjectIdTime extracts the time component from an ObjectID.
func ObjectIdTime(id ObjectId) time.Time {
	return id.Timestamp()
}

// ----- Global debug/logging -----

// Logger is the interface for custom log output.
type Logger interface {
	Output(calldepth int, s string) error
}

var (
	globalDebug  bool
	globalLogger Logger = log.New(nil, "", 0) // silent by default
	globalMu     sync.Mutex
)

// SetDebug enables or disables debug logging.
// When enabled, the driver will log detailed information about operations.
func SetDebug(enabled bool) {
	globalMu.Lock()
	defer globalMu.Unlock()
	globalDebug = enabled
}

// SetLogger sets a custom logger for the package.
// Pass nil to disable logging.
func SetLogger(l Logger) {
	globalMu.Lock()
	defer globalMu.Unlock()
	if l == nil {
		globalLogger = log.New(nil, "", 0)
	} else {
		globalLogger = l
	}
}

// debugLog logs a message if debug logging is enabled.
// It is used internally by the package when SetDebug(true) is enabled.
func debugLog(format string, args ...interface{}) {
	globalMu.Lock()
	d := globalDebug
	l := globalLogger
	globalMu.Unlock()
	if d && l != nil {
		_ = l.Output(2, fmt.Sprintf(format, args...))
	}
}

// ----- ChangeStream -----

// ChangeStreamEvent represents a single change event from a MongoDB change stream.
type ChangeStreamEvent struct {
	// OperationType is the type of operation ("insert", "update", "replace", "delete", "invalidate", etc.)
	OperationType string `bson:"operationType"`
	// FullDocument is the full document after the change (for insert/replace/update with fullDocument enabled).
	FullDocument bson.Raw `bson:"fullDocument,omitempty"`
	// DocumentKey contains the _id of the document that changed.
	DocumentKey bson.D `bson:"documentKey,omitempty"`
	// Namespace contains the database and collection names.
	Namespace struct {
		DB         string `bson:"db"`
		Collection string `bson:"coll"`
	} `bson:"ns"`
	// UpdateDescription describes the fields changed in an update operation.
	UpdateDescription *struct {
		UpdatedFields   bson.M   `bson:"updatedFields"`
		RemovedFields   []string `bson:"removedFields"`
		TruncatedArrays []struct {
			Field   string `bson:"field"`
			NewSize int32  `bson:"newSize"`
		} `bson:"truncatedArrays,omitempty"`
	} `bson:"updateDescription,omitempty"`
	// ResumeToken is the token used to resume this change stream.
	ResumeToken bson.Raw `bson:"_id"`
	// ClusterTime is the operation timestamp.
	ClusterTime bson.Timestamp `bson:"clusterTime,omitempty"`
}

// ChangeStream wraps a mongo-driver change stream with an mgo-friendly API.
// Use Next() to iterate over events, and Close() when done.
// Requires MongoDB 3.6+ with a replica set or sharded cluster.
type ChangeStream struct {
	mu      sync.Mutex
	cs      *mongo.ChangeStream
	session *Session
	err     error
	closed  bool
}

// newChangeStream creates a new ChangeStream wrapper.
func newChangeStream(cs *mongo.ChangeStream, s *Session) *ChangeStream {
	return &ChangeStream{cs: cs, session: s}
}

// Next advances the change stream to the next event and decodes it into result.
// Returns true if an event was decoded. Returns false when the stream is exhausted
// or an error occurs.
func (cs *ChangeStream) Next(result interface{}) bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.closed || cs.cs == nil {
		return false
	}
	ctx := context.Background()
	if !cs.cs.Next(ctx) {
		if err := cs.cs.Err(); err != nil {
			cs.err = err
		}
		return false
	}
	if result != nil {
		if err := cs.cs.Decode(result); err != nil {
			cs.err = err
			return false
		}
	}
	return true
}

// NextEvent returns the next change event decoded into a ChangeStreamEvent.
func (cs *ChangeStream) NextEvent() (ChangeStreamEvent, bool) {
	var ev ChangeStreamEvent
	ok := cs.Next(&ev)
	return ev, ok
}

// ResumeToken returns the resume token for the most recently returned event.
// This can be passed to Watch options to resume from a specific point.
func (cs *ChangeStream) ResumeToken() bson.Raw {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.cs == nil {
		return nil
	}
	return cs.cs.ResumeToken()
}

// Err returns any error that occurred during iteration.
func (cs *ChangeStream) Err() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.err != nil {
		return cs.err
	}
	if cs.cs != nil {
		return cs.cs.Err()
	}
	return nil
}

// Close closes the change stream and releases resources.
func (cs *ChangeStream) Close() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.closed || cs.cs == nil {
		return nil
	}
	cs.closed = true
	return cs.cs.Close(context.Background())
}

// ----- Errors -----

var (
	// ErrSessionClosed is returned when an operation is attempted on a closed session.
	ErrSessionClosed = errors.New("mongogo: session is closed")
)

// TimeSeriesOptions holds options for creating a time series collection (MongoDB 5.0+).
type TimeSeriesOptions struct {
	// TimeField is the name of the field that contains the date (required).
	TimeField string
	// MetaField is the name of the field that contains metadata (optional).
	MetaField string
	// Granularity is "seconds", "minutes", or "hours" (optional).
	Granularity string
}

// CreateCollectionOptions provides extended options for CreateCollection.
// These are passed through to the underlying driver.
type CreateCollectionOptions struct {
	// Capped if true, creates a capped collection.
	Capped bool
	// MaxBytes is the maximum size in bytes for a capped collection.
	MaxBytes int64
	// MaxDocs is the maximum number of documents for a capped collection.
	MaxDocs int64
	// TimeSeries creates a time-series collection (MongoDB 5.0+).
	TimeSeries *TimeSeriesOptions
	// Collation specifies the default collation for the collection.
	Collation *Collation
	// Validator specifies the validation rules for documents.
	Validator interface{}
	// ValidationLevel is "off", "moderate", or "strict".
	ValidationLevel string
	// ValidationAction is "error" or "warn".
	ValidationAction string
	// StorageEngine specifies storage engine options.
	StorageEngine interface{}
	// EncryptedFields provides encrypted fields config (MongoDB 6.0+ FLE2).
	EncryptedFields interface{}
}

