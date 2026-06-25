package mongogo

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// Session represents a communication session with the database.
//
// All Session methods are concurrency-safe and may be called from multiple
// goroutines. The Session wraps the official MongoDB Go driver v2 client,
// providing an mgo-compatible API.
type Session struct {
	mu          sync.RWMutex
	client      *mongo.Client
	defaultdb   string
	sourcedb    string
	mode        Mode
	safe        *Safe
	dialCred    *Credential
	sockTimeout time.Duration
	syncTimeout time.Duration
	poolLimit   int
	dialInfo    *DialInfo
	closed      bool
}

// Dial establishes a new session to the cluster identified by the given seed server(s).
//
// The url format is:
//
//	[mongodb://][user:pass@]host1[:port1][,host2[:port2],...][/database][?options]
//
// Supported options:
//   - authSource=<db>
//   - authMechanism=<mechanism>
//   - replicaSet=<name>
//   - maxPoolSize=<limit>
//   - connect=direct|replicaSet
//   - tls=true|false
func Dial(rawurl string) (*Session, error) {
	session, err := DialWithTimeout(rawurl, 10*time.Second)
	if err == nil {
		session.SetSyncTimeout(1 * time.Minute)
		session.SetSocketTimeout(1 * time.Minute)
	}
	return session, err
}

// DialWithTimeout works like Dial, but uses timeout as the amount of time to
// wait for a server to respond.
func DialWithTimeout(rawurl string, timeout time.Duration) (*Session, error) {
	info, err := ParseURL(rawurl)
	if err != nil {
		return nil, err
	}
	info.Timeout = timeout
	return DialWithInfo(info)
}

// ParseURL parses a MongoDB URL and returns a DialInfo.
func ParseURL(rawurl string) (*DialInfo, error) {
	if !strings.HasPrefix(rawurl, "mongodb://") && !strings.HasPrefix(rawurl, "mongodb+srv://") {
		rawurl = "mongodb://" + rawurl
	}

	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, fmt.Errorf("cannot parse URL: %v", err)
	}

	info := &DialInfo{}

	// Hosts
	if u.Host != "" {
		info.Addrs = strings.Split(u.Host, ",")
		// Ensure default port
		for i, addr := range info.Addrs {
			if !strings.Contains(addr, ":") && !strings.HasSuffix(addr, "]") {
				info.Addrs[i] = addr + ":27017"
			}
		}
	}

	// Database
	if u.Path != "" {
		info.Database = strings.TrimPrefix(u.Path, "/")
	}

	// Credentials
	if u.User != nil {
		info.Username = u.User.Username()
		info.Password, _ = u.User.Password()
	}

	// Options
	q := u.Query()
	if v := q.Get("authSource"); v != "" {
		info.Source = v
	}
	if v := q.Get("authMechanism"); v != "" {
		info.Mechanism = v
	}
	if v := q.Get("gssapiServiceName"); v != "" {
		info.Service = v
	}
	if v := q.Get("replicaSet"); v != "" {
		info.ReplicaSetName = v
	}
	if v := q.Get("maxPoolSize"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("bad value for maxPoolSize: %v", v)
		}
		info.PoolLimit = n
	}
	if v := q.Get("connect"); v == "direct" {
		info.Direct = true
	}
	if v := q.Get("appName"); v != "" {
		info.AppName = v
	}

	return info, nil
}

// DialWithInfo establishes a new session using the provided DialInfo.
func DialWithInfo(info *DialInfo) (*Session, error) {
	opts := options.Client()

	// Build hosts
	if len(info.Addrs) > 0 {
		var hosts []string
		for _, addr := range info.Addrs {
			if !strings.Contains(addr, ":") {
				addr += ":27017"
			}
			hosts = append(hosts, addr)
		}
		opts.SetHosts(hosts)
	}

	// Direct connection
	if info.Direct {
		opts.SetDirect(true)
	}

	// Replica set
	if info.ReplicaSetName != "" {
		opts.SetReplicaSet(info.ReplicaSetName)
	}

	// Timeouts
	timeout := info.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	opts.SetConnectTimeout(timeout)
	opts.SetServerSelectionTimeout(timeout)

	// Pool settings
	poolLimit := int64(100)
	if info.PoolLimit > 0 {
		poolLimit = int64(info.PoolLimit)
	}
	opts.SetMaxPoolSize(uint64(poolLimit))

	if info.MinPoolSize > 0 {
		opts.SetMinPoolSize(uint64(info.MinPoolSize))
	}
	if info.MaxConnIdleTime > 0 {
		opts.SetMaxConnIdleTime(info.MaxConnIdleTime)
	}

	// Auth
	if info.Username != "" {
		source := info.Source
		if source == "" {
			source = info.Database
			if source == "" {
				source = "admin"
			}
		}
		mechanism := info.Mechanism
		if mechanism == "" {
			mechanism = "SCRAM-SHA-256"
		}
		cred := options.Credential{
			AuthMechanism: mechanism,
			AuthSource:    source,
			Username:      info.Username,
			Password:      info.Password,
		}
		if info.Service != "" {
			cred.AuthMechanismProperties = map[string]string{
				"SERVICE_NAME": info.Service,
			}
			if info.ServiceHost != "" {
				cred.AuthMechanismProperties["SERVICE_HOST"] = info.ServiceHost
			}
		}
		opts.SetAuth(cred)
	}

	// App name
	if info.AppName != "" {
		opts.SetAppName(info.AppName)
	}

	// TLS
	if info.TLSConfig != nil {
		if tlsCfg, ok := info.TLSConfig.(*tls.Config); ok {
			opts.SetTLSConfig(tlsCfg)
		}
	}

	// Build URI fallback
	if len(info.Addrs) > 0 {
		scheme := "mongodb"
		uriStr := fmt.Sprintf("%s://%s", scheme, strings.Join(info.Addrs, ","))
		if info.Database != "" {
			uriStr += "/" + info.Database
		}
		opts.ApplyURI(uriStr) //nolint - we set individual options above which override
	}

	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, fmt.Errorf("mongogo: connect failed: %v", err)
	}

	defaultdb := info.Database
	if defaultdb == "" {
		defaultdb = "test"
	}
	sourcedb := info.Source
	if sourcedb == "" {
		sourcedb = info.Database
		if sourcedb == "" {
			sourcedb = "admin"
		}
	}

	session := &Session{
		client:      client,
		defaultdb:   defaultdb,
		sourcedb:    sourcedb,
		mode:        Strong,
		poolLimit:   int(poolLimit),
		syncTimeout: timeout,
		sockTimeout: timeout,
		dialInfo:    info,
	}
	session.SetSafe(&Safe{})

	// Ping to verify connectivity
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		client.Disconnect(context.Background()) //nolint
		return nil, fmt.Errorf("mongogo: ping failed: %v", err)
	}

	if info.Username != "" {
		session.dialCred = &Credential{
			Username:    info.Username,
			Password:    info.Password,
			Mechanism:   info.Mechanism,
			Service:     info.Service,
			ServiceHost: info.ServiceHost,
			Source:      sourcedb,
		}
	}

	return session, nil
}

// Clone creates a copy of the session that shares the same cluster.
// The cloned session will have the same settings as the original.
func (s *Session) Clone() *Session {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.clone()
}

func (s *Session) clone() *Session {
	ns := &Session{
		client:      s.client,
		defaultdb:   s.defaultdb,
		sourcedb:    s.sourcedb,
		mode:        s.mode,
		safe:        s.safe,
		dialCred:    s.dialCred,
		sockTimeout: s.sockTimeout,
		syncTimeout: s.syncTimeout,
		poolLimit:   s.poolLimit,
		dialInfo:    s.dialInfo,
	}
	return ns
}

// Copy creates a copy of the session that shares the same cluster,
// same as Clone.
func (s *Session) Copy() *Session {
	return s.Clone()
}

// New creates a new session that shares the same cluster as s,
// but does not inherit read preference, write concern, etc.
func (s *Session) New() *Session {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ns := &Session{
		client:      s.client,
		defaultdb:   s.defaultdb,
		sourcedb:    s.sourcedb,
		mode:        Strong,
		poolLimit:   s.poolLimit,
		syncTimeout: s.syncTimeout,
		sockTimeout: s.sockTimeout,
		dialInfo:    s.dialInfo,
	}
	ns.SetSafe(&Safe{})
	return ns
}

// Close terminates the session. Close should always be called when a session
// is no longer needed.
func (s *Session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.client.Disconnect(ctx) //nolint
	}
}

// Ping verifies that the server is alive and reachable.
func (s *Session) Ping() error {
	ctx, cancel := s.timeoutContext()
	defer cancel()
	rp := s.readPref()
	return s.client.Ping(ctx, rp)
}

// SetMode sets the session consistency mode.
func (s *Session) SetMode(consistency Mode, refresh bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mode = consistency
}

// Mode returns the current consistency mode.
func (s *Session) Mode() Mode {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mode
}

// SetSafe sets the write concern for the session.
// Pass nil to disable write concern (fire-and-forget).
func (s *Session) SetSafe(safe *Safe) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.safe = safe
}

// Safe returns the current write concern settings.
func (s *Session) Safe() (safe *Safe) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.safe
}

// SetSyncTimeout sets how long the session will wait for servers to synchronize.
func (s *Session) SetSyncTimeout(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.syncTimeout = d
}

// SetSocketTimeout sets the timeout for socket operations.
func (s *Session) SetSocketTimeout(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sockTimeout = d
}

// SetPoolLimit sets the per-server connection pool limit.
func (s *Session) SetPoolLimit(limit int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.poolLimit = limit
}

// SetCursorTimeout is a no-op for compatibility. Cursor timeouts are
// managed by the server-side configuration in MongoDB 4.0+.
func (s *Session) SetCursorTimeout(d time.Duration) {
	// no-op: cursor timeout is server-side in modern MongoDB
}

// DB returns a database instance for the given name.
// If name is empty, the default database is used.
func (s *Session) DB(name string) *Database {
	if name == "" {
		name = s.defaultdb
	}
	return &Database{Session: s, Name: name}
}

// DatabaseNames returns a list of database names on the server.
func (s *Session) DatabaseNames() ([]string, error) {
	ctx, cancel := s.timeoutContext()
	defer cancel()
	return s.client.ListDatabaseNames(ctx, bson.D{})
}

// Run executes the given command on the "admin" database.
func (s *Session) Run(cmd interface{}, result interface{}) error {
	return s.DB("admin").Run(cmd, result)
}

// BuildInfo returns version and build information about the server.
func (s *Session) BuildInfo() (BuildInfo, error) {
	var info BuildInfo
	err := s.Run(bson.D{{Key: "buildInfo", Value: 1}}, &info)
	return info, err
}

// LiveServers returns the addresses of the servers currently in use.
func (s *Session) LiveServers() []string {
	// Note: The official driver manages server discovery internally.
	// We return the configured addresses as a fallback.
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.dialInfo != nil {
		return s.dialInfo.Addrs
	}
	return nil
}

// Refresh forces the session to re-read the cluster topology.
// With the official driver, this is a no-op as it manages topology automatically.
func (s *Session) Refresh() {
	// no-op: the official driver handles topology refresh automatically.
}

// EnsureSafe sets the safe write concern only if the current one is less safe.
func (s *Session) EnsureSafe(safe *Safe) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.safe == nil {
		s.safe = safe
	}
}

// Login authenticates with the given credentials.
func (s *Session) Login(cred *Credential) error {
	// With the official driver, authentication happens at connection time.
	// This method stores the credential for informational purposes.
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dialCred = cred
	return nil
}

// LogoutAll logs out from all previously authenticated databases.
func (s *Session) LogoutAll() {
	// With the official driver, logout is implicit on connection close.
}

// internalClient returns the underlying mongo.Client for advanced use cases.
func (s *Session) internalClient() *mongo.Client {
	return s.client
}

// SetBSONInfo is a no-op for API compatibility with mgo.
// In mongo-driver v2, BSON codec configuration is handled via bsoncodec.Registry.
func (s *Session) SetBSONInfo(info interface{}) {
	// no-op: BSON tag configuration is not supported at session level in driver v2.
}

// FsyncLock forces the server to flush all pending writes to disk and
// locks the server against further writes. Call FsyncUnlock to re-enable writes.
// Primarily used for backup operations.
// Note: This requires admin privileges.
func (s *Session) FsyncLock() error {
	return s.DB("admin").Run(bson.D{{Key: "fsync", Value: 1}, {Key: "lock", Value: true}}, nil)
}

// FsyncUnlock unlocks the server after a FsyncLock call.
func (s *Session) FsyncUnlock() error {
	ctx, cancel := s.timeoutContext()
	defer cancel()
	return s.client.Database("admin").RunCommand(ctx, bson.D{{Key: "fsyncUnlock", Value: 1}}).Err()
}

// Fsync flushes all pending writes to disk.
func (s *Session) Fsync(async bool) error {
	cmd := bson.D{{Key: "fsync", Value: 1}}
	if async {
		cmd = append(cmd, bson.E{Key: "async", Value: true})
	}
	return s.DB("admin").Run(cmd, nil)
}

// Watch opens a change stream on the entire deployment (all databases).
// pipeline is an optional aggregation pipeline to filter/transform events.
// Requires MongoDB 4.0+ with a replica set or sharded cluster.
func (s *Session) Watch(pipeline interface{}, opts ...options.Lister[options.ChangeStreamOptions]) (*ChangeStream, error) {
	if pipeline == nil {
		pipeline = bson.A{}
	}
	ctx, cancel := s.timeoutContext()
	defer cancel()
	cs, err := s.client.Watch(ctx, pipeline, opts...)
	if err != nil {
		return nil, err
	}
	return newChangeStream(cs, s), nil
}

// CurrentOp returns information about operations currently running on the server.
func (s *Session) CurrentOp() (bson.M, error) {
	var result bson.M
	err := s.DB("admin").Run(bson.D{{Key: "currentOp", Value: 1}}, &result)
	return result, err
}

// KillOp terminates a running operation by its opId.
func (s *Session) KillOp(opID int64) error {
	return s.DB("admin").Run(bson.D{{Key: "killOp", Value: 1}, {Key: "op", Value: opID}}, nil)
}

// readPref returns the mongo-driver ReadPreference based on the current mode.
func (s *Session) readPref() *readpref.ReadPref {
	s.mu.RLock()
	defer s.mu.RUnlock()
	switch s.mode {
	case Primary: // Also covers Strong (same value = 2)
		return readpref.Primary()
	case PrimaryPreferred:
		return readpref.PrimaryPreferred()
	case Secondary:
		return readpref.Secondary()
	case SecondaryPreferred:
		return readpref.SecondaryPreferred()
	case Nearest, Eventual:
		return readpref.Nearest()
	case Monotonic:
		return readpref.SecondaryPreferred()
	default:
		return readpref.Primary()
	}
}

// timeoutContext creates a context with the session's socket timeout.
func (s *Session) timeoutContext() (context.Context, context.CancelFunc) {
	s.mu.RLock()
	d := s.sockTimeout
	s.mu.RUnlock()
	if d == 0 {
		return context.WithCancel(context.Background())
	}
	return context.WithTimeout(context.Background(), d)
}

// writeConcernOptions returns a CollectionOptionsBuilder based on Safe settings.
// Used when creating collection-level operations that need specific write concerns.
func (s *Session) writeConcernOptions() *options.CollectionOptionsBuilder {
	s.mu.RLock()
	safe := s.safe
	s.mu.RUnlock()

	if safe == nil {
		return options.Collection().SetWriteConcern(writeConcernUnacknowledged())
	}
	return options.Collection().SetWriteConcern(buildWriteConcern(safe))
}

