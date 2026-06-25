package mongogo

import (
	"context"
	"fmt"
	"io"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// GridFS represents a GridFS file store for a database.
type GridFS struct {
	db     *Database
	bucket *mongo.GridFSBucket
	prefix string
}

// newGridFS creates a new GridFS instance for the given database and prefix.
func newGridFS(db *Database, prefix string) *GridFS {
	if prefix == "" {
		prefix = "fs"
	}
	opts := options.GridFSBucket().SetName(prefix)
	bucket := db.Session.client.Database(db.Name).GridFSBucket(opts)
	return &GridFS{
		db:     db,
		bucket: bucket,
		prefix: prefix,
	}
}

// GridFile represents a file stored in GridFS.
type GridFile struct {
	bucket      *mongo.GridFSBucket
	stream      *mongo.GridFSUploadStream
	readStream  *mongo.GridFSDownloadStream
	metadata    interface{}
	filename    string
	contentType string
	mode        gridFileMode
	id          interface{}
	// cached file info from GetFile (read mode only)
	fileInfo *mongo.GridFSFile
}

type gridFileMode int

const (
	gridFileModeWrite gridFileMode = iota
	gridFileModeRead
)

// GridFileInfo contains metadata about a GridFS file.
type GridFileInfo struct {
	Id          interface{} `bson:"_id"`
	Filename    string      `bson:"filename"`
	ContentType string      `bson:"contentType,omitempty"`
	Length      int64       `bson:"length"`
	ChunkSize   int32       `bson:"chunkSize"`
	UploadDate  time.Time   `bson:"uploadDate"`
	MD5         string      `bson:"md5,omitempty"`
	Metadata    bson.Raw    `bson:"metadata,omitempty"`
}

// Create creates a new file in GridFS for writing.
// Returns a GridFile that implements io.WriteCloser.
func (gfs *GridFS) Create(filename string) (*GridFile, error) {
	return gfs.OpenFile(filename, "")
}

// OpenFile opens a new file in GridFS for writing with a content type.
func (gfs *GridFS) OpenFile(filename, contentType string) (*GridFile, error) {
	ctx, cancel := gfs.db.Session.timeoutContext()
	defer cancel()

	var opts []options.Lister[options.GridFSUploadOptions]
	if contentType != "" {
		o := options.GridFSUpload().SetMetadata(bson.D{{Key: "contentType", Value: contentType}})
		opts = append(opts, o)
	}

	stream, err := gfs.bucket.OpenUploadStream(ctx, filename, opts...)
	if err != nil {
		return nil, err
	}

	return &GridFile{
		bucket:      gfs.bucket,
		stream:      stream,
		filename:    filename,
		contentType: contentType,
		mode:        gridFileModeWrite,
	}, nil
}

// Open opens an existing file in GridFS for reading by filename.
// It opens the most recently uploaded version.
func (gfs *GridFS) Open(filename string) (*GridFile, error) {
	ctx, cancel := gfs.db.Session.timeoutContext()
	defer cancel()

	stream, err := gfs.bucket.OpenDownloadStreamByName(ctx, filename)
	if err != nil {
		return nil, convertGridFSError(err)
	}

	gf := &GridFile{
		bucket:     gfs.bucket,
		readStream: stream,
		filename:   filename,
		mode:       gridFileModeRead,
	}
	// Cache the file info for accessor methods.
	if stream != nil {
		gf.fileInfo = stream.GetFile()
		if gf.fileInfo != nil {
			gf.id = gf.fileInfo.ID
		}
	}
	return gf, nil
}

// OpenId opens an existing file in GridFS for reading by its ObjectID.
func (gfs *GridFS) OpenId(id interface{}) (*GridFile, error) {
	ctx, cancel := gfs.db.Session.timeoutContext()
	defer cancel()

	stream, err := gfs.bucket.OpenDownloadStream(ctx, id)
	if err != nil {
		return nil, convertGridFSError(err)
	}

	gf := &GridFile{
		bucket:     gfs.bucket,
		id:         id,
		readStream: stream,
		mode:       gridFileModeRead,
	}
	// Cache the file info for accessor methods.
	if stream != nil {
		gf.fileInfo = stream.GetFile()
	}
	return gf, nil
}

// OpenNext iterates over a GridQuery and opens the next file for reading.
// Returns false when no more files are available.
// Example usage:
//
//	var f *mongogo.GridFile
//	iter := gfs.Find(nil).Iter()
//	for gfs.OpenNext(iter, &f) {
//	    // use f
//	    f.Close()
//	}
func (gfs *GridFS) OpenNext(iter *GridIter, file **GridFile) bool {
	if iter == nil {
		return false
	}
	var info GridFileInfo
	if !iter.Next(&info) {
		*file = nil
		return false
	}
	gf, err := gfs.OpenId(info.Id)
	if err != nil {
		*file = nil
		return false
	}
	*file = gf
	return true
}

// FindId returns a GridQuery that matches the file with the given ID.
func (gfs *GridFS) FindId(id interface{}) *GridQuery {
	return gfs.Find(bson.D{{Key: "_id", Value: id}})
}

// Remove removes a file from GridFS by filename.
func (gfs *GridFS) Remove(filename string) error {
	ctx, cancel := gfs.db.Session.timeoutContext()
	defer cancel()

	// Find the file first to get the ID
	cursor, err := gfs.bucket.Find(ctx, bson.D{{Key: "filename", Value: filename}})
	if err != nil {
		return err
	}
	defer cursor.Close(context.Background()) //nolint

	var found bool
	for cursor.Next(ctx) {
		found = true
		var file struct {
			ID interface{} `bson:"_id"`
		}
		if err := cursor.Decode(&file); err != nil {
			return err
		}
		if err := gfs.bucket.Delete(ctx, file.ID); err != nil {
			return err
		}
	}
	if !found {
		return ErrNotFound
	}
	return nil
}

// RemoveId removes a file from GridFS by its ID.
func (gfs *GridFS) RemoveId(id interface{}) error {
	ctx, cancel := gfs.db.Session.timeoutContext()
	defer cancel()
	return gfs.bucket.Delete(ctx, id)
}

// Find returns a query for GridFS files matching the selector.
func (gfs *GridFS) Find(selector interface{}) *GridQuery {
	return &GridQuery{
		bucket:   gfs.bucket,
		db:       gfs.db,
		selector: selector,
	}
}

// GridQuery represents a query over GridFS files.
type GridQuery struct {
	bucket   *mongo.GridFSBucket
	db       *Database
	selector interface{}
	sort     bson.D
	limit    int32
	skip     int32
}

// GridIter is an iterator over GridFS file metadata results.
type GridIter struct {
	results []GridFileInfo
	pos     int
	err     error
}

// Next advances the iterator and decodes the next GridFileInfo into result.
// Returns true if a result was found.
func (it *GridIter) Next(result *GridFileInfo) bool {
	if it.pos >= len(it.results) {
		return false
	}
	*result = it.results[it.pos]
	it.pos++
	return true
}

// Err returns any error that occurred during iteration.
func (it *GridIter) Err() error {
	return it.err
}

// Close closes the iterator.
func (it *GridIter) Close() error {
	it.pos = len(it.results)
	return it.err
}

// Sort sets the sort order for the grid query.
func (q *GridQuery) Sort(fields ...string) *GridQuery {
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

// Limit sets the maximum number of results to return.
func (q *GridQuery) Limit(n int) *GridQuery {
	q.limit = int32(n)
	return q
}

// Skip sets the number of results to skip.
func (q *GridQuery) Skip(n int) *GridQuery {
	q.skip = int32(n)
	return q
}

// All decodes all matching file metadata into result (pointer to slice of GridFileInfo).
func (q *GridQuery) All(result *[]GridFileInfo) error {
	ctx, cancel := q.db.Session.timeoutContext()
	defer cancel()

	opts := options.GridFSFind()
	if q.sort != nil {
		opts.SetSort(q.sort)
	}
	if q.limit > 0 {
		opts.SetLimit(q.limit)
	}
	if q.skip > 0 {
		opts.SetSkip(q.skip)
	}

	cursor, err := q.bucket.Find(ctx, q.selector, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(context.Background()) //nolint
	return cursor.All(ctx, result)
}

// One decodes the first matching file metadata into result.
func (q *GridQuery) One(result *GridFileInfo) error {
	orig := q.limit
	q.limit = 1
	var results []GridFileInfo
	if err := q.All(&results); err != nil {
		q.limit = orig
		return err
	}
	q.limit = orig
	if len(results) == 0 {
		return ErrNotFound
	}
	*result = results[0]
	return nil
}

// Iter returns a GridIter for iterating over matching GridFS file metadata.
func (q *GridQuery) Iter() *GridIter {
	var results []GridFileInfo
	err := q.All(&results)
	return &GridIter{
		results: results,
		err:     err,
	}
}

// Count returns the number of files matching the query.
func (q *GridQuery) Count() (int, error) {
	var results []GridFileInfo
	if err := q.All(&results); err != nil {
		return 0, err
	}
	return len(results), nil
}

// --- GridFile methods ---

// Write writes data to a GridFS file opened for writing.
func (f *GridFile) Write(b []byte) (int, error) {
	if f.mode != gridFileModeWrite || f.stream == nil {
		return 0, ErrCursor
	}
	return f.stream.Write(b)
}

// Read reads data from a GridFS file opened for reading.
func (f *GridFile) Read(b []byte) (int, error) {
	if f.mode != gridFileModeRead || f.readStream == nil {
		return 0, ErrCursor
	}
	return f.readStream.Read(b)
}

// Close closes the GridFile. For write mode, this commits the upload.
func (f *GridFile) Close() error {
	if f.mode == gridFileModeWrite && f.stream != nil {
		err := f.stream.Close()
		f.stream = nil
		return err
	}
	if f.mode == gridFileModeRead && f.readStream != nil {
		err := f.readStream.Close()
		f.readStream = nil
		return err
	}
	return nil
}

// SetMeta sets the metadata for the GridFS file.
func (f *GridFile) SetMeta(meta interface{}) {
	f.metadata = meta
}

// GetMeta decodes the file's metadata into result.
// result should be a pointer to a struct or bson.M.
func (f *GridFile) GetMeta(result interface{}) error {
	if f.fileInfo == nil {
		return nil
	}
	if f.fileInfo.Metadata == nil {
		return nil
	}
	return bson.Unmarshal(f.fileInfo.Metadata, result)
}

// Name returns the filename of the GridFS file.
func (f *GridFile) Name() string {
	return f.filename
}

// ContentType returns the content type of the GridFS file.
func (f *GridFile) ContentType() string {
	return f.contentType
}

// Id returns the _id of the GridFS file.
// For write mode, this returns the FileID from the upload stream.
// For read mode, this returns the ID from the file metadata.
func (f *GridFile) Id() interface{} {
	if f.mode == gridFileModeWrite && f.stream != nil {
		return f.stream.FileID
	}
	if f.fileInfo != nil {
		return f.fileInfo.ID
	}
	return f.id
}

// Size returns the size of the GridFS file in bytes.
// Only available in read mode after opening the file.
func (f *GridFile) Size() int64 {
	if f.fileInfo != nil {
		return f.fileInfo.Length
	}
	return 0
}

// UploadDate returns the time the file was uploaded to GridFS.
// Only available in read mode.
func (f *GridFile) UploadDate() time.Time {
	if f.fileInfo != nil {
		return f.fileInfo.UploadDate
	}
	return time.Time{}
}

// MD5 returns the MD5 checksum of the file.
// Note: MongoDB deprecated MD5 checksums in GridFS in MongoDB 4.0.
// This returns an empty string as the official driver no longer computes MD5.
func (f *GridFile) MD5() string {
	// MongoDB 4.0+ no longer computes MD5 in GridFS by default.
	return ""
}

// Seek sets the read position of the file.
// Only whence=0 (io.SeekStart) is supported.
// Implemented via the driver's Skip method for forward seeks.
func (f *GridFile) Seek(offset int64, whence int) (int64, error) {
	if f.mode != gridFileModeRead || f.readStream == nil {
		return 0, ErrCursor
	}
	if whence != 0 {
		return 0, fmt.Errorf("mongogo: GridFile.Seek only supports io.SeekStart (whence=0)")
	}
	// The driver supports Skip (forward-only skip).
	_, err := f.readStream.Skip(offset)
	if err != nil {
		return 0, err
	}
	return offset, nil
}

// convertGridFSError converts gridfs errors to mongogo errors.
func convertGridFSError(err error) error {
	if err == nil {
		return nil
	}
	if err == mongo.ErrFileNotFound {
		return ErrNotFound
	}
	return err
}

// UploadFromStream uploads data from a reader directly to GridFS.
func (gfs *GridFS) UploadFromStream(filename string, source io.Reader) (interface{}, error) {
	ctx, cancel := gfs.db.Session.timeoutContext()
	defer cancel()
	id, err := gfs.bucket.UploadFromStream(ctx, filename, source)
	if err != nil {
		return nil, err
	}
	return id, nil
}

// DownloadToStream downloads a file from GridFS to a writer by filename.
func (gfs *GridFS) DownloadToStream(filename string, dst io.Writer) (int64, error) {
	ctx, cancel := gfs.db.Session.timeoutContext()
	defer cancel()
	return gfs.bucket.DownloadToStreamByName(ctx, filename, dst)
}

// DownloadToStreamByID downloads a file from GridFS to a writer by its ID.
func (gfs *GridFS) DownloadToStreamByID(id interface{}, dst io.Writer) (int64, error) {
	ctx, cancel := gfs.db.Session.timeoutContext()
	defer cancel()
	return gfs.bucket.DownloadToStream(ctx, id, dst)
}

// Drop drops the entire GridFS bucket (files and chunks collections).
func (gfs *GridFS) Drop() error {
	ctx, cancel := gfs.db.Session.timeoutContext()
	defer cancel()
	return gfs.bucket.Drop(ctx)
}

