package mongogo

import (
	"context"
	"sync"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Iter represents a cursor iterator over query results.
// It wraps the mongo-driver Cursor and provides an mgo-compatible API.
type Iter struct {
	mu      sync.Mutex
	cursor  *mongo.Cursor
	err     error
	ctx     context.Context
	cancel  context.CancelFunc
	session *Session
	closed  bool
}

// newIter creates a new Iter from a mongo-driver cursor.
func newIter(cursor *mongo.Cursor, err error, s *Session) *Iter {
	ctx := context.Background()
	var cancel context.CancelFunc
	if s != nil && s.sockTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, s.sockTimeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	return &Iter{
		cursor:  cursor,
		err:     err,
		ctx:     ctx,
		cancel:  cancel,
		session: s,
	}
}

// Next decodes the next document in the cursor into result.
// Returns true if a document was found, false if the cursor is exhausted or an error occurred.
func (it *Iter) Next(result interface{}) bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.err != nil {
		return false
	}
	if it.cursor == nil {
		it.err = ErrCursor
		return false
	}
	if it.closed {
		return false
	}

	if !it.cursor.Next(it.ctx) {
		if err := it.cursor.Err(); err != nil {
			it.err = err
		}
		return false
	}

	if err := it.cursor.Decode(result); err != nil {
		it.err = err
		return false
	}
	return true
}

// All decodes all remaining documents in the cursor into result (pointer to slice).
func (it *Iter) All(result interface{}) error {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.err != nil {
		return it.err
	}
	if it.cursor == nil {
		return ErrCursor
	}

	defer it.closeLocked()
	return it.cursor.All(it.ctx, result)
}

// Err returns any error that occurred during iteration.
func (it *Iter) Err() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.err != nil {
		return it.err
	}
	if it.cursor != nil {
		return it.cursor.Err()
	}
	return nil
}

// Close closes the cursor, releasing any server-side resources.
func (it *Iter) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.closeLocked()
}

func (it *Iter) closeLocked() error {
	if it.closed {
		return nil
	}
	it.closed = true
	if it.cancel != nil {
		it.cancel()
	}
	if it.cursor != nil {
		return it.cursor.Close(context.Background())
	}
	return nil
}

// Timeout returns true if the iterator timed out waiting for the next result.
// With mongo-driver v2, context deadline exceeded is treated as a timeout.
func (it *Iter) Timeout() bool {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.ctx == nil {
		return false
	}
	select {
	case <-it.ctx.Done():
		return true
	default:
		return false
	}
}

// Done returns true if the iterator has been exhausted or closed.
func (it *Iter) Done() bool {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.closed || it.cursor == nil {
		return true
	}
	return false
}

// Data returns the raw BSON bytes of the current document.
// This is useful when you need the raw bytes without decoding into a Go struct.
// Must be called after a successful call to Next.
func (it *Iter) Data() []byte {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.cursor == nil {
		return nil
	}
	return it.cursor.Current
}

// RawNext advances the cursor and returns the raw BSON bytes of the next document.
// Returns nil when the cursor is exhausted or an error occurs.
func (it *Iter) RawNext() []byte {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.err != nil || it.cursor == nil || it.closed {
		return nil
	}
	if !it.cursor.Next(it.ctx) {
		if err := it.cursor.Err(); err != nil {
			it.err = err
		}
		return nil
	}
	return it.cursor.Current
}

