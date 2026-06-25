package mongogo

import (
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// buildWriteConcern converts a mongogo Safe to a mongo-driver WriteConcern.
// Note: WTimeout is no longer supported in driver v2; use context timeouts instead.
func buildWriteConcern(safe *Safe) *writeconcern.WriteConcern {
	if safe == nil {
		return writeconcern.Unacknowledged()
	}

	wc := &writeconcern.WriteConcern{}

	if safe.WMode != "" {
		wc.W = safe.WMode
	} else if safe.W > 0 {
		wc.W = safe.W
	} else {
		// Default: acknowledged (w=1)
		wc.W = 1
	}

	if safe.J {
		j := true
		wc.Journal = &j
	}

	// Note: safe.WTimeout is intentionally ignored in driver v2.
	// Use context.WithTimeout() for write timeout control.

	return wc
}

// writeConcernUnacknowledged returns an unacknowledged write concern.
func writeConcernUnacknowledged() *writeconcern.WriteConcern {
	return writeconcern.Unacknowledged()
}

// WriteConcernMajority returns a majority write concern.
// This is a convenience helper for setting majority write concerns via SetSafe.
func WriteConcernMajority() *Safe {
	return &Safe{WMode: "majority", J: true}
}

