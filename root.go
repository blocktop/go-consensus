package consensus

import (
	"time"

	"github.com/mxmCherry/movavg"
)

// Root carries a pointer to the current root block of the
// consensus tree. Since we are constantly confirming blocks, the
// block pointed to in this struct will continually change, and all
// blocks that point to this struct will immediately reflect that change.
// However, the Root itself should only change in extreme
// conditions, such as the network abandons the root. This root should
// essentially represent the 'genesis' block, even though the genesis
// block is not actively tracked in the consensus system.
type root struct {
	id                   int
	cblock               *block
	consecutiveLocalHits uint
	lastHitTimestamp     int64
	hitRate              *movavg.SMA // in blocks/sec
	hits                 uint64
}

var rootID int

func getRootID() int {
	rootID++
	return rootID
}

func newRoot(cblock *block) *root {
	croot := &root{id: getRootID(), cblock: cblock}

	return croot
}

func (r *root) recordHit(isLocal bool) {
	now := time.Now().UnixNano()
	if r.lastHitTimestamp == 0 {
		r.hits = 0
		r.consecutiveLocalHits = 0
		r.lastHitTimestamp = now
	}
	if isLocal {
		r.consecutiveLocalHits++
	} else {
		r.consecutiveLocalHits = 0
	}

	deltaT := (now - r.lastHitTimestamp)
	r.hitRate.Add(float64(deltaT))
	r.lastHitTimestamp = now

	r.hits++
}

func (r1 *root) equal(r2 *root) bool {
	if r1 == nil || r2 == nil {
		return false
	}
	return r1.cblock.equal(r2.cblock)
}