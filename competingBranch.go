package consensus

import (
	"github.com/blocktop/go-spec"
)

type CompetingBranch struct {
	blocks               []spec.Block
	rootID               int
	consecutiveLocalHits int
	hitRate              float64
}

// Enforce interface at compile-time.
var _ spec.CompetingBranch = (*CompetingBranch)(nil)

func (b *CompetingBranch) Blocks() []spec.Block {
	return b.blocks
}

func (b *CompetingBranch) RootID() int {
	return b.rootID
}

func (b *CompetingBranch) ConsecutiveLocalHits() int {
	return b.consecutiveLocalHits
}

func (b *CompetingBranch) HitRate() float64 {
	return b.hitRate
}
