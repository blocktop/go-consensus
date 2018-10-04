package consensus

import (
	"sync"

	spec "github.com/blocktop/go-spec"
)

// Competition isolates the data that needs to be communicated
// to the blockchain for competition from the rest of the consensus
// system. This helps reduce lock contention and coupling between adding
// blocks and generating blocks.
type Competition struct {
	sync.Mutex
	branch []spec.Block
	switchHeads bool
}

// Enforce interface at compile-time.
var _ spec.Competition = (*Competition)(nil)

// Branch returns the current competition branch up to the block number
// requested. The block number should be the one for which the local 
// blockchain will next generate a block.
func (c *Competition) Branch(desiredHeadBlockNumber uint64) ([]spec.Block, bool) {
	switchedHeads := c.switchHeads
	c.switchHeads = false

	if c.branch == nil {
		return nil, switchedHeads
	}

	c.Lock()
	defer c.Unlock()

	headBlockNumber := c.branch[0].BlockNumber()

	// If client is asking for greater blocknumber than head,
	// we cannot give them anything yet.
	if desiredHeadBlockNumber > headBlockNumber {
		return nil, switchedHeads
	}

	// When client is just starting up, their head will be zero.
	// Give them the current state.
	if desiredHeadBlockNumber == 0 {
		return c.branch, switchedHeads
	}

	rootIndex := len(c.branch) - 1
	rootBlockNumber := c.branch[rootIndex].BlockNumber()

	// If asking for block below consensus buffer level, then client needs
	// to catch up. Send full branch.
	if desiredHeadBlockNumber < rootBlockNumber + uint64(consensusBuffer) {
		return c.branch, switchedHeads
	}

	desiredHeadIndex := headBlockNumber - desiredHeadBlockNumber
	return c.branch[desiredHeadIndex:], switchedHeads
}

func (c *Competition) setBranch(branch []spec.Block) {
	c.branch = branch
}