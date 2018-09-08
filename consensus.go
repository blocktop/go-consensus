/*
Package consensus provides a blockchain consensus finding system. It allows
client programs to submit blocks to be tracked and to retrieve the 'best' head block
for the next round of block computation. A lightweight block interface is provided
to allow the package to retrieve basic information about the block.

The default criteria for branch comparison is blockchain height. Client programs may
provide a comparator function to inject their own criteria for evaluation.

Example pseudo code of client program:
		var c *Consensus = consensus.NewConsensus(0, nil)
		...

		// This function is called whenever a block is received from the network
		// MyBlock implements the consensus.Block interface
		func receiveBlockFromNetwork(b MyBlock) {
			if !c.WasSeen(b) {
				// preform any validation or processing required to
				// ensure block is worthy of submission
				if validate(b) {
					c.AddBlock(b)
				}
			}
		}

		// This function continuously competes for the next block
		func competeForBlocks() {
			for {
				// don't peg the CPU
				// MyBlock implements the consensus.Block interface

				branch := c.GetBestBranch()
				if branch == nil {
					// no best branch is available, wait for more blocks
					time.Sleep(50)
					// to come in from the network
					continue
				}

				// generate the next block based on the head of the best branch
				var head MyBlock = branch[0].(MyBlock)
				var nextBlock MyBlock = generateNextBlock(head)
				c.setCompeted(head)
				sendBlockToNetwork(nextBlock)
				c.AddBlock(nextBlock)
			}
		}
*/
package consensus

import (
	"errors"
	"sync"

	stats "github.com/blckit/go-consensus/stats"
	spec "github.com/blckit/go-spec"
)

// Consensus tracks incoming blocks that are younger than Depth. It tracks
// all the branches of blocks and prunes according to rules defined in the
// CompareBlocks function. Client programs can retrieve the most favorable head
// for its next block computation using the GetBestBranch method.
type Consensus struct {
	sync.Mutex

	// The depth at which blocks are considered confirmed
	ConsensusDepth uint

	// The spec used to determine which blocks are in play
	CompareBlocks spec.BlockComparator

	// Currently active blocks in the consensus finder
	blocks map[string]spec.Block

	// Current known heads in the consensus finder
	heads []string

	// spec.Blocks already submitted for a given parent block
	alreadySeen map[string][]string

	// Max lock number that the local node has already maxCompeted for
	maxCompeted int64

	// Blocks disqualified from consideration
	disqualified []spec.Block

	// Function to be called when block is confirmed (removed as an old block)
	onBlockConfirmed spec.BlockConfirmedHandler

	// Stats of the consensus system
	Stats *stats.ConsensusStats
}

// NewConsensus constructs a new Consensus instance with the specified Depth
// and blockComparator function. If Depth is 0 then DefaultDepth is
// used. If blockComparator is nil then DefaultBlockComparator is used.
func New(consensusDepth uint, blockComparator spec.BlockComparator) *Consensus {
	if blockComparator == nil {
		panic(errors.New("blockComparator must be provided"))
	}

	c := &Consensus{ConsensusDepth: consensusDepth, CompareBlocks: blockComparator}

	c.blocks = make(map[string]spec.Block)
	c.heads = make([]string, 0)
	c.alreadySeen = make(map[string][]string)
	c.Stats = stats.NewConsensusStats()
	c.maxCompeted = int64(-1)
	return c
}

// WasSeen returns true if the given block has already been sent to the
// AddBlock method.
func (c *Consensus) WasSeen(block spec.Block) bool {
	c.Lock()
	seen := c.alreadySeen[block.GetParentID()]
	c.Unlock()
	
	if seen == nil {
		return false
	}

	blockID := block.GetID()

	for _, id := range seen {
		if id == blockID {
			return true
		}
	}
	return false
}

// SetCompeted is called by the client program to tell the Consensus instance
// that the client program has already generated a block for the given head.
// This block will no longer be returned as a head by GetBestBranch.
func (c *Consensus) SetCompeted(head spec.Block) {
	if c.blocks[head.GetID()] == nil {
		return
	}
	if int64(head.GetBlockNumber()) > c.maxCompeted {
		c.maxCompeted = int64(head.GetBlockNumber())
	}
}

// AddBlock adds the given block to the consensus tracking. Sibling and
// children branches are pruned according to the rules in the spec.BlockComparator
// function. The function returns true if the block was added, or false
// if the new block was pruned in the process of being added.
func (c *Consensus) AddBlock(block spec.Block) (added bool) {
	if c.WasSeen(block) {
		return false
	}
	c.setSeen(block)

	// make sure block is not too old
	if c.getDepth(block) > int(c.ConsensusDepth) {
		return false
	}

	// make sure parent was not aready eliminated
	parentID := block.GetParentID()
	for _, elimParent := range c.disqualified {
		if elimParent.GetID() == parentID {
			return false
		}
	}

	blockID := block.GetID()
	parent := c.blocks[parentID]

	if parent != nil && block.GetBlockNumber() != parent.GetBlockNumber()+1 {
		return false
	}

	c.Lock()
	defer c.Unlock()
	
	if parent != nil {
		// remove head
		c.removeHead(parentID)
	}

	// remove unfavorable siblings
	// note: the parent does not have to be in the system, just collect blocks with same parentID
	siblings := append(c.getChildren(parentID), block)
	favorableSibling := c.disqualifyUnfavorables(siblings)
	if favorableSibling.GetID() != blockID {
		c.disqualified = append(c.disqualified, block)
		return false // new block was not favorable against siblings
	}

	children := c.getChildren(blockID)
	if len(children) == 0 {
		// this will be a head block
		c.addHead(blockID)
	} else {
		// take this opportunity to keep the most favorable children
		c.disqualifyUnfavorables(children)
	}

	c.blocks[blockID] = block
	c.removeOldBlocks()

	go c.Stats.AddBlock(block)

	return true
}

func (c *Consensus) OnBlockConfirmed(f spec.BlockConfirmedHandler) {
	c.onBlockConfirmed = f
}

// GetBestBranch returns the most favorable branch for the client program to
// compete for the next block. The method returns nil if there are no
// favorable branches. Otherwise it returns the best branch as an array of
// blocks with the head block as the zeroth element.
//
// If more than one favorable branch is found, then one of them chosen at
// random will be returned.
func (c *Consensus) GetBestBranch() []spec.Block {
	c.Lock()
	defer c.Unlock()

	if len(c.heads) == 0 {
		return nil
	}

	hs := make([]spec.Block, 0)
	maxHead := uint64(0)
	for _, headID := range c.heads {
		head := c.blocks[headID]
		blockNumber := head.GetBlockNumber()
		if int64(blockNumber) > c.maxCompeted && (blockNumber == 0 || c.getParent(headID) != nil) {
			hs = append(hs, head)
			if blockNumber > maxHead {
				maxHead = blockNumber
			}
		}
	}

	for i := 0; i < len(hs); i++ {
		if hs[i].GetBlockNumber() < maxHead {
			hs = append(hs[:i], hs[i+1:]...)
		}
	}

	bestHead := c.CompareBlocks(hs)

	if bestHead == nil {
		return nil
	}

	return c.getBranch(bestHead)
}

type blockForCompare struct {
	block     spec.Block
	maxNumber uint64
}

// disqualifies unfavorable blocks and returns the most favorable
func (c *Consensus) disqualifyUnfavorables(blocks []spec.Block) spec.Block {
	if len(blocks) == 0 {
		return nil
	}
	if len(blocks) == 1 {
		return blocks[0]
	}

	favorable := c.CompareBlocks(blocks)
	favorableID := favorable.GetID()
	for _, b := range blocks {
		blockID := b.GetID()
		if blockID != favorableID {
			c.removeBranch(blockID, true)
		}
	}

	return favorable
}

func (c *Consensus) getBranchMaxBlockNumber(block spec.Block) uint64 {
	maxBlockNumber := block.GetBlockNumber()
	children := c.getChildren(block.GetID())
	for _, child := range children {
		childMaxBlockNumber := c.getBranchMaxBlockNumber(child)
		if childMaxBlockNumber > maxBlockNumber {
			maxBlockNumber = childMaxBlockNumber
		}
	}

	return maxBlockNumber
}

func (c *Consensus) getBranch(block spec.Block) []spec.Block {
	this := []spec.Block{block}
	parent := c.blocks[block.GetParentID()]
	if parent == nil {
		return this
	}
	return append(this, c.getBranch(parent)...)
}

func (c *Consensus) setSeen(block spec.Block) {
	parentID := block.GetParentID()

	c.Lock()
	seen := c.alreadySeen[parentID]

	if seen == nil {
		seen = make([]string, 0)
	}

	c.alreadySeen[parentID] = append(seen, block.GetID())
	c.Unlock()
}

func (c *Consensus) addHead(blockID string) {
	if !c.isHead(blockID) {
		c.heads = append(c.heads, blockID)
	}
}

func (c *Consensus) isHead(blockID string) bool {
	for _, id := range c.heads {
		if id == blockID {
			return true
		}
	}

	return false
}

func (c *Consensus) getParent(blockID string) spec.Block {
	block := c.blocks[blockID]
	if block == nil {
		return nil
	}

	parentID := block.GetParentID()

	return c.blocks[parentID]
}

func (c *Consensus) getChildren(parentBlockID string) []spec.Block {
	children := make([]spec.Block, 0)

	for _, b := range c.blocks {
		if b.GetParentID() == parentBlockID {
			children = append(children, b)
		}
	}

	return children
}

func (c *Consensus) removeBranch(blockID string, disqualify bool) {
	children := c.getChildren(blockID)
	for _, child := range children {
		c.removeBranch(child.GetID(), disqualify)
	}

	c.removeBlock(blockID, disqualify)
}

func (c *Consensus) removeBlock(blockID string, disqualify bool) {
	block := c.blocks[blockID]
	if block == nil {
		return
	}

	c.removeHead(blockID)
	delete(c.blocks, blockID)
	delete(c.alreadySeen, blockID)
	if disqualify {
		c.disqualifyBlock(block)
	}
}

func (c *Consensus) disqualifyBlock(block spec.Block) {
	blockID := block.GetID()
	for _, e := range c.disqualified {
		if e.GetID() == blockID {
			return
		}
	}
	c.disqualified = append(c.disqualified, block)

	go c.Stats.DisqualifyBlock(block)

	for _, child := range c.getChildren(blockID) {
		c.disqualifyBlock(child)
	}
}

func (c *Consensus) removeHead(blockID string) {
	c.heads = deleteFromSlice(c.heads, blockID)
}

func (c *Consensus) removeOldBlocks() {
	maxBlockNumber := c.getMaxBlockNumber()
	for i := 0; i < len(c.disqualified); i++ {
		block := c.disqualified[i]
		blockNumber := block.GetBlockNumber()
		if maxBlockNumber >= blockNumber && uint(maxBlockNumber-blockNumber) > c.ConsensusDepth {
			go c.Stats.RemoveBlock(block)
			c.disqualified = append(c.disqualified[:i], c.disqualified[i+1:]...)
		}
	}

	confirmedCandidates := make([]spec.Block, 0)
	minTrackedBlockNumber := maxBlockNumber - uint64(c.ConsensusDepth)
	for _, block := range c.blocks {
		blockNumber := block.GetBlockNumber()
		if maxBlockNumber < uint64(c.ConsensusDepth) {
			return // blockchain is two short to begin confirmation
		}
		if blockNumber < minTrackedBlockNumber {
			confirmedCandidates = append(confirmedCandidates, block)
		}
	}
	if len(confirmedCandidates) == 0 {
		return
	}
	if len(confirmedCandidates) == 1 {
		c.confirmBlock(confirmedCandidates[0])
		return
	}

	// run-off among candidates for confirmation
	// get min block number and filter candidates by minimum
	minBlockNumber := ^uint64(0)
	for _, conf := range confirmedCandidates {
		if conf.GetBlockNumber() < minBlockNumber {
			minBlockNumber = conf.GetBlockNumber()
		}
	}
	for i, conf := range confirmedCandidates {
		if conf.GetBlockNumber() != minBlockNumber {
			confirmedCandidates = append(confirmedCandidates[:i], confirmedCandidates[i+1:]...)
		}
	}

	blockToConfirm := c.CompareBlocks(confirmedCandidates)
	if blockToConfirm == nil {
		return
	}
	c.confirmBlock(blockToConfirm) // which will also remove it without disqualifying it

	// remove the rest as diqualified
	for _, conf := range confirmedCandidates {
		if conf.GetID() != blockToConfirm.GetID() {
			c.removeBlock(conf.GetID(), true)
		}
	}
}

func (c *Consensus) confirmBlock(block spec.Block) {
	if c.onBlockConfirmed != nil {
		go c.onBlockConfirmed(block)
	}
	c.removeBlock(block.GetID(), false)
	go c.Stats.RemoveBlock(block)
}

func (c *Consensus) getMaxBlockNumber() uint64 {
	var max uint64
	for _, headID := range c.heads {
		blockNumber := c.blocks[headID].GetBlockNumber()
		if blockNumber > max {
			max = blockNumber
		}
	}

	return max
}

func (c *Consensus) getDepth(block spec.Block) int {
	return int(int64(c.getMaxBlockNumber()) - int64(block.GetBlockNumber()))
}

func deleteFromSlice(slice []string, value string) []string {
	for i, v := range slice {
		if v == value {
			return append(slice[:i], slice[i+1:]...)
		}
	}

	return slice
}

func getIndex(slice []string, value string) int {
	for i, v := range slice {
		if v == value {
			return i
		}
	}

	return -1
}
