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
	"math/rand"
	"sync"

	stats "github.com/blckit/go-consensus/stats"
	spec "github.com/blckit/go-interface"
)

// Consensus tracks incoming blocks that are younger than Depth. It tracks
// all the branches of blocks and prunes according to rules defined in the
// CompareBlocks function. Client programs can retrieve the most favorable head
// for its next block computation using the GetBestBranch method.
type Consensus struct {
	sync.Mutex

	// The spec used for governing the consensus system
	ConsensusSpec spec.ConsensusSpec

	// The spec used to determine which blocks are in play
	CompetitionSpec spec.CompetitionSpec

	// Currently active blocks in the consensus finder
	blocks map[string]spec.Block

	// Current known heads in the consensus finder
	heads []string

	// spec.Blocks already submitted for a given parent block
	alreadySeen map[string][]string

	// Parent blocks that the local node has already competed for
	competed []string

	// Blocks eleminated from consideration
	eliminated []spec.Block

	// Function to be called when block is confirmed (removed as an old block)
	onBlockConfirmed spec.BlockConfirmedHandler

	// Stats of the consensus system
	Stats *stats.ConsensusStats
}

// NewConsensus constructs a new Consensus instance with the specified Depth
// and blockComparator function. If Depth is 0 then DefaultDepth is
// used. If blockComparator is nil then DefaultBlockComparator is used.
func New(consensusSpec spec.ConsensusSpec, competitionSpec spec.CompetitionSpec) *Consensus {
	if consensusSpec == nil {
		panic(errors.New("consensusSpec must be provided"))
	}

	if competitionSpec == nil {
		panic(errors.New("competitionSpec must be provided"))
	}

	c := &Consensus{ConsensusSpec: consensusSpec, CompetitionSpec: competitionSpec}

	c.blocks = make(map[string]spec.Block)
	c.heads = make([]string, 0)
	c.alreadySeen = make(map[string][]string)
	c.competed = make([]string, 0)
	c.Stats = stats.NewConsensusStats()
	return c
}

// WasSeen returns true if the given block has already been sent to the
// AddBlock method.
func (c *Consensus) WasSeen(block spec.Block) bool {
	seen := c.alreadySeen[block.GetParentID()]
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
	headID := head.GetID()
	if c.blocks[headID] == nil {
		return
	}

	if !c.wasCompeted(headID) {
		c.competed = append(c.competed, headID)
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

	// make sure parent was not aready eliminated
	parentID := block.GetParentID()
	for _, elimParent := range c.eliminated {
		if elimParent.GetID() == parentID {
			return false
		}
	}

	blockID := block.GetID()
	parent := c.blocks[parentID]

	if (parent != nil && block.GetBlockNumber() != parent.GetBlockNumber() + 1) {
		return false
	}

	c.Lock()
		if parent != nil {
			// remove head
			c.removeHead(parentID)

			// remove unfavorable siblings
			siblings := append(c.getChildren(parentID), block)
			favorableSiblings := c.removeUnfavorables(siblings)
			var newBlockOK bool
			for _, s := range favorableSiblings {
				if s.GetID() == blockID {
					newBlockOK = true
					break
				}
			}

			if !newBlockOK {
				c.eliminated = append(c.eliminated, block)
				c.Unlock()
				return false // new block was not favorable against siblings
			}
		}

		children := c.getChildren(blockID)
		if len(children) == 0 {
			// this will be a head block
			c.addHead(blockID)
		} else {
			// keep the most favorable children
			c.removeUnfavorables(children)
		}

		c.blocks[blockID] = block
		c.removeOldBlocks()
	c.Unlock()

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
	
	for _, headID := range c.heads {
		head := c.blocks[headID]
		if !c.wasCompeted(headID) && (head.GetBlockNumber() == 0 || c.getParent(headID) != nil) {
			hs = append(hs, head)
		}
	}

	favorables := c.getFavorables(hs)

	if len(favorables) == 0 {
		return nil
	}

	if len(favorables) == 1 {
		return c.getBranch(favorables[0])
	}

	// if multiple favorables, choose one at random
	return c.getBranch(favorables[rand.Intn(len(favorables))])
}

type blockForCompare struct {
	block     spec.Block
	maxNumber uint64
}

func (c *Consensus) removeUnfavorables(blocks []spec.Block) []spec.Block {
	if len(blocks) < 2 {
		return blocks
	}

	favorables := c.getFavorables(blocks)
	for _, b := range blocks {
		var bOK bool
		bid := b.GetID()
		for _, f := range favorables {
			if bid == f.GetID() {
				bOK = true
				break
			}
		}

		if !bOK {
			c.removeBranch(bid)
		}
	}

	return favorables
}

func (c *Consensus) getFavorables(blocks []spec.Block) []spec.Block {
	if len(blocks) < 2 {
		return blocks
	}

	bs := make([]blockForCompare, len(blocks))
	for i, b := range blocks {
		bs[i] = blockForCompare{block: b, maxNumber: c.getBranchMaxBlockNumber(b)}
	}

	comparator := c.CompetitionSpec.GetBlockComparator()

	// compare all blocks to each other
	i := 0
	j := i + 1
	for i < len(bs)-1 {
		switch comparator(bs[i].block, bs[i].maxNumber, bs[j].block, bs[j].maxNumber) {
		case spec.ComparatorResultKeepFirst:
			bs = append(bs[:j], bs[j+1:]...)
			// jth element is already the next block, so don't increment j
		case spec.ComparatorResultKeepSecond:
			bs = append(bs[:i], bs[i+1:]...)
			j = i + 1 // reset j since ith element is now the next block
		case spec.ComparatorResultKeepNeither:
			bs = append(bs[:i], bs[i+1:]...)
			bs = append(bs[:j], bs[j+1:]...)
			j = i + 1 // reset j since ith element is now the next block
		case spec.ComparatorResultKeepBoth:
			j++	
		}

		if j >= len(bs)-1 {
			i++
			j = i + 1
		}
	}

	result := make([]spec.Block, len(bs))
	for i, x := range bs {
		result[i] = x.block
	}

	return result
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
	seen := c.alreadySeen[parentID]

	if seen == nil {
		seen = make([]string, 0)
	}

	c.alreadySeen[parentID] = append(seen, block.GetID())
}

func (c *Consensus) wasCompeted(parentBlockID string) bool {
	for _, id := range c.competed {
		if id == parentBlockID {
			return true
		}
	}

	return false
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

func (c *Consensus) removeBranch(blockID string) {
	children := c.getChildren(blockID)
	for _, child := range children {
		c.removeBranch(child.GetID())
	}

	c.removeBlock(blockID)
}

func (c *Consensus) removeBlock(blockID string) {
	block := c.blocks[blockID]
	if block == nil {
		return
	}

	c.removeHead(blockID)
	delete(c.blocks, blockID)
	delete(c.alreadySeen, blockID)
	c.competed = deleteFromSlice(c.competed, blockID)
	c.elminateBlock(block)
}

func (c *Consensus) elminateBlock(block spec.Block) {
	blockID := block.GetID()
	for _, e := range c.eliminated {
		if e.GetID() == blockID {
			return
		}
	}
	c.eliminated = append(c.eliminated, block)
	go c.Stats.EliminateBlock(block)
}

func (c *Consensus) removeHead(blockID string) {
	c.heads = deleteFromSlice(c.heads, blockID)
}

func (c *Consensus) removeOldBlocks() {
	maxBlockNumber := c.getMaxBlockNumber()
	confirmedIDs := make([]string, 0)
	for blockID, block := range c.blocks {
		if c.ConsensusSpec.IsBlockConfirmed(block, maxBlockNumber) {
			confirmedIDs = append(confirmedIDs, blockID)
		}
	}
	if len(confirmedIDs) > 1 {
		// TODO: we should have reached consensus, need to figure out logging 
		// or should we alert the client program 
		// or should we not emit any events and wait for to problem to resolve by receiving more blocks?
		// or its possible that a fork was resolved and now we have to confirm several blocks in a row
	}
	for _, blockID := range confirmedIDs {
		if c.onBlockConfirmed != nil {
			go c.onBlockConfirmed(c.blocks[blockID])
		}
		c.removeBlock(blockID)
	}

	depth := uint64(c.ConsensusSpec.GetDepth())
	for i := 0; i < len(c.eliminated); i++ {
		block := c.eliminated[i]
		blockNumber := block.GetBlockNumber()
		if maxBlockNumber >= blockNumber && maxBlockNumber - blockNumber > depth {
			go c.Stats.RemoveBlock(block)
			c.eliminated = append(c.eliminated[:i], c.eliminated[i+1:]...)
		}
	}
}

func (c *Consensus) getMaxBlockNumber() uint64 {
	var max uint64
	for _, b := range c.blocks {
		blockNumber := b.GetBlockNumber()
		if blockNumber > max {
			max = blockNumber
		}
	}

	return max
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
