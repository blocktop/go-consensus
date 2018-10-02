// Copyright © 2018 J. Strobus White.
// This file is part of the blocktop blockchain development kit.
//
// Blocktop is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Blocktop is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with blocktop. If not, see <http://www.gnu.org/licenses/>.

package consensus

import (
	"errors"
	"sync"
	"time"

	spec "github.com/blocktop/go-spec"
	"github.com/spf13/viper"
)

// Consensus tracks incoming blocks that are younger than Depth. It tracks
// all the branches of blocks and prunes according to rules defined in the
// compareBlocks function. Client programs can retrieve the most favorable head
// for its next block computation using the GetBestBranch method.
type Consensus struct {
	compareBlocks         spec.BlockComparator
	blocks                *sync.Map
	heads                 *sync.Map
	localBlocks           *sync.Map
	alreadySeen           *sync.Map
	competed              *sync.Map
	root                  *consensusRoot
	disqualified          *sync.Map
	headTimer             *time.Timer
	bestHead              *consensusBlock
	onBlockConfirmed      spec.BlockConfirmationHandler
	onLocalBlockConfirmed spec.BlockConfirmationHandler
	onCompete             spec.BranchCompetitionHandler
	confirming            bool
	evaluating            bool
	lastConfirmed         bool
	sync.Mutex
}

type consensusBlock struct {
	block       spec.Block
	blockID     string
	parentID    string
	root        *consensusRoot
	parent      *consensusBlock
	children    *sync.Map
	blockNumber uint64
}

type consensusRoot struct {
	cblock *consensusBlock
}

// compile-time interface check
var _ spec.Consensus = (*Consensus)(nil)

var consensus *Consensus
var maxDepth int
var consensusDepth int
var consensusBuffer int
var blockInterval time.Duration
var addDisqualified bool

func NewConsensus(blockComparator spec.BlockComparator) *Consensus {
	if consensus != nil {
		panic("Consensus object already created")
	}

	if blockComparator == nil {
		panic(errors.New("blockComparator must be provided"))
	}

	c := &Consensus{compareBlocks: blockComparator}

	c.blocks = &sync.Map{}
	c.heads = &sync.Map{}
	c.alreadySeen = &sync.Map{}
	c.localBlocks = &sync.Map{}
	c.competed = &sync.Map{}
	c.disqualified = &sync.Map{}
	c.root = &consensusRoot{}

	consensusDepth = viper.GetInt("blockchain.consensus.depth")
	consensusBuffer = viper.GetInt("blockchain.consensus.depthBuffer")
	maxDepth = consensusDepth - consensusBuffer
	blockInterval = viper.GetDuration("blockchain.blockInterval")
	addDisqualified = viper.GetBool("blockchain.metrics.trackall")

	consensus = c
	return c
}

func (c *Consensus) OnBlockConfirmed(f spec.BlockConfirmationHandler) {
	c.onBlockConfirmed = f
}

func (c *Consensus) OnLocalBlockConfirmed(f spec.BlockConfirmationHandler) {
	c.onLocalBlockConfirmed = f
}

func (c *Consensus) OnCompete(f spec.BranchCompetitionHandler) {
	c.onCompete = f
}

// WasSeen returns true if the given block has already been sent to the
// AddBlock method.
func (c *Consensus) WasSeen(block spec.Block) bool {
	return exists(c.alreadySeen, block.Hash())
}

// SetCompeted is called by the client program to tell the Consensus instance
// that the client program has already generated a block for the given head.
// This block will no longer be returned as a head by GetBestBranch.
func (c *Consensus) SetCompeted(head spec.Block) {
	c.competed.Store(head.Hash(), getBlock(c.blocks, head.Hash()))
}

// AddBlock adds the given block to the consensus tracking. Sibling and
// children branches are pruned according to the rules in the spec.BlockComparator
// function. The function returns true if the block was added, or false
// if the new block was pruned in the process of being added.
func (c *Consensus) AddBlock(block spec.Block, isLocal bool) (added bool) {
	startTime := time.Now().UnixNano()

	c.Lock()
	defer c.Unlock()

	if c.WasSeen(block) {
		return false
	}

	blockID := block.Hash()
	parentID := block.ParentHash()

	cblock := &consensusBlock{
		block:       block,
		blockID:     block.Hash(),
		parentID:    block.ParentHash(),
		blockNumber: block.BlockNumber()}

	c.alreadySeen.Store(blockID, cblock)

	// If block is too old then ignore it.
	blockDepth := int(c.getMaxBlockNumber() - block.BlockNumber())
	if blockDepth > maxDepth-1 {
		return false
	}

	// Get the parent block, if any.
	parent := getBlock(c.blocks, parentID)
	cblock.parent = parent

	if parent != nil && block.BlockNumber() != parent.blockNumber+1 {
		// If new block number is no 1 greater than parent then ignore it.
		return false
	}

	// If parent was already eliminated, then new block is also eliminated.
	if exists(c.disqualified, parentID) {
		if addDisqualified {
			metrics.AddBlock(block)
			go metrics.DisqualifyBlock(block)
		}
		return false
	}

	var siblings *sync.Map

	if parent == nil {
		if block.BlockNumber() == 0 || c.root.cblock == nil {
			// New block is genesis or no other root is known yet.
			c.root.cblock = cblock
			cblock.root = c.root
		}

		// Get all blocks with same parentID as the new block.
		// Parent might not be in the system as a consensusBlock,
		// so search by ID.
		siblings = c.getChildren(parentID)
	} else {
		cblock.root = parent.root
		siblings = parent.children
	}

	// Add new block as child of its parent.
	siblings.Store(blockID, cblock)

	// Collect orphans and set as children of new block.
	cblock.children = c.getChildren(blockID)

	// Compare all siblings and keep only the most favorable one.
	// Note this could displace the current best head or the
	// branch leading to it.
	favorableSibling := c.disqualifyUnfavorables(siblings, true)

	// Eliminate new block if it was not favorable against siblings.
	if favorableSibling == nil || favorableSibling.blockID != blockID {
		if addDisqualified {
			metrics.AddBlock(block)
			go metrics.DisqualifyBlock(block)
		}
		return false
	}

	go metrics.AddBlock(block)

	// By definition the new block replaces the parent as head,
	// if it was one.
	c.heads.Delete(parentID)

	if hasChild(cblock) {
		// Take this opportunity to keep the most favorable former orphan.
		c.disqualifyUnfavorables(cblock.children, true)
	} else {
		// No children, so it's a head.
		c.heads.Store(blockID, cblock)
	}

	// Record the fact that that new block was produced by the local node.
	if isLocal {
		c.localBlocks.Store(blockID, cblock)
	}

	// Add the new block to the main block map.
	c.blocks.Store(blockID, cblock)

	// Confirm any blocks that are deeper than threshold.
	c.confirmBlocks()

	// Evaluate heads for next round of competition.
	c.evaluateHeads()

	duration := time.Now().UnixNano() - startTime
	metrics.BlockAddDuration(duration)

	return true
}

// evaluateHeads returns the most favorable branch for the client program to
// compete for the next block. The method returns nil if there are no
// favorable branches. Otherwise it returns the best branch as an array of
// blocks with the head block as the zeroth element.
func (c *Consensus) evaluateHeads() {
	var hasHead bool
	var bestHead *consensusBlock
	var bestRootHead *consensusBlock
	maxHead := uint64(0)
	maxRootHead := uint64(0)

	// Take the head attached to the root, otherwiese the one with the highest
	// block number.
	c.heads.Range(func(hid interface{}, cb interface{}) bool {
		hasHead = true
		chead := cb.(*consensusBlock)
		head := chead.block
		headID := head.Hash()
		blockNumber := head.BlockNumber()

		// Favor the heads of the root block
		//		fmt.Printf("%v\n", c.root)
		//		fmt.Printf("%v\n", chead.root)

		if c.root != nil && c.root.cblock != nil && chead.root != nil && chead.root.cblock != nil && 
		equal(chead.root.cblock, c.root.cblock) {
			if chead.blockNumber > maxRootHead || chead.blockNumber == 0 {
				maxRootHead = chead.blockNumber
				bestRootHead = chead
			}
		}

		if !exists(c.competed, headID) && (blockNumber == 0 || chead.parent != nil) {
			if blockNumber > maxHead {
				maxHead = blockNumber
				bestHead = chead
			}
		}
		return true
	})
	if !hasHead {
		return
	}

	// Favor the head from the root we are currently tracking.
	if bestRootHead != nil {
		bestHead = bestRootHead
		maxHead = maxRootHead
	}

	if bestHead == nil || exists(c.competed, bestHead.blockID) {
		return
	}

	isNewBestHead := (c.bestHead == nil) || !equal(c.bestHead, bestHead)
	if !isNewBestHead {
		return
	}
	c.bestHead = bestHead

	branch := c.getBranch(bestHead.block)
	if branch != nil && len(branch) > 0 {
		if c.headTimer != nil {
			c.headTimer.Stop()
			c.headTimer = nil
		}
		now := time.Now().UnixNano()
		latestTime := time.Duration(now) - blockInterval
		wait := time.Duration(bestHead.block.Timestamp())*time.Millisecond - latestTime
		if wait <= 0 {
			// ready to compete now
			go c.onCompete(branch)
		} else {
			// compete after wait time
			c.headTimer = time.AfterFunc(wait, func() {
				go c.onCompete(branch)
				c.headTimer = nil
			})
		}
	}
}

// disqualifies unfavorable blocks and returns the most favorable
func (c *Consensus) disqualifyUnfavorables(bmap *sync.Map, removeBranch bool) *consensusBlock {
	blocks := make([]spec.Block, 0)
	bmap.Range(func(blockID interface{}, cblock interface{}) bool {
		blocks = append(blocks, cblock.(*consensusBlock).block)
		return true
	})
	if len(blocks) == 0 {
		return nil
	}
	if len(blocks) == 1 {
		res, _ := bmap.Load(blocks[0].Hash())
		return res.(*consensusBlock)
	}

	favorable := c.compareBlocks(blocks)
	favorableID := favorable.Hash()
	if removeBranch {
		for _, b := range blocks {
			blockID := b.Hash()
			if blockID != favorableID {
				cblock := getBlock(bmap, blockID)
				if cblock != nil {
					c.removeBranch(cblock, true)
				}
			}
		}
	}

	res := getBlock(bmap, favorableID)
	return res
}

func (c *Consensus) getBranch(block spec.Block) []spec.Block {
	this := []spec.Block{block}
	cparent, _ := c.blocks.Load(block.ParentHash())
	if cparent == nil {
		return this
	}
	parent := cparent.(*consensusBlock).block
	if parent == nil {
		return this
	}
	return append(this, c.getBranch(parent)...)
}

func (c *Consensus) removeBranch(cblock *consensusBlock, disqualify bool) {
	blockID := cblock.blockID
	if !exists(c.blocks, blockID) {
		return
	}

	if cblock.children != nil {
		cblock.children.Range(func(childID interface{}, child interface{}) bool {
			c.removeBranch(child.(*consensusBlock), disqualify)
			return true
		})
	}
	if cblock.parent != nil && cblock.parent.children != nil {
		cblock.parent.children.Delete(blockID)
	}

	c.removeBlock(cblock, disqualify)
}

func (c *Consensus) removeBlock(cblock *consensusBlock, disqualify bool) {
	blockID := cblock.blockID
	if !exists(c.blocks, blockID) {
		return
	}

	// If this block was a root, then we are blowing up the tree.
	if c.root != nil && c.root.cblock != nil && c.root.cblock.blockID == blockID {
		panic("removing the root")
	}

	c.localBlocks.Delete(blockID)
	c.heads.Delete(blockID)
	c.alreadySeen.Delete(blockID)
	c.competed.Delete(blockID)

	if disqualify {
		c.disqualifyBlock(cblock)
	}
	cblock.parent = nil
	cblock.children = nil
	cblock.root = nil

	c.blocks.Delete(blockID)
}

func (c *Consensus) disqualifyBlock(cblock *consensusBlock) {
	blockID := cblock.blockID
	if exists(c.disqualified, blockID) {
		return
	}
	c.disqualified.Store(blockID, cblock)

	go metrics.DisqualifyBlock(cblock.block)

	if cblock.children == nil {
		return
	}

	cblock.children.Range(func(childID interface{}, child interface{}) bool {
		if child != nil {
			c.disqualifyBlock(child.(*consensusBlock))
		}
		return true
	})
}

func (c *Consensus) confirmBlocks() {
	if c.root == nil || c.root.cblock == nil {
		return
	}
	maxBlockNumber := c.getMaxBlockNumber()
	minBlockNumber := c.root.cblock.blockNumber

	c.disqualifyOldBlocks(maxBlockNumber)
	c.removeDisqualified(maxBlockNumber)

	depth := (maxBlockNumber - minBlockNumber)
	if depth < uint64(consensusDepth) {
		return // blockchain is too short to begin confirmation
	}

	confirmCount := depth - uint64(consensusDepth-1)

	for confirmCount > 0 {
		if c.root == nil || c.root.cblock == nil {
			return
		}
		maxChildBlockNumber, maxChild := c.analyzeRoot(c.root.cblock, maxBlockNumber)
		if maxChildBlockNumber >= maxBlockNumber {
			rootBlock := c.root.cblock
			c.root.cblock = maxChild
			c.confirmBlock(rootBlock)
		} else {
			// the root was not ready to confirm due to activity at the leaves
			return
		}
		confirmCount--
	}
}

func (c *Consensus) analyzeRoot(cblock *consensusBlock, maxBlockNumber uint64) (uint64, *consensusBlock) {
	bufferZoneLow := maxBlockNumber - uint64(maxDepth+1)
	bufferZoneHigh := maxBlockNumber - uint64(consensusBuffer)
	if !hasChild(cblock) {
		if cblock.blockNumber < bufferZoneLow {
			if c.root.cblock != nil && c.root.cblock.blockID == cblock.blockID {
				c.root = c.bestHead.root
			}
			c.removeBlock(cblock, true)
			return 0, nil
		}
		return cblock.blockNumber, cblock
	}

	if cblock.children == nil {
		return cblock.blockNumber, cblock
	}

	maxChildBlockNumber := uint64(0)
	var maxChild *consensusBlock
	cblock.children.Range(func(cid interface{}, ch interface{}) bool {
		child := ch.(*consensusBlock)
		childHeight, _ := c.analyzeRoot(child, maxBlockNumber)

		// Eliminate the branch if this child is in the low buffer zone and its
		// maximum child does not reach to the high buffer zone.
		if cblock.blockNumber < bufferZoneLow && childHeight < bufferZoneHigh {
			c.removeBranch(child, true)
		} else if childHeight > maxChildBlockNumber {
			maxChildBlockNumber = childHeight
			maxChild = child
		}
		return true
	})
	return maxChildBlockNumber, maxChild
}

/*
	// run-off among candidates for confirmation
	// get min block number and filter candidates by minimum
	minBlockNumber := ^uint64(0)
	for _, conf := range confirmedCandidates {
		if conf.BlockNumber() < minBlockNumber {
			minBlockNumber = conf.BlockNumber()
		}
	}
	for i := 0; i < len(confirmedCandidates); i++ {
		conf := confirmedCandidates[i]
		if conf.BlockNumber() != minBlockNumber {
			confirmedCandidates = append(confirmedCandidates[:i], confirmedCandidates[i+1:]...)
		}
	}

	blockToConfirm := c.compareBlocks(confirmedCandidates)
	if blockToConfirm == nil {
		return
	}
	confBlock := getBlock(c.blocks, blockToConfirm.Hash())
	if confBlock != nil {
		c.confirmBlock(confBlock) // which will also remove it without disqualifying it
	}

	// remove the rest as diqualified
	for _, conf := range confirmedCandidates {
		if conf.Hash() != blockToConfirm.Hash() {
			rmBlock := getBlock(c.blocks, conf.Hash())
			if rmBlock != nil {
				c.removeBlock(rmBlock, true)
			}
		}
	}
*/

func (c *Consensus) removeDisqualified(maxBlockNumber uint64) {
	c.disqualified.Range(func(bid interface{}, cb interface{}) bool {
		block := cb.(*consensusBlock).block
		blockNumber := block.BlockNumber()
		if maxBlockNumber >= blockNumber && int(maxBlockNumber-blockNumber) > maxDepth + 1 {
			go metrics.RemoveBlock(block)
			c.disqualified.Delete(bid.(string))
		}
		return true
	})
}

func (c *Consensus) disqualifyOldBlocks(maxBlockNumber uint64) {
	c.blocks.Range(func(bid interface{}, cb interface{}) bool {
		cblock := cb.(*consensusBlock)
		depth := int(maxBlockNumber - cblock.blockNumber)
		if depth > maxDepth && !hasChild(cblock) {
			if equal(c.root.cblock, cblock) {
				c.root = c.bestHead.root
			}
			c.removeBlock(cblock, true)
		}
		return true
	})
}

func (c *Consensus) isLocal(blockID string) bool {
	return exists(c.localBlocks, blockID)
}

func (c *Consensus) confirmBlock(cblock *consensusBlock) {
	block := cblock.block
	blockID := block.Hash()
	if c.isLocal(blockID) {
		go c.onLocalBlockConfirmed(block)
	} else {
		go c.onBlockConfirmed(block)
	}

	c.removeBlock(cblock, false)
	go metrics.RemoveBlock(block)
}

func (c *Consensus) getMaxBlockNumber() uint64 {
	var max uint64
	c.heads.Range(func(hid interface{}, cb interface{}) bool {
		blockNumber := cb.(*consensusBlock).blockNumber
		if blockNumber > max {
			max = blockNumber
		}
		return true
	})

	return max
}

func (c *Consensus) getChildren(parentID string) *sync.Map {
	children := &sync.Map{}
	c.blocks.Range(func(bid interface{}, cb interface{}) bool {
		cblk := cb.(*consensusBlock)
		if cblk.parent != nil && cblk.parentID == parentID {
			children.Store(cblk.blockID, cblk)
		}
		return true
	})
	return children
}

func hasChild(cblock *consensusBlock) bool {
	if cblock == nil || cblock.children == nil {
		return false
	}
	var hasChild bool
	cblock.children.Range(func(cid interface{}, child interface{}) bool {
		hasChild = true
		return false
	})
	return hasChild
}

func exists(smap *sync.Map, key string) bool {
	_, ok := smap.Load(key)
	return ok
}

func getBlock(smap *sync.Map, key string) *consensusBlock {
	cblock, ok := smap.Load(key)
	if !ok {
		return nil
	}
	return cblock.(*consensusBlock)
}

func equal(cb1, cb2 *consensusBlock) bool {
	if cb1 == nil || cb2 == nil {
		return false
	}
	return cb1.blockID == cb2.blockID
}
