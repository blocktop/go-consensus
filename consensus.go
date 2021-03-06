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

	"github.com/golang/glog"

	kernel "github.com/blocktop/go-kernel"
	spec "github.com/blocktop/go-spec"
	"github.com/mxmCherry/movavg"
	"github.com/spf13/viper"
)

// Consensus tracks incoming blocks that are younger than Depth. It tracks
// all the branches of blocks and prunes according to rules defined in the
// compareBlocks function. Client programs can retrieve the most favorable head
// for its next block computation using the GetBestBranch method.
type Consensus struct {
	compareBlocks         spec.BlockComparator
	roots                 map[int]*root
	blocks                *sync.Map
	localBlocks           *sync.Map
	alreadySeen           *sync.Map
	competed              *sync.Map
	confirmingRootID      int
	disqualified          *sync.Map
	headTimer             *time.Timer
	onBlockConfirmed      spec.BlockConfirmationHandler
	onLocalBlockConfirmed spec.BlockConfirmationHandler
	confirming            bool
	evaluating            bool
	lastConfirmed         bool
	sync.Mutex
}

// compile-time interface check
var _ spec.Consensus = (*Consensus)(nil)

var consensus *Consensus
var maxDepth int
var consensusDepth int
var consensusBuffer int
var addDisqualified bool
var hitRateSMAWindow int

func NewConsensus(blockComparator spec.BlockComparator) *Consensus {
	if consensus != nil {
		panic("Consensus object already created")
	}

	if blockComparator == nil {
		panic(errors.New("blockComparator must be provided"))
	}

	c := &Consensus{compareBlocks: blockComparator}

	c.roots = make(map[int]*root)
	c.blocks = &sync.Map{}
	c.alreadySeen = &sync.Map{}
	c.localBlocks = &sync.Map{}
	c.competed = &sync.Map{}
	c.disqualified = &sync.Map{}

	addDisqualified = viper.GetBool("blockchain.metrics.trackall")
	kernel.OnInit(func() {
		consensusTime := viper.GetDuration("blockchain.consensus.time")
		blockInterval := kernel.Time().BlockInterval()
		consensusDepth = int(consensusTime / blockInterval)
		if consensusDepth < 5 {
			consensusDepth = 5
		}
		consensusBuffer = consensusDepth * 10 / 100 // 10% of depth
		if consensusBuffer < 2 {
			consensusBuffer = 2
		}
		maxDepth = consensusDepth - consensusBuffer

		glog.Infof("Consensus depth is %d blocks", consensusDepth)
		glog.Infof("Consensus buffer is %d blocks", consensusBuffer)

		hitRateSMAWindow = int(consensusDepth * 2)
	})

	consensus = c
	return c
}

func (c *Consensus) OnBlockConfirmed(f spec.BlockConfirmationHandler) {
	c.onBlockConfirmed = f
}

func (c *Consensus) OnLocalBlockConfirmed(f spec.BlockConfirmationHandler) {
	c.onLocalBlockConfirmed = f
}

func (c *Consensus) SetConfirmingRoot(rootID int) {
	c.confirmingRootID = rootID
}

func (c *Consensus) Evaluate() spec.Competition {
	c.evaluateRoots()

	if len(c.roots) == 0 {
		glog.V(1).Infoln("Evaluate: leaving, unable to find head for competition")
		return nil
	}

	comp := newCompetition()

	for _, croot := range c.roots {
		branch := &CompetingBranch{
			blocks:               c.getBranch(croot.head.block),
			rootID:               croot.id,
			consecutiveLocalHits: croot.consecutiveLocalHits,
			hitRate:              croot.hitRate.Avg()}
		comp.addBranch(croot.id, branch)
	}

	glog.V(1).Infof("Evaluate: leaving, success with %d roots", len(c.roots))

	return comp
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
// function.
func (c *Consensus) AddBlocks(blocks []spec.Block, isLocal bool) (res *spec.AddBlocksResponse) {
	startTime := time.Now().UnixNano()

	res = &spec.AddBlocksResponse{}

	// All blocks should have same parentID and blockNumber. Verify.
	// Also build an index of the incoming array by hash.
	var parentID string
	var blockNumber uint64
	var sema bool
	index := make(map[string]int)

	for i, b := range blocks {
		if !sema {
			parentID = b.ParentHash()
			blockNumber = b.BlockNumber()
			sema = true
		} else {
			if b.ParentHash() != parentID || b.BlockNumber() != uint64(blockNumber) {
				res.Error = errors.New("all blocks must have same parent hash and block number")
				return
			}
		}
		index[b.Hash()] = i
	}

	glog.V(2).Infof("AddBlocks: comparing %d blocks", len(blocks))

	// Get the winning block amongst these siblings.
	addBlock := c.compareBlocks(blocks)
	blockID := addBlock.Hash()
	blockNumber = addBlock.BlockNumber()
	parentID = addBlock.ParentHash()
	blocki := index[addBlock.Hash()]
	res.DisqualifiedBlocks = append(blocks[:blocki], blocks[blocki+1:]...)

	var logLocal string
	if isLocal {
		logLocal = "local "
	}
	glog.V(2).Infof("AddBlocks: Entering %sblock %d:%s", logLocal, blockNumber, blockID[:6])

	c.Lock()
	defer c.Unlock()

	if c.WasSeen(addBlock) {
		glog.V(2).Infof("AddBlocks: leaving, already saw block %d:%s", blockNumber, blockID[:6])
		return nil
	}

	// If block is too old then ignore it.
	// NEEDSWORK: global maximum or root's branch maximum?
	maxBlockNumber := c.getMaxBlockNumber()
	if maxBlockNumber >= blockNumber {
		blockDepth := int(maxBlockNumber - blockNumber)
		if blockDepth > maxDepth-1 {
			glog.V(2).Infof("AddBlocks: leaving, depth %d to great of block %d:%s", blockDepth, blockNumber, blockID[:6])
			res.DisqualifiedBlocks = blocks
			return
		}
	}

	// The ConsensusBlock is a wrapper for the block while it is
	// being tracking within the consensus system.
	cblock := &block{
		block:       addBlock,
		blockID:     blockID,
		parentID:    parentID,
		blockNumber: blockNumber}

	// Get the parent block, if any, and add to the ConsensusBlock.
	parent := getBlock(c.blocks, parentID)
	cblock.parent = parent

	// If we are tracking the parent, then check to make sure the
	// block number has incremented by only 1. Ignore the block otherwise.
	if parent != nil && blockNumber != parent.blockNumber+1 {
		glog.V(1).Infof("AddBlocks: leaving, parent block %d, new block %d:%s", parent.blockNumber, blockNumber, blockID[:6])
		res.DisqualifiedBlocks = blocks
		return
	}

	// If parent was already eliminated, then new block is also eliminated,
	// and we can ignore it in the tracking system.
	if exists(c.disqualified, parentID) {
		if addDisqualified {
			metrics.AddBlock(addBlock)
			go metrics.DisqualifyBlock(addBlock)
		}
		glog.V(1).Infof("AddBlocks: leaving, disqualified parent %s of block %d:%s", parentID[:6], blockNumber, blockID[:6])
		res.DisqualifiedBlocks = blocks
		return
	}

	var siblings []*block

	if parent == nil {

		croot := newRoot(cblock)
		// Yes, these are a circular references. We need to take precautions when
		// changing to prevent leaks. Use the cblock.setRoot function.
		cblock.root = croot
		croot.head = cblock

		// If this is the genesis block and no other consensus root has been
		// set yet, the make this block the confirming root.
		if blockNumber == 0 && c.confirmingRootID == 0 {
			glog.V(2).Infof("AddBlocks: setting confirming root %d to genesis block: %s", croot.id, blockID[:6])
			c.confirmingRootID = croot.id
		}

		// Hitrate is a simple moving average covering the nominal span of time
		// in microseconds that blocks are tracked to confirmation in the system.
		croot.hitRate = movavg.NewSMA(hitRateSMAWindow)

		// Get all blocks with same parentID as the new block.
		// Parent might not be in the system as a consensusBlock,
		// so search by ID. Note for genesis block, parentID will be "" so
		// siblings will be all submitted genesis blocks.
		siblings = c.getChildren(parentID)
	}

	// If we are tracking this block's parent, then the new block has the
	// same root as the parent and the siblings are the parent's children.
	if parent != nil {
		siblings = parent.children
		cblock.root = parent.root
	}

	// Include the new block as child of its parent.
	siblings = append(siblings, cblock)

	// Collect orphans and attach as children of new block. If any of the
	// orphans has a block number that is not one greater than the new
	// block, then we ignore the new block.
	orphans := c.getChildren(blockID)
	if len(orphans) > 0 {
		badBlockNumber := false
		for _, corphan := range orphans {
			if corphan.blockNumber != blockNumber+1 {
				badBlockNumber = true
				break
			}
		}
		if badBlockNumber {
			glog.V(1).Infof("AddBlocks: leaving, incorrect block number relative to children of block %d:%s", blockNumber, blockID[:6])
			res.DisqualifiedBlocks = blocks
			return
		}
		// Evaluate children against each other and eliminate unfavoarables.
		remainingOrphan := c.disqualifyUnfavorables(orphans, true)

		// The orphan's root will be the same as its newfound parent.
		children := make([]*block, 0)
		if remainingOrphan != nil {
			glog.V(2).Infof("AddBlocks: setting child branch to newly added root block %d:%s", blockNumber, blockID[:6])
			remainingOrphan.setRoot(cblock.root)
			children = []*block{remainingOrphan}
		}
		cblock.children = children
	} else {
		cblock.children = make([]*block, 0)
	}

	// Now that orphans are reattached to parents, we can deal with
	// siblings. We need orphans attached in case the next line
	// disqualifies the new block thereby disqualifying the former orphans.
	// Compare all siblings and keep only the most favorable one.
	// Note this could displace the current best head or the
	// branch leading to it.
	favorableSibling := c.disqualifyUnfavorables(siblings, true)
	if favorableSibling != nil && parent != nil {
		parent.children = []*block{favorableSibling}
	}

	// Eliminate new block if it was not favorable against siblings.
	if favorableSibling == nil || favorableSibling.blockID != blockID {
		if addDisqualified {
			metrics.AddBlock(addBlock)
			go metrics.DisqualifyBlock(addBlock)
		}
		glog.V(1).Infof("AddBlocks: leaving, sibling was more favorable than block %d:%s", blockNumber, blockID[:6])
		res.DisqualifiedBlocks = blocks
		return
	}

	// ***
	// Beyond this point we know definitively that we will be adding
	// the block into the tracking system.
	// ***

	// Mark the block as "already seen" so we don't try to add it
	// again. This record will be maintained until the block exits
	// the consensus system. At that time, the block would be too
	// old to add into the system again anyway.
	c.alreadySeen.Store(blockID, cblock)

	// Record the new block in the metrics tracker.
	go metrics.AddBlock(addBlock)

	// Record a "hit" on this root. This helps to determine how
	// popular this root is for the confirming algorithm. It also
	// tracks how many consecutive local blocks have been added
	// to prevent an echo chamber.
	cblock.root.recordHit(isLocal)

	// Save a record of local blocks so that they can be identified
	// upon confirmation.
	if isLocal {
		c.localBlocks.Store(blockID, cblock)
	}

	// Add the root the main root map.
	if c.roots[cblock.root.id] == nil {
		c.roots[cblock.root.id] = cblock.root
	}

	// Add the new block to the main block map.
	c.blocks.Store(blockID, cblock)

	duration := time.Now().UnixNano() - startTime
	metrics.BlockAddDuration(duration)

	glog.V(1).Infof("AddBlocks: leaving, success for block %d:%s", blockNumber, blockID[:6])
	res.AddedBlock = addBlock
	res.AddedToRoot = cblock.root.id
	res.MaxBlockNumber = cblock.branchHead().blockNumber
	return
}

// evaluateRoots determines the best head for the blockchain to generate
// the next block against for each root.
func (c *Consensus) evaluateRoots() {
	glog.V(1).Infoln("evaluateRoots: entering")

	for _, croot := range c.roots {
		croot.setHead()
	}

	// TODO: competition should contain a map of the current root/head
	// competition landscape so that Kernel can determine if it wants
	// to switch roots.

	/*
		// Favor the head from the root we are currently tracking,
		// unless it has too many consecutive local hits (echo chamber) or
		// has a lower hit rate than another head. If those conditions
		// happen, then we switch confirming roots and start building on
		// a differnt fork of the blockchain.
		var bestAlternateHead *block
		croot := c.confirmingRoot

		// No confirming root, need to get onto the best head and set root.
		switchHeads := bestRootHead == nil
		if switchHeads {
			glog.V(2).Infoln("evaluateHeads: need to switch heads and set confirming root")
		}

		// Echo chamber test
		if !switchHeads {
			// TODO: need to wait for kernel to give green light to switch
			//switchHeads = croot.consecutiveLocalHits > uint(consensusDepth*20/100) // 20% consensus depth TODO make a config item
			if switchHeads {
				glog.V(2).Infof("evaluateHeads: might switch heads, confirming root had %d consencutive local hits", croot.consecutiveLocalHits)
			}
		}

		// Hit rate test
		if len(bestHeads) > 0 {
			bestHitRate := float64(10e20) // very large so we can find min
			for _, eHead := range bestHeads {
				hitRate := eHead.hitRate.Avg()
				// We consider this alternate head only if its root's hit rate is greater than
				// zero (hitrate is zero when root and head are same) and the number of
				// hits on this root is more than 10% of consensus depth (it has been around for
				// a few rounds or confirming root is nil).
				if hitRate < bestHitRate && hitRate > 0 && eHead.hits > uint64(consensusDepth*10/100) {
					bestAlternateHead = eHead.chead
					bestHitRate = hitRate
				}
			}
			if croot != nil {
				betterHitRate := bestHitRate < croot.hitRate.Avg()*0.5
				if betterHitRate {
					glog.V(2).Infof("evaluateHeads: maybe switching heads, alternate root had %f vs. %f hits rate", bestHitRate, croot.hitRate.Avg())
				}

				// TODO: need to wait for kernel to give the green light to switch
				//switchHeads = switchHeads || betterHitRate // 50% faster than the confirming root's hit rate
			}
		}

		bestHead := bestRootHead
		var switchedHeads bool
		if switchHeads && bestAlternateHead != nil {
			glog.V(2).Infoln("evaluateHeads: switching roots")
			bestHead = bestAlternateHead
			switchedHeads = true
		}

		// Unable to find a head for competition.
		if bestHead == nil {
			glog.V(1).Infoln("evaluateHeads: leaving, unable to find head for competition")
			c.competition.setBranch(nil, 0, false)
			return
		}

		// If there is no confirming root, then set it to the root of the best head.
		bestHeadHeight := int(bestHead.blockNumber - bestHead.root.cblock.blockNumber)
		if c.confirmingRoot == nil && bestHeadHeight > maxDepth {
			glog.V(2).Infof("evaluateHeads: setting confirming root to %d", bestHead.root.id)
			c.confirmingRoot = bestHead.root
		}

		branch := c.getBranch(bestHead.block)
		c.competition.setBranch(branch, bestHead.root.id, switchedHeads)

		glog.V(1).Infof("evaluateHeads: leaving, success with head %d:%s", branch[0].BlockNumber(), branch[0].Hash()[:6])
	*/
}

// disqualifies unfavorable blocks and returns the most favorable
func (c *Consensus) disqualifyUnfavorables(cblocks []*block, removeBranch bool) *block {
	if len(cblocks) == 0 {
		return nil
	}
	if len(cblocks) == 1 {
		return cblocks[0]
	}

	blocks := make([]spec.Block, len(cblocks))
	for i, cb := range cblocks {
		blocks[i] = cb.block
	}

	favorable := c.compareBlocks(blocks)
	favorableID := favorable.Hash()
	if removeBranch {
		for _, b := range blocks {
			blockID := b.Hash()
			if blockID != favorableID {
				cblock := getBlock(c.blocks, blockID)
				if cblock != nil {
					c.removeBranch(cblock, true)
				}
			}
		}
	}

	for _, cb := range cblocks {
		if cb.blockID == favorableID {
			return cb
		}
	}
	return nil
}

func (c *Consensus) getBranch(b spec.Block) []spec.Block {
	this := []spec.Block{b}
	cparent, _ := c.blocks.Load(b.ParentHash())
	if cparent == nil {
		return this
	}
	parent := cparent.(*block).block
	if parent == nil {
		return this
	}
	return append(this, c.getBranch(parent)...)
}

func (c *Consensus) removeBranch(cblock *block, disqualify bool) {
	blockID := cblock.blockID
	if !exists(c.blocks, blockID) {
		return
	}

	if cblock.children != nil {
		for _, child := range cblock.children {
			c.removeBranch(child, disqualify)
		}
	}
	if cblock.parent != nil && cblock.parent.children != nil {
		pchildren := cblock.parent.children
		for i, child := range pchildren {
			if child.blockID == blockID {
				cblock.parent.children = append(pchildren[:i], pchildren[i+1:]...)
			}
		}
	}

	c.removeBlock(cblock, disqualify)
}

func (c *Consensus) removeBlock(cblock *block, disqualify bool) {
	blockID := cblock.blockID
	if !exists(c.blocks, blockID) {
		return
	}

	// If this block was the confirming root, then we are blowing up our system.
	// The caller should have accounted for this and handled it.
	if c.confirmingRootID > 0 && c.roots[c.confirmingRootID].cblock.equal(cblock) {
		panic("removing the root")
	}

	c.localBlocks.Delete(blockID)
	c.alreadySeen.Delete(blockID)
	c.competed.Delete(blockID)

	if disqualify {
		c.disqualifyBlock(cblock)
	}

	// manage circular references
	cblock.parent = nil
	cblock.children = nil
	cblock.root = nil

	c.blocks.Delete(blockID)
}

func (c *Consensus) disqualifyBlock(cblock *block) {
	blockID := cblock.blockID
	if exists(c.disqualified, blockID) {
		return
	}
	c.disqualified.Store(blockID, cblock)

	go metrics.DisqualifyBlock(cblock.block)

	if cblock.children == nil {
		return
	}

	for _, child := range cblock.children {
		if child != nil {
			c.disqualifyBlock(child)
		}
	}
}

// This function runs within the mutex lock of c.AddBlock
func (c *Consensus) ConfirmBlocks() {
	glog.V(1).Infoln("ConfirmBlocks: entering")

	var confRoot *root
	if c.confirmingRootID > 0 {
		confRoot = c.roots[c.confirmingRootID]
	}
	if confRoot == nil || confRoot.cblock == nil {
		glog.V(1).Infoln("ConfirmBlocks: leaving, no confirming root")
		return
	}

	// This is done by analyzeRoot
	//c.disqualifyOldBlocks(maxBlockNumber)

	maxChildBlockNumber, maxChild := c.analyzeRoot(confRoot.cblock)
	minBlockNumber := confRoot.cblock.blockNumber
	depth := (maxChildBlockNumber - minBlockNumber)

	// If the distance between the root and the max is to small, then
	// this root is not ready for confirmation.
	if depth < uint64(consensusDepth) {
		glog.V(1).Infoln("ConfirmBlocks: leaving, root has not grown to consensus depth")
		return
	}

	c.removeDisqualified(confRoot, maxChildBlockNumber)

	// Pass the confirming root on to the child and confirm the current root.
	confirmBlock := confRoot.cblock
	confRoot.cblock = maxChild
	c.confirmBlock(confirmBlock)

	glog.V(1).Infoln("ConfirmBlocks: leaving, success")
}

func (c *Consensus) analyzeRoot(cblock *block) (uint64, *block) {
	if cblock.children == nil || len(cblock.children) == 0 {
		return cblock.blockNumber, cblock
	}

	maxChildBlockNumber := uint64(0)
	maxChildBlockNumbers := make(map[string]uint64) // [childID]maxChildBlockNumber
	var maxChild *block

	// Recurse children to find the maximum block number in the branch.
	for _, child := range cblock.children {
		childHeight, _ := c.analyzeRoot(child)

		maxChildBlockNumbers[child.blockID] = childHeight

		if childHeight > maxChildBlockNumber {
			maxChildBlockNumber = childHeight
			maxChild = child
		}
	}

	// If the current block is below the consensus buffer, then remove any child
	// branches that do not reach beyond the upper buffer. These are abandoned branches.
	var confRoot *root
	if c.confirmingRootID > 0 {
		confRoot = c.roots[c.confirmingRootID]
	}
	if maxChildBlockNumber >= uint64(consensusBuffer) {
		bufferZoneHigh := maxChildBlockNumber - uint64(consensusBuffer)
		for childID, childHeight := range maxChildBlockNumbers {
			if childHeight < bufferZoneHigh {
				// This block is on the threshold of being confirmed and it has no children.
				// Since it is blelow this threshold, it is impossible for new blocks to be
				// added to it as children, so it can safely be removed. But we need to check
				// if it is actually the confirming block right now. If so, we need to nilify
				// that and let c.evaluateHeads determine the new confirming block. It should
				// be very unusual to end up doing that.
				child := getBlock(c.blocks, childID)
				if confRoot != nil && confRoot.cblock.equal(child) {
					glog.V(1).Infof("analyzeRoot: setting confirming root %d to nil", confRoot.id)
					c.confirmingRootID = 0
				}
				c.removeBlock(child, true)
			}
		}
	}

	return maxChildBlockNumber, maxChild
}

func (c *Consensus) removeDisqualified(croot *root, maxChildBlockNumber uint64) {

	c.disqualified.Range(func(bid interface{}, cb interface{}) bool {
		cblock := cb.(*block)
		block := cblock.block
		blockNumber := block.BlockNumber()
		if croot.equal(cblock.root) && maxChildBlockNumber >= blockNumber && int(maxChildBlockNumber-blockNumber) > maxDepth+1 {
			go metrics.RemoveBlock(block)
			c.disqualified.Delete(bid.(string))
		}
		return true
	})
}

func (c *Consensus) disqualifyOldBlocks(maxBlockNumber uint64) {
	var confRoot *root
	if c.confirmingRootID > 0 {
		confRoot = c.roots[c.confirmingRootID]
	}
	c.blocks.Range(func(bid interface{}, cb interface{}) bool {
		cblock := cb.(*block)
		if maxBlockNumber >= cblock.blockNumber {
			depth := int(maxBlockNumber - cblock.blockNumber)
			if depth > maxDepth && cblock != nil && !cblock.hasChild() {
				if confRoot != nil && confRoot.cblock.equal(cblock) {
					glog.V(1).Infof("disqualifyOldBlocks: setting confirming root %d to nil", confRoot.id)
					c.confirmingRootID = 0
				}
				c.removeBlock(cblock, true)
			}
		}
		return true
	})
}

func (c *Consensus) isLocal(blockID string) bool {
	return exists(c.localBlocks, blockID)
}

func (c *Consensus) confirmBlock(cblock *block) {
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
	for _, croot := range c.roots {
		blockNumber := croot.head.blockNumber
		if blockNumber > max {
			max = blockNumber
		}
	}

	return max
}

func (c *Consensus) getChildren(parentID string) []*block {
	children := make([]*block, 0)
	count := 0
	c.blocks.Range(func(bid interface{}, cb interface{}) bool {
		cblk := cb.(*block)
		if cblk.parent != nil && cblk.parentID == parentID {
			children = append(children, cblk)
			count++
		}
		return true
	})
	return children
}

func exists(smap *sync.Map, key string) bool {
	_, ok := smap.Load(key)
	return ok
}

func getBlock(smap *sync.Map, key string) *block {
	cblock, ok := smap.Load(key)
	if !ok {
		return nil
	}
	return cblock.(*block)
}
