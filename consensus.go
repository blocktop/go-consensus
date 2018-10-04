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
	blocks                *sync.Map
	heads                 *sync.Map
	localBlocks           *sync.Map
	alreadySeen           *sync.Map
	competed              *sync.Map
	confirmingRoot        *consensusRoot
	competition           *Competition
	disqualified          *sync.Map
	headTimer             *time.Timer
	onBlockConfirmed      spec.BlockConfirmationHandler
	onLocalBlockConfirmed spec.BlockConfirmationHandler
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

// consensusRoot carries a pointer to the current root block of the
// consensus tree. Since we are constantly confirming blocks, the
// block pointed to in this struct will continually change, and all
// blocks that point to this struct will immediately reflect that change.
type consensusRoot struct {
	cblock               *consensusBlock
	consecutiveLocalHits uint
	lastHitTimestamp     int64
	hitRate              *movavg.SMA // in µs/block
	hits                 uint64
}

// compile-time interface check
var _ spec.Consensus = (*Consensus)(nil)

var consensus *Consensus
var maxDepth int
var consensusDepth int
var consensusBuffer int
var blockInterval time.Duration
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

	c.blocks = &sync.Map{}
	c.heads = &sync.Map{}
	c.alreadySeen = &sync.Map{}
	c.localBlocks = &sync.Map{}
	c.competed = &sync.Map{}
	c.disqualified = &sync.Map{}
	c.competition = &Competition{}

	consensusDepth = viper.GetInt("blockchain.consensus.depth")
	consensusBuffer = viper.GetInt("blockchain.consensus.buffer")
	maxDepth = consensusDepth - consensusBuffer
	blockInterval = viper.GetDuration("blockchain.blockInterval")
	addDisqualified = viper.GetBool("blockchain.metrics.trackall")
	hitRateSMAWindow = int(blockInterval/time.Microsecond) * consensusDepth

	consensus = c
	return c
}

func (c *Consensus) OnBlockConfirmed(f spec.BlockConfirmationHandler) {
	c.onBlockConfirmed = f
}

func (c *Consensus) OnLocalBlockConfirmed(f spec.BlockConfirmationHandler) {
	c.onLocalBlockConfirmed = f
}

func (c *Consensus) Competition() spec.Competition {
	return c.competition
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
func (c *Consensus) AddBlock(block spec.Block, isLocal bool) (added bool) {
	startTime := time.Now().UnixNano()

	glog.V(2).Infof("AddBlock: Entering block %d:%s", block.BlockNumber(), block.Hash()[:6])

	c.Lock()
	defer c.Unlock()

	if c.WasSeen(block) {
		glog.V(2).Infof("AddBlock: leaving, already saw block %d:%s", block.BlockNumber(), block.Hash()[:6])
		return false
	}

	// If block is too old then ignore it.
	blockDepth := int(c.getMaxBlockNumber() - block.BlockNumber())
	if blockDepth > maxDepth-1 {
		glog.V(2).Infof("AddBlock: leaving, depth %d to great of block %d:%s", blockDepth, block.BlockNumber(), block.Hash()[:6])
		return false
	}

	blockID := block.Hash()
	parentID := block.ParentHash()

	// The ConsensusBlock is a wrapper for the block while it is
	// being tracking within the consensus system.
	cblock := &consensusBlock{
		block:       block,
		blockID:     block.Hash(),
		parentID:    block.ParentHash(),
		blockNumber: block.BlockNumber()}

	// Get the parent block, if any, and add to the ConsensusBlock.
	parent := getBlock(c.blocks, parentID)
	cblock.parent = parent

	// If we are tracking the parent, then check to make sure the
	// block number has incremented by only 1. Ignore the block otherwise.
	if parent != nil && block.BlockNumber() != parent.blockNumber+1 {
		glog.V(1).Infof("AddBlock: leaving, parent block %d, new block %d:%s", parent.blockNumber, block.BlockNumber(), block.Hash()[:6])
		return false
	}

	// If parent was already eliminated, then new block is also eliminated,
	// and we can ignore it in the tracking system.
	if exists(c.disqualified, parentID) {
		if addDisqualified {
			metrics.AddBlock(block)
			go metrics.DisqualifyBlock(block)
		}
		glog.V(1).Infof("AddBlock: leaving, disqualified parent of block %d:%s", block.BlockNumber(), block.Hash()[:6])
		return false
	}

	var siblings *sync.Map

	if parent == nil {

		croot := &consensusRoot{}
		// Yes, this is a circular reference. We need to take precautions when
		// changing to prevent leaks. Use the c.setRoot function.
		croot.cblock = cblock
		cblock.root = croot

		// If this is the genesis block and no other consensus root has been
		// set yet, the make this block the confirming root.
		if block.BlockNumber() == 0 && c.confirmingRoot == nil {
			glog.V(2).Infof("AddBlock: setting confirming root to genesis block: %s", block.Hash()[:6])
			c.confirmingRoot = croot
		}

		// Hitrate is a simple moving average covering the nominal span of time
		// in microseconds that blocks are tracked to confirmation in the system.
		croot.hitRate = movavg.NewSMA(hitRateSMAWindow)

		// Get all blocks with same parentID as the new block.
		// Parent might not be in the system as a consensusBlock,
		// so search by ID. Note for genesis block, parentID will be "" so
		// siblings will be all submitted genesis blocks.
		siblings, _ = c.getChildren(parentID)
	}

	// If we are tracking this block's parent, then the new block has the
	// same root as the parent and the siblings are the parent's children.
	if parent != nil {
		siblings = parent.children
		cblock.root = parent.root
	}

	// Include the new block as child of its parent.
	siblings.Store(blockID, cblock)

	// Collect orphans and attach as children of new block. If any of the
	// orphans has a block number that is not one greater than the new
	// block, then we ignore the new block.
	orphans, childCount := c.getChildren(blockID)
	if childCount > 0 {
		badBlockNumber := false
		orphans.Range(func(oid, o interface{}) bool {
			corphan := o.(*consensusBlock)
			if corphan.blockNumber != block.BlockNumber()+1 {
				badBlockNumber = true
				return false
			}
			return true
		})
		if badBlockNumber {
			glog.V(1).Infof("AddBlock: leaving, incorrect block number relative to children of block %d:%s", block.BlockNumber(), block.Hash()[:6])
			return false
		}
		// Evaluate children against each other and eliminate unfavoarables.
		remainingOrphan := c.disqualifyUnfavorables(orphans, true)

		// The orphan's root will be the same as its newfound parent.
		childCount = 0
		children := &sync.Map{}
		if remainingOrphan != nil {
			childCount = 1
			glog.V(2).Infof("AddBlock: setting child branch to newly added root block %d:%s", block.BlockNumber(), block.Hash()[:6])
			c.setRoot(remainingOrphan, cblock.root)
			children.Store(remainingOrphan.blockID, remainingOrphan)
		}
		cblock.children = children
	} else {
		cblock.children = &sync.Map{}
	}

	// Now that orphans are reattached to parents, we can deal with
	// siblings. We need orphans attached in case the next line
	// disqualifies the new block thereby disqualifying the former orphans.
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
		glog.V(1).Infof("AddBlock: leaving, sibling was more favorable than block %d:%s", block.BlockNumber(), block.Hash()[:6])
		return false
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
	go metrics.AddBlock(block)

	// Record a "hit" on this root. This helps to determine how
	// popular this root is for the confirming algorithm. It also
	// tracks how many consecutive local blocks have been added
	// to prevent an echo chamber.
	recordHit(cblock.root, isLocal)

	// Save a record of local blocks so that they can be identified
	// upon confirmation.
	if isLocal {
		c.localBlocks.Store(blockID, cblock)
	}

	// By definition the new block replaces the parent as head,
	// if it was one.
	if exists(c.heads, parentID) {
		glog.V(2).Infof("AddBlock: removing head, parent of block %d:%s", block.BlockNumber(), block.Hash()[:6])
		c.heads.Delete(parentID)
	}
	if childCount == 0 {
		glog.V(2).Infof("AddBlock: setting head block %d:%s", block.BlockNumber(), block.Hash()[:6])
		c.heads.Store(blockID, cblock)
	}

	// Add the new block to the main block map.
	c.blocks.Store(blockID, cblock)

	// Confirm any blocks that are deeper than threshold.
	c.confirmBlocks()

	// Evaluate heads for next round of competition.
	c.evaluateHeads()

	duration := time.Now().UnixNano() - startTime
	metrics.BlockAddDuration(duration)

	glog.V(1).Infof("AddBlock: leaving, success for block %d:%s", block.BlockNumber(), block.Hash()[:6])
	return true
}

type evalHead struct {
	chead          *consensusBlock
	hitRate        *movavg.SMA
	hits           uint64
	maxBlockNumber uint64
}

// evaluateHeads determines the best head for the blockchain to generate
// the next block against. It places the entire branch, from root to
// head into a queue for the blockchain to pickup when ready. The zeroth
// element of the branch is the head.
//
// The function favors the head(s) of the currently confirming root block.
// However, if that root is no longer getting the majority of attention
// from the rest of the network, then c.evaluateHeads may decide to
// switch roots to a more active one.
//
// This function runs within the mutex lock of c.AddBlock
func (c *Consensus) evaluateHeads() {
	glog.V(1).Infoln("evaluateHeads: entering")

	var hasHead bool
	var bestRootHead *consensusBlock
	var maxRootHead uint64
	bestHeads := make(map[string]*evalHead)

	// Find both the best head attached to the confirming root, and the
	// best heads not attached to the confirming root.

	c.heads.Range(func(hid interface{}, cb interface{}) bool {
		hasHead = true
		chead := cb.(*consensusBlock)
		headID := chead.blockID
		blockNumber := chead.blockNumber

		// We have already competed for this head, so continue the loop.
		if exists(c.competed, headID) {
			return true
		}

		// Find the maximum block number under the current confirming root, if any.
		if equalRoots(c.confirmingRoot, chead.root) {
			if blockNumber > maxRootHead || (maxRootHead == 0 && blockNumber == 0) {
				maxRootHead = blockNumber
				bestRootHead = chead
			}
		} else {
			// Find the maximum block number under each alternate root.
			rootID := chead.root.cblock.blockID
			bestHead := bestHeads[rootID]
			if bestHead == nil {
				bestHeads[rootID] = &evalHead{
					chead:          chead,
					hitRate:        chead.root.hitRate,
					hits:           chead.root.hits,
					maxBlockNumber: blockNumber}
			} else if blockNumber > bestHead.maxBlockNumber {
				bestHead.maxBlockNumber = blockNumber
			}
		}
		return true
	})
	if !hasHead {
		glog.V(2).Infoln("evaluateHeads: no head was found")
		return
	}

	// Favor the head from the root we are currently tracking,
	// unless it has too many consecutive local hits (echo chamber) or
	// has a lower hit rate than another head. If those conditions
	// happen, then we switch confirming roots and start building on
	// a differnt fork of the blockchain.
	var bestAlternateHead *consensusBlock
	croot := c.confirmingRoot

	// No confirming root.
	switchHeads := bestRootHead == nil
	if switchHeads {
		glog.V(2).Infoln("evaluateHeads: need to switch heads, no head based on confirming root")
	}

	// Echo chamber test
	if !switchHeads {
		switchHeads = croot.consecutiveLocalHits > uint(consensusDepth*20/100) // 20% consensus depth TODO make a config item
		if switchHeads {
			glog.V(2).Infof("evaluateHeads: maybe switching heads, confirming root had %d consencutive local hits", croot.consecutiveLocalHits)
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
			// a few rounds).
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

			switchHeads = switchHeads || betterHitRate // 50% faster than the confirming root's hit rate
		}
	}

	bestHead := bestRootHead
	if switchHeads && bestAlternateHead != nil {
		glog.V(2).Infoln("evaluateHeads: switching to alternative head")
		bestHead = bestAlternateHead
	}

	// Unable to find a head for competition.
	if bestHead == nil {
		glog.V(1).Infoln("evaluateHeads: leaving, unable to find head for competition")
		return
	}

	// If there is no confirming root, then set it to the root of the best head.
	if c.confirmingRoot == nil {
		glog.V(2).Infoln("evaluateHeads: setting confirmating root")
		c.confirmingRoot = bestHead.root
	}

	branch := c.getBranch(bestHead.block)
	if branch != nil && len(branch) > 0 {
		go c.competition.setBranch(branch)
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

	// If this block was the confirming root, then we are blowing up our system.
	// The caller should have accounted for this and handled it.
	if c.confirmingRoot != nil && equalCBlocks(c.confirmingRoot.cblock, cblock) {
		panic("removing the root")
	}

	c.localBlocks.Delete(blockID)
	c.heads.Delete(blockID)
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

// This function runs within the mutex lock of c.AddBlock
func (c *Consensus) confirmBlocks() {
	glog.V(1).Infoln("confirmBlocks: entering")

	confRoot := c.confirmingRoot
	if confRoot == nil || confRoot.cblock == nil {
		glog.V(1).Infoln("confirmBlocks: leaving, no confirming root")
		return
	}

	// This is the global max block number, not necessarily the max
	// block number of the confirming root.
	maxBlockNumber := c.getMaxBlockNumber()

	// NEEDSWORK: disqualify only on alternate roots, analyze root will do it on confirming root.
	c.disqualifyOldBlocks(maxBlockNumber)
	c.removeDisqualified(maxBlockNumber)

	maxChildBlockNumber, maxChild := c.analyzeRoot(confRoot.cblock)
	minBlockNumber := confRoot.cblock.blockNumber
	depth := (maxChildBlockNumber - minBlockNumber)

	// If the distance between the root and the max is to small, then
	// this root is not ready for confirmation.
	if depth < uint64(consensusDepth) {
		glog.V(1).Infoln("confirmBlocks: leaving, root has not grown to consensus depth")
		return
	}

	// Pass the confirming root on to the child and confir the current root.
	confirmBlock := confRoot.cblock
	c.confirmingRoot.cblock = maxChild
	c.confirmBlock(confirmBlock)

	glog.V(1).Infoln("confirmBlocks: leaving, success")
}

func (c *Consensus) analyzeRoot(cblock *consensusBlock) (uint64, *consensusBlock) {
	if cblock.children == nil {
		return cblock.blockNumber, cblock
	}

	maxChildBlockNumber := uint64(0)
	maxChildBlockNumbers := make(map[string]uint64)
	var maxChild *consensusBlock
	var hasChild bool

	// Recurse children to find the maximum block number in the branch.
	cblock.children.Range(func(cid interface{}, ch interface{}) bool {
		hasChild = true
		child := ch.(*consensusBlock)
		childHeight, _ := c.analyzeRoot(child)

		maxChildBlockNumbers[cid.(string)] = childHeight

		if childHeight > maxChildBlockNumber {
			maxChildBlockNumber = childHeight
			maxChild = child
		}
		return true
	})

	if !hasChild {
		return cblock.blockNumber, cblock
	}

	// If the current block is below the consensus buffer, then remove any child
	// branches that do not reach beyond the upper buffer. These are abandoned branches.
	bufferZoneHigh := maxChildBlockNumber - uint64(consensusBuffer)
	for childID, childHeight := range maxChildBlockNumbers {
		if childHeight < bufferZoneHigh {
			// This block is on the threshold of being confirmed and it has no children.
			// Since it is blelow this threshold, it is impossible for new blocks to be
			// added to it as children, so it can safely be removed. But we need to check
			// if it is actually the confirming block right now. If so, we need to nilify
			// that and let c.evaluateHeads determine the new confirming block. It should
			// be very unusual to end up doing that.
			child := getBlock(cblock.children, childID)
			if c.confirmingRoot != nil && equalCBlocks(c.confirmingRoot.cblock, child) {
				glog.V(1).Infoln("analyzeRoot: setting confirming root to nil")
				c.confirmingRoot = nil
			}
			c.removeBlock(child, true)
		}
	}

	return maxChildBlockNumber, maxChild
}

func (c *Consensus) removeDisqualified(maxBlockNumber uint64) {
	c.disqualified.Range(func(bid interface{}, cb interface{}) bool {
		block := cb.(*consensusBlock).block
		blockNumber := block.BlockNumber()
		if maxBlockNumber >= blockNumber && int(maxBlockNumber-blockNumber) > maxDepth+1 {
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
			if c.confirmingRoot != nil && equalCBlocks(c.confirmingRoot.cblock, cblock) {
				c.confirmingRoot = nil
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

func (c *Consensus) getChildren(parentID string) (*sync.Map, int) {
	children := &sync.Map{}
	count := 0
	c.blocks.Range(func(bid interface{}, cb interface{}) bool {
		cblk := cb.(*consensusBlock)
		if cblk.parent != nil && cblk.parentID == parentID {
			children.Store(cblk.blockID, cblk)
			count++
		}
		return true
	})
	return children, count
}

func (c *Consensus) setRoot(cblock *consensusBlock, croot *consensusRoot) {
	// cleanup up ciruclar reference if any
	cblock.children.Range(func(cid, cc interface{}) bool {
		cchild := cc.(*consensusBlock)
		c.setRoot(cchild, croot)
		return true
	})

	if cblock.blockID == croot.cblock.blockID {
		oldRoot := cblock.root
		cblock.root = croot
		oldRoot.cblock = nil
	} else {
		cblock.root = nil
	}
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

func equalRoots(cr1, cr2 *consensusRoot) bool {
	if cr1 == nil || cr2 == nil {
		return false
	}
	return equalCBlocks(cr1.cblock, cr2.cblock)
}

func equalCBlocks(cb1, cb2 *consensusBlock) bool {
	if cb1 == nil || cb2 == nil {
		return false
	}
	return cb1.blockID == cb2.blockID
}

func recordHit(root *consensusRoot, isLocal bool) {
	now := time.Now().UnixNano()
	if root.lastHitTimestamp == 0 {
		root.hits = 0
		root.consecutiveLocalHits = 0
		root.lastHitTimestamp = now
	}
	if isLocal {
		root.consecutiveLocalHits++
	} else {
		root.consecutiveLocalHits = 0
	}

	deltaT := (now - root.lastHitTimestamp) / int64(time.Microsecond)
	root.hitRate.Add(float64(deltaT))
	root.lastHitTimestamp = now

	root.hits++
}
