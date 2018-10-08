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
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	push "github.com/blocktop/go-push-components"

	spec "github.com/blocktop/go-spec"
	"github.com/disiqueira/gotree"
	"github.com/fatih/color"
)

type consensusTree struct {
	sync.Mutex
	Roots           []*treeBlock `json:"roots"`
	MinBlockNumber  uint64       `json:"minBlockNumber,string"`
	MaxBlockNumber  uint64       `json:"maxBlockNumber,string"`
	UpdateTimestamp int64        `json:"updateTimestamp"` //millisecond
	queue           *push.PushQueue
	blocks          map[string]*treeBlock
}

type treeBlock struct {
	ID             string       `json:"id"`
	Name           string       `json:"name"`
	Children       []*treeBlock `json:"children"`
	BlockNumber    uint64       `json:"blockNumber,string"`
	IsDisqualified bool         `json:"isDisqualified"`
	IsLocal        bool         `json:"isLocal"`
	parentID       string
}

type updateTask struct {
	taskType int
	block    spec.Block
}

const (
	queueHeight int64 = 100000
	taskTypeAdd int   = iota
	taskTypeDisqualify
	taskTypeRemove
)

var tree *consensusTree

func init() {
	t := &consensusTree{}
	tree = t
	t.Roots = make([]*treeBlock, 0)
	t.UpdateTimestamp = time.Now().UnixNano() / int64(time.Millisecond)
	t.blocks = make(map[string]*treeBlock, 0)
	t.queue = push.NewPushQueue(1, 1000, func(item interface{}) {
		t.processQueueItem(item.(*updateTask))
	})
	t.queue.Start()
}

func (t *consensusTree) getJSON() (string, error) {
	jsonBytes, err := json.Marshal(t)
	if err != nil {
		return "null", err
	}
	return string(jsonBytes), nil
}

func (t *consensusTree) getText() string {
	tree := gotree.New("consensus")

	roots := make([]*treeBlock, len(t.Roots))
	copy(roots, t.Roots)

	for _, r := range roots {
		t.buildTreeText(tree, r)
	}
	return tree.Print()
}

func (t *consensusTree) buildTreeText(n gotree.Tree, b *treeBlock) {
	name := b.Name
	if b.IsDisqualified {
		c := color.New(color.Faint)
		name = c.Sprint(b.Name)
		/*
			// strikethrough to indicate disqualified
			runes := []rune(text)
			strike := make([]rune, len(runes)*2)
			for i, r := range runes {
				strike[i*2] = r
				strike[i*2+1] = rune(0x336)
			}
			text = string(strike)
		*/
	} else if b.IsLocal {
		name = color.HiGreenString(name)
	}

	node := n.Add(name)

	for _, c := range b.Children {
		t.buildTreeText(node, c)
	}
}

func (t *consensusTree) sortForText(blocks []*treeBlock) []*treeBlock {
	if len(blocks) < 2 {
		return blocks
	}
	result := make([]*treeBlock, len(blocks))
	sort.SliceStable(blocks, func(i, j int) bool { return !blocks[i].IsDisqualified })
	sort.SliceStable(blocks, func(i, j int) bool { return len(blocks[i].Children) < len(blocks[j].Children) })

	// no children
	withChildren := make([]*treeBlock, 0)
	for _, b := range blocks {
		if len(b.Children) == 0 {
			result = append(result, b)
		} else {
			withChildren = append(withChildren, b)
		}
	}

	if len(withChildren) == 0 {
		return result
	}
	return append(result, withChildren...)
}

func (t *consensusTree) add(b spec.Block) {
	t.queue.Put(&updateTask{taskType: taskTypeAdd, block: b})
}

func (t *consensusTree) disqualify(b spec.Block) {
	t.queue.Put(&updateTask{taskType: taskTypeDisqualify, block: b})
}

func (t *consensusTree) remove(b spec.Block) {
	t.queue.Put(&updateTask{taskType: taskTypeRemove, block: b})
}

func (t *consensusTree) processQueueItem(task *updateTask) {
	switch task.taskType {
	case taskTypeAdd:
		t.addTask(task.block)

	case taskTypeDisqualify:
		t.disqualifyTask(task.block)

	case taskTypeRemove:
		t.removeTask(task.block)
	}
}

func (t *consensusTree) addTask(b spec.Block) {
	block := &treeBlock{
		ID:          b.Hash(),
		Name:        fmt.Sprintf("block %d:%s", b.BlockNumber(), b.Hash()[:6]),
		parentID:    b.ParentHash(),
		BlockNumber: b.BlockNumber(),
		Children:    make([]*treeBlock, 0),
		IsLocal:     consensus.isLocal(b.Hash())}

	t.Lock()
	defer t.Unlock()

	parent := t.blocks[block.parentID]

	if parent == nil {
		// no parent, so this is a root
		t.Roots = append(t.Roots, block)
	} else {
		// add to parent's children
		parent.Children = append(parent.Children, block)
	}

	// see if one of the existing roots has new block as parent
	// then attach it as new block's children, and remove from roots
	for i := 0; i < len(t.Roots); i++ {
		root := t.Roots[i]
		if root.parentID == block.ID {
			block.Children = append(block.Children, root)
			t.Roots = append(t.Roots[:i], t.Roots[i+1:]...)
		}
	}
	t.blocks[block.ID] = block

	if block.BlockNumber > t.MaxBlockNumber {
		t.MaxBlockNumber = block.BlockNumber
	}
}

func (t *consensusTree) disqualifyTask(b spec.Block) {
	blockID := b.Hash()

	t.Lock()
	defer t.Unlock()

	block := t.blocks[blockID]
	if block == nil {
		return
	}

	t.disqualifyRecurse(block)
}

func (t *consensusTree) disqualifyRecurse(block *treeBlock) {
	block.IsDisqualified = true
	for _, c := range block.Children {
		t.disqualifyRecurse(c)
	}
}

func (t *consensusTree) removeTask(b spec.Block) {
	blockID := b.Hash()

	t.Lock()
	defer t.Unlock()

	block := t.blocks[blockID]
	if block == nil {
		return
	}

	t.removeRecurse(block)

	min := ^uint64(0)
	for _, r := range t.Roots {
		if r.BlockNumber < min {
			min = r.BlockNumber
		}
	}
	t.MinBlockNumber = min
}

func (t *consensusTree) removeRecurse(block *treeBlock) {
	parent := t.blocks[block.parentID]
	if parent != nil {
		t.removeRecurse(parent)
	}

	// remove from roots and promote children to roots
	for i, r := range t.Roots {
		if r.ID == block.ID {
			t.Roots = append(t.Roots[:i], t.Roots[i+1:]...)
			break
		}
	}
	t.Roots = append(t.Roots, block.Children...)
	// release references
	block.Children = nil

	delete(t.blocks, block.ID)
}
