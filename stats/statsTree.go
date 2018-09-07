package stats

import (
	"context"
	"math"
	"sync"
	"time"

	spec "github.com/blckit/go-spec"
	q "github.com/golang-collections/go-datastructures/queue"
)

type StatsTree struct {
	sync.Mutex
	Roots           []*TreeBlock `json:"roots"`
	MinBlockNumber  uint64       `json:"minBlockNumber,string"`
	MaxBlockNumber  uint64       `json:"maxBlockNumber,string"`
	UpdateTimestamp int64        `json:"updateTimestamp"` //millisecond
	queue           *q.Queue
	started         bool
	frameRate       float64
	blocks          map[string]*TreeBlock
	OnChange        func() `json:"-"`
}

type TreeBlock struct {
	ID             string       `json:"id"`
	Children       []*TreeBlock `json:"children"`
	BlockNumber    uint64       `json:"blockNumber,string"`
	IsDisqualified bool         `json:"IsDisqualified"`
	parentID       string
}

const (
	queueHeight      int64   = 100000
	defaultFrameRate float64 = 30.0
	taskTypeAdd      int     = iota
	taskTypeDisqualify
	taskTypeRemove
)

func newStatsTree() *StatsTree {
	t := &StatsTree{}
	t.Roots = make([]*TreeBlock, 0)
	t.UpdateTimestamp = time.Now().UnixNano() / int64(time.Millisecond)
	t.queue = q.New(queueHeight)
	t.frameRate = defaultFrameRate
	t.blocks = make(map[string]*TreeBlock, 0)

	return t
}

func (t *StatsTree) add(b spec.Block) {
	block := &TreeBlock{
		ID:          b.GetID(),
		parentID:    b.GetParentID(),
		BlockNumber: b.GetBlockNumber(),
		Children:    make([]*TreeBlock, 0)}

	t.queue.Put([]interface{}{taskTypeAdd, block})
}

func (t *StatsTree) disqualify(b spec.Block) {
	t.queue.Put([]interface{}{taskTypeDisqualify, b.GetID()})
}

func (t *StatsTree) remove(b spec.Block) {
	t.queue.Put([]interface{}{taskTypeRemove, b.GetID()})
}

type newTreeProcess struct {
	roots  []*TreeBlock
	blocks map[string]*TreeBlock
}

func (t *StatsTree) setFrameRate(rate float64) {
	t.frameRate = rate
}

func (t *StatsTree) start() {
	if t.started {
		return
	}

	t.started = true

	go t.processLoop()
}

func (t *StatsTree) stop() {
	if t.started {
		t.started = false
	}
}

func (t *StatsTree) processLoop() {
	for t.started {
		// copy the tree
		proc := &newTreeProcess{}
		proc.blocks = make(map[string]*TreeBlock, len(t.blocks))
		proc.roots = make([]*TreeBlock, len(t.Roots))
		for i, root := range t.Roots {
			proc.roots[i] = copyBlock(root, proc)
		}

		frameTime := time.Duration(math.Round(float64(1000000)/t.frameRate)) * time.Microsecond
		ctx, cancel := context.WithTimeout(context.Background(), frameTime)

		updated := t.processQueueItems(ctx, frameTime, proc)

		cancel()

		if updated {
			min := ^uint64(0)
			max := uint64(0)
			for _, r := range proc.roots {
				rootMax := getMaxBlockNumber(r)
				if rootMax > max {
					max = rootMax
				}
				if r.BlockNumber < min {
					min = r.BlockNumber
				}
			}

			t.Lock()
			t.Roots = proc.roots
			t.blocks = proc.blocks
			t.MinBlockNumber = min
			t.MaxBlockNumber = max
			t.UpdateTimestamp = time.Now().UnixNano() / int64(time.Millisecond)
			t.Unlock()

			if t.OnChange != nil {
				go t.OnChange()
			}
		}
	}
}

func (t *StatsTree) processQueueItems(ctx context.Context, frameTime time.Duration, proc *newTreeProcess) bool {
	var updated bool
	for {
		select {
		case <-ctx.Done():
			return updated

		default:
			if t.queue.Empty() {
				time.Sleep(frameTime / 10)
			} else {
				data, err := t.queue.Get(1)
				if err != nil {
					// log err
					continue
				}

				updated = t.processQueueItem(data, proc) || updated
			}
		}
	}
}

func (t *StatsTree) processQueueItem(dataSet []interface{}, proc *newTreeProcess) bool {
	data := dataSet[0].([]interface{})
	taskType := data[0].(int)
	switch taskType {
	case taskTypeAdd:
		block := data[1].(*TreeBlock)
		parent := proc.blocks[block.parentID]
		add(block, parent, proc)
		return true

	case taskTypeDisqualify:
		blockID := data[1].(string)
		block := proc.blocks[blockID]
		if block == nil {
			return false
		}
		disqualify(block)
		return true

	case taskTypeRemove:
		blockID := data[1].(string)
		block := proc.blocks[blockID]
		if block == nil {
			return false
		}
		remove(block, proc)
		return true

	default:
		panic("unknown task type")
	}
}

func add(block *TreeBlock, parent *TreeBlock, proc *newTreeProcess) {
	if parent == nil {
		// no parent, so this is a root
		proc.roots = append(proc.roots, block)
	} else {
		// add to parent's children
		parent.Children = append(parent.Children, block)
	}
	// see if one of the existing roots has new block as parent
	// then attach it as new block's children, and remove from roots
	for i := 0; i < len(proc.roots); i++ {
		root := proc.roots[i]
		if root.parentID == block.ID {
			block.Children = append(block.Children, root)
			proc.roots = append(proc.roots[:i], proc.roots[i+1:]...)
		}
	}
	proc.blocks[block.ID] = block
}

func disqualify(block *TreeBlock) {
	block.IsDisqualified = true
	for _, c := range block.Children {
		disqualify(c)
	}
}

func remove(block *TreeBlock, proc *newTreeProcess) {
	parent := proc.blocks[block.parentID]
	if parent != nil {
		remove(parent, proc)
	}

	// remove from roots and promote children to roots
	for i, r := range proc.roots {
		if r.ID == block.ID {
			proc.roots = append(proc.roots[:i], proc.roots[i+1:]...)
			break
		}
	}
	proc.roots = append(proc.roots, block.Children...)
	// release references
	block.Children = nil

	delete(proc.blocks, block.ID)
}

func getMaxBlockNumber(b *TreeBlock) uint64 {
	max := b.BlockNumber
	for _, c := range b.Children {
		childMax := getMaxBlockNumber(c)
		if childMax > max {
			max = childMax
		}
	}
	return max
}

func copyBlock(b *TreeBlock, proc *newTreeProcess) *TreeBlock {
	newBlock := &TreeBlock{
		ID:             b.ID,
		parentID:       b.parentID,
		IsDisqualified: b.IsDisqualified}

	newBlock.Children = make([]*TreeBlock, len(b.Children))
	for i, c := range b.Children {
		newBlock.Children[i] = copyBlock(c, proc)
	}

	proc.blocks[b.ID] = newBlock
	return newBlock
}
