package stats

import (
	"context"
	"math"
	"sync"
	"time"

	spec "github.com/blckit/go-interface"
	q "github.com/golang-collections/go-datastructures/queue"
)

type StatsTree struct {
	sync.Mutex
	Roots           []*Block `json:"roots"`
	MinBlockNumber  uint64   `json:"minBlockNumber,string"`
	MaxBlockNumber  uint64   `json:"maxBlockNumber,string"`
	UpdateTimestamp int      `json:"updateTimestamp"` //millisecond
	queue           *q.Queue
	started         bool
	frameRate       float64
	blocks          map[string]*Block
	OnFrameReady    func() `json:"-"`
}

type Block struct {
	ID           string   `json:"id"`
	Children     []*Block `json:"children"`
	BlockNumber  uint64   `json:"blockNumber,string"`
	IsEliminated bool     `json:"isEliminated"`
	parentID     string
}

const (
	queueHeight      int64   = 100000
	defaultFrameRate float64 = 30.0
	taskTypeAdd      int     = iota
	taskTypeEliminate
	taskTypeRemove
)

func newStatsTree() *StatsTree {
	t := &StatsTree{}
	t.Roots = make([]*Block, 0)
	t.UpdateTimestamp = int(time.Now().UnixNano() / 1000000)
	t.queue = q.New(queueHeight)
	t.frameRate = defaultFrameRate
	t.blocks = make(map[string]*Block, 0)

	return t
}

func (t *StatsTree) add(b spec.Block) {
	block := &Block{
		ID:          b.GetID(),
		parentID:    b.GetParentID(),
		BlockNumber: b.GetBlockNumber(),
		Children:    make([]*Block, 0)}

	t.queue.Put([]interface{}{taskTypeAdd, block})
}

func (t *StatsTree) eliminate(b spec.Block) {
	t.queue.Put([]interface{}{taskTypeEliminate, b.GetID()})
}

func (t *StatsTree) remove(b spec.Block) {
	t.queue.Put([]interface{}{taskTypeRemove, b.GetID()})
}

type newTreeProcess struct {
	roots  []*Block
	blocks map[string]*Block
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
		proc.blocks = make(map[string]*Block, len(t.blocks))
		proc.roots = make([]*Block, len(t.Roots))
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
			t.UpdateTimestamp = int(time.Now().UnixNano() / 1000000) // millisecond
			t.Unlock()

			if t.OnFrameReady != nil {
				go t.OnFrameReady()
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
		block := data[1].(*Block)
		parent := proc.blocks[block.parentID]
		add(block, parent, proc)
		return true

	case taskTypeEliminate:
		blockID := data[1].(string)
		block := proc.blocks[blockID]
		if block == nil {
			return false
		}
		eliminate(block)
		return true

	case taskTypeRemove:
		blockID := data[1].(string)
		block := proc.blocks[blockID]
		if block == nil {
			return false
		}
		remove(block, proc)
		parent := proc.blocks[block.parentID]
		if parent != nil {
			for i, c := range parent.Children {
				if c.ID == blockID {
					parent.Children = append(parent.Children[:i], parent.Children[i+1:]...)
					break
				}
			}
		}
		return true

	default:
		panic("unknown task type")
	}
}

func add(block *Block, parent *Block, proc *newTreeProcess) {
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

func eliminate(block *Block) {
	block.IsEliminated = true
	for _, c := range block.Children {
		eliminate(c)
	}
}

func remove(block *Block, proc *newTreeProcess) {
	for _, c := range block.Children {
		remove(c, proc)
	}
	delete(proc.blocks, block.ID)
	block.Children = nil
}

func getMaxBlockNumber(b *Block) uint64 {
	max := b.BlockNumber
	for _, c := range b.Children {
		childMax := getMaxBlockNumber(c)
		if childMax > max {
			max = childMax
		}
	}
	return max
}

func copyBlock(b *Block, proc *newTreeProcess) *Block {
	newBlock := &Block{
		ID:           b.ID,
		parentID:     b.parentID,
		IsEliminated: b.IsEliminated}

	newBlock.Children = make([]*Block, len(b.Children))
	for i, c := range b.Children {
		newBlock.Children[i] = copyBlock(c, proc)
	}

	proc.blocks[b.ID] = newBlock
	return newBlock
}
