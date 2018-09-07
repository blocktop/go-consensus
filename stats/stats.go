package stats

import (
	"sync"
	"time"

	spec "github.com/blckit/go-spec"
)

type ConsensusStats struct {
	sync.Mutex

	// Tracks average duration in nanoseconds blocks were in the system
	// by depth they were removed
	AvgDurationAtDepth map[uint64]int64 `json:"avgDurationAtDepth"`

	// Tracks number of blocks exited at each depth
	BlockDepthExitCount map[uint64]uint64 `json:"BlockDepthExitCount"`

	// Tracks the number of blocks entered at each depth
	BlockDepthEnterCount map[uint64]uint64 `json:"BlockDepthEnterCount"`

	// Total blocks that have been processed
	TotalBlocks uint64 `json:"totalBlocks,string"`

	// Blocks being actively tracked including diqualified blocks
	ActiveBlockCount uint64 `json:"activeBlockCount,string`

	// Stats on blocks organized in depth tree
	Tree *StatsTree `json:"tree"`

	// Last update timestamp in millisconds
	UpdateTimestamp int64 `json:"updateTimestamp"` // milliseconds

	// time block entered the system as UnixNano
	timeEntered map[string]int64

	// Maximum block number currently in the system
	maxBlockNumber uint64

	started bool
}

func NewConsensusStats() *ConsensusStats {
	s := &ConsensusStats{}

	s.timeEntered = make(map[string]int64, 0)
	s.BlockDepthEnterCount = make(map[uint64]uint64, 0)
	s.BlockDepthExitCount = make(map[uint64]uint64, 0)
	s.AvgDurationAtDepth = make(map[uint64]int64)
	s.Tree = newStatsTree()
	return s
}

func (s *ConsensusStats) Start() {
	if s.started {
		return
	}
	s.started = true
	s.Tree.start()
}

func (s *ConsensusStats) Stop() {
	if !s.started {
		return
	}
	s.started = false
	s.Tree.stop()
}

func (s *ConsensusStats) AddBlock(b spec.Block) {
	if !s.started {
		return
	}

	blockID := b.GetID()
	blockNumber := b.GetBlockNumber()
	timeEntered := time.Now().UnixNano()
	s.ActiveBlockCount++
	s.TotalBlocks++

	s.Lock()
	if blockNumber > s.maxBlockNumber {
		s.maxBlockNumber = blockNumber
	}
	depth := s.maxBlockNumber - blockNumber
	s.timeEntered[blockID] = timeEntered
	s.UpdateTimestamp = timeEntered / int64(time.Millisecond)
	s.BlockDepthEnterCount[depth]++
	s.Unlock()

	s.Tree.add(b)
}

func (s *ConsensusStats) DisqualifyBlock(b spec.Block) {
	if !s.started {
		return
	}

	blockID := b.GetID()
	blockNumber := b.GetBlockNumber()
	timeExited := time.Now().UnixNano()

	s.Lock()
	duration := timeExited - s.timeEntered[blockID]
	s.UpdateTimestamp = timeExited / int64(time.Millisecond)
	depth := s.maxBlockNumber - blockNumber
	sumDuration := s.AvgDurationAtDepth[depth]*int64(s.BlockDepthExitCount[depth]) + duration
	s.BlockDepthExitCount[depth]++
	s.AvgDurationAtDepth[depth] = sumDuration / int64(s.BlockDepthExitCount[depth])
	delete(s.timeEntered, blockID)
	s.Unlock()

	s.Tree.disqualify(b)
}

func (s *ConsensusStats) RemoveBlock(b spec.Block) {
	if !s.started {
		return
	}
	// TODO more are being removed than added??
	s.ActiveBlockCount--
	s.Tree.remove(b)
}
