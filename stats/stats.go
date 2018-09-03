package stats

import (
	"sync"
	"time"

	spec "github.com/blckit/go-interface"
)

type ConsensusStats struct {
	sync.Mutex

	// Tracks average duration in nanoseconds blocks were in the system
	// by depth they were removed
	AvgDurationAtDepth map[uint]uint64 `json:"avgDurationAtDepth"`

	// Tracks number of blocks exited at each depth
	BlockExitCount map[uint]uint64 `json:"blockExitCount"`

	// Tracks the number of blocks entered at each depth
	BlockEnterCount map[uint]uint64 `json:"blockEnterCount"`

	// Total blocks being tracked
	BlockCount uint64 `json:"blockCount,string"`

	// Stats on blocks organized in depth tree
	Tree *StatsTree `json:"tree"`

	// Last update timestamp in UnixNano
	UpdateTimestamp int `json:"updateTimestamp"` // milliseconds

	// time block entered the system as UnixNano
	timeEntered map[string]int64

	// Maximum block number currently in the system
	maxBlockNumber uint64
}

func NewConsensusStats() *ConsensusStats {
	s := &ConsensusStats{}

	s.timeEntered = make(map[string]int64, 0)
	s.BlockEnterCount = make(map[uint]uint64, 0)
	s.BlockExitCount = make(map[uint]uint64, 0)
	s.AvgDurationAtDepth = make(map[uint]uint64)
	s.Tree = newStatsTree()
	return s
}

func (s *ConsensusStats) AddBlock(b spec.Block) {
	blockID := b.GetID()
	blockNumber := b.GetBlockNumber()
	timeEntered := time.Now().UnixNano()
	s.BlockCount++

	s.Lock()
	if blockNumber > s.maxBlockNumber {
		s.maxBlockNumber = blockNumber
	}
	depth := uint(s.maxBlockNumber - blockNumber)
	s.timeEntered[blockID] = timeEntered
	s.UpdateTimestamp = int(timeEntered / int64(time.Millisecond))
	s.BlockEnterCount[depth]++
	s.Unlock()

	s.Tree.add(b)
}

func (s *ConsensusStats) EliminateBlock(b spec.Block) {
	blockID := b.GetID()
	blockNumber := b.GetBlockNumber()
	timeExited := time.Now().UnixNano()
	s.BlockCount--

	s.Lock()
	duration := uint64(timeExited - s.timeEntered[blockID])
	s.UpdateTimestamp = int(timeExited / int64(time.Millisecond))
	depth := uint(s.maxBlockNumber - blockNumber)
	sumDuration := s.AvgDurationAtDepth[depth]*s.BlockExitCount[depth] + duration
	s.BlockExitCount[depth]++
	s.AvgDurationAtDepth[depth] = sumDuration / s.BlockExitCount[depth]
	delete(s.timeEntered, blockID)
	s.Unlock()

	s.Tree.eliminate(b)
}

func (s *ConsensusStats) RemoveBlock(b spec.Block) {
	s.Tree.remove(b)
}
