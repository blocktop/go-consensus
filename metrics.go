package consensus

import (
	"fmt"
	"sync"
	"time"

	spec "github.com/blckit/go-spec"
	mtr "github.com/rcrowley/go-metrics"
)

type metricItems struct {
	DurationAtDepth  map[uint64]mtr.Histogram
	BlockDepthExit   mtr.Histogram
	BlockDepthEntry  mtr.Histogram
	TotalBlocks      mtr.Counter
	ActiveBlocks     mtr.Counter
	UpdatedTimestamp int64
	MaxBlockNumber   uint64
	statBlocks       map[string]*statBlock
	registry         mtr.Registry
	sync.Mutex
}

type statBlock struct {
	block       spec.Block
	timeEntered int64
	disqualifed bool
}

var metrics *metricItems = &metricItems{}

func init() {
	m := metrics

	registry := mtr.NewPrefixedRegistry("consensus - ")
	m.registry = registry

	m.DurationAtDepth = make(map[uint64]mtr.Histogram, 0)
	m.BlockDepthExit = mtr.GetOrRegisterHistogram("block depth at exit", registry, mtr.NewUniformSample(500))
	m.BlockDepthEntry = mtr.GetOrRegisterHistogram("block depth at entry", registry, mtr.NewUniformSample(500))
	m.TotalBlocks = mtr.GetOrRegisterCounter("total blocks", registry)
	m.ActiveBlocks = mtr.GetOrRegisterCounter("active blocks", registry)

	m.statBlocks = make(map[string]*statBlock)

	m.updateTimestamp()
}

func (m *metricItems) updateTimestamp() {
	m.UpdatedTimestamp = time.Now().UnixNano() / int64(time.Millisecond)
}

func (m *metricItems) AddBlock(b spec.Block) {
	blockID := b.GetID()
	blockNumber := b.GetBlockNumber()
	if blockNumber > m.MaxBlockNumber {
		m.MaxBlockNumber = blockNumber
	}
	depth := int64(m.MaxBlockNumber - blockNumber)

	m.Lock()
	defer m.Unlock()

	if m.statBlocks[blockID] != nil {
		return
	}

	m.statBlocks[blockID] = &statBlock{
		block:       b,
		timeEntered: time.Now().UnixNano(),
		disqualifed: false}

	m.ActiveBlocks.Inc(1)
	m.TotalBlocks.Inc(1)

	m.BlockDepthEntry.Update(depth)

	m.updateTimestamp()

	tree.add(b)
}

func (m *metricItems) DisqualifyBlock(b spec.Block) {
	blockID := b.GetID()
	blockNumber := b.GetBlockNumber()
	depth := m.MaxBlockNumber - blockNumber

	m.Lock()
	defer m.Unlock()

	statB := m.statBlocks[blockID]
	if statB == nil || statB.disqualifed {
		return
	}

	duration := time.Now().UnixNano() - statB.timeEntered
	m.blockExit(depth, duration)

	m.updateTimestamp()

	tree.disqualify(b)
}

func (m *metricItems) RemoveBlock(b spec.Block) {
	blockID := b.GetID()

	m.Lock()
	defer m.Unlock()

	statB := m.statBlocks[blockID]
	if statB == nil {
		return
	}

	m.ActiveBlocks.Dec(1)

	if !statB.disqualifed {
		duration := time.Now().UnixNano() - statB.timeEntered
		depth := m.MaxBlockNumber - b.GetBlockNumber()
		m.blockExit(depth, duration)
	}

	delete(m.statBlocks, blockID)

	tree.remove(b)
}

func (m *metricItems) blockExit(depth uint64, duration int64) {
	m.BlockDepthExit.Update(int64(depth))

	hist := mtr.GetOrRegisterHistogram(fmt.Sprintf("duration to depth %d", depth), m.registry, mtr.NewUniformSample(500))
	hist.Update(duration / int64(time.Millisecond))
	m.DurationAtDepth[depth] = hist
}
