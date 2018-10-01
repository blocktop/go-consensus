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
	"sync"
	"time"

	spec "github.com/blocktop/go-spec"
	mtr "github.com/rcrowley/go-metrics"
)

type metricItems struct {
	BlockAddTime     mtr.Histogram
	BlockConfirmTime mtr.Histogram
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

	m.BlockAddTime = mtr.GetOrRegisterHistogram("block add time", registry, mtr.NewUniformSample(500))
	m.BlockConfirmTime = mtr.GetOrRegisterHistogram("block add time", registry, mtr.NewUniformSample(500))
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
	blockID := b.Hash()
	blockNumber := b.BlockNumber()
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

func (m *metricItems) BlockAddDuration(duration int64) {
	m.Lock()
	defer m.Unlock()
	m.BlockAddTime.Update(duration)
}


func (m *metricItems) DisqualifyBlock(b spec.Block) {
	blockID := b.Hash()

	m.Lock()
	defer m.Unlock()

	statB := m.statBlocks[blockID]
	if statB == nil || statB.disqualifed {
		return
	}

	m.updateTimestamp()

	tree.disqualify(b)
}

func (m *metricItems) RemoveBlock(b spec.Block) {
	blockID := b.Hash()

	m.Lock()
	defer m.Unlock()

	statB := m.statBlocks[blockID]
	if statB == nil {
		return
	}

	m.ActiveBlocks.Dec(1)

	if !statB.disqualifed {
		duration := time.Now().UnixNano() - statB.timeEntered
		depth := m.MaxBlockNumber - b.BlockNumber()
		m.BlockDepthExit.Update(int64(depth))
		m.BlockConfirmTime.Update(duration)
	}

	delete(m.statBlocks, blockID)

	tree.remove(b)
}
