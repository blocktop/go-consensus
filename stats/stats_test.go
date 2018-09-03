package stats

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/blckit/go-consensus/mock"
)

var _ = Describe("Stats", func() {

	Describe("#AddBlock", func() {

		It("adds block and updates stats", func() {
			s := NewConsensusStats()
			b := mock.NewBlock("111", "000", uint64(1))

			s.AddBlock(b)
			Expect(s.timeEntered["111"] > 0).To(BeTrue())
			Expect(s.BlockCount).To(Equal(uint64(1)))
			Expect(s.BlockEnterCount[0]).To(Equal(uint64(1)))
		})
	})

	Describe("#EliminateBlock", func() {

		It("eliminates block and updates stats", func() {
			s := NewConsensusStats()
			b1a := mock.NewBlock("111a", "000", uint64(1))
			b1b := mock.NewBlock("111b", "000", uint64(1))
			b2 := mock.NewBlock("222", "111a", uint64(2))

			s.AddBlock(b1a)
			s.AddBlock(b1b)
			s.AddBlock(b2)

			time.Sleep(10 * time.Millisecond)
			s.EliminateBlock(b1a)
			Expect(s.BlockExitCount[1]).To(Equal(uint64(1)))
			//fmt.Printf("avg duration: %d\n", s.AvgDurationAtDepth[1])
			Expect(s.AvgDurationAtDepth[1] > uint64(10*time.Millisecond)).To(BeTrue())
			Expect(s.AvgDurationAtDepth[1] < uint64(15*time.Millisecond)).To(BeTrue())
			Expect(s.BlockCount).To(Equal(uint64(2)))
			Expect(s.timeEntered["111a"]).To(Equal(int64(0)))

			time.Sleep(10 * time.Millisecond)
			s.EliminateBlock(b1b)
			Expect(s.BlockExitCount[1]).To(Equal(uint64(2)))
			Expect(s.AvgDurationAtDepth[1] > uint64(15*time.Millisecond)).To(BeTrue())
			//fmt.Printf("avg duration: %d\n", s.AvgDurationAtDepth[1])
			Expect(s.AvgDurationAtDepth[1] < uint64(20*time.Millisecond)).To(BeTrue())
			Expect(s.BlockCount).To(Equal(uint64(1)))
			Expect(s.timeEntered["111b"]).To(Equal(int64(0)))
		})
	})
})

func BenchmarkAddBlock(b *testing.B) {
	s := NewConsensusStats()
	blockNumber := uint64(0)
	var blockID string
	for n := 0; n < b.N; n++ {
		prevBlockID := blockID
		blockID := "b" + strconv.FormatUint(blockNumber, 10)
		b := mock.NewBlock(blockID, prevBlockID, blockNumber)
		s.AddBlock(b)
		blockNumber++
	}
}

func BenchmarkEliminateBlock(b *testing.B) {
	s := NewConsensusStats()
	blockNumber := uint64(0)
	blocks := make(map[string]mock.Block, 0)
	var blockID string
	for n := 0; n < b.N+100; n++ {
		prevBlockID := blockID
		blockID := "b" + strconv.FormatUint(blockNumber, 10)
		b := mock.NewBlock(blockID, prevBlockID, blockNumber)
		s.AddBlock(b)
		blockNumber++
	}
	b.ResetTimer()
	for n := b.N; n >= 0; n-- {
		blockID := "b" + strconv.FormatInt(int64(n), 10)
		s.EliminateBlock(blocks[blockID])
	}

	sb, _ := json.MarshalIndent(s, "", "  ")
	fmt.Printf("%v\n", string(sb))
}
