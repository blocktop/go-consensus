package stats

import (
	"strconv"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/blckit/go-test-support/mock"
)

var _ = Describe("Stats", func() {

	Describe("#AddBlock", func() {

		It("adds block and updates stats", func() {
			s := NewConsensusStats()
			s.Start()
			b := mock.NewBlock("111", "000", uint64(1))

			s.AddBlock(b)
			Expect(s.timeEntered["111"] > 0).To(BeTrue())
			Expect(s.TotalBlocks).To(Equal(uint64(1)))
			Expect(s.BlockDepthEnterCount[0]).To(Equal(uint64(1)))
		})
	})

	Describe("#DisqualifyBlock", func() {

		It("Disqualifies block and updates stats", func() {
			s := NewConsensusStats()
			s.Start()
			b1a := mock.NewBlock("111a", "000", uint64(1))
			b1b := mock.NewBlock("111b", "000", uint64(1))
			b2 := mock.NewBlock("222", "111a", uint64(2))

			s.AddBlock(b1a)
			s.AddBlock(b1b)
			s.AddBlock(b2)

			time.Sleep(10 * time.Millisecond)
			s.DisqualifyBlock(b1a)
			Expect(s.BlockDepthExitCount[1]).To(Equal(uint64(1)))
			//fmt.Printf("avg duration: %d\n", s.AvgDurationAtDepth[1])
			Expect(s.AvgDurationAtDepth[1] > int64(10*time.Millisecond)).To(BeTrue())
			Expect(s.AvgDurationAtDepth[1] < int64(15*time.Millisecond)).To(BeTrue())
			Expect(s.TotalBlocks).To(Equal(uint64(3)))
			Expect(s.timeEntered["111a"]).To(Equal(int64(0)))

			time.Sleep(10 * time.Millisecond)
			s.DisqualifyBlock(b1b)
			Expect(s.BlockDepthExitCount[1]).To(Equal(uint64(2)))
			Expect(s.AvgDurationAtDepth[1] > int64(15*time.Millisecond)).To(BeTrue())
			//fmt.Printf("avg duration: %d\n", s.AvgDurationAtDepth[1])
			Expect(s.AvgDurationAtDepth[1] < int64(20*time.Millisecond)).To(BeTrue())
			Expect(s.TotalBlocks).To(Equal(uint64(3)))
			Expect(s.timeEntered["111b"]).To(Equal(int64(0)))
		})
	})
})

func BenchmarkAddBlock(b *testing.B) {
	s := NewConsensusStats()
	s.Start()

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

func BenchmarkDisqualifyBlock(b *testing.B) {
	s := NewConsensusStats()
	s.Start()

	blockNumber := uint64(0)
	blocks := make(map[string]*mock.Block, 0)
	var blockID string
	for n := 0; n < b.N+100; n++ {
		prevBlockID := blockID
		blockID := "b" + strconv.FormatUint(blockNumber, 10)
		b := mock.NewBlock(blockID, prevBlockID, blockNumber)
		blocks[blockID] = b
		s.AddBlock(b)
		blockNumber++
	}
	b.ResetTimer()
	for n := b.N; n >= 0; n-- {
		blockID := "b" + strconv.FormatInt(int64(n), 10)
		s.DisqualifyBlock(blocks[blockID])
	}
}
