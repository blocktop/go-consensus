package consensus

import (
	"context"
	"math/rand"
	"strconv"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	depthcons "github.com/blckit/go-consensus-depth"
)

type BlockMock struct {
	id, parentID string
	blockNumber  uint64
}

func (b BlockMock) GetID() string {
	return b.id
}
func (b BlockMock) GetParentID() string {
	return b.parentID
}
func (b BlockMock) GetBlockNumber() uint64 {
	return b.blockNumber
}


var _ = Describe("Consensus", func() {

	Describe("#NewConsensus", func() {

		It("creates instance with all args specified", func() {
			var c *Consensus = GetInst()
			Expect(c.ConsensusSpec).ToNot(BeNil())
			Expect(c.CompetitionSpec).ToNot(BeNil())
			Expect(len(c.blocks)).To(Equal(0))
			Expect(len(c.heads)).To(Equal(0))
			Expect(len(c.alreadySeen)).To(Equal(0))
			Expect(len(c.competed)).To(Equal(0))
		})

		It("panics when args are nil", func() {
			f := func() {
				NewConsensus(nil, &depthcons.DepthCompetiton{})
			}

			Expect(f).To(Panic())

			f = func() {
				NewConsensus(&depthcons.DepthConsensus{}, nil)
			}

			Expect(f).To(Panic())
		})
	})

	Describe("#WasSeen", func() {

		It("returns false if parent does not exist", func() {
			c := GetInst()
			b := BlockMock{id: "abc", parentID: "parent123"}
			Expect(c.WasSeen(b)).To(BeFalse())
		})

		It("returns true if block is in seen array", func() {
			c := GetInst()
			b := BlockMock{id: "abc", parentID: "parent123"}
			c.alreadySeen["parent123"] = []string{"abc"}

			Expect(c.WasSeen(b)).To(BeTrue())
		})
	})

	Describe("#SetCompeted", func() {

		It("does nothing if block is not being tracked", func() {
			c := GetInst()
			c.SetCompeted(BlockMock{id: "parent123"})

			Expect(len(c.competed)).To(Equal(0))
		})

		It("adds block ID to array", func() {
			c := GetInst()
			b := BlockMock{id: "parent123", parentID: "parent456"}
			c.blocks["parent123"] = b
			c.SetCompeted(BlockMock{id: "parent123"})

			Expect(c.competed[0]).To(Equal("parent123"))
		})
	})

	Describe("#AddBlock", func() {

		It("adds genesis block", func() {
			c := GetInst()
			b := BlockMock{id: "genesis", parentID: "222", blockNumber: 0}
			ok := c.AddBlock(b)

			Expect(ok).To(BeTrue())
			Expect(c.WasSeen(b)).To(BeTrue())
			Expect(c.heads).To(ContainElement("genesis"))
			Expect(c.blocks["genesis"]).ToNot(BeNil())
		})

		It("does not add same block twice", func() {
			c := GetInst()
			b := BlockMock{id: "genesis", parentID: "222", blockNumber: 0}
			ok := c.AddBlock(b)
			Expect(ok).To(BeTrue())

			ok = c.AddBlock(b)
			Expect(ok).To(BeFalse())
		})

		It("removes parent as head and sets new block as head", func() {
			c := GetInst()
			b1 := BlockMock{id: "genesis", parentID: "", blockNumber: 0}
			b2 := BlockMock{id: "111", parentID: "genesis", blockNumber: 1}

			c.AddBlock(b1)
			Expect(c.heads).To(ContainElement("genesis"))

			c.AddBlock(b2)
			Expect(c.heads).To(ContainElement("111"))
			Expect(len(c.heads)).To(Equal(1))
		})

		It("removes unfavorable siblings", func() {
			c := GetInst()
			b1 := BlockMock{id: "genesis", parentID: "", blockNumber: 0}
			b2a := BlockMock{id: "111a", parentID: "genesis", blockNumber: 1}
			c.AddBlock(b1)
			c.AddBlock(b2a)

			b3b := BlockMock{id: "222b", parentID: "111b", blockNumber: 2}
			b4b := BlockMock{id: "333b", parentID: "222b", blockNumber: 3}
			b5b := BlockMock{id: "444b", parentID: "333b", blockNumber: 4}
			c.AddBlock(b3b)
			c.AddBlock(b4b)
			c.AddBlock(b5b)

			b2b := BlockMock{id: "111b", parentID: "genesis", blockNumber: 1}
			c.AddBlock(b2b)

			Expect(c.blocks["111a"]).To(BeNil())
			Expect(c.blocks["111b"]).ToNot(BeNil())
			Expect(c.blocks["222b"]).ToNot(BeNil())
			Expect(c.blocks["333b"]).ToNot(BeNil())
			Expect(c.blocks["444b"]).ToNot(BeNil())
			Expect(c.blocks["genesis"]).ToNot(BeNil())
			Expect(len(c.heads)).To(Equal(1))
			Expect(c.heads).To(ContainElement("444b"))
		})

		It("does not add block if sibling is more favorable", func() {
			c := GetInst()
			b1 := BlockMock{id: "genesis", parentID: "", blockNumber: 0}
			b2a := BlockMock{id: "111a", parentID: "genesis", blockNumber: 1}
			b3a := BlockMock{id: "222a", parentID: "111a", blockNumber: 2}
			b4a := BlockMock{id: "333a", parentID: "222a", blockNumber: 3}
			b5a := BlockMock{id: "444a", parentID: "333a", blockNumber: 4}
			b2b := BlockMock{id: "111b", parentID: "genesis", blockNumber: 1}

			c.AddBlock(b1)
			c.AddBlock(b2a)
			c.AddBlock(b3a)
			c.AddBlock(b4a)
			c.AddBlock(b5a)
			c.AddBlock(b2b)

			Expect(c.blocks["111a"]).ToNot(BeNil())
			Expect(c.blocks["222a"]).ToNot(BeNil())
			Expect(c.blocks["333a"]).ToNot(BeNil())
			Expect(c.blocks["444a"]).ToNot(BeNil())
			Expect(c.blocks["111b"]).To(BeNil())
			Expect(c.blocks["genesis"]).ToNot(BeNil())
			Expect(len(c.heads)).To(Equal(1))
			Expect(c.heads).To(ContainElement("444a"))
		})

		It("keeps most favorable child branch", func() {
			c := GetInst()
			b1a := BlockMock{id: "111a", parentID: "zzz", blockNumber: 1}
			c.AddBlock(b1a)

			b1b := BlockMock{id: "111b", parentID: "zzz", blockNumber: 1}
			b2b := BlockMock{id: "222b", parentID: "111b", blockNumber: 2}
			b3b := BlockMock{id: "333b", parentID: "222b", blockNumber: 3}
			b4b := BlockMock{id: "444b", parentID: "333b", blockNumber: 4}
			c.AddBlock(b1b)
			c.AddBlock(b2b)
			c.AddBlock(b3b)
			c.AddBlock(b4b)

			Expect(len(c.heads)).To(Equal(2))
			Expect(c.heads).To(ContainElement("111a"))
			Expect(c.heads).To(ContainElement("444b"))

			b0 := BlockMock{id: "zzz", parentID: "not_used", blockNumber: 0}
			c.AddBlock(b0)

			Expect(len(c.blocks)).To(Equal(5))
			Expect(c.blocks["zzz"]).ToNot(BeNil())
			Expect(c.blocks["111b"]).ToNot(BeNil())
			Expect(c.blocks["222b"]).ToNot(BeNil())
			Expect(c.blocks["333b"]).ToNot(BeNil())
			Expect(c.blocks["444b"]).ToNot(BeNil())
			Expect(len(c.heads)).To(Equal(1))
			Expect(c.heads).To(ContainElement("444b"))
		})
	})

	Describe("#GetBestBranch", func() {

		It("returns nil if no heads", func() {
			c := GetInst()
			Expect(c.GetBestBranch()).To(BeNil())
		})

		It("returns if head already competed", func() {
			c := GetInst()
			b1 := BlockMock{id: "111", parentID: "zzz", blockNumber: 0}
			b2 := BlockMock{id: "222", parentID: "111", blockNumber: 1}
			c.AddBlock(b1)
			c.AddBlock(b2)
			Expect(c.heads).To(ContainElement("222"))

			c.SetCompeted(b2)
			Expect(c.GetBestBranch()).To(BeNil())
		})

		It("returns genesis", func() {
			c := GetInst()
			b1 := BlockMock{id: "111", parentID: "zzz", blockNumber: 0}
			c.AddBlock(b1)

			head := c.GetBestBranch()
			Expect(head[0].GetID()).To(Equal("111"))
		})

		It("returns when best head has no parent", func() {
			c := GetInst()
			b1 := BlockMock{id: "111", parentID: "zzz", blockNumber: 1}
			c.AddBlock(b1)

			Expect(c.GetBestBranch()).To(BeNil())
		})

		It("chooses most favorable when heads are same block number", func() {
			c := GetInst()
			b1a := BlockMock{id: "111a", parentID: "zzz", blockNumber: 5}
			b2a := BlockMock{id: "222a", parentID: "111a", blockNumber: 6}
			c.AddBlock(b1a)
			c.AddBlock(b2a)

			b1b := BlockMock{id: "111b", parentID: "zzz", blockNumber: 5}
			b2b := BlockMock{id: "222b", parentID: "111b", blockNumber: 6}
			c.AddBlock(b1b)
			c.AddBlock(b2b)

			head := c.GetBestBranch()
			Expect(head[0].GetID()).To(Equal("222b"))
			Expect(head[len(head)-1].GetID()).To(Equal("111b"))
		})

		It("chooses most favorable", func() {
			c := GetInst()
			b1a := BlockMock{id: "111a", parentID: "zzz", blockNumber: 3}
			b2a := BlockMock{id: "222a", parentID: "111a", blockNumber: 4}
			c.AddBlock(b1a)
			c.AddBlock(b2a)

			b1b := BlockMock{id: "111b", parentID: "zzz", blockNumber: 5}
			b2b := BlockMock{id: "222b", parentID: "111b", blockNumber: 6}
			b3b := BlockMock{id: "333b", parentID: "222b", blockNumber: 7}
			c.AddBlock(b1b)
			c.AddBlock(b2b)
			c.AddBlock(b3b)

			head := c.GetBestBranch()
			Expect(len(head)).To(Equal(3))
			Expect(head[0].GetID()).To(Equal("333b"))
			Expect(head[1].GetID()).To(Equal("222b"))
			Expect(head[2].GetID()).To(Equal("111b"))
		})

		It("chooses random favorable when more than one", func() {
			c := GetInst()
			b1a := BlockMock{id: "111a", parentID: "zzz", blockNumber: 4}
			b2a := BlockMock{id: "222a", parentID: "111a", blockNumber: 5}
			c.AddBlock(b1a)
			c.AddBlock(b2a)

			b1b := BlockMock{id: "111b", parentID: "zzz", blockNumber: 5}
			b2b := BlockMock{id: "222b", parentID: "111b", blockNumber: 6}
			b3b := BlockMock{id: "333b", parentID: "222b", blockNumber: 7}
			c.AddBlock(b1b)
			c.AddBlock(b2b)
			c.AddBlock(b3b)

			// there will be two favorable heads, one of which will be chosen at random
			// loop until the function under test chooses each, timeout and fail in 100ms
			var fav1, fav2 bool

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			for !fav1 || !fav2 {
				select {
				case <-ctx.Done():
					Fail("Did not randomly select both favorables in reasonable time")
					break

				default:
					head := c.GetBestBranch()
					headID := head[0].GetID()
					if headID == "222a" {
						fav1 = true
					}
					if headID == "333b" {
						fav2 = true
					}
				}
			}
		})
	})
})

func BenchmarkAddBlock(b *testing.B) {
	c := GetInst()
	blockNumber := uint64(0)
	parentBlockID := ""
	for n := 0; n < b.N; n++ {
		blockID := "b" + strconv.FormatUint(blockNumber, 10)
		block := BlockMock{id: blockID, parentID: parentBlockID, blockNumber: blockNumber}
		added := c.AddBlock(block)
		if !added {
			b.Fail()
		} else {
			blockNumber++
			parentBlockID = blockID
		}
	}
}

func BenchmarkAddBlockWithBranching(b *testing.B) {
	c := GetInst()
	blockNumber := uint64(0)
	blockIDi := blockNumber
	parentBlockID := ""
	for n := 0; n < b.N; n++ {
		blockID := "b" + strconv.FormatUint(blockIDi, 10)
		block := BlockMock{id: blockID, parentID: parentBlockID, blockNumber: blockNumber}
		added := c.AddBlock(block)
		if !added {
			b.Fail()
		} else if rand.Intn(4) > 0 {  // 3/4 of time continue the branch, 1/4 of time create a sibling
			blockNumber++
			parentBlockID = blockID
		}
		blockIDi++
	}
}

func GetInst() *Consensus {
	return NewConsensus(&depthcons.DepthConsensus{}, &depthcons.DepthCompetiton{})
}
