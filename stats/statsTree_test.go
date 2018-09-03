package stats

import (
	"math"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("StatsTree", func() {

	Describe("#newStatsTree", func() {

		It("creates instance and populates fields", func() {
			t := newStatsTree()
			Expect(t.UpdateTimestamp).ToNot(Equal(0))
			Expect(t.queue).ToNot(BeNil())
			Expect(len(t.Roots)).To(Equal(0))
			Expect(t.frameRate).To(Equal(defaultFrameRate))
		})
	})

	Describe("#add", func() {

		It("adds blocks to tree", func(done Done) {
			t, _ := buildTree()

			t.OnFrameReady = func() {
				defer GinkgoRecover()
				t.stop()

				Expect(t.blocks["111"]).ToNot(BeNil())
				Expect(t.blocks["222"]).ToNot(BeNil())
				Expect(t.blocks["331"]).ToNot(BeNil())
				Expect(t.blocks["332"]).ToNot(BeNil())
				Expect(t.blocks["333"]).ToNot(BeNil())

				Expect(len(t.Roots)).To(Equal(1))
				root := t.Roots[0]
				Expect(root.ID).To(Equal("111"))
				Expect(root.BlockNumber).To(Equal(uint64(5)))
				Expect(root.parentID).To(Equal("000"))
				Expect(len(root.Children)).To(Equal(1))
				Expect(root.Children[0].ID).To(Equal("222"))
				children := t.blocks["222"].Children
				Expect(len(children)).To(Equal(3))
				ids := ""
				for _, c := range children {
					ids += c.ID
				}
				Expect(len(ids)).To(Equal(9))
				Expect(strings.Contains(ids, "331")).To(BeTrue())
				Expect(strings.Contains(ids, "332")).To(BeTrue())
				Expect(strings.Contains(ids, "333")).To(BeTrue())

				close(done)
			}

			t.start()
		})

		It("adds new root to tree", func(done Done) {
			t, _ := buildTree()
			b0 := BlockMock{id: "000", parentID: "not_used", blockNumber: 4}
			t.add(b0)

			t.OnFrameReady = func() {
				defer GinkgoRecover()
				t.stop()

				Expect(t.blocks["000"]).ToNot(BeNil())
				Expect(len(t.Roots)).To(Equal(1))
				root := t.Roots[0]
				Expect(root.ID).To(Equal("000"))
				Expect(len(root.Children)).To(Equal(1))
				child := root.Children[0]
				Expect(child.ID).To(Equal("111"))

				close(done)
			}

			t.start()
		})
	})

	Describe("#eliminate", func() {

		It("marks branch as eliminated", func(done Done) {
			t, blocks := buildTree()

			t.eliminate(blocks["222"])

			t.OnFrameReady = func() {
				defer GinkgoRecover()
				t.stop()

				Expect(t.blocks["222"].IsEliminated).To(BeTrue())
				Expect(t.blocks["331"].IsEliminated).To(BeTrue())
				Expect(t.blocks["332"].IsEliminated).To(BeTrue())
				Expect(t.blocks["333"].IsEliminated).To(BeTrue())

				close(done)
			}

			t.start()
		})
	})

	Describe("#remove", func() {

		It("removes branch", func(done Done) {
			t, blocks := buildTree()

			t.remove(blocks["222"])

			t.OnFrameReady = func() {
				defer GinkgoRecover()
				t.stop()

				Expect(t.blocks["222"]).To(BeNil())
				Expect(t.blocks["331"]).To(BeNil())
				Expect(t.blocks["332"]).To(BeNil())
				Expect(t.blocks["333"]).To(BeNil())

				Expect(len(t.blocks["111"].Children)).To(Equal(0))

				close(done)
			}

			t.start()
		})
	})

	Describe("frame rate", func() {

		It("runs frames at the specified rate", func(done Done) {
			t, _ := buildTree()

			var frameStart int64
			acceptableFrameTime := time.Duration(math.Round(float64(1000000)/t.frameRate)*float64(1.2)) * time.Microsecond
			var sumFrameTimes time.Duration
			frameCount := 10

			t.OnFrameReady = func() {
				frameEnd := time.Now().UnixNano()
				defer GinkgoRecover()
				frameCount--
				if frameCount == 0 {
					t.stop()

					avgFrameTime := sumFrameTimes / 10
					Expect(avgFrameTime < acceptableFrameTime).To(BeTrue())

					close(done)
					return
				}

				frameTime := time.Duration(frameEnd - frameStart)
				sumFrameTimes += frameTime

				blockID := strconv.FormatInt(int64(frameCount), 10)
				b := BlockMock{id: blockID, parentID: "333", blockNumber: 8}
				t.add(b)

				frameStart = time.Now().UnixNano()
			}

			frameStart = time.Now().UnixNano()
			t.start()
		})

		It("increases frame rate", func(done Done) {
			t, _ := buildTree()

			var frameStart int64
			acceptableFrameTime := time.Duration(math.Round(float64(1000000)/t.frameRate)*float64(1.2)) * time.Microsecond
			frameCount := 20
			var sumFrameTimes time.Duration

			t.OnFrameReady = func() {
				frameEnd := time.Now().UnixNano()
				defer GinkgoRecover()
				frameCount--
				if frameCount == 0 {
					t.stop()

					avgFrameTime := sumFrameTimes / 10
					Expect(avgFrameTime < acceptableFrameTime).To(BeTrue())

					close(done)
					return
				}

				frameTime := time.Duration(frameEnd - frameStart)
				sumFrameTimes += frameTime

				if frameCount == 10 {
					avgFrameTime := sumFrameTimes / 10
					Expect(avgFrameTime < acceptableFrameTime).To(BeTrue())

					t.frameRate *= 2
					acceptableFrameTime = time.Duration(math.Round(float64(1000000)/t.frameRate)*float64(1.2)) * time.Microsecond
					sumFrameTimes = 0
				}

				blockID := strconv.FormatInt(int64(frameCount), 10)
				b := BlockMock{id: blockID, parentID: "333", blockNumber: 8}
				t.add(b)

				frameStart = time.Now().UnixNano()
			}

			frameStart = time.Now().UnixNano()
			t.start()
		})

	})
})

func buildTree() (*StatsTree, map[string]BlockMock) {
	t := newStatsTree()
	b1 := BlockMock{id: "111", parentID: "000", blockNumber: 5}
	b2 := BlockMock{id: "222", parentID: "111", blockNumber: 6}
	b3a := BlockMock{id: "331", parentID: "222", blockNumber: 7}
	b3b := BlockMock{id: "332", parentID: "222", blockNumber: 7}
	b3c := BlockMock{id: "333", parentID: "222", blockNumber: 7}
	t.add(b1)
	t.add(b2)
	t.add(b3a)
	t.add(b3b)
	t.add(b3c)

	blocks := map[string]BlockMock{b1.id: b1, b2.id: b2, b3a.id: b3a, b3b.id: b3b, b3c.id: b3c}
	return t, blocks
}
