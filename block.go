package consensus

import spec "github.com/blocktop/go-spec"

type block struct {
	block       spec.Block
	blockID     string
	parentID    string
	root        *root
	parent      *block
	children    []*block
	blockNumber uint64
}

func (b1 *block) equal(b2 *block) bool {
	if b1 == nil || b2 == nil {
		return false
	}
	return b1.blockID == b2.blockID
}

func (b *block) hasChild() bool {
	if b == nil || b.children == nil {
		return false
	}
	return len(b.children) > 0
}

func (b *block) setRoot(croot *root) {
	// cleanup up ciruclar reference if any
	for _, cchild := range b.children {
		cchild.setRoot(croot)
	}

	if b.equal(croot.cblock) {
		oldRoot := b.root
		b.root = croot
		oldRoot.cblock = nil
	} else {
		b.root = nil
	}
}

func (b *block) branchHead() *block {
	maxNum := b.blockNumber
	maxBlock := b

	if b.children == nil {
		return b
	}

	for _, cchild := range b.children {
		maxChild := cchild.branchHead()
		maxChildNum := maxChild.blockNumber
		if maxChildNum > maxNum {
			maxNum = cchild.blockNumber
			maxBlock = maxChild
		}
	}
	return maxBlock
}
