package mock

import (
	"time"

	spec "github.com/blckit/go-interface"
)

type Block struct {
	id, parentID           string
	blockNumber, timestamp uint64
}

func NewBlock(id, parentID string, blockNumber uint64) Block {
	timestamp := uint64(time.Now().UnixNano()) / uint64(time.Millisecond)
	return Block{id: id, parentID: parentID, blockNumber: blockNumber, timestamp: timestamp}
}

func (b Block) GetID() string {
	return b.id
}
func (b Block) GetParentID() string {
	return b.parentID
}
func (b Block) GetBlockNumber() uint64 {
	return b.blockNumber
}
func (b Block) Validate() bool {
	return true
}
func (b Block) GetTransactions() []spec.Transaction {
	return []spec.Transaction{}
}
func (b Block) GetTimestamp() uint64 {
	return b.timestamp
}
