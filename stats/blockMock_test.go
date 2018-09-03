package stats

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
