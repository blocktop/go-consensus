package mock

type Transaction struct {
	id string
}

func NewTransaction(id string) Transaction {
	return Transaction{id: id}
}

func (t Transaction) GetID() string {
	return t.id
}