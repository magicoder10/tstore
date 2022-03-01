package mutation

import (
	"tstore/data"
)

type TransactionInput struct {
	Mutations map[string][]data.Mutation `json:"mutations"` // key: schema name, value: mutation
}
