package mutation

import (
	"tstore/data"
)

type Transaction struct {
	ID        uint64                     `json:"id"`
	Mutations map[string][]data.Mutation `json:"mutations"` // key: schema name, value: mutation
}
