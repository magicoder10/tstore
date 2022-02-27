package mutation

type Transaction struct {
	ID        uint64                `json:"id"`
	Mutations map[string][]Mutation `json:"mutations"` // key: schema name, value: mutation
}
