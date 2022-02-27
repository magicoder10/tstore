package mutation

import (
	"tstore/data"
)

type TransactionInput struct {
	Mutations map[string][]Mutation `json:"mutations"` // key: schema name, value: mutation
}

type EntityInput struct {
	EntityID                   uint64                 `json:"entity_id"`
	SchemaName                 string                 `json:"schema_name"`
	AttributesToCreateOrUpdate map[string]interface{} `json:"attributes_to_create_or_update"`
	AttributesToDelete         []string               `json:"attributes_to_delete"`
}

type SchemaInput struct {
	Name                       string               `json:"name"`
	AttributesToCreateOrUpdate map[string]data.Type `json:"attributes_to_create_or_update"`
	AttributesToDelete         []string             `json:"attributes_to_delete"`
}
