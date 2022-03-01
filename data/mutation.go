package data

type MutationType string

const (
	CreateSchemaMutation           MutationType = "createSchema"
	DeleteSchemaMutation           MutationType = "deleteSchema"
	CreateSchemaAttributesMutation MutationType = "createSchemaAttributes"
	DeleteSchemaAttributesMutation MutationType = "deleteSchemaAttributes"

	CreateEntityMutation           MutationType = "createEntity"
	DeleteEntityMutation           MutationType = "deleteEntity"
	CreateEntityAttributesMutation MutationType = "createEntityAttributes"
	DeleteEntityAttributesMutation MutationType = "deleteEntityAttributes"
	UpdateEntityAttributesMutation MutationType = "updateEntityAttributes"
)

type Mutation struct {
	Type        MutationType `json:"type"`
	SchemaInput SchemaInput  `json:"schema_input"`
	EntityInput EntityInput  `json:"entity_input"`
}
