package mutation

type Type string

const (
	CreateSchemaMutation   Type = "createSchema"
	DeleteSchemaMutation   Type = "deleteSchema"
	CreateSchemaAttributes Type = "createSchemaAttributes"
	DeleteSchemaAttributes Type = "deleteSchemaAttributes"

	CreateEntityMutation   Type = "createEntity"
	DeleteEntityMutation   Type = "deleteEntity"
	CreateEntityAttributes Type = "createEntityAttributes"
	DeleteEntityAttributes Type = "deleteEntityAttributes"
	UpdateEntityAttributes Type = "updateEntityAttributes"
)

type Mutation struct {
	Type        Type        `json:"type"`
	SchemaInput SchemaInput `json:"schema_input"`
	EntityInput EntityInput `json:"entity_input"`
}
