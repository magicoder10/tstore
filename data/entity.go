package data

type Entity struct {
	ID         uint64                 `json:"id"`
	SchemaName string                 `json:"schema_name"`
	Attributes map[string]interface{} `json:"attributes"` // key: attribute ID, value: attribute value
}
