package data

type Schema struct {
	Name       string          `json:"name"`
	Attributes map[string]Type `json:"attributes"` // key: attribute name, value: attribute type name
}
