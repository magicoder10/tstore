package data

import (
	"tstore/history"
)

type Schema struct {
	Name       string          `json:"name"`
	Attributes map[string]Type `json:"attributes"` // key: attribute name, value: attribute type name
}

type SchemaValueHistory struct {
	name             string
	attributeHistory history.KeyValue[uint64, string, Type, Type]
}

func (s SchemaValueHistory) Value(commitID uint64) Schema {
	return Schema{
		Name:       s.name,
		Attributes: s.attributeHistory.ListLatestValuesAt(commitID),
	}
}

func (s SchemaValueHistory) AddNewVersion(commitID uint64, change map[string]history.Version[Type]) {
	for attribute, version := range change {
		s.attributeHistory.AddNewVersion(commitID, attribute, version.Status, version.Value)
	}
}

func NewSchemaValueHistory(name string) SchemaValueHistory {
	return SchemaValueHistory{
		name: name,
		attributeHistory: history.NewKeyValue[uint64, string, Type, Type](func() history.ValueHistory[uint64, Type, Type] {
			return (history.ValueHistory[uint64, Type, Type])(history.NewSingleValueHistory[uint64, Type]())
		}),
	}
}
