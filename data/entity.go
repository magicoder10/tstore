package data

import (
	"tstore/history"
)

type Entity struct {
	ID         uint64                 `json:"id"`
	SchemaName string                 `json:"schema_name"`
	Attributes map[string]interface{} `json:"attributes"` // key: attribute ID, value: attribute value
}

type EntityValueHistory struct {
	id               uint64
	schemaName       string
	attributeHistory history.KeyValue[uint64, string, interface{}, interface{}]
}

func (e EntityValueHistory) Value(commitID uint64) Entity {
	return Entity{
		ID:         e.id,
		SchemaName: e.schemaName,
		Attributes: e.attributeHistory.ListLatestValuesAt(commitID),
	}
}

func (e EntityValueHistory) AddNewVersion(commitID uint64, change map[string]history.Version[interface{}]) bool {
	for attribute, version := range change {
		if !e.attributeHistory.AddNewVersion(commitID, attribute, version.Status, version.Value) {
			return false
		}
	}

	return true
}

func NewEntityValueHistory(id uint64, schema string) EntityValueHistory {
	return EntityValueHistory{
		id:         id,
		schemaName: schema,
		attributeHistory: history.NewKeyValue[uint64, string, interface{}, interface{}](func() history.ValueHistory[uint64, interface{}, interface{}] {
			return (history.ValueHistory[uint64, interface{}, interface{}])(history.NewSingleValueHistory[uint64, interface{}]())
		}),
	}
}
