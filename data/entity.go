package data

import (
	"tstore/history"
)

type Entity struct {
	ID         uint64                 `json:"id"`
	SchemaName string                 `json:"schema_name"`
	Attributes map[string]interface{} `json:"attributes"` // key: attribute ID, value: attribute value
}

type EntityInput struct {
	EntityID                   uint64                 `json:"entity_id"`
	SchemaName                 string                 `json:"schema_name"`
	AttributesToCreateOrUpdate map[string]interface{} `json:"attributes_to_create_or_update"`
	AttributesToDelete         []string               `json:"attributes_to_delete"`
}

// TODO: handle schema name change

type EntityValueHistory struct {
	idHistory         *history.History[uint64, uint64, uint64]
	schemaNameHistory *history.History[uint64, string, string]
	attributeHistory  history.KeyValue[uint64, string, interface{}, interface{}]
}

func (e EntityValueHistory) Value(commitID uint64) Entity {
	id, _ := e.idHistory.Value(commitID)
	schemaName, _ := e.schemaNameHistory.Value(commitID)
	return Entity{
		ID:         id,
		SchemaName: schemaName,
		Attributes: e.attributeHistory.ListAllLatestValuesAt(commitID),
	}
}

func (e EntityValueHistory) AddNewVersion(commitID uint64, mutation Mutation) bool {
	switch mutation.Type {
	case CreateEntityMutation:
		e.idHistory.AddNewVersion(commitID, history.CreatedVersionStatus, mutation.EntityInput.EntityID)
		e.schemaNameHistory.AddNewVersion(commitID, history.CreatedVersionStatus, mutation.EntityInput.SchemaName)
		for attribute, dataType := range mutation.EntityInput.AttributesToCreateOrUpdate {
			e.attributeHistory.AddNewVersion(commitID, attribute, history.CreatedVersionStatus, dataType)
		}
	case DeleteEntityMutation:
		e.idHistory.AddNewVersion(commitID, history.DeletedVersionStatus, 0)
		e.schemaNameHistory.AddNewVersion(commitID, history.DeletedVersionStatus, "")
		attributes := e.attributeHistory.ListAllLatestValuesAt(commitID)
		for attribute := range attributes {
			e.attributeHistory.AddNewVersion(commitID, attribute, history.DeletedVersionStatus, NoneDataType)
		}
	case CreateEntityAttributesMutation:
		for attribute, dataType := range mutation.EntityInput.AttributesToCreateOrUpdate {
			e.attributeHistory.AddNewVersion(commitID, attribute, history.CreatedVersionStatus, dataType)
		}
	case DeleteEntityAttributesMutation:
		for _, attribute := range mutation.EntityInput.AttributesToDelete {
			e.attributeHistory.AddNewVersion(commitID, attribute, history.DeletedVersionStatus, "")
		}
	case UpdateEntityAttributesMutation:
		for attribute, dataType := range mutation.EntityInput.AttributesToCreateOrUpdate {
			e.attributeHistory.AddNewVersion(commitID, attribute, history.UpdatedVersionStatus, dataType)
		}
	default:
		return false
	}

	return true
}

func newEntityValueHistory() EntityValueHistory {
	return EntityValueHistory{
		idHistory: history.New[uint64, uint64, uint64](
			(history.ValueHistory[uint64, uint64, uint64])(history.NewSingleValueHistory[uint64, uint64]())),
		schemaNameHistory: history.New[uint64, string, string](
			(history.ValueHistory[uint64, string, string])(history.NewSingleValueHistory[uint64, string]())),
		attributeHistory: history.NewKeyValue[uint64, string, interface{}, interface{}](func() history.ValueHistory[uint64, interface{}, interface{}] {
			return (history.ValueHistory[uint64, interface{}, interface{}])(history.NewSingleValueHistory[uint64, interface{}]())
		}),
	}
}
