package data

import (
	"tstore/history"
)

type Schema struct {
	Name       string          `json:"name"`
	Attributes map[string]Type `json:"attributes"` // key: attribute name, value: attribute type name
}

type SchemaInput struct {
	Name                       string          `json:"name"`
	AttributesToCreateOrUpdate map[string]Type `json:"attributes_to_create_or_update"`
	AttributesToDelete         []string        `json:"attributes_to_delete"`
}

// TODO: handle schema name change

type SchemaValueHistory struct {
	nameHistory      *history.History[uint64, string, string]
	attributeHistory history.KeyValue[uint64, string, Type, Type]
}

func (s SchemaValueHistory) Value(commitID uint64) Schema {
	name, _ := s.nameHistory.Value(commitID)
	return Schema{
		Name:       name,
		Attributes: s.attributeHistory.ListAllLatestValuesAt(commitID),
	}
}

func (s SchemaValueHistory) AddNewVersion(commitID uint64, mutation Mutation) bool {
	switch mutation.Type {
	case CreateSchemaMutation:
		s.nameHistory.AddNewVersion(commitID, history.CreatedVersionStatus, mutation.SchemaInput.Name)
		for attribute, dataType := range mutation.SchemaInput.AttributesToCreateOrUpdate {
			s.attributeHistory.AddNewVersion(commitID, attribute, history.CreatedVersionStatus, dataType)
		}
	case DeleteSchemaMutation:
		s.nameHistory.AddNewVersion(commitID, history.DeletedVersionStatus, "")
		attributes := s.attributeHistory.ListAllLatestValuesAt(commitID)
		for attribute := range attributes {
			s.attributeHistory.AddNewVersion(commitID, attribute, history.DeletedVersionStatus, NoneDataType)
		}
	case CreateSchemaAttributesMutation:
		for attribute, dataType := range mutation.SchemaInput.AttributesToCreateOrUpdate {
			s.attributeHistory.AddNewVersion(commitID, attribute, history.CreatedVersionStatus, dataType)
		}
	case DeleteSchemaAttributesMutation:
		for _, attribute := range mutation.SchemaInput.AttributesToDelete {
			s.attributeHistory.AddNewVersion(commitID, attribute, history.DeletedVersionStatus, NoneDataType)
		}
	default:
		return false
	}

	return true
}

func newSchemaValueHistory() SchemaValueHistory {
	return SchemaValueHistory{
		nameHistory: history.New[uint64, string, string](
			(history.ValueHistory[uint64, string, string])(history.NewSingleValueHistory[uint64, string]())),
		attributeHistory: history.NewKeyValue[uint64, string, Type, Type](
			func() history.ValueHistory[uint64, Type, Type] {
				return (history.ValueHistory[uint64, Type, Type])(history.NewSingleValueHistory[uint64, Type]())
			}),
	}
}
