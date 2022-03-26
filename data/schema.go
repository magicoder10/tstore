package data

import (
	"log"
	"path"

	"tstore/history"
	"tstore/idgen"
	"tstore/storage"
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
	nameHistory       *history.History[uint64, string, string]
	attributesHistory history.KeyValue[uint64, string, Type, Type]
}

func (s SchemaValueHistory) Value(commitID uint64) (Schema, bool, error) {
	schema := Schema{}
	name, nameExist, err := s.nameHistory.Value(commitID)
	if err != nil {
		log.Println(err)
		return Schema{}, false, err
	}

	if nameExist {
		schema.Name = name
	}

	attributes, attributesExist, err := s.attributesHistory.ListAllLatestValuesAt(commitID)
	if err != nil {
		log.Println(err)
		return Schema{}, false, err
	}

	if attributesExist {
		schema.Attributes = attributes
	}

	exist := nameExist || attributesExist
	return schema, exist, nil
}

func (s SchemaValueHistory) AddVersion(commitID uint64, mutation Mutation) (bool, error) {
	// TODO: return true when all AddVersion for a given mutation returns true
	var updated bool

	switch mutation.Type {
	case CreateSchemaMutation:
		nameUpdated, err := s.nameHistory.AddVersion(commitID, history.CreatedVersionStatus, mutation.SchemaInput.Name)
		if err != nil {
			log.Println(err)
			return false, err
		}

		updated = updated || nameUpdated

		for attribute, dataType := range mutation.SchemaInput.AttributesToCreateOrUpdate {
			attributeUpdated, err := s.attributesHistory.AddVersion(
				commitID, attribute, history.CreatedVersionStatus, dataType)
			if err != nil {
				log.Println(err)
				return false, err
			}

			updated = updated || attributeUpdated
		}
	case DeleteSchemaMutation:
		nameUpdated, err := s.nameHistory.AddVersion(commitID, history.DeletedVersionStatus, "")
		if err != nil {
			log.Println(err)
			return false, err
		}

		updated = updated || nameUpdated

		attributes, _, err := s.attributesHistory.ListAllLatestValuesAt(commitID)
		if err != nil {
			log.Println(err)
			return false, err
		}

		for attribute := range attributes {
			attributeUpdated, err := s.attributesHistory.AddVersion(
				commitID, attribute, history.DeletedVersionStatus, NoneDataType)
			if err != nil {
				log.Println(err)
				return false, err
			}

			updated = updated || attributeUpdated
		}
	case CreateSchemaAttributesMutation:
		for attribute, dataType := range mutation.SchemaInput.AttributesToCreateOrUpdate {
			attributeUpdated, err := s.attributesHistory.AddVersion(
				commitID, attribute, history.CreatedVersionStatus, dataType)
			if err != nil {
				log.Println(err)
				return false, err
			}

			updated = updated || attributeUpdated
		}
	case DeleteSchemaAttributesMutation:
		for _, attribute := range mutation.SchemaInput.AttributesToDelete {
			attributeUpdated, err := s.attributesHistory.AddVersion(commitID, attribute, history.DeletedVersionStatus, NoneDataType)
			if err != nil {
				log.Println(err)
				return false, err
			}

			updated = updated || attributeUpdated
		}
	default:
		return false, nil
	}

	return updated, nil
}

func (s SchemaValueHistory) RemoveVersion(commitID uint64) (bool, error) {
	nameRemoved, err := s.nameHistory.RemoveVersion(commitID)
	if err != nil {
		log.Println(err)
		return false, err
	}

	attributesRemoved, err := s.attributesHistory.RemoveVersion(commitID)
	if err != nil {
		log.Println(err)
		return false, err
	}

	return nameRemoved || attributesRemoved, nil
}

func newSchemaValueHistory(storagePath string, refGen *idgen.IDGen, rawMap storage.RawMap) (SchemaValueHistory, error) {
	nameHistory, err := history.New[uint64, string, string](
		path.Join(storagePath, "nameHistory"),
		refGen,
		rawMap,
		func(storagePath string) (history.ValueHistory[uint64, string, string], error) {
			return history.NewSingleValueHistory[uint64, string](storagePath, refGen, rawMap)
		})
	if err != nil {
		return SchemaValueHistory{}, err
	}

	attributesHistory, err := history.NewKeyValue[uint64, string, Type, Type](
		path.Join(storagePath, "attributesHistory"),
		refGen,
		rawMap,
		func(storagePath string) (history.ValueHistory[uint64, Type, Type], error) {
			return history.NewSingleValueHistory[uint64, Type](storagePath, refGen, rawMap)
		})
	if err != nil {
		return SchemaValueHistory{}, err
	}

	return SchemaValueHistory{
		nameHistory:       nameHistory,
		attributesHistory: attributesHistory,
	}, nil
}
