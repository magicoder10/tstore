package data

import (
	"log"
	"path"

	"tstore/history"
	"tstore/idgen"
	"tstore/storage"
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
	attributesHistory history.KeyValue[uint64, string, interface{}, interface{}]
}

func (e EntityValueHistory) Value(commitID uint64) (Entity, bool, error) {
	entity := Entity{}
	id, idExist, err := e.idHistory.Value(commitID)
	if err != nil {
		log.Println(err)
		return Entity{}, false, err
	}

	if idExist {
		entity.ID = id
	}

	schemaName, schemaNameExist, err := e.schemaNameHistory.Value(commitID)
	if err != nil {
		log.Println(err)
		return Entity{}, false, err
	}

	if schemaNameExist {
		entity.SchemaName = schemaName
	}

	attributes, attributesExist, err := e.attributesHistory.ListAllLatestValuesAt(commitID)
	if err != nil {
		log.Println(err)
		return Entity{}, false, err
	}

	if attributesExist {
		entity.Attributes = attributes
	}

	exist := idExist || schemaNameExist || attributesExist
	return entity, exist, nil
}

func (e EntityValueHistory) AddVersion(commitID uint64, mutation Mutation) (bool, error) {
	// TODO: return true when all AddVersion for a given mutation returns true
	var updated bool
	switch mutation.Type {
	case CreateEntityMutation:
		idUpdated, err := e.idHistory.AddVersion(commitID, history.CreatedVersionStatus, mutation.EntityInput.EntityID)
		if err != nil {
			log.Println(err)
			return false, err
		}

		updated = updated || idUpdated

		schemaNameUpdated, err := e.schemaNameHistory.AddVersion(
			commitID, history.CreatedVersionStatus, mutation.EntityInput.SchemaName)
		if err != nil {
			log.Println(err)
			return false, err
		}

		updated = updated || schemaNameUpdated

		for attribute, dataType := range mutation.EntityInput.AttributesToCreateOrUpdate {
			attributeUpdated, err := e.attributesHistory.AddVersion(commitID, attribute, history.CreatedVersionStatus, dataType)
			if err != nil {
				log.Println(err)
				return false, err
			}

			updated = updated || attributeUpdated
		}
	case DeleteEntityMutation:
		idUpdated, err := e.idHistory.AddVersion(commitID, history.DeletedVersionStatus, 0)
		if err != nil {
			log.Println(err)
			return false, err
		}

		updated = updated || idUpdated

		schemaNameUpdated, err := e.schemaNameHistory.AddVersion(commitID, history.DeletedVersionStatus, "")
		if err != nil {
			log.Println(err)
			return false, err
		}

		updated = updated || schemaNameUpdated

		attributes, _, err := e.attributesHistory.ListAllLatestValuesAt(commitID)
		if err != nil {
			log.Println(err)
			return false, err
		}

		for attribute := range attributes {
			attributeUpdated, err := e.attributesHistory.AddVersion(
				commitID, attribute, history.DeletedVersionStatus, NoneDataType)
			if err != nil {
				log.Println(err)
				return false, err
			}

			updated = updated || attributeUpdated
		}
	case CreateEntityAttributesMutation:
		for attribute, dataType := range mutation.EntityInput.AttributesToCreateOrUpdate {
			attributeUpdated, err := e.attributesHistory.AddVersion(commitID, attribute, history.CreatedVersionStatus, dataType)
			if err != nil {
				log.Println(err)
				return false, err
			}

			updated = updated || attributeUpdated
		}
	case DeleteEntityAttributesMutation:
		for _, attribute := range mutation.EntityInput.AttributesToDelete {
			attributeUpdated, err := e.attributesHistory.AddVersion(commitID, attribute, history.DeletedVersionStatus, NoneDataType)
			if err != nil {
				log.Println(err)
				return false, err
			}

			updated = updated || attributeUpdated
		}
	case UpdateEntityAttributesMutation:
		for attribute, dataType := range mutation.EntityInput.AttributesToCreateOrUpdate {
			attributeUpdated, err := e.attributesHistory.AddVersion(commitID, attribute, history.UpdatedVersionStatus, dataType)
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

func (e EntityValueHistory) RemoveVersion(commitID uint64) (bool, error) {
	histRemoved, err := e.idHistory.RemoveVersion(commitID)
	if err != nil {
		log.Println(err)
		return false, err
	}

	schemaNameRemoved, err := e.schemaNameHistory.RemoveVersion(commitID)
	if err != nil {
		log.Println(err)
		return false, err
	}

	attributesRemoved, err := e.attributesHistory.RemoveVersion(commitID)
	if err != nil {
		log.Println(err)
		return false, err
	}

	return histRemoved || schemaNameRemoved || attributesRemoved, nil
}

func newEntityValueHistory(storagePath string, refGen *idgen.IDGen, rawMap storage.RawMap) (EntityValueHistory, error) {
	idHistory, err := history.New[uint64, uint64, uint64](
		path.Join(storagePath, "idHistory"),
		refGen,
		rawMap,
		func(storagePath string) (history.ValueHistory[uint64, uint64, uint64], error) {
			return history.NewSingleValueHistory[uint64, uint64](storagePath, rawMap), nil
		})
	if err != nil {
		return EntityValueHistory{}, err
	}

	schemaNameHistory, err := history.New[uint64, string, string](
		path.Join(storagePath, "schemaNameHistory"),
		refGen,
		rawMap,
		func(storagePath string) (history.ValueHistory[uint64, string, string], error) {
			return history.NewSingleValueHistory[uint64, string](storagePath, rawMap), nil
		})
	if err != nil {
		return EntityValueHistory{}, err
	}

	return EntityValueHistory{
		idHistory:         idHistory,
		schemaNameHistory: schemaNameHistory,
		attributesHistory: history.NewKeyValue[uint64, string, interface{}, interface{}](
			path.Join(storagePath, "attributesHistory"),
			refGen,
			rawMap,
			func(valueStoragePath string) (history.ValueHistory[uint64, interface{}, interface{}], error) {
				return history.NewSingleValueHistory[uint64, interface{}](valueStoragePath, rawMap), nil
			}),
	}, nil
}
