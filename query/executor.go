package query

import (
	"tstore/data"
	"tstore/query/lang"
)

type Executor struct {
	dataStorage *data.Storage
}

func (e Executor) QueryEntities(commitID uint64, query lang.Expression) ([]data.Entity, error) {
	collector, err := evaluateCollector(query)
	if err != nil {
		return nil, err
	}

	entities := e.getEntitiesAtCommit(commitID)
	return collector(entities), nil
}

func (e Executor) QueryEntityGroups(commitID uint64, query lang.Expression) (Groups, error) {
	groupCollector, err := evaluateGroupCollector(query)
	if err != nil {
		return nil, err
	}

	entities := e.getEntitiesAtCommit(commitID)
	return groupCollector(entities), nil
}

func (e Executor) getEntitiesAtCommit(commitID uint64) []data.Entity {
	entityMap := e.dataStorage.EntityHistories.ListAllLatestValuesAt(commitID)
	entities := make([]data.Entity, 0)
	for _, entity := range entityMap {
		entities = append(entities, entity)
	}

	return entities
}

func NewExecutor(dataStorage *data.Storage) Executor {
	return Executor{dataStorage: dataStorage}
}
