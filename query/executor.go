package query

import (
	"tstore/data"
	"tstore/query/lang"
)

type Executor struct {
	dataStorage *data.Storage
}

func (e Executor) QueryEntities(transactionID uint64, query lang.Expression) ([]data.Entity, error) {
	collector, err := evaluateCollector(query)
	if err != nil {
		return nil, err
	}

	entities, err := e.getEntitiesAtTransaction(transactionID)
	if err != nil {
		return nil, err
	}

	return collector(entities), nil
}

func (e Executor) QueryEntityGroups(transactionID uint64, query lang.Expression) (Groups, error) {
	groupCollector, err := evaluateGroupCollector(query)
	if err != nil {
		return nil, err
	}

	entities, err := e.getEntitiesAtTransaction(transactionID)
	if err != nil {
		return nil, err
	}

	return groupCollector(entities), nil
}

func (e Executor) getEntitiesAtTransaction(transactionID uint64) ([]data.Entity, error) {
	// TODO: query entities for a given transaction only
	return e.dataStorage.ReadAllEntities()
}

func NewExecutor(dataStorage *data.Storage) Executor {
	return Executor{dataStorage: dataStorage}
}
