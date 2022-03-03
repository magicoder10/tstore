package query

import (
	"tstore/data"
	"tstore/history"
	"tstore/query/lang"
)

type Executor struct {
	dataStorage *data.Storage
}

func (e Executor) QueryEntitiesAtCommit(commitID uint64, query lang.Expression) ([]data.Entity, error) {
	collector, err := evaluateCollector(CreateEntityAttributeSelector, query)
	if err != nil {
		return nil, err
	}

	entities := e.getEntitiesAtCommit(commitID)
	return collector(entities), nil
}

func (e Executor) QueryEntityGroupsAtCommit(commitID uint64, query lang.Expression) (Groups[data.Entity], error) {
	groupCollector, err := evaluateGroupCollector(CreateEntityAttributeSelector, query)
	if err != nil {
		return nil, err
	}

	entities := e.getEntitiesAtCommit(commitID)
	return groupCollector(entities), nil
}

func (e Executor) QueryEntitiesBetweenCommits(
	beginCommitID uint64,
	endCommitID uint64,
	query lang.Expression) (map[uint64][]history.Version[data.Entity], error) {
	collector, err := evaluateCollector(CreateEntityVersionAttributeSelector, query)
	if err != nil {
		return nil, err
	}

	versionGroups := e.dataStorage.EntityHistories.FindAllChangesBetween(beginCommitID, endCommitID)
	for entityID, versions := range versionGroups {
		versionGroups[entityID] = collector(versions)
	}

	return versionGroups, nil
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
