package query

import (
	"tstore/data"
	"tstore/history"
	"tstore/query/lang"
)

type Executor struct {
	dataWithVersion *data.WithVersion
}

func (e Executor) QueryEntitiesAtCommit(commitID uint64, query lang.Expression) ([]data.Entity, error) {
	collector, err := evaluateCollector(CreateEntityAttributeSelector, query)
	if err != nil {
		return nil, err
	}

	entities, err := e.getEntitiesAtCommit(commitID)
	if err != nil {
		return nil, err
	}

	return collector(entities), nil
}

func (e Executor) QueryEntityGroupsAtCommit(commitID uint64, query lang.Expression) (Groups[data.Entity], error) {
	groupCollector, err := evaluateGroupCollector(CreateEntityAttributeSelector, query)
	if err != nil {
		return nil, err
	}

	entities, err := e.getEntitiesAtCommit(commitID)
	if err != nil {
		return nil, err
	}

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

	versionGroups, err := e.dataWithVersion.EntityHistories.FindAllChangesBetween(beginCommitID, endCommitID)
	if err != nil {
		return nil, err
	}

	for entityID, versions := range versionGroups {
		versionGroups[entityID] = collector(versions)
	}

	return versionGroups, nil
}

func (e Executor) getEntitiesAtCommit(commitID uint64) ([]data.Entity, error) {
	entityMap, _, err := e.dataWithVersion.EntityHistories.ListAllLatestValuesAt(commitID)
	if err != nil {
		return nil, err
	}

	entities := make([]data.Entity, 0)
	for _, entity := range entityMap {
		entities = append(entities, entity)
	}

	return entities, nil
}

func NewExecutor(dataWithVersion *data.WithVersion) Executor {
	return Executor{dataWithVersion: dataWithVersion}
}
