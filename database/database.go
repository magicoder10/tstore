package database

import (
	"tstore/data"
	"tstore/history"
	"tstore/idgen"
	"tstore/mutation"
	"tstore/query"
	"tstore/query/lang"
	"tstore/storage"
)

type Database struct {
	storagePath     string
	rawMap          storage.RawMap
	dataWithVersion *data.WithVersion
	mutator         *mutation.Mutator
	queryExecutor   query.Executor
}

func (d Database) CreateTransaction(transactionInput mutation.TransactionInput) error {
	return d.mutator.CreateTransaction(transactionInput)
}

func (d Database) QueryEntitiesAtCommit(commitID uint64, query lang.Expression) ([]data.Entity, error) {
	return d.queryExecutor.QueryEntitiesAtCommit(commitID, query)
}

func (d Database) QueryEntityGroupsAtCommit(commitID uint64, query lang.Expression) (query.Groups[data.Entity], error) {
	return d.queryExecutor.QueryEntityGroupsAtCommit(commitID, query)
}

func (d Database) QueryEntitiesBetweenCommits(
	beginCommitID uint64,
	endCommitID uint64,
	query lang.Expression) (map[uint64][]history.Version[data.Entity], error) {
	return d.queryExecutor.QueryEntitiesBetweenCommits(beginCommitID, endCommitID, query)
}

func (d Database) GetLatestCommit() (data.Commit, error) {
	count, err := d.dataWithVersion.CountCommits()
	if err != nil {
		return data.Commit{}, err
	}

	if count < 1 {
		return data.Commit{}, nil
	}

	return d.dataWithVersion.GetLatestCommit()
}

func (d Database) DeleteAllData() error {
	return d.rawMap.Delete(d.storagePath)
}

func NewDatabase(storagePath string, refGen *idgen.IDGen, rawMap storage.RawMap) (Database, error) {
	dataWithVersion, err := data.NewWithVersion(storagePath, refGen, rawMap)
	if err != nil {
		return Database{}, err
	}

	mutator, err := mutation.NewMutator(storagePath, refGen, rawMap, dataWithVersion)
	if err != nil {
		return Database{}, err
	}

	mutator.Start()
	return Database{
		storagePath:     storagePath,
		rawMap:          rawMap,
		dataWithVersion: dataWithVersion,
		mutator:         mutator,
		queryExecutor:   query.NewExecutor(dataWithVersion),
	}, nil
}
