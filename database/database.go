package database

import (
	"tstore/data"
	"tstore/history"
	"tstore/mutation"
	"tstore/query"
	"tstore/query/lang"
	"tstore/storage"
)

type Database struct {
	databaseStorage Storage
	dataStorage     *data.Storage
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
	commits, err := d.dataStorage.ReadAllCommits()
	if err != nil {
		return data.Commit{}, err
	}

	if len(commits) < 1 {
		return data.Commit{
			CommittedTransactionID: 0,
		}, nil
	}

	return commits[len(commits)-1], nil
}

func (d Database) DeleteAllData() error {
	return d.databaseStorage.DeleteAllData()
}

func NewDatabase(name string, rawMap storage.RawMap) (Database, error) {
	dataStorage := data.NewStorage(name, rawMap)
	mutator, err := mutation.NewMutator(dataStorage, name)
	if err != nil {
		return Database{}, err
	}
	mutator.Start()

	return Database{
		databaseStorage: newStorage(name),
		dataStorage:     dataStorage,
		mutator:         mutator,
		queryExecutor:   query.NewExecutor(dataStorage),
	}, nil
}
