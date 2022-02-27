package database

import (
	"errors"

	"tstore/data"
	"tstore/mutation"
	"tstore/query"
	"tstore/query/lang"
)

type Database struct {
	databaseStorage Storage
	dataStorage     data.Storage
	mutator         *mutation.Mutator
	queryExecutor   query.Executor
}

func (d Database) CreateTransaction(transactionInput mutation.TransactionInput) error {
	return d.mutator.CreateTransaction(transactionInput)
}

func (d Database) QueryEntities(transactionID uint64, query lang.Expression) ([]data.Entity, error) {
	return d.queryExecutor.QueryEntities(transactionID, query)
}

func (d Database) QueryEntityGroups(transactionID uint64, query lang.Expression) (query.Groups, error) {
	return d.queryExecutor.QueryEntityGroups(transactionID, query)
}

func (d Database) GetLatestCommit() (data.Commit, error) {
	commits, err := d.dataStorage.ReadAllCommits()
	if err != nil {
		return data.Commit{}, err
	}

	if len(commits) < 1 {
		return data.Commit{}, errors.New("no commit found")
	}

	return commits[len(commits)-1], nil
}

func (d Database) DeleteAllData() error {
	return d.databaseStorage.DeleteAllData()
}

func NewDatabase(name string) (Database, error) {
	dataStorage, err := data.NewStorage(name)
	if err != nil {
		return Database{}, err
	}

	mutator, err := mutation.NewMutator(&dataStorage, name)
	if err != nil {
		return Database{}, err
	}
	mutator.Start()

	return Database{
		databaseStorage: newStorage(name),
		dataStorage:     dataStorage,
		mutator:         mutator,
		queryExecutor:   query.NewExecutor(&dataStorage),
	}, nil
}
