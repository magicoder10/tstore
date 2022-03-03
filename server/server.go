package server

import (
	"fmt"

	"tstore/data"
	"tstore/database"
	"tstore/history"
	"tstore/mutation"
	"tstore/query"
	"tstore/query/lang"
)

type Server struct {
	databases map[string]database.Database
}

func (s Server) ListAllDatabases() []string {
	dbNames := make([]string, 0)
	for dbName := range s.databases {
		dbNames = append(dbNames, dbName)
	}

	return dbNames
}

func (s Server) CreateDatabase(name string) error {
	if _, ok := s.databases[name]; ok {
		return fmt.Errorf("database already exist: name=%v", name)
	}

	db, err := database.NewDatabase(name)
	if err != nil {
		return err
	}

	s.databases[name] = db
	return writeDatabases(s.ListAllDatabases())
}

func (s Server) DeleteDatabase(name string) error {
	db, ok := s.databases[name]
	if !ok {
		return fmt.Errorf("database not found: name=%v", name)
	}

	delete(s.databases, name)
	err := writeDatabases(s.ListAllDatabases())
	if err != nil {
		return err
	}

	return db.DeleteAllData()
}

func (s Server) CreateTransaction(dbName string, transactionInput mutation.TransactionInput) error {
	db, ok := s.databases[dbName]
	if !ok {
		return fmt.Errorf("database not found: name=%v", dbName)
	}

	return db.CreateTransaction(transactionInput)
}

func (s Server) QueryEntitiesAtCommit(dbName string, transactionID uint64, query lang.Expression) ([]data.Entity, error) {
	db, ok := s.databases[dbName]
	if !ok {
		return nil, fmt.Errorf("database not found: name=%v", dbName)
	}

	return db.QueryEntitiesAtCommit(transactionID, query)
}

func (s Server) QueryEntityGroupsAtCommit(
	dbName string,
	transactionID uint64,
	query lang.Expression,
) (query.Groups[data.Entity], error) {
	db, ok := s.databases[dbName]
	if !ok {
		return nil, fmt.Errorf("database not found: name=%v", dbName)
	}

	return db.QueryEntityGroupsAtCommit(transactionID, query)
}

func (s Server) QueryEntitiesBetweenCommits(
	dbName string,
	beginCommitID uint64,
	endCommitID uint64,
	query lang.Expression) (map[uint64][]history.Version[data.Entity], error) {
	db, ok := s.databases[dbName]
	if !ok {
		return nil, fmt.Errorf("database not found: name=%v", dbName)
	}

	return db.QueryEntitiesBetweenCommits(beginCommitID, endCommitID, query)
}

func (s Server) GetLatestCommit(dbName string) (data.Commit, error) {
	db, ok := s.databases[dbName]
	if !ok {
		return data.Commit{}, fmt.Errorf("database not found: name=%v", dbName)
	}

	return db.GetLatestCommit()
}

func newServer() (Server, error) {
	databases, err := readDatabases()
	if err != nil {
		fmt.Printf("%v\n", err)
		databases = make([]string, 0)
	}

	dbMap := make(map[string]database.Database)
	for _, dbName := range databases {
		db, err := database.NewDatabase(dbName)
		if err != nil {
			return Server{}, err
		}

		dbMap[dbName] = db
	}

	return Server{
		databases: dbMap,
	}, nil
}
