package server

import (
	"fmt"
	"log"
	"path"

	"tstore/data"
	"tstore/database"
	"tstore/history"
	"tstore/idgen"
	"tstore/mutation"
	"tstore/query"
	"tstore/query/lang"
	"tstore/reliable"
	"tstore/storage"
)

type Server struct {
	rawMap          storage.RawMap
	refGen          *idgen.IDGen
	dataStoragePath string
	databasesMap    reliable.Map[string, bool]
	databases       map[string]database.Database
}

func (s Server) ListAllDatabases() ([]string, error) {
	return s.databasesMap.Keys()
}

func (s Server) CreateDatabase(name string) error {
	contain, err := s.databasesMap.Contain(name)
	if err != nil {
		log.Println(err)
		return err
	}
	if contain {
		return fmt.Errorf("database already exist: name=%v", name)
	}

	err = s.databasesMap.Set(name, true)
	if err != nil {
		log.Println(err)
		return err
	}

	storagePath := path.Join(s.dataStoragePath, name)
	db, err := database.NewDatabase(storagePath, s.refGen, s.rawMap)
	if err != nil {
		return err
	}

	s.databases[name] = db
	return nil
}

func (s Server) DeleteDatabase(name string) error {
	db, ok := s.databases[name]
	if !ok {
		return fmt.Errorf("database not found: name=%v", name)
	}

	delete(s.databases, name)
	err := s.databasesMap.Delete(name)
	if err != nil {
		log.Println(err)
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
	rawMap := storage.NewLocalFileMap("./userData")
	dbMap := make(map[string]database.Database)
	refGen, err := idgen.New(path.Join("idGens", "refGen"), rawMap, 5)
	if err != nil {
		return Server{}, err
	}

	databasesPath := path.Join("databases")
	databasesMap, err := reliable.NewMap[string, bool](path.Join(databasesPath, "map"), refGen, rawMap)
	if err != nil {
		return Server{}, err
	}

	databaseNames, err := databasesMap.Keys()
	if err != nil {
		log.Println(err)
		return Server{}, err
	}

	dataStoragePath := path.Join(databasesPath, "data")
	for _, dbName := range databaseNames {
		db, err := database.NewDatabase(path.Join(dataStoragePath, dbName), refGen, rawMap)
		if err != nil {
			return Server{}, err
		}

		dbMap[dbName] = db
	}

	return Server{
		rawMap:          rawMap,
		refGen:          refGen,
		dataStoragePath: path.Join(databasesPath, "data"),
		databasesMap:    databasesMap,
		databases:       dbMap,
	}, nil
}
