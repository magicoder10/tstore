package data

import (
	"path"
	"strconv"

	"tstore/history"
	"tstore/storage"
)

// TODO: persist data

type Storage struct {
	commits         []Commit
	SchemaHistories history.KeyValue[uint64, string, Schema, Mutation] `json:"schema_histories"`
	EntityHistories history.KeyValue[uint64, uint64, Entity, Mutation] `json:"entity_histories"`
}

func (s *Storage) AppendCommit(commit Commit) error {
	s.commits = append(s.commits, commit)
	return nil
}

func (s Storage) ReadAllCommits() ([]Commit, error) {
	return s.commits, nil
}

func NewStorage(dbName string, rawMap storage.RawMap) *Storage {
	return &Storage{
		commits: make([]Commit, 0),
		SchemaHistories: history.NewKeyValue[uint64, string, Schema, Mutation](
			func(schemaName string) history.ValueHistory[uint64, Schema, Mutation] {
				return newSchemaValueHistory(
					path.Join(dbName, "SchemaHistories", schemaName), rawMap)
			}),
		EntityHistories: history.NewKeyValue[uint64, uint64, Entity, Mutation](
			func(entityID uint64) history.ValueHistory[uint64, Entity, Mutation] {
				return newEntityValueHistory(
					path.Join(dbName, "EntityHistories", strconv.FormatUint(entityID, 10)), rawMap)
			}),
	}
}
