package data

import (
	"path"

	"tstore/history"
	"tstore/idgen"
	"tstore/reliable"
	"tstore/storage"
)

// TODO: persist data

type WithVersion struct {
	commits         reliable.List[Commit]
	SchemaHistories history.KeyValue[uint64, string, Schema, Mutation] `json:"schema_histories"`
	EntityHistories history.KeyValue[uint64, uint64, Entity, Mutation] `json:"entity_histories"`
}

func (w *WithVersion) AppendCommit(commit Commit) error {
	return w.commits.Append(commit)
}

func (w WithVersion) CountCommits() (int, error) {
	return w.commits.Length()
}

func (w WithVersion) GetLatestCommit() (Commit, error) {
	return w.commits.Peek()
}

func NewWithVersion(storagePath string, refGen *idgen.IDGen, rawMap storage.RawMap) (*WithVersion, error) {
	commitsPath := path.Join(storagePath, "commits")
	commits, err := reliable.NewList[Commit](commitsPath, refGen, rawMap)
	if err != nil {
		return nil, err
	}

	schemaHistoriesPath := path.Join(storagePath, "schemaHistories")
	entityHistoriesPath := path.Join(storagePath, "entityHistories")
	return &WithVersion{
		commits: commits,
		SchemaHistories: history.NewKeyValue[uint64, string, Schema, Mutation](
			schemaHistoriesPath,
			refGen,
			rawMap,
			func(storagePath string) (history.ValueHistory[uint64, Schema, Mutation], error) {
				return newSchemaValueHistory(storagePath, refGen, rawMap)
			}),
		EntityHistories: history.NewKeyValue[uint64, uint64, Entity, Mutation](
			entityHistoriesPath,
			refGen,
			rawMap,
			func(storagePath string) (history.ValueHistory[uint64, Entity, Mutation], error) {
				return newEntityValueHistory(storagePath, refGen, rawMap)
			}),
	}, nil
}
