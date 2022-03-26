package history

import (
	"log"
	"path"

	"tstore/idgen"
	"tstore/reliable"
	"tstore/storage"
	"tstore/types"
)

type ValueHistory[CommitID types.Comparable, Value any, Change any] interface {
	// Value returns whether the value is present at the given commit
	Value(commitID CommitID) (Value, bool, error)
	AddVersion(commitID CommitID, change Change) (bool, error)
	RemoveVersion(commitID CommitID) (bool, error)
}

type SingleValueHistory[CommitID types.Comparable, Value any] struct {
	storagePath string
	commitsMap  reliable.Map[CommitID, Value]
}

func (s SingleValueHistory[CommitID, Value]) Value(commitID CommitID) (Value, bool, error) {
	contain, err := s.commitsMap.Contain(commitID)
	if err != nil {
		log.Println(err)
		return *new(Value), false, err
	}

	if !contain {
		return *new(Value), false, nil
	}

	value, err := s.commitsMap.Get(commitID)
	return value, true, err
}

func (s SingleValueHistory[CommitID, Value]) AddVersion(commitID CommitID, change Value) (bool, error) {
	contain, err := s.commitsMap.Contain(commitID)
	if err != nil {
		log.Println(err)
		return false, err
	}

	if contain {
		return false, nil
	}

	err = s.commitsMap.Set(commitID, change)
	return true, err
}

func (s SingleValueHistory[CommitID, Value]) RemoveVersion(commitID CommitID) (bool, error) {
	contain, err := s.commitsMap.Contain(commitID)
	if err != nil {
		log.Println(err)
		return false, err
	}

	if !contain {
		return false, nil
	}

	return true, s.commitsMap.Delete(commitID)
}

func NewSingleValueHistory[CommitID types.Comparable, Value any](
	storagePath string,
	refGen *idgen.IDGen,
	rawMap storage.RawMap,
) (*SingleValueHistory[CommitID, Value], error) {
	commitsMap, err := reliable.NewMap[CommitID, Value](path.Join(storagePath, "commits"), refGen, rawMap)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	return &SingleValueHistory[CommitID, Value]{
		storagePath: storagePath,
		commitsMap:  commitsMap,
	}, nil
}
