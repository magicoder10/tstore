package history

import (
	"encoding/json"
	"fmt"
	"log"
	"path"

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
	rawMap      storage.RawMap
}

func (s SingleValueHistory[CommitID, Value]) Value(commitID CommitID) (Value, bool, error) {
	buf, err := s.rawMap.Get(s.commitPath(commitID))
	if err != nil {
		log.Println(err)
		return *new(Value), false, err
	}

	var value Value
	err = json.Unmarshal(buf, &value)
	return value, true, err
}

func (s SingleValueHistory[CommitID, Value]) AddVersion(commitID CommitID, change Value) (bool, error) {
	commitPath := s.commitPath(commitID)
	contain, err := s.rawMap.Contain(commitPath)
	if err != nil {
		log.Println(err)
		return false, err
	}

	if contain {
		return false, nil
	}

	buf, err := json.Marshal(change)
	if err != nil {
		log.Println(err)
		return false, err
	}

	err = s.rawMap.Set(commitPath, buf)
	return true, err
}

func (s SingleValueHistory[CommitID, Value]) RemoveVersion(commitID CommitID) (bool, error) {
	commitPath := s.commitPath(commitID)
	contain, err := s.rawMap.Contain(commitPath)
	if err != nil {
		log.Println(err)
		return false, err
	}

	if !contain {
		return false, nil
	}

	return true, s.rawMap.Delete(commitPath)
}

func (s SingleValueHistory[CommitID, Value]) commitPath(commitID CommitID) string {
	return path.Join(s.storagePath, "commits", fmt.Sprintf("%v", commitID))
}

func NewSingleValueHistory[CommitID types.Comparable, Value any](
	storagePath string,
	rawMap storage.RawMap,
) SingleValueHistory[CommitID, Value] {
	return SingleValueHistory[CommitID, Value]{
		storagePath: storagePath,
		rawMap:      rawMap,
	}
}
