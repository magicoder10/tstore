package history

import (
	"tstore/types"
)

type ValueHistory[CommitID types.Comparable, Value any, Change any] interface {
	Value(commitID CommitID) Value
	AddNewVersion(commitID CommitID, change Change) bool
}

type SingleValueHistory[CommitID types.Comparable, Value any] struct {
	commits map[CommitID]Value
}

func (s SingleValueHistory[CommitID, Value]) Value(commitID CommitID) Value {
	return s.commits[commitID]
}

func (s *SingleValueHistory[CommitID, Value]) AddNewVersion(commitID CommitID, change Value) bool {
	_, ok := s.commits[commitID]
	if ok {
		return false
	}

	s.commits[commitID] = change
	return true
}

func NewSingleValueHistory[CommitID types.Comparable, Value any]() *SingleValueHistory[CommitID, Value] {
	return &SingleValueHistory[CommitID, Value]{
		commits: make(map[CommitID]Value),
	}
}
