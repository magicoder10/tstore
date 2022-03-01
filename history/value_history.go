package history

import (
	"tstore/types"
)

type ValueHistory[CommitID types.Comparable, Value any, Change any] interface {
	Value(commitID CommitID) Value
	AddNewVersion(commitID CommitID, change Change) bool
}

type SingleValueHistory[CommitID types.Comparable, Value any] struct {
	Commits map[CommitID]Value `json:"commits"`
}

func (s SingleValueHistory[CommitID, Value]) Value(commitID CommitID) Value {
	return s.Commits[commitID]
}

func (s *SingleValueHistory[CommitID, Value]) AddNewVersion(commitID CommitID, change Value) bool {
	_, ok := s.Commits[commitID]
	if ok {
		return false
	}

	s.Commits[commitID] = change
	return true
}

func NewSingleValueHistory[CommitID types.Comparable, Value any]() *SingleValueHistory[CommitID, Value] {
	return &SingleValueHistory[CommitID, Value]{
		Commits: make(map[CommitID]Value),
	}
}
