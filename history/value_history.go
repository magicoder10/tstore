package history

import (
	"tstore/types"
)

type ValueHistory[CommitID types.Comparable, Value any, Change any] interface {
	Value(commitID CommitID) Value
	AddVersion(commitID CommitID, change Change) bool
	RemoveVersion(commitID CommitID) bool
}

type SingleValueHistory[CommitID types.Comparable, Value any] struct {
	Commits map[CommitID]Value `json:"commits"`
}

func (s SingleValueHistory[CommitID, Value]) Value(commitID CommitID) Value {
	return s.Commits[commitID]
}

func (s *SingleValueHistory[CommitID, Value]) AddVersion(commitID CommitID, change Value) bool {
	_, ok := s.Commits[commitID]
	if ok {
		return false
	}

	s.Commits[commitID] = change
	return true
}

func (s *SingleValueHistory[CommitID, Value]) RemoveVersion(commitID CommitID) bool {
	_, ok := s.Commits[commitID]
	if !ok {
		return false
	}

	delete(s.Commits, commitID)
	return true
}

func NewSingleValueHistory[CommitID types.Comparable, Value any]() *SingleValueHistory[CommitID, Value] {
	return &SingleValueHistory[CommitID, Value]{
		Commits: make(map[CommitID]Value),
	}
}
