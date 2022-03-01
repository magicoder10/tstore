package history

import (
	"tstore/types"
)

type KeyValue[
	CommitID types.Comparable,
	Key types.Comparable,
	Value any,
	Change any] struct {
	Histories          map[Key]*History[CommitID, Value, Change] `json:"histories"`
	createValueHistory func() ValueHistory[CommitID, Value, Change]
}

func (k KeyValue[CommitID, Key, Value, Change]) FindLatestValueAt(targetCommitID CommitID, key Key) (Value, bool) {
	history, ok := k.Histories[key]
	if !ok {
		return *new(Value), false
	}

	return history.Value(targetCommitID)
}

func (k KeyValue[CommitID, Key, Value, Change]) ListAllLatestValuesAt(targetCommitID CommitID) map[Key]Value {
	pairs := make(map[Key]Value)
	for key, history := range k.Histories {
		value, ok := history.Value(targetCommitID)
		if !ok {
			continue
		}

		pairs[key] = value
	}

	return pairs
}

func (k KeyValue[CommitID, Key, Value, Change]) FindChangesBetween(
	beginCommitID CommitID,
	endCommitID CommitID,
	key Key,
) []Version[Value] {
	history, ok := k.Histories[key]
	if !ok {
		return nil
	}

	return history.ChangesBetween(beginCommitID, endCommitID)
}

func (k KeyValue[CommitID, Key, Value, Change]) AddNewVersion(
	commitID CommitID,
	key Key,
	versionStatus VersionStatus,
	change Change,
) bool {
	history, ok := k.Histories[key]
	if !ok {
		history = New(k.createValueHistory())
	}

	succeed := history.AddNewVersion(commitID, versionStatus, change)
	if !succeed {
		return false
	}

	k.Histories[key] = history
	return true
}

func NewKeyValue[
	CommitID types.Comparable,
	Key types.Comparable,
	Value any,
	Change any](
	createValueHistory func() ValueHistory[CommitID, Value, Change],
) KeyValue[CommitID, Key, Value, Change] {
	return KeyValue[CommitID, Key, Value, Change]{
		Histories:          make(map[Key]*History[CommitID, Value, Change]),
		createValueHistory: createValueHistory,
	}
}
