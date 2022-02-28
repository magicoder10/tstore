package history

import (
	"tstore/types"
)

type KeyValue[
	CommitID types.Comparable,
	Key types.Comparable,
	Value any,
	Change any] struct {
	histories          map[Key]*History[CommitID, Value, Change]
	createValueHistory func() ValueHistory[CommitID, Value, Change]
}

func (k KeyValue[CommitID, Key, Value, Change]) FindLatestValueAt(targetCommitID CommitID, key Key) (Value, bool) {
	history, ok := k.histories[key]
	if !ok {
		return *new(Value), false
	}

	return history.Value(targetCommitID)
}

func (k KeyValue[CommitID, Key, Value, Change]) ListLatestValuesAt(targetCommitID CommitID) map[Key]Value {
	pairs := make(map[Key]Value)
	for key, history := range k.histories {
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
	history, ok := k.histories[key]
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
	history, ok := k.histories[key]
	if !ok {
		history = New(k.createValueHistory())
	}

	succeed := history.AddNewVersion(commitID, versionStatus, change)
	if !succeed {
		return false
	}

	k.histories[key] = history
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
		histories:          make(map[Key]*History[CommitID, Value, Change]),
		createValueHistory: createValueHistory,
	}
}
