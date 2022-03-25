package history

import (
	"log"

	"tstore/types"
)

type KeyValue[
	CommitID types.Comparable,
	Key types.Comparable,
	Value any,
	Change any] struct {
	Histories          map[Key]*History[CommitID, Value, Change] `json:"histories"`
	createValueHistory func(key Key) ValueHistory[CommitID, Value, Change]
}

func (k KeyValue[CommitID, Key, Value, Change]) FindLatestValueAt(targetCommitID CommitID, key Key) (Value, bool, error) {
	history, ok := k.Histories[key]
	if !ok {
		return *new(Value), false, nil
	}

	return history.Value(targetCommitID)
}

func (k KeyValue[CommitID, Key, Value, Change]) ListAllLatestValuesAt(targetCommitID CommitID) (map[Key]Value, bool, error) {
	pairs := make(map[Key]Value)
	var present bool
	for key, history := range k.Histories {
		value, valuePresent, err := history.Value(targetCommitID)
		if err != nil {
			log.Println(err)
			return nil, false, err
		}

		present = present || valuePresent
		pairs[key] = value
	}

	return pairs, present, nil
}

func (k KeyValue[CommitID, Key, Value, Change]) FindChangesBetween(
	beginCommitID CommitID,
	endCommitID CommitID,
	key Key,
) ([]Version[Value], error) {
	history, ok := k.Histories[key]
	if !ok {
		return nil, nil
	}

	return history.ChangesBetween(beginCommitID, endCommitID)
}

func (k KeyValue[CommitID, Key, Value, Change]) FindAllChangesBetween(
	beginCommitID CommitID,
	endCommitID CommitID,
) (map[Key][]Version[Value], error) {
	values := make(map[Key][]Version[Value])
	for key, history := range k.Histories {
		versions, err := history.ChangesBetween(beginCommitID, endCommitID)
		if err != nil {
			log.Println(err)
			return nil, err
		}

		values[key] = versions
	}

	return values, nil
}

func (k KeyValue[CommitID, Key, Value, Change]) AddVersion(
	commitID CommitID,
	key Key,
	versionStatus VersionStatus,
	change Change,
) (bool, error) {
	history, ok := k.Histories[key]
	if !ok {
		history = New(k.createValueHistory(key))
	}

	succeed, err := history.AddVersion(commitID, versionStatus, change)
	if err != nil {
		log.Println(err)
		return false, err
	}

	if !succeed {
		return false, nil
	}

	k.Histories[key] = history
	return true, nil
}

func (k KeyValue[CommitID, Key, Value, Change]) RemoveVersion(commitID CommitID) (bool, error) {
	var hasDeletion bool
	for _, hist := range k.Histories {
		removed, err := hist.RemoveVersion(commitID)
		if err != nil {
			log.Println(err)
			return false, err
		}

		hasDeletion = hasDeletion || removed
	}

	return hasDeletion, nil
}

func NewKeyValue[
	CommitID types.Comparable,
	Key types.Comparable,
	Value any,
	Change any](
	createValueHistory func(key Key) ValueHistory[CommitID, Value, Change],
) KeyValue[CommitID, Key, Value, Change] {
	return KeyValue[CommitID, Key, Value, Change]{
		Histories:          make(map[Key]*History[CommitID, Value, Change]),
		createValueHistory: createValueHistory,
	}
}
