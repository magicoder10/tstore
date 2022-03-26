package history

import (
	"fmt"
	"log"
	"path"

	"tstore/idgen"
	"tstore/storage"
	"tstore/types"
)

type KeyValue[
	CommitID types.Comparable,
	Key types.Comparable,
	Value any,
	Change any] struct {
	storagePath        string
	refGen             *idgen.IDGen
	rawMap             storage.RawMap
	Histories          map[Key]*History[CommitID, Value, Change] `json:"histories"`
	createValueHistory func(storagePath string) (ValueHistory[CommitID, Value, Change], error)
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
	log.Printf(
		"[KeyValue][AddVersion] commitID=%v, key=%v, versionStatus=%v, change=%v\n",
		commitID,
		key,
		versionStatus,
		change)
	history, ok := k.Histories[key]
	historyPath := path.Join(k.storagePath, "histories", fmt.Sprintf("%v", key))

	if !ok {
		log.Println("[KeyValue][AddVersion] create history")
		var err error
		history, err = New(historyPath, k.refGen, k.rawMap, k.createValueHistory)
		if err != nil {
			log.Println(err)
			return false, err
		}
	}

	succeed, err := history.AddVersion(commitID, versionStatus, change)
	log.Printf("[KeyValue][AddVersion] try add new version to history, succeed=%v\n", succeed)
	if err != nil {
		log.Println(err)
		return false, err
	}

	if !succeed {
		return false, nil
	}

	log.Println("[KeyValue][AddVersion] added new version to history")
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
	storagePath string,
	refGen *idgen.IDGen,
	rawMap storage.RawMap,
	createValueHistory func(storagePath string) (ValueHistory[CommitID, Value, Change], error),
) KeyValue[CommitID, Key, Value, Change] {
	return KeyValue[CommitID, Key, Value, Change]{
		storagePath:        storagePath,
		refGen:             refGen,
		rawMap:             rawMap,
		Histories:          make(map[Key]*History[CommitID, Value, Change]),
		createValueHistory: createValueHistory,
	}
}
