package history

import (
	"fmt"
	"log"
	"path"

	"tstore/idgen"
	"tstore/reliable"
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
	histories          reliable.Map[Key, bool]
	createValueHistory func(storagePath string) (ValueHistory[CommitID, Value, Change], error)
}

func (k KeyValue[CommitID, Key, Value, Change]) FindLatestValueAt(targetCommitID CommitID, key Key) (Value, bool, error) {
	contain, err := k.histories.Contain(key)
	if err != nil {
		log.Println(err)
		return *new(Value), false, err
	}

	if !contain {
		return *new(Value), false, nil
	}

	hist, err := k.getHistory(key)
	if err != nil {
		log.Println(err)
		return *new(Value), false, err
	}

	return hist.Value(targetCommitID)
}

func (k KeyValue[CommitID, Key, Value, Change]) ListAllLatestValuesAt(targetCommitID CommitID) (map[Key]Value, bool, error) {
	pairs := make(map[Key]Value)
	var present bool
	keys, err := k.histories.Keys()
	if err != nil {
		log.Println(err)
		return nil, false, err
	}

	for _, key := range keys {
		hist, err := k.getHistory(key)
		if err != nil {
			log.Println(err)
			return nil, false, err
		}

		value, valuePresent, err := hist.Value(targetCommitID)
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
	contain, err := k.histories.Contain(key)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	if !contain {
		return nil, nil
	}

	hist, err := k.getHistory(key)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	return hist.ChangesBetween(beginCommitID, endCommitID)
}

func (k KeyValue[CommitID, Key, Value, Change]) FindAllChangesBetween(
	beginCommitID CommitID,
	endCommitID CommitID,
) (map[Key][]Version[Value], error) {
	values := make(map[Key][]Version[Value])

	keys, err := k.histories.Keys()
	if err != nil {
		log.Println(err)
		return nil, err
	}

	for _, key := range keys {
		hist, err := k.getHistory(key)
		versions, err := hist.ChangesBetween(beginCommitID, endCommitID)
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
	contain, err := k.histories.Contain(key)
	if err != nil {
		log.Println(err)
		return false, err
	}

	if !contain {
		log.Println("[KeyValue][AddVersion] create history")
		err = k.histories.Set(key, true)
		if err != nil {
			log.Println(err)
			return false, err
		}
	}

	hist, err := k.getHistory(key)
	if err != nil {
		log.Println(err)
		return false, err
	}

	succeed, err := hist.AddVersion(commitID, versionStatus, change)
	log.Printf("[KeyValue][AddVersion] try add new version to history, succeed=%v\n", succeed)
	if err != nil {
		log.Println(err)
		return false, err
	}

	if !succeed {
		return false, nil
	}

	log.Println("[KeyValue][AddVersion] added new version to history")
	return true, nil
}

func (k KeyValue[CommitID, Key, Value, Change]) RemoveVersion(commitID CommitID) (bool, error) {
	var hasDeletion bool
	keys, err := k.histories.Keys()
	if err != nil {
		log.Println(err)
		return false, err
	}

	for _, key := range keys {
		hist, err := k.getHistory(key)
		if err != nil {
			log.Println(err)
			return false, err
		}

		removed, err := hist.RemoveVersion(commitID)
		if err != nil {
			log.Println(err)
			return false, err
		}

		hasDeletion = hasDeletion || removed
	}

	return hasDeletion, nil
}

func (k KeyValue[CommitID, Key, Value, Change]) getHistory(key Key) (*History[CommitID, Value, Change], error) {
	historyPath := path.Join(k.storagePath, "histories", fmt.Sprintf("%v", key))
	return New[CommitID, Value, Change](historyPath, k.refGen, k.rawMap, k.createValueHistory)
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
) (KeyValue[CommitID, Key, Value, Change], error) {
	histories, err := reliable.NewMap[Key, bool](path.Join(storagePath, "histories"), refGen, rawMap)
	if err != nil {
		return *new(KeyValue[CommitID, Key, Value, Change]), err
	}

	return KeyValue[CommitID, Key, Value, Change]{
		storagePath:        storagePath,
		refGen:             refGen,
		rawMap:             rawMap,
		histories:          histories,
		createValueHistory: createValueHistory,
	}, nil
}
