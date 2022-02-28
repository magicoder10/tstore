package history

//
//import (
//	"fmt"
//
//	"tstore/types"
//)
//
//type KeyValueHistory[
//	CommitID types.Comparable,
//	Key types.Comparable,
//	Value any,
//	Change any] struct {
//	commitMap          map[CommitID]map[Key]VersionStatus
//	valueHistories     map[Key]ValueHistory[CommitID, Value, Change]
//	commitHistories    map[Key][]CommitID
//	createValueHistory func() ValueHistory[CommitID, Value, Change]
//}
//
//func (k KeyValueHistory[CommitID, Key, Value, Change]) FindLatestValueAt(targetCommitID CommitID, key Key) (Value, bool) {
//	commitHistory, ok := k.commitHistories[key]
//	if !ok {
//		return *new(Value), false
//	}
//
//	endCommitID, err := findLargestSmallerThan[CommitID](commitHistory, targetCommitID)
//	if err != nil {
//		return *new(Value), false
//	}
//
//	versionStatus := k.commitMap[endCommitID][key]
//	if versionStatus == DeletedVersionStatus {
//		return *new(Value), false
//	} else {
//		return k.valueHistories[key].Value(endCommitID), true
//	}
//}
//
//func (k KeyValueHistory[CommitID, Key, Value, Change]) ListLatestValuesAt(targetCommitID CommitID) map[Key]Value {
//	pairs := make(map[Key]Value)
//	for key := range k.commitHistories {
//		value, ok := k.FindLatestValueAt(targetCommitID, key)
//		if !ok {
//			continue
//		}
//
//		pairs[key] = value
//	}
//
//	return pairs
//}
//
//func (k KeyValueHistory[CommitID, Key, Value, Change]) FindChangesBetween(
//	beginCommitID CommitID,
//	endCommitID CommitID,
//	key Key,
//) []Version[Value] {
//	commitHistory, ok := k.commitHistories[key]
//	if !ok {
//		return nil
//	}
//
//	inBetweenCommitIDs := findAllInBetween(commitHistory, beginCommitID, endCommitID)
//
//	valueHistory := k.valueHistories[key]
//	var versions []Version[Value]
//
//	for _, commitID := range inBetweenCommitIDs {
//		versionStatus := k.commitMap[commitID][key]
//
//		var value Value
//		if versionStatus != DeletedVersionStatus {
//			value = valueHistory.Value(commitID)
//		}
//
//		version := Version[Value]{
//			Status: versionStatus,
//			Value:  value,
//		}
//		versions = append(versions, version)
//	}
//
//	return versions
//}
//
//func (k KeyValueHistory[CommitID, Key, Value, Change]) AddNewVersion(
//	commitID CommitID,
//	key Key,
//	versionStatus VersionStatus,
//	change Change,
//) {
//	valueHistory, ok := k.valueHistories[key]
//	if !ok {
//		valueHistory = k.createValueHistory()
//	}
//
//	if versionStatus != DeletedVersionStatus {
//		valueHistory.AddNewVersion(commitID, change)
//	}
//
//	k.valueHistories[key] = valueHistory
//	k.commitHistories[key] = append(([]CommitID)(k.commitHistories[key]), commitID)
//
//	mp, ok := k.commitMap[commitID]
//	if !ok {
//		mp = make(map[Key]VersionStatus)
//	}
//
//	mp[key] = versionStatus
//	k.commitMap[commitID] = mp
//}
//
//func findAllInBetween[Item types.Comparable](sortedItems []Item, begin Item, end Item) []Item {
//	between := make([]Item, 0)
//
//	for _, item := range sortedItems {
//		if item >= begin && item <= end {
//			between = append(between, item)
//		}
//	}
//
//	return between
//}
//
//func findLargestSmallerThan[Item types.Comparable](sortedItems []Item, end Item) (Item, error) {
//	for index := len(sortedItems) - 1; index >= 0; index-- {
//		item := sortedItems[index]
//		if item <= end {
//			return item, nil
//		}
//	}
//
//	return *new(Item), fmt.Errorf("item not found")
//}
//
//func NewKeyValueHistory[
//	CommitID types.Comparable,
//	Key types.Comparable,
//	Value any,
//	Change any](
//	createValueHistory func() ValueHistory[CommitID, Value, Change],
//) KeyValueHistory[CommitID, Key, Value, Change] {
//	return KeyValueHistory[CommitID, Key, Value, Change]{
//		commitMap:          make(map[CommitID]map[Key]VersionStatus),
//		valueHistories:     make(map[Key]ValueHistory[CommitID, Value, Change]),
//		commitHistories:    make(map[Key][]CommitID),
//		createValueHistory: createValueHistory,
//	}
//}
