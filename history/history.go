package history

import (
	"log"
	"path"

	"tstore/idgen"
	"tstore/reliable"
	"tstore/storage"
	"tstore/types"
)

type History[
	CommitID types.Comparable,
	Value any,
	Change any] struct {
	commitsMap    reliable.Map[CommitID, VersionStatus]
	valueHistory  ValueHistory[CommitID, Value, Change]
	commitHistory reliable.List[CommitID]
}

func (h History[CommitID, Value, Change]) Value(targetCommitID CommitID) (Value, bool, error) {
	// TODO: use channel to implement iterator for large data set
	commits, err := h.commitHistory.Items()
	if err != nil {
		log.Println(err)
		return *new(Value), false, err
	}

	endCommitID, found := findLargestSmallerThan[CommitID](commits, targetCommitID)
	if !found {
		return *new(Value), false, nil
	}

	versionStatus, err := h.commitsMap.Get(endCommitID)
	if err != nil {
		log.Println(err)
		return *new(Value), false, err
	}

	if versionStatus == DeletedVersionStatus {
		return *new(Value), false, nil
	} else {
		return h.valueHistory.Value(endCommitID)
	}
}

func (h History[CommitID, Value, Change]) ChangesBetween(
	beginCommitID CommitID,
	endCommitID CommitID,
) ([]Version[Value], error) {
	commits, err := h.commitHistory.Items()
	if err != nil {
		return []Version[Value]{}, err
	}

	inBetweenCommitIDs := findAllInBetween(commits, beginCommitID, endCommitID)
	var versions []Version[Value]

	for _, commitID := range inBetweenCommitIDs {
		versionStatus, err := h.commitsMap.Get(endCommitID)
		if err != nil {
			log.Println(err)
			return []Version[Value]{}, err
		}

		value, _, err := h.valueHistory.Value(commitID)
		if err != nil {
			log.Println(err)
			return []Version[Value]{}, err
		}

		version := Version[Value]{
			Status: versionStatus,
			Value:  value,
		}
		versions = append(versions, version)
	}

	return versions, nil
}

func (h *History[CommitID, Value, Change]) AddVersion(
	commitID CommitID,
	versionStatus VersionStatus,
	change Change,
) (bool, error) {
	contain, err := h.commitsMap.Contain(commitID)
	if err != nil {
		return false, err
	}

	if contain {
		return false, nil
	}

	var updated bool
	if versionStatus != DeletedVersionStatus {
		updated, err = h.valueHistory.AddVersion(commitID, change)
		if err != nil {
			log.Println(err)
			return false, err
		}
	}

	err = h.commitHistory.Append(commitID)
	if err != nil {
		log.Println(err)
		return false, err
	}

	return updated, h.commitsMap.Set(commitID, versionStatus)
}

func (h *History[CommitID, Value, Change]) RemoveVersion(commitID CommitID) (bool, error) {
	contain, err := h.commitsMap.Contain(commitID)
	if err != nil {
		return false, err
	}

	if contain {
		return false, nil
	}

	removed, err := h.valueHistory.RemoveVersion(commitID)
	if err != nil {
		log.Println(err)
		return false, err
	}

	commitLen, err := h.commitHistory.Length()
	if err != nil {
		log.Println(err)
		return false, err
	}

	if commitLen > 0 {
		_, err = h.commitHistory.Pop()
		if err != nil {
			log.Println(err)
			return false, err
		}
	}

	return removed, h.commitsMap.Delete(commitID)
}

func New[
	CommitID types.Comparable,
	Value any,
	Change any](
	storagePath string,
	refGen *idgen.IDGen,
	rawMap storage.RawMap,
	createValueHistory func(storagePath string) (ValueHistory[CommitID, Value, Change], error),
) (*History[CommitID, Value, Change], error) {
	commitHistoryPath := path.Join(storagePath, "commitHistory")
	commitIDs, err := reliable.NewList[CommitID](commitHistoryPath, refGen, rawMap)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	valueHistory, err := createValueHistory(path.Join(storagePath, "valueHistory"))
	if err != nil {
		log.Println(err)
		return nil, err
	}

	reliableMap, err := reliable.NewMap[CommitID, VersionStatus](path.Join(storagePath, "commits"), refGen, rawMap)
	if err != nil {
		return nil, err
	}

	return &History[CommitID, Value, Change]{
		commitsMap:    reliableMap,
		valueHistory:  valueHistory,
		commitHistory: commitIDs,
	}, nil
}

func findAllInBetween[Item types.Comparable](sortedItems []Item, begin Item, end Item) []Item {
	between := make([]Item, 0)

	for _, item := range sortedItems {
		if item >= begin && item <= end {
			between = append(between, item)
		}
	}

	return between
}

func findLargestSmallerThan[Item types.Comparable](sortedItems []Item, end Item) (Item, bool) {
	for index := len(sortedItems) - 1; index >= 0; index-- {
		item := sortedItems[index]
		if item <= end {
			return item, true
		}
	}

	return *new(Item), false
}
