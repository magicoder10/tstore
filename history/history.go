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
	CommitMap     map[CommitID]VersionStatus            `json:"commit_map"`
	ValueHistory  ValueHistory[CommitID, Value, Change] `json:"value_history"`
	CommitHistory reliable.List[CommitID]
}

func (h History[CommitID, Value, Change]) Value(targetCommitID CommitID) (Value, bool, error) {
	commits, err := h.CommitHistory.Items()
	if err != nil {
		log.Println(err)
		return *new(Value), false, err
	}

	endCommitID, found := findLargestSmallerThan[CommitID](commits, targetCommitID)
	if !found {
		return *new(Value), false, nil
	}

	versionStatus := h.CommitMap[endCommitID]
	if versionStatus == DeletedVersionStatus {
		return *new(Value), false, nil
	} else {
		return h.ValueHistory.Value(endCommitID)
	}
}

func (h History[CommitID, Value, Change]) ChangesBetween(
	beginCommitID CommitID,
	endCommitID CommitID,
) ([]Version[Value], error) {
	commits, err := h.CommitHistory.Items()
	if err != nil {
		return []Version[Value]{}, err
	}

	inBetweenCommitIDs := findAllInBetween(commits, beginCommitID, endCommitID)
	var versions []Version[Value]

	for _, commitID := range inBetweenCommitIDs {
		versionStatus := h.CommitMap[commitID]
		value, _, err := h.ValueHistory.Value(commitID)
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
	_, ok := h.CommitMap[commitID]
	if ok {
		return false, nil
	}

	var updated bool
	var err error

	if versionStatus != DeletedVersionStatus {
		updated, err = h.ValueHistory.AddVersion(commitID, change)
		if err != nil {
			log.Println(err)
			return false, err
		}
	}

	err = h.CommitHistory.Append(commitID)
	if err != nil {
		log.Println(err)
		return false, err
	}

	h.CommitMap[commitID] = versionStatus
	return updated, nil
}

func (h *History[CommitID, Value, Change]) RemoveVersion(commitID CommitID) (bool, error) {
	_, ok := h.CommitMap[commitID]
	if ok {
		return false, nil
	}

	removed, err := h.ValueHistory.RemoveVersion(commitID)
	if err != nil {
		log.Println(err)
		return false, err
	}

	commitLen, err := h.CommitHistory.Length()
	if err != nil {
		log.Println(err)
		return false, err
	}

	if commitLen > 0 {
		_, err = h.CommitHistory.Pop()
		if err != nil {
			log.Println(err)
			return false, err
		}
	}

	delete(h.CommitMap, commitID)
	return removed, nil
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

	return &History[CommitID, Value, Change]{
		CommitMap:     make(map[CommitID]VersionStatus),
		ValueHistory:  valueHistory,
		CommitHistory: commitIDs,
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
