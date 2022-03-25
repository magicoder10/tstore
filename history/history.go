package history

import (
	"log"

	"tstore/types"
)

type History[
	CommitID types.Comparable,
	Value any,
	Change any] struct {
	CommitMap     map[CommitID]VersionStatus            `json:"commit_map"`
	ValueHistory  ValueHistory[CommitID, Value, Change] `json:"value_history"`
	CommitHistory []CommitID                            `json:"commit_history"`
}

func (h History[CommitID, Value, Change]) Value(targetCommitID CommitID) (Value, bool, error) {
	endCommitID, found := findLargestSmallerThan[CommitID](h.CommitHistory, targetCommitID)
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
	inBetweenCommitIDs := findAllInBetween(h.CommitHistory, beginCommitID, endCommitID)
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
			return false, err
		}
	}

	h.CommitHistory = append(h.CommitHistory, commitID)
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

	h.CommitHistory = h.CommitHistory[:len(h.CommitHistory)-1]
	delete(h.CommitMap, commitID)
	return removed, nil
}

func New[
	CommitID types.Comparable,
	Value any,
	Change any](
	valueHistory ValueHistory[CommitID, Value, Change],
) *History[CommitID, Value, Change] {
	return &History[CommitID, Value, Change]{
		CommitMap:     make(map[CommitID]VersionStatus),
		ValueHistory:  valueHistory,
		CommitHistory: make([]CommitID, 0),
	}
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
