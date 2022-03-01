package history

import (
	"fmt"

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

func (h History[CommitID, Value, Change]) Value(targetCommitID CommitID) (Value, bool) {
	endCommitID, err := findLargestSmallerThan[CommitID](h.CommitHistory, targetCommitID)
	if err != nil {
		return *new(Value), false
	}

	versionStatus := h.CommitMap[endCommitID]
	if versionStatus == DeletedVersionStatus {
		return *new(Value), false
	} else {
		return h.ValueHistory.Value(endCommitID), true
	}
}

func (h History[CommitID, Value, Change]) ChangesBetween(
	beginCommitID CommitID,
	endCommitID CommitID,
) []Version[Value] {
	inBetweenCommitIDs := findAllInBetween(h.CommitHistory, beginCommitID, endCommitID)
	var versions []Version[Value]

	for _, commitID := range inBetweenCommitIDs {
		versionStatus := h.CommitMap[commitID]

		var value Value
		if versionStatus != DeletedVersionStatus {
			value = h.ValueHistory.Value(commitID)
		}

		version := Version[Value]{
			Status: versionStatus,
			Value:  value,
		}
		versions = append(versions, version)
	}

	return versions
}

func (h *History[CommitID, Value, Change]) AddNewVersion(
	commitID CommitID,
	versionStatus VersionStatus,
	change Change,
) bool {
	_, ok := h.CommitMap[commitID]
	if ok {
		return false
	}

	if versionStatus != DeletedVersionStatus {
		h.ValueHistory.AddNewVersion(commitID, change)
	}

	h.CommitHistory = append(([]CommitID)(h.CommitHistory), commitID)
	h.CommitMap[commitID] = versionStatus

	return true
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

func findLargestSmallerThan[Item types.Comparable](sortedItems []Item, end Item) (Item, error) {
	for index := len(sortedItems) - 1; index >= 0; index-- {
		item := sortedItems[index]
		if item <= end {
			return item, nil
		}
	}

	return *new(Item), fmt.Errorf("item not found")
}
