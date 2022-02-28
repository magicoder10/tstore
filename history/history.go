package history

import (
	"fmt"

	"tstore/types"
)

type History[
	CommitID types.Comparable,
	Value any,
	Change any] struct {
	commitMap     map[CommitID]VersionStatus
	valueHistory  ValueHistory[CommitID, Value, Change]
	commitHistory []CommitID
}

func (h History[CommitID, Value, Change]) Value(targetCommitID CommitID) (Value, bool) {
	endCommitID, err := findLargestSmallerThan[CommitID](h.commitHistory, targetCommitID)
	if err != nil {
		return *new(Value), false
	}

	versionStatus := h.commitMap[endCommitID]
	if versionStatus == DeletedVersionStatus {
		return *new(Value), false
	} else {
		return h.valueHistory.Value(endCommitID), true
	}
}

func (h History[CommitID, Value, Change]) ChangesBetween(
	beginCommitID CommitID,
	endCommitID CommitID,
) []Version[Value] {
	inBetweenCommitIDs := findAllInBetween(h.commitHistory, beginCommitID, endCommitID)
	var versions []Version[Value]

	for _, commitID := range inBetweenCommitIDs {
		versionStatus := h.commitMap[commitID]

		var value Value
		if versionStatus != DeletedVersionStatus {
			value = h.valueHistory.Value(commitID)
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
	_, ok := h.commitMap[commitID]
	if ok {
		return false
	}

	if versionStatus != DeletedVersionStatus {
		h.valueHistory.AddNewVersion(commitID, change)
	}

	h.commitHistory = append(([]CommitID)(h.commitHistory), commitID)
	h.commitMap[commitID] = versionStatus

	return true
}

func New[
	CommitID types.Comparable,
	Value any,
	Change any](
	valueHistory ValueHistory[CommitID, Value, Change],
) *History[CommitID, Value, Change] {
	return &History[CommitID, Value, Change]{
		commitMap:     make(map[CommitID]VersionStatus),
		valueHistory:  valueHistory,
		commitHistory: make([]CommitID, 0),
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
