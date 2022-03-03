package query

import (
	"fmt"
)

type Collector[Item any] func(items []Item) []Item

func Find[Item any](filter Filter[Item]) Collector[Item] {
	return func(items []Item) []Item {
		found := make([]Item, 0)
		for _, item := range items {
			if filter(item) {
				found = append(found, item)
			}
		}

		return found
	}
}

func Take[Item any](collector Collector[Item], topCount int) Collector[Item] {
	return func(items []Item) []Item {
		collected := collector(items)
		return collected[:topCount]
	}
}

// Sort

func Asc[Item any](collector Collector[Item], selector Selector[Item]) Collector[Item] {
	return func(items []Item) []Item {
		// TODO: implement me
		return items
	}
}

func Desc[Item any](collector Collector[Item], selector Selector[Item]) Collector[Item] {
	return func(items []Item) []Item {
		// TODO: implement me
		return items
	}
}

// Group collector

type GroupCollector[Item any] func(items []Item) Groups[Item]

func GroupBy[Item any](collector Collector[Item], selector Selector[Item]) GroupCollector[Item] {
	return func(items []Item) Groups[Item] {
		collected := collector(items)
		groups := make(Groups[Item])
		for _, item := range collected {
			key := fmt.Sprintf("%v", selector(item))
			groups[key] = append(([]Item)(groups[key]), item)
		}

		return groups
	}
}

func EachGroup[Item any](groupCollector GroupCollector[Item], collector Collector[Item]) GroupCollector[Item] {
	return func(items []Item) Groups[Item] {
		collected := groupCollector(items)
		newGroups := make(Groups[Item])
		for value, entries := range collected {
			newGroups[value] = collector(entries)
		}

		return newGroups
	}
}
