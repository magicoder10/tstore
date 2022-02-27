package query

import (
	"fmt"

	"tstore/data"
)

type Collector = func(entities []data.Entity) []data.Entity

func Find(filter Filter) Collector {
	return func(entities []data.Entity) []data.Entity {
		found := make([]data.Entity, 0)
		for _, entity := range entities {
			if filter(entity) {
				found = append(found, entity)
			}
		}

		return found
	}
}

func Take(collector Collector, topCount int) Collector {
	return func(input []data.Entity) []data.Entity {
		collected := collector(input)
		return collected[:topCount]
	}
}

// Sort

func Asc(collector Collector, selector Selector) Collector {
	return func(input []data.Entity) []data.Entity {
		// TODO: implement me
		return input
	}
}

func Desc(collector Collector, selector Selector) Collector {
	return func(input []data.Entity) []data.Entity {
		// TODO: implement me
		return input
	}
}

// Group collector

type GroupCollector func(input []data.Entity) Groups

func GroupBy(collector Collector, selector Selector) GroupCollector {
	return func(input []data.Entity) Groups {
		collected := collector(input)
		groups := make(Groups)
		for _, entity := range collected {
			key := fmt.Sprintf("%v", selector(entity))
			groups[key] = append(groups[key], entity)
		}

		return groups
	}
}

func EachGroup(groupCollector GroupCollector, collector Collector) GroupCollector {
	return func(input []data.Entity) Groups {
		collected := groupCollector(input)
		newGroups := make(Groups)
		for value, entities := range collected {
			newGroups[value] = collector(entities)
		}

		return newGroups
	}
}
