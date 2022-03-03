package query

import (
	"strings"

	"tstore/types"
)

type Filter[Item any] func(item Item) bool

// Logical filters

func And[Item any](filter1 Filter[Item], filter2 Filter[Item]) Filter[Item] {
	return func(item Item) bool {
		return filter1(item) && filter2(item)
	}
}

func Or[Item any](filter1 Filter[Item], filter2 Filter[Item]) Filter[Item] {
	return func(item Item) bool {
		return filter1(item) || filter2(item)
	}
}

func Not[Item any](filter Filter[Item]) Filter[Item] {
	return func(item Item) bool {
		return !filter(item)
	}
}

// Comparison filters

func All[Item any](item Item) bool {
	return true
}

type Selector[Item any] func(item Item) interface{}

func EqualTo[Item any, Value types.Equatable](selector Selector[Item], target Value) Filter[Item] {
	return func(item Item) bool {
		return selector(item).(Value) == target
	}
}

func Contains[Item any](selector Selector[Item], target string) Filter[Item] {
	return func(item Item) bool {
		return strings.Contains(selector(item).(string), target)
	}
}

func GreaterThan[Item any, Value types.Comparable](selector Selector[Item], target Value) Filter[Item] {
	return func(item Item) bool {
		return selector(item).(Value) > target
	}
}

func GreaterThanOrEqualTo[Item any, Value types.Comparable](selector Selector[Item], target Value) Filter[Item] {
	return func(item Item) bool {
		return selector(item).(Value) >= target
	}
}

func LessThan[Item any, Value types.Comparable](selector Selector[Item], target Value) Filter[Item] {
	return func(item Item) bool {
		return selector(item).(Value) < target
	}
}

func LessThanOrEqualTo[Item any, Value types.Comparable](selector Selector[Item], target Value) Filter[Item] {
	return func(item Item) bool {
		return selector(item).(Value) <= target
	}
}
