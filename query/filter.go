package query

import (
	"strings"

	"tstore/data"
)

type Filter func(entity data.Entity) bool

// Logical filters

func And(filter1 Filter, filter2 Filter) Filter {
	return func(entity data.Entity) bool {
		return filter1(entity) && filter2(entity)
	}
}

func Or(filter1 Filter, filter2 Filter) Filter {
	return func(entity data.Entity) bool {
		return filter1(entity) || filter2(entity)
	}
}

func Not(filter Filter) Filter {
	return func(entity data.Entity) bool {
		return !filter(entity)
	}
}

// Comparison filters

type Selector func(entity data.Entity) interface{}

func EqualTo[Value data.Equatable](selector Selector, target Value) Filter {
	return func(entity data.Entity) bool {
		return selector(entity).(Value) == target
	}
}

func Contains(selector Selector, target string) Filter {
	return func(entity data.Entity) bool {
		return strings.Contains(selector(entity).(string), target)
	}
}

func GreaterThan[Value data.Comparable](selector Selector, target Value) Filter {
	return func(entity data.Entity) bool {
		return selector(entity).(Value) > target
	}
}

func GreaterThanOrEqualTo[Value data.Comparable](selector Selector, target Value) Filter {
	return func(entity data.Entity) bool {
		return selector(entity).(Value) >= target
	}
}

func LessThan[Value data.Comparable](selector Selector, target Value) Filter {
	return func(entity data.Entity) bool {
		return selector(entity).(Value) < target
	}
}

func LessThanOrEqualTo[Value data.Comparable](selector Selector, target Value) Filter {
	return func(entity data.Entity) bool {
		return selector(entity).(Value) <= target
	}
}
