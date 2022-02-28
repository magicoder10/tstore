package lang

import (
	"tstore/types"
)

type Filter Expression

// Logical filters

func And(filter1 Filter, filter2 Filter) Filter {
	return Filter(Expression{
		IsValue:        false,
		Operator:       AndOperator,
		Inputs:         []Expression{Expression(filter1), Expression(filter2)},
		OutputDataType: FilterExpressionDataType,
	})
}

func Or(filter1 Filter, filter2 Filter) Filter {
	return Filter(Expression{
		IsValue:        false,
		Operator:       OrOperator,
		Inputs:         []Expression{Expression(filter1), Expression(filter2)},
		OutputDataType: FilterExpressionDataType,
	})
}

func Not(filter Filter) Filter {
	return Filter(Expression{
		IsValue:        false,
		Operator:       NotOperator,
		Inputs:         []Expression{Expression(filter)},
		OutputDataType: FilterExpressionDataType,
	})
}

// Comparison filters

func EqualTo[Value comparable](attribute string, target Value) Filter {
	return comparison(EqualToOperator, attribute, target)
}

func Contain[Value comparable](attribute string, target Value) Filter {
	return comparison(ContainsOperator, attribute, target)
}

func GreaterThan[Value types.Comparable](attribute string, target Value) Filter {
	return comparison(GreaterThanOperator, attribute, target)
}

func GreaterThanOrEqualTo[Value types.Comparable](attribute string, target Value) Filter {
	return comparison(GreaterThanOrEqualToOperator, attribute, target)
}

func LessThan[Value types.Comparable](attribute string, target Value) Filter {
	return comparison(LessThanOperator, attribute, target)
}

func LessThanOrEqualTo[Value types.Comparable](attribute string, target Value) Filter {
	return comparison(LessThanOrEqualToOperator, attribute, target)
}

func comparison[Value any](operator Operator, attribute string, target Value) Filter {
	return Filter(Expression{
		IsValue:  false,
		Operator: operator,
		Inputs: []Expression{
			{
				IsValue:        true,
				OutputDataType: StringDataType,
				Value:          attribute,
			},
			{
				IsValue:        true,
				OutputDataType: GetDataType(target),
				Value:          String(target),
			},
		},
		OutputDataType: FilterExpressionDataType,
	})
}
