package lang

type Operator string

const (
	NoneOperator                 Operator = ""
	AndOperator                  Operator = "And"
	OrOperator                   Operator = "Or"
	NotOperator                  Operator = "Not"
	EqualToOperator              Operator = "EqualTo"
	ContainsOperator             Operator = "Contains"
	LessThanOperator             Operator = "LessThan"
	LessThanOrEqualToOperator    Operator = "LessThanOrEqualTo"
	GreaterThanOperator          Operator = "GreaterThan"
	GreaterThanOrEqualToOperator Operator = "GreaterThanOrEqualTo"
	FindOperator                 Operator = "Find"
	TakeOperator                 Operator = "Take"
	AscOperator                  Operator = "Asc"
	DescOperator                 Operator = "Desc"
	GroupByOperator              Operator = "GroupBy"
	EachGroupOperator            Operator = "EachGroup"
)
