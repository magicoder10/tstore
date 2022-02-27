package lang

type Expression struct {
	IsValue        bool
	Value          string
	Operator       Operator
	Inputs         []Expression
	OutputDataType DataType
}
