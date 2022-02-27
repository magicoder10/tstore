package lang

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"tstore/data"
)

type DataType string

const (
	IntDataType                      DataType = "int"
	DecimalDataType                  DataType = "decimal"
	BoolDataType                     DataType = "bool"
	StringDataType                   DataType = "string"
	RuneDataType                     DataType = "rune"
	DatetimeDataType                 DataType = "datetime"
	FilterExpressionDataType         DataType = "filterExpression"
	CollectorExpressionDataType      DataType = "collectorExpression"
	GroupCollectorExpressionDataType DataType = "groupCollectorExpression"
)

var ToDatabaseDataType = map[DataType]data.Type{
	IntDataType:      data.IntDataType,
	DecimalDataType:  data.DecimalDataType,
	BoolDataType:     data.BoolDataType,
	StringDataType:   data.StringDataType,
	RuneDataType:     data.RuneDataType,
	DatetimeDataType: data.DatetimeDataType,
}

var FromDatabaseDataType = map[data.Type]DataType{
	data.IntDataType:      IntDataType,
	data.DecimalDataType:  DecimalDataType,
	data.BoolDataType:     BoolDataType,
	data.StringDataType:   StringDataType,
	data.RuneDataType:     RuneDataType,
	data.DatetimeDataType: DatetimeDataType,
}

func GetDataType(value interface{}) DataType {
	switch value.(type) {
	case int8, int16, int, int64, uint8, uint16, uint32, uint64:
		return IntDataType
	case float32, float64:
		return DecimalDataType
	case bool:
		return BoolDataType
	case string:
		return StringDataType
	case rune:
		return RuneDataType
	case time.Time:
		return DatetimeDataType
	default:
		return StringDataType
	}
}

func String(value interface{}) string {
	return fmt.Sprintf("%v", value)
}

func ParseValue(dataType DataType, input string) (interface{}, error) {

	switch dataType {
	case IntDataType:
		return strconv.ParseInt(input, 10, 64)
	case DecimalDataType:
		return strconv.ParseFloat(input, 64)
	case BoolDataType:
		return strconv.ParseBool(input)
	case StringDataType:
		return input, nil
	case RuneDataType:
		if len(input) != 1 {
			return nil, errors.New("must contain 1 rune")
		}

		return input[0], nil
	case DatetimeDataType:
		return time.Parse(time.RFC3339, input)
	default:
		return nil, errors.New("unknown expression")
	}
}
