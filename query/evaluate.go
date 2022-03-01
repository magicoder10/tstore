package query

import (
	"errors"
	"fmt"
	"time"

	"tstore/data"
	"tstore/query/lang"
	"tstore/types"
)

func evaluateCollector(expression lang.Expression) (Collector, error) {
	collector, dataType, err := evaluateExpression(expression)
	if err != nil {
		return nil, err
	}

	if dataType != lang.CollectorExpressionDataType {
		return nil, errors.New("must be collector")
	}

	return collector.(Collector), nil
}

func evaluateGroupCollector(expression lang.Expression) (GroupCollector, error) {
	collector, dataType, err := evaluateExpression(expression)
	if err != nil {
		return nil, err
	}

	if dataType != lang.GroupCollectorExpressionDataType {
		return nil, errors.New("must be groupCollector")
	}

	return collector.(GroupCollector), nil
}

func evaluateExpression(expression lang.Expression) (interface{}, lang.DataType, error) {
	if expression.IsValue {
		value, err := lang.ParseValue(expression.OutputDataType, expression.Value)
		return value, expression.OutputDataType, err
	}

	switch expression.Operator {
	case lang.AndOperator:
		if len(expression.Inputs) != 2 {
			return nil, "", errors.New("and must have 2 parameters")
		}

		return evaluateAnd(expression.Inputs[0], expression.Inputs[1])
	case lang.OrOperator:
		if len(expression.Inputs) != 2 {
			return nil, "", errors.New("and must have 2 parameters")
		}

		return evaluateOr(expression.Inputs[0], expression.Inputs[1])
	case lang.NotOperator:
		if len(expression.Inputs) != 1 {
			return nil, "", errors.New("and must have 1 parameter")
		}

		return evaluateNot(expression.Inputs[0])
	case lang.AllOperator:
		return All, lang.FilterExpressionDataType, nil
	case lang.EqualToOperator:
		if len(expression.Inputs) != 2 {
			return nil, "", errors.New("and must have 2 parameters")
		}

		return evaluateEqualTo(expression.Inputs[0], expression.Inputs[1])
	case lang.ContainsOperator:
		if len(expression.Inputs) != 2 {
			return nil, "", errors.New("and must have 2 parameters")
		}

		return evaluateContains(expression.Inputs[0], expression.Inputs[1])
	case
		lang.LessThanOperator,
		lang.LessThanOrEqualToOperator,
		lang.GreaterThanOperator,
		lang.GreaterThanOrEqualToOperator:
		if len(expression.Inputs) != 2 {
			return nil, "", errors.New("and must have 2 parameters")
		}

		return evaluateComparison(expression.Operator, expression.Inputs[0], expression.Inputs[1])
	case lang.FindOperator:
		if len(expression.Inputs) != 1 {
			return nil, "", errors.New("and must have 1 parameter")
		}

		return evaluateFind(expression.Inputs[0])
	case lang.TakeOperator:
		if len(expression.Inputs) != 2 {
			return nil, "", errors.New("and must have 2 parameters")
		}

		return evaluateTake(expression.Inputs[0], expression.Inputs[1])
	case lang.AscOperator:
		if len(expression.Inputs) != 2 {
			return nil, "", errors.New("and must have 2 parameters")
		}

		return evaluateSort(expression.Inputs[0], expression.Inputs[1], Asc)
	case lang.DescOperator:
		if len(expression.Inputs) != 2 {
			return nil, "", errors.New("and must have 2 parameters")
		}

		return evaluateSort(expression.Inputs[0], expression.Inputs[1], Desc)
	case lang.GroupByOperator:
		if len(expression.Inputs) != 2 {
			return nil, "", errors.New("and must have 2 parameters")
		}

		return evaluateGroupBy(expression.Inputs[0], expression.Inputs[1])
	case lang.EachGroupOperator:
		if len(expression.Inputs) != 2 {
			return nil, "", errors.New("and must have 2 parameters")
		}

		return evaluateEachGroup(expression.Inputs[0], expression.Inputs[1])
	default:
		return nil, "", fmt.Errorf("unknown operator: %v", expression.Operator)
	}
}

type AndEvaluator struct {
	first  lang.Expression
	second lang.Expression
}

func evaluateAnd(filter1 lang.Expression, filter2 lang.Expression) (Filter, lang.DataType, error) {
	filter1Result, dataType, err := evaluateExpression(filter1)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.FilterExpressionDataType {
		return nil, "", errors.New("only accept filter as the 1st parameter")
	}

	filter2Result, dataType, err := evaluateExpression(filter2)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.FilterExpressionDataType {
		return nil, "", errors.New("only accept filter as the 2nd parameter")
	}

	return And(filter1Result.(Filter), filter2Result.(Filter)), lang.FilterExpressionDataType, nil
}

func evaluateOr(filter1 lang.Expression, filter2 lang.Expression) (Filter, lang.DataType, error) {
	filter1Result, dataType, err := evaluateExpression(filter1)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.FilterExpressionDataType {
		return nil, "", errors.New("only accept filter as the 1st parameter")
	}

	filter2Result, dataType, err := evaluateExpression(filter2)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.FilterExpressionDataType {
		return nil, "", errors.New("only accept filter as the 2nd parameter")
	}

	return Or(filter1Result.(Filter), filter2Result.(Filter)), lang.FilterExpressionDataType, nil
}

func evaluateNot(filter lang.Expression) (Filter, lang.DataType, error) {
	filterResult, dataType, err := evaluateExpression(filter)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.FilterExpressionDataType {
		return nil, "", errors.New("only accept filter as parameter")
	}

	return Not(filterResult.(Filter)), lang.FilterExpressionDataType, nil
}

func evaluateEqualTo(attribute lang.Expression, target lang.Expression) (Filter, lang.DataType, error) {
	attributeResult, dataType, err := evaluateExpression(attribute)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.StringDataType {
		return nil, "", errors.New("only accept string as the 1st parameter")
	}

	targetResult, dataType, err := evaluateExpression(target)
	if err != nil {
		return nil, "", err
	}

	selector := getAttributeSelector(attributeResult.(string))

	switch dataType {
	case lang.IntDataType:
		return EqualTo(selector, targetResult.(int)), lang.FilterExpressionDataType, nil
	case lang.DecimalDataType:
		return EqualTo(selector, targetResult.(float64)), lang.FilterExpressionDataType, nil
	case lang.StringDataType:
		return EqualTo(selector, targetResult.(string)), lang.FilterExpressionDataType, nil
	case lang.RuneDataType:
		return EqualTo(selector, targetResult.(rune)), lang.FilterExpressionDataType, nil
	case lang.BoolDataType:
		return EqualTo(selector, targetResult.(bool)), lang.FilterExpressionDataType, nil
	case lang.DatetimeDataType:
		return EqualTo(selector, targetResult.(time.Time)), lang.FilterExpressionDataType, nil
	default:
		return nil, "", fmt.Errorf("unsupported data type: %v", dataType)
	}
}

func evaluateContains(attribute lang.Expression, target lang.Expression) (Filter, lang.DataType, error) {
	attributeResult, dataType, err := evaluateExpression(attribute)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.StringDataType {
		return nil, "", errors.New("only accept string as the 1st parameter")
	}

	targetResult, dataType, err := evaluateExpression(target)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.StringDataType {
		return nil, "", errors.New("only accept string as the 2nd parameter")
	}

	selector := getAttributeSelector(attributeResult.(string))
	return Contains(selector, targetResult.(string)), lang.FilterExpressionDataType, nil
}

func evaluateComparison(
	operator lang.Operator,
	attribute lang.Expression,
	target lang.Expression,
) (Filter, lang.DataType, error) {
	attributeResult, dataType, err := evaluateExpression(attribute)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.StringDataType {
		return nil, "", errors.New("only accept string as the 1st parameter")
	}

	targetResult, dataType, err := evaluateExpression(target)
	if err != nil {
		return nil, "", err
	}

	switch dataType {
	case lang.IntDataType:
		return createComparisonFilter(operator, attributeResult.(string), targetResult.(int))
	case lang.DecimalDataType:
		return createComparisonFilter(operator, attributeResult.(string), targetResult.(float64))
	case lang.StringDataType:
		return createComparisonFilter(operator, attributeResult.(string), targetResult.(string))
	case lang.RuneDataType:
		return createComparisonFilter(operator, attributeResult.(string), targetResult.(rune))
	default:
		return nil, "", fmt.Errorf("unsupported data type: %v", dataType)
	}
}

func createComparisonFilter[Value types.Comparable](operator lang.Operator, attribute string, target Value) (Filter, lang.DataType, error) {
	selector := getAttributeSelector(attribute)
	switch operator {
	case lang.LessThanOperator:
		return LessThan(selector, target), lang.FilterExpressionDataType, nil
	case lang.LessThanOrEqualToOperator:
		return LessThanOrEqualTo(selector, target), lang.FilterExpressionDataType, nil
	case lang.GreaterThanOperator:
		return GreaterThan(selector, target), lang.FilterExpressionDataType, nil
	case lang.GreaterThanOrEqualToOperator:
		return GreaterThanOrEqualTo(selector, target), lang.FilterExpressionDataType, nil
	default:
		return nil, "", fmt.Errorf("unsupported operator: %v", operator)
	}
}
func evaluateFind(filter lang.Expression) (Collector, lang.DataType, error) {
	filterResult, dataType, err := evaluateExpression(filter)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.FilterExpressionDataType {
		return nil, "", errors.New("only accept filter as the 1st parameter")
	}

	return Find(filterResult.(Filter)), lang.CollectorExpressionDataType, nil
}

func evaluateTake(collector lang.Expression, topCount lang.Expression) (Collector, lang.DataType, error) {
	collectorResult, dataType, err := evaluateExpression(collector)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.CollectorExpressionDataType {
		return nil, "", errors.New("only accept collector as the 1st parameter")
	}

	topCountResult, dataType, err := evaluateExpression(topCount)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.IntDataType {
		return nil, "", errors.New("only accept int as the 2nd parameter")
	}

	return Take(collectorResult.(Collector), topCountResult.(int)), lang.CollectorExpressionDataType, nil
}

func evaluateSort(
	collector lang.Expression,
	attribute lang.Expression,
	createSortCollector func(collector Collector, selector Selector) Collector,
) (Collector, lang.DataType, error) {
	collectorResult, dataType, err := evaluateExpression(collector)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.CollectorExpressionDataType {
		return nil, "", errors.New("only accept collector as the 1st parameter")
	}

	attributeResult, dataType, err := evaluateExpression(attribute)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.StringDataType {
		return nil, "", errors.New("only accept string as the 2nd parameter")
	}

	selector := getAttributeSelector(attributeResult.(string))
	return createSortCollector(collectorResult.(Collector), selector), lang.CollectorExpressionDataType, nil
}

func evaluateGroupBy(
	collector lang.Expression,
	attribute lang.Expression,
) (GroupCollector, lang.DataType, error) {
	collectorResult, dataType, err := evaluateExpression(collector)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.CollectorExpressionDataType {
		return nil, "", errors.New("only accept collector as the 1st parameter")
	}

	attributeResult, dataType, err := evaluateExpression(attribute)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.StringDataType {
		return nil, "", errors.New("only accept string as the 2nd parameter")
	}

	selector := getAttributeSelector(attributeResult.(string))
	return GroupBy(collectorResult.(Collector), selector), lang.GroupCollectorExpressionDataType, nil
}

func evaluateEachGroup(
	groupCollector lang.Expression,
	collector lang.Expression,
) (GroupCollector, lang.DataType, error) {
	groupCollectorResult, dataType, err := evaluateExpression(groupCollector)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.GroupCollectorExpressionDataType {
		return nil, "", errors.New("only accept group collector as the 1st parameter")
	}

	collectorResult, dataType, err := evaluateExpression(collector)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.CollectorExpressionDataType {
		return nil, "", errors.New("only accept collector as the 2nd parameter")
	}

	finalGroupCollector := EachGroup(groupCollectorResult.(GroupCollector), collectorResult.(Collector))
	return finalGroupCollector, lang.GroupCollectorExpressionDataType, nil
}

func getAttributeSelector(attribute string) Selector {
	switch attribute {
	case lang.IDAttribute:
		return func(entity data.Entity) interface{} {
			return entity.ID
		}
	case lang.SchemaAttribute:
		return func(entity data.Entity) interface{} {
			return entity.SchemaName
		}
	default:
		return func(entity data.Entity) interface{} {
			return entity.Attributes[attribute]
		}
	}
}
