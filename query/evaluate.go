package query

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"tstore/data"
	"tstore/history"
	"tstore/query/lang"
	"tstore/types"
)

type SelectorCreator[Item any] func(attribute string) (Selector[Item], error)

func evaluateCollector[Item any](createAttributeSelector SelectorCreator[Item], expression lang.Expression) (Collector[Item], error) {
	collector, dataType, err := evaluateExpression(createAttributeSelector, expression)
	if err != nil {
		return nil, err
	}

	if dataType != lang.CollectorExpressionDataType {
		return nil, errors.New("must be collector")
	}

	return collector.(Collector[Item]), nil
}

func evaluateGroupCollector[Item any](createAttributeSelector SelectorCreator[Item], expression lang.Expression) (GroupCollector[Item], error) {
	collector, dataType, err := evaluateExpression(createAttributeSelector, expression)
	if err != nil {
		return nil, err
	}

	if dataType != lang.GroupCollectorExpressionDataType {
		return nil, errors.New("must be groupCollector")
	}

	return collector.(GroupCollector[Item]), nil
}

func evaluateExpression[Item any](
	createAttributeSelector SelectorCreator[Item],
	expression lang.Expression,
) (interface{}, lang.DataType, error) {
	if expression.IsValue {
		value, err := lang.ParseValue(expression.OutputDataType, expression.Value)
		return value, expression.OutputDataType, err
	}

	switch expression.Operator {
	case lang.AndOperator:
		if len(expression.Inputs) != 2 {
			return nil, "", errors.New("and must have 2 parameters")
		}

		return evaluateAnd(createAttributeSelector, expression.Inputs[0], expression.Inputs[1])
	case lang.OrOperator:
		if len(expression.Inputs) != 2 {
			return nil, "", errors.New("and must have 2 parameters")
		}

		return evaluateOr(createAttributeSelector, expression.Inputs[0], expression.Inputs[1])
	case lang.NotOperator:
		if len(expression.Inputs) != 1 {
			return nil, "", errors.New("and must have 1 parameter")
		}

		return evaluateNot(createAttributeSelector, expression.Inputs[0])
	case lang.AllOperator:
		return All[Item], lang.FilterExpressionDataType, nil
	case lang.EqualToOperator:
		if len(expression.Inputs) != 2 {
			return nil, "", errors.New("and must have 2 parameters")
		}

		return evaluateEqualTo(createAttributeSelector, expression.Inputs[0], expression.Inputs[1])
	case lang.ContainsOperator:
		if len(expression.Inputs) != 2 {
			return nil, "", errors.New("and must have 2 parameters")
		}

		return evaluateContains(createAttributeSelector, expression.Inputs[0], expression.Inputs[1])
	case
		lang.LessThanOperator,
		lang.LessThanOrEqualToOperator,
		lang.GreaterThanOperator,
		lang.GreaterThanOrEqualToOperator:
		if len(expression.Inputs) != 2 {
			return nil, "", errors.New("and must have 2 parameters")
		}

		return evaluateComparison(
			createAttributeSelector,
			expression.Operator,
			expression.Inputs[0],
			expression.Inputs[1])
	case lang.FindOperator:
		if len(expression.Inputs) != 1 {
			return nil, "", errors.New("and must have 1 parameter")
		}

		return evaluateFind(createAttributeSelector, expression.Inputs[0])
	case lang.TakeOperator:
		if len(expression.Inputs) != 2 {
			return nil, "", errors.New("and must have 2 parameters")
		}

		return evaluateTake(createAttributeSelector, expression.Inputs[0], expression.Inputs[1])
	case lang.AscOperator:
		if len(expression.Inputs) != 2 {
			return nil, "", errors.New("and must have 2 parameters")
		}

		return evaluateSort(createAttributeSelector, expression.Inputs[0], expression.Inputs[1], Asc[Item])
	case lang.DescOperator:
		if len(expression.Inputs) != 2 {
			return nil, "", errors.New("and must have 2 parameters")
		}

		return evaluateSort(
			createAttributeSelector,
			expression.Inputs[0],
			expression.Inputs[1],
			Desc[Item])
	case lang.GroupByOperator:
		if len(expression.Inputs) != 2 {
			return nil, "", errors.New("and must have 2 parameters")
		}

		return evaluateGroupBy(createAttributeSelector, expression.Inputs[0], expression.Inputs[1])
	case lang.EachGroupOperator:
		if len(expression.Inputs) != 2 {
			return nil, "", errors.New("and must have 2 parameters")
		}

		return evaluateEachGroup(createAttributeSelector, expression.Inputs[0], expression.Inputs[1])
	default:
		return nil, "", fmt.Errorf("unknown operator: %v", expression.Operator)
	}
}

type AndEvaluator struct {
	first  lang.Expression
	second lang.Expression
}

func evaluateAnd[Item any](createAttributeSelector SelectorCreator[Item], filter1 lang.Expression, filter2 lang.Expression) (Filter[Item], lang.DataType, error) {
	filter1Result, dataType, err := evaluateExpression(createAttributeSelector, filter1)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.FilterExpressionDataType {
		return nil, "", errors.New("only accept filter as the 1st parameter")
	}

	filter2Result, dataType, err := evaluateExpression(createAttributeSelector, filter2)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.FilterExpressionDataType {
		return nil, "", errors.New("only accept filter as the 2nd parameter")
	}

	return And(filter1Result.(Filter[Item]), filter2Result.(Filter[Item])), lang.FilterExpressionDataType, nil
}

func evaluateOr[Item any](
	createAttributeSelector SelectorCreator[Item],
	filter1 lang.Expression,
	filter2 lang.Expression,
) (Filter[Item], lang.DataType, error) {
	filter1Result, dataType, err := evaluateExpression(createAttributeSelector, filter1)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.FilterExpressionDataType {
		return nil, "", errors.New("only accept filter as the 1st parameter")
	}

	filter2Result, dataType, err := evaluateExpression(createAttributeSelector, filter2)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.FilterExpressionDataType {
		return nil, "", errors.New("only accept filter as the 2nd parameter")
	}

	return Or[Item](filter1Result.(Filter[Item]), filter2Result.(Filter[Item])), lang.FilterExpressionDataType, nil
}

func evaluateNot[Item any](
	createAttributeSelector SelectorCreator[Item],
	filter lang.Expression,
) (Filter[Item], lang.DataType, error) {
	filterResult, dataType, err := evaluateExpression(createAttributeSelector, filter)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.FilterExpressionDataType {
		return nil, "", errors.New("only accept filter as parameter")
	}

	return Not[Item](filterResult.(Filter[Item])), lang.FilterExpressionDataType, nil
}

func evaluateEqualTo[Item any](
	createAttributeSelector SelectorCreator[Item],
	attribute lang.Expression,
	target lang.Expression,
) (Filter[Item], lang.DataType, error) {
	attributeResult, dataType, err := evaluateExpression(createAttributeSelector, attribute)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.StringDataType {
		return nil, "", errors.New("only accept string as the 1st parameter")
	}

	targetResult, dataType, err := evaluateExpression(createAttributeSelector, target)
	if err != nil {
		return nil, "", err
	}

	selector, err := createAttributeSelector(attributeResult.(string))
	if err != nil {
		return nil, "", err
	}

	switch dataType {
	case lang.IntDataType:
		return EqualTo[Item, int](selector, targetResult.(int)), lang.FilterExpressionDataType, nil
	case lang.DecimalDataType:
		return EqualTo[Item, float64](selector, targetResult.(float64)), lang.FilterExpressionDataType, nil
	case lang.StringDataType:
		return EqualTo[Item, string](selector, targetResult.(string)), lang.FilterExpressionDataType, nil
	case lang.RuneDataType:
		return EqualTo[Item, rune](selector, targetResult.(rune)), lang.FilterExpressionDataType, nil
	case lang.BoolDataType:
		return EqualTo[Item, bool](selector, targetResult.(bool)), lang.FilterExpressionDataType, nil
	case lang.DatetimeDataType:
		return EqualTo[Item, time.Time](selector, targetResult.(time.Time)), lang.FilterExpressionDataType, nil
	default:
		return nil, "", fmt.Errorf("unsupported data type: %v", dataType)
	}
}

func evaluateContains[Item any](
	createAttributeSelector SelectorCreator[Item],
	attribute lang.Expression,
	target lang.Expression,
) (Filter[Item], lang.DataType, error) {
	attributeResult, dataType, err := evaluateExpression(createAttributeSelector, attribute)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.StringDataType {
		return nil, "", errors.New("only accept string as the 1st parameter")
	}

	targetResult, dataType, err := evaluateExpression(createAttributeSelector, target)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.StringDataType {
		return nil, "", errors.New("only accept string as the 2nd parameter")
	}

	selector, err := createAttributeSelector(attributeResult.(string))
	if err != nil {
		return nil, "", err
	}
	return Contains[Item](selector, targetResult.(string)), lang.FilterExpressionDataType, nil
}

func evaluateComparison[Item any](
	createAttributeSelector SelectorCreator[Item],
	operator lang.Operator,
	attribute lang.Expression,
	target lang.Expression,
) (Filter[Item], lang.DataType, error) {
	attributeResult, dataType, err := evaluateExpression(createAttributeSelector, attribute)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.StringDataType {
		return nil, "", errors.New("only accept string as the 1st parameter")
	}

	targetResult, dataType, err := evaluateExpression(createAttributeSelector, target)
	if err != nil {
		return nil, "", err
	}

	switch dataType {
	case lang.IntDataType:
		return createComparisonFilter[Item](createAttributeSelector, operator, attributeResult.(string), targetResult.(int))
	case lang.DecimalDataType:
		return createComparisonFilter[Item](createAttributeSelector, operator, attributeResult.(string), targetResult.(float64))
	case lang.StringDataType:
		return createComparisonFilter[Item](createAttributeSelector, operator, attributeResult.(string), targetResult.(string))
	case lang.RuneDataType:
		return createComparisonFilter[Item](createAttributeSelector, operator, attributeResult.(string), targetResult.(rune))
	default:
		return nil, "", fmt.Errorf("unsupported data type: %v", dataType)
	}
}

func createComparisonFilter[Item any, Value types.Comparable](
	createAttributeSelector SelectorCreator[Item],
	operator lang.Operator,
	attribute string,
	target Value,
) (Filter[Item], lang.DataType, error) {
	selector, err := createAttributeSelector(attribute)
	if err != nil {
		return nil, "", err
	}

	switch operator {
	case lang.LessThanOperator:
		return LessThan[Item](selector, target), lang.FilterExpressionDataType, nil
	case lang.LessThanOrEqualToOperator:
		return LessThanOrEqualTo[Item](selector, target), lang.FilterExpressionDataType, nil
	case lang.GreaterThanOperator:
		return GreaterThan[Item](selector, target), lang.FilterExpressionDataType, nil
	case lang.GreaterThanOrEqualToOperator:
		return GreaterThanOrEqualTo[Item](selector, target), lang.FilterExpressionDataType, nil
	default:
		return nil, "", fmt.Errorf("unsupported operator: %v", operator)
	}
}
func evaluateFind[Item any](
	createAttributeSelector SelectorCreator[Item],
	filter lang.Expression,
) (Collector[Item], lang.DataType, error) {
	filterResult, dataType, err := evaluateExpression(createAttributeSelector, filter)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.FilterExpressionDataType {
		return nil, "", errors.New("only accept filter as the 1st parameter")
	}

	return Find(filterResult.(Filter[Item])), lang.CollectorExpressionDataType, nil
}

func evaluateTake[Item any](
	createAttributeSelector SelectorCreator[Item],
	collector lang.Expression,
	topCount lang.Expression,
) (Collector[Item], lang.DataType, error) {
	collectorResult, dataType, err := evaluateExpression(createAttributeSelector, collector)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.CollectorExpressionDataType {
		return nil, "", errors.New("only accept collector as the 1st parameter")
	}

	topCountResult, dataType, err := evaluateExpression(createAttributeSelector, topCount)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.IntDataType {
		return nil, "", errors.New("only accept int as the 2nd parameter")
	}

	return Take(collectorResult.(Collector[Item]), topCountResult.(int)), lang.CollectorExpressionDataType, nil
}

func evaluateSort[Item any](
	createAttributeSelector SelectorCreator[Item],
	collector lang.Expression,
	attribute lang.Expression,
	createSortCollector func(collector Collector[Item], selector Selector[Item]) Collector[Item],
) (Collector[Item], lang.DataType, error) {
	collectorResult, dataType, err := evaluateExpression(createAttributeSelector, collector)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.CollectorExpressionDataType {
		return nil, "", errors.New("only accept collector as the 1st parameter")
	}

	attributeResult, dataType, err := evaluateExpression(createAttributeSelector, attribute)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.StringDataType {
		return nil, "", errors.New("only accept string as the 2nd parameter")
	}

	selector, err := createAttributeSelector(attributeResult.(string))
	if err != nil {
		return nil, "", err
	}

	return createSortCollector(collectorResult.(Collector[Item]), selector), lang.CollectorExpressionDataType, nil
}

func evaluateGroupBy[Item any](
	createAttributeSelector SelectorCreator[Item],
	collector lang.Expression,
	attribute lang.Expression,
) (GroupCollector[Item], lang.DataType, error) {
	collectorResult, dataType, err := evaluateExpression(createAttributeSelector, collector)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.CollectorExpressionDataType {
		return nil, "", errors.New("only accept collector as the 1st parameter")
	}

	attributeResult, dataType, err := evaluateExpression(createAttributeSelector, attribute)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.StringDataType {
		return nil, "", errors.New("only accept string as the 2nd parameter")
	}

	selector, err := createAttributeSelector(attributeResult.(string))
	if err != nil {
		return nil, "", err
	}

	return GroupBy[Item](collectorResult.(Collector[Item]), selector), lang.GroupCollectorExpressionDataType, nil
}

func evaluateEachGroup[Item any](
	createAttributeSelector SelectorCreator[Item],
	groupCollector lang.Expression,
	collector lang.Expression,
) (GroupCollector[Item], lang.DataType, error) {
	groupCollectorResult, dataType, err := evaluateExpression(createAttributeSelector, groupCollector)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.GroupCollectorExpressionDataType {
		return nil, "", errors.New("only accept group collector as the 1st parameter")
	}

	collectorResult, dataType, err := evaluateExpression(createAttributeSelector, collector)
	if err != nil {
		return nil, "", err
	}
	if dataType != lang.CollectorExpressionDataType {
		return nil, "", errors.New("only accept collector as the 2nd parameter")
	}

	finalGroupCollector := EachGroup[Item](groupCollectorResult.(GroupCollector[Item]), collectorResult.(Collector[Item]))
	return finalGroupCollector, lang.GroupCollectorExpressionDataType, nil
}

func CreateEntityAttributeSelector(attribute string) (Selector[data.Entity], error) {
	switch attribute {
	case lang.IDAttribute:
		return func(entity data.Entity) interface{} {
			return entity.ID
		}, nil
	case lang.SchemaAttribute:
		return func(entity data.Entity) interface{} {
			return entity.SchemaName
		}, nil
	default:
		return func(entity data.Entity) interface{} {
			return entity.Attributes[attribute]
		}, nil
	}
}

func CreateEntityVersionAttributeSelector(attribute string) (Selector[history.Version[data.Entity]], error) {
	switch attribute {
	case "Status":
		return func(version history.Version[data.Entity]) interface{} {
			return version.Status
		}, nil
	default:
		paths := strings.Split(attribute, "/")
		if len(paths) < 2 {
			return nil, fmt.Errorf("invalid attribute: %v", attribute)
		}

		return func(version history.Version[data.Entity]) interface{} {
			selector, _ := CreateEntityAttributeSelector(paths[1])
			return selector(version.Value)
		}, nil
	}
}
