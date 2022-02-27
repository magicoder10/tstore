package lang

type Collector Expression

func Find(filter Filter) Collector {
	return Collector(Expression{
		IsValue:        false,
		Operator:       FindOperator,
		Inputs:         []Expression{Expression(filter)},
		OutputDataType: CollectorExpressionDataType,
	})
}

func Take(collector Collector, topCount int) Collector {
	return Collector(Expression{
		IsValue:  false,
		Operator: TakeOperator,
		Inputs: []Expression{
			Expression(collector),
			{
				IsValue:        true,
				OutputDataType: GetDataType(topCount),
				Value:          String(topCount),
			},
		},
		OutputDataType: CollectorExpressionDataType,
	})
}

func Asc(collector Collector, attribute string) Collector {
	return Collector(Expression{
		IsValue:  false,
		Operator: AscOperator,
		Inputs: []Expression{
			Expression(collector),
			{
				IsValue:        true,
				OutputDataType: GetDataType(attribute),
				Value:          String(attribute),
			},
		},
		OutputDataType: CollectorExpressionDataType,
	})
}

func Desc(collector Collector, attribute string) Collector {
	return Collector(Expression{
		IsValue:  false,
		Operator: DescOperator,
		Inputs: []Expression{
			Expression(collector),
			{
				IsValue:        true,
				OutputDataType: GetDataType(attribute),
				Value:          String(attribute),
			},
		},
		OutputDataType: CollectorExpressionDataType,
	})
}

type GroupCollector Expression

func GroupBy(collector Collector, attribute string) GroupCollector {
	return GroupCollector(Expression{
		IsValue:  false,
		Operator: GroupByOperator,
		Inputs: []Expression{
			Expression(collector),
			{
				IsValue:        true,
				OutputDataType: GetDataType(attribute),
				Value:          String(attribute),
			},
		},
		OutputDataType: GroupCollectorExpressionDataType,
	})
}

func EachGroup(groupCollector GroupCollector, collector Collector) GroupCollector {
	return GroupCollector(Expression{
		IsValue:  false,
		Operator: EachGroupOperator,
		Inputs: []Expression{
			Expression(groupCollector),
			Expression(collector),
		},
		OutputDataType: GroupCollectorExpressionDataType,
	})
}
