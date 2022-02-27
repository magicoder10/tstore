package proto

import (
	"fmt"

	"tstore/data"
	"tstore/mutation"
	"tstore/query"
	"tstore/query/lang"
)

var fromProtoDataType = map[DataType]lang.DataType{
	DataType_Int:      lang.IntDataType,
	DataType_Decimal:  lang.DecimalDataType,
	DataType_Bool:     lang.BoolDataType,
	DataType_String:   lang.StringDataType,
	DataType_Rune:     lang.RuneDataType,
	DataType_Datetime: lang.DatetimeDataType,
}

var fromProtoMutationType = map[MutationType]mutation.Type{
	MutationType_CreateSchema:           mutation.CreateSchemaMutation,
	MutationType_DeleteSchema:           mutation.DeleteSchemaMutation,
	MutationType_CreateSchemaAttributes: mutation.CreateSchemaAttributes,
	MutationType_DeleteSchemaAttributes: mutation.DeleteSchemaAttributes,
	MutationType_CreateEntity:           mutation.CreateEntityMutation,
	MutationType_DeleteEntity:           mutation.DeleteEntityMutation,
	MutationType_CreateEntityAttributes: mutation.CreateEntityAttributes,
	MutationType_DeleteEntityAttributes: mutation.DeleteEntityAttributes,
	MutationType_UpdateEntityAttributes: mutation.UpdateEntityAttributes,
}

var fromProtoOperator = map[Operator]lang.Operator{
	Operator_None:                 lang.NoneOperator,
	Operator_And:                  lang.AndOperator,
	Operator_Or:                   lang.OrOperator,
	Operator_Not:                  lang.NotOperator,
	Operator_EqualTo:              lang.EqualToOperator,
	Operator_Contains:             lang.ContainsOperator,
	Operator_LessThan:             lang.LessThanOperator,
	Operator_LessThanOrEqualTo:    lang.LessThanOrEqualToOperator,
	Operator_GreaterThan:          lang.GreaterThanOperator,
	Operator_GreaterThanOrEqualTo: lang.GreaterThanOrEqualToOperator,
	Operator_Find:                 lang.FindOperator,
	Operator_Take:                 lang.TakeOperator,
	Operator_Asc:                  lang.AscOperator,
	Operator_Desc:                 lang.DescOperator,
	Operator_GroupBy:              lang.GroupByOperator,
	Operator_EachGroup:            lang.EachGroupOperator,
}

func FromProtoTransactionInput(protoTransactionInput *Transaction) (mutation.TransactionInput, error) {
	mutationsMap := make(map[string][]mutation.Mutation)
	for schema, protoMutations := range protoTransactionInput.Mutations {
		mutations, err := fromProtoMutations(protoMutations)
		if err != nil {
			return mutation.TransactionInput{}, err
		}

		mutationsMap[schema] = mutations
	}

	return mutation.TransactionInput{Mutations: mutationsMap}, nil
}

func FromProtoExpression(protoExpression *Expression) *lang.Expression {
	if protoExpression == nil {
		return nil
	}

	inputs := make([]lang.Expression, 0)
	for _, inputProtoExpression := range protoExpression.Inputs {
		inputExpression := FromProtoExpression(inputProtoExpression)
		inputs = append(inputs, *inputExpression)
	}

	return &lang.Expression{
		IsValue:        protoExpression.IsValue,
		Value:          protoExpression.Value,
		Operator:       fromProtoOperator[protoExpression.Operator],
		Inputs:         inputs,
		OutputDataType: fromProtoDataType[protoExpression.OutputDataType],
	}
}

func FromProtoCommit(protoCommit *Commit) data.Commit {
	return data.Commit{
		CommittedTransactionID: protoCommit.CommittedTransactionId,
		CommittedAt:            protoCommit.CommittedAt.AsTime(),
	}
}

func FromProtoEntities(protoEntities *Entities) ([]data.Entity, error) {
	if protoEntities == nil {
		return nil, nil
	}

	entities := make([]data.Entity, 0)
	for _, protoEntity := range protoEntities.Entities {
		entity, err := fromProtoEntity(protoEntity)
		if err != nil {
			return nil, err
		}

		entities = append(entities, entity)
	}

	return entities, nil
}

func FromProtoGroups(protoGroups *Groups) (query.Groups, error) {
	groups := make(query.Groups)
	for key, protoEntities := range protoGroups.Groups {
		entities, err := FromProtoEntities(protoEntities)
		if err != nil {
			return nil, err
		}

		groups[key] = entities
	}

	return groups, nil
}

func fromProtoEntity(protoEntity *Entity) (data.Entity, error) {
	attributes := make(map[string]interface{})
	for attribute, protoValue := range protoEntity.Attributes {
		value, err := fromProtoValue(protoValue)
		if err != nil {
			return data.Entity{}, err
		}

		attributes[attribute] = value
	}

	return data.Entity{
		ID:         protoEntity.Id,
		SchemaName: protoEntity.SchemaName,
		Attributes: attributes,
	}, nil
}

func fromProtoMutations(protoMutations *Mutations) ([]mutation.Mutation, error) {
	if protoMutations == nil {
		return nil, nil
	}

	mutations := make([]mutation.Mutation, 0)
	for _, protoMutation := range protoMutations.Mutations {
		mut, err := fromProtoMutation(protoMutation)
		if err != nil {
			return nil, err
		}

		mutations = append(mutations, mut)
	}

	return mutations, nil
}

func fromProtoMutation(protoMutation *Mutation) (mutation.Mutation, error) {
	schemaInput, err := fromProtoSchemaInput(protoMutation.SchemaInput)
	if err != nil {
		return mutation.Mutation{}, err
	}

	entityInput, err := fromProtoEntityInput(protoMutation.EntityInput)
	if err != nil {
		return mutation.Mutation{}, err
	}

	return mutation.Mutation{
		Type:        fromProtoMutationType[protoMutation.Type],
		SchemaInput: schemaInput,
		EntityInput: entityInput,
	}, nil
}

func fromProtoSchemaInput(protoSchemaInput *SchemaInput) (mutation.SchemaInput, error) {
	if protoSchemaInput == nil {
		return mutation.SchemaInput{}, nil
	}

	createOrUpdateAttributes := make(map[string]data.Type)
	for attribute, dataType := range protoSchemaInput.AttributesToCreateOrUpdate {
		langDataType := fromProtoDataType[dataType]
		dbDataType, ok := lang.ToDatabaseDataType[langDataType]
		if !ok {
			return mutation.SchemaInput{}, fmt.Errorf("unsupported dataType: %v", dataType)
		}
		createOrUpdateAttributes[attribute] = dbDataType
	}

	return mutation.SchemaInput{
		Name:                       protoSchemaInput.Name,
		AttributesToCreateOrUpdate: createOrUpdateAttributes,
		AttributesToDelete:         protoSchemaInput.AttributesToDelete,
	}, nil
}

func fromProtoEntityInput(protoEntityInput *EntityInput) (mutation.EntityInput, error) {
	if protoEntityInput == nil {
		return mutation.EntityInput{}, nil
	}

	createOrUpdateAttributes := make(map[string]interface{})
	for attribute, protoValue := range protoEntityInput.AttributesToCreateOrUpdate {
		value, err := fromProtoValue(protoValue)
		if err != nil {
			return mutation.EntityInput{}, err
		}

		createOrUpdateAttributes[attribute] = value
	}

	return mutation.EntityInput{
		EntityID:                   protoEntityInput.EntityID,
		SchemaName:                 protoEntityInput.SchemaName,
		AttributesToCreateOrUpdate: createOrUpdateAttributes,
		AttributesToDelete:         protoEntityInput.AttributesToDelete,
	}, nil
}

func fromProtoValue(protoValue *Value) (interface{}, error) {
	return lang.ParseValue(fromProtoDataType[protoValue.Type], protoValue.Content)
}
