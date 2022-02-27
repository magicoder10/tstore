package proto

import (
	"tstore/data"
	"tstore/mutation"
	"tstore/query/lang"

	"google.golang.org/protobuf/types/known/timestamppb"
)

var toProtoDataType = map[lang.DataType]DataType{
	lang.IntDataType:      DataType_Int,
	lang.DecimalDataType:  DataType_Decimal,
	lang.BoolDataType:     DataType_Bool,
	lang.StringDataType:   DataType_String,
	lang.RuneDataType:     DataType_Rune,
	lang.DatetimeDataType: DataType_Datetime,
}

var toProtoMutationType = map[mutation.Type]MutationType{
	mutation.CreateSchemaMutation:   MutationType_CreateSchema,
	mutation.DeleteSchemaMutation:   MutationType_DeleteSchema,
	mutation.CreateSchemaAttributes: MutationType_CreateSchemaAttributes,
	mutation.DeleteSchemaAttributes: MutationType_DeleteSchemaAttributes,
	mutation.CreateEntityMutation:   MutationType_CreateEntity,
	mutation.DeleteEntityMutation:   MutationType_DeleteEntity,
	mutation.CreateEntityAttributes: MutationType_CreateEntityAttributes,
	mutation.DeleteEntityAttributes: MutationType_DeleteEntityAttributes,
	mutation.UpdateEntityAttributes: MutationType_UpdateEntityAttributes,
}

var toProtoOperator = map[lang.Operator]Operator{
	lang.AndOperator:                  Operator_And,
	lang.OrOperator:                   Operator_Or,
	lang.NotOperator:                  Operator_Not,
	lang.EqualToOperator:              Operator_EqualTo,
	lang.ContainsOperator:             Operator_Contains,
	lang.LessThanOperator:             Operator_LessThan,
	lang.LessThanOrEqualToOperator:    Operator_LessThanOrEqualTo,
	lang.GreaterThanOperator:          Operator_GreaterThan,
	lang.GreaterThanOrEqualToOperator: Operator_GreaterThanOrEqualTo,
	lang.FindOperator:                 Operator_Find,
	lang.TakeOperator:                 Operator_Take,
	lang.AscOperator:                  Operator_Asc,
	lang.DescOperator:                 Operator_Desc,
	lang.GroupByOperator:              Operator_GroupBy,
	lang.EachGroupOperator:            Operator_EachGroup,
}

func ToProtoDatabases(dbNames []string) *Databases {
	return &Databases{Database: dbNames}
}

func ToProtoTransaction(transactionInput mutation.TransactionInput) *Transaction {
	protoMutations := make(map[string]*Mutations)
	for schema, mutations := range transactionInput.Mutations {
		protoMutations[schema] = toProtoMutations(mutations)
	}

	return &Transaction{Mutations: protoMutations}
}

func ToProtoExpression(expression lang.Expression) *Expression {
	inputs := make([]*Expression, 0)
	for _, inputExpression := range expression.Inputs {
		inputProtoExpression := ToProtoExpression(inputExpression)
		inputs = append(inputs, inputProtoExpression)
	}

	return &Expression{
		IsValue:        expression.IsValue,
		Value:          expression.Value,
		Operator:       toProtoOperator[expression.Operator],
		Inputs:         inputs,
		OutputDataType: toProtoDataType[expression.OutputDataType],
	}
}

func toProtoMutations(mutations []mutation.Mutation) *Mutations {
	protoMutations := make([]*Mutation, 0)
	for _, mut := range mutations {
		protoMutations = append(protoMutations, toProtoMutation(mut))
	}

	return &Mutations{Mutations: protoMutations}
}

func toProtoMutation(mut mutation.Mutation) *Mutation {
	schemaInput := toProtoSchemaInput(mut.SchemaInput)
	entityInput := toProtoEntityInput(mut.EntityInput)
	return &Mutation{
		Type:        toProtoMutationType[mut.Type],
		SchemaInput: schemaInput,
		EntityInput: entityInput,
	}
}

func toProtoSchemaInput(schemaInput mutation.SchemaInput) *SchemaInput {
	createOrUpdateAttributes := make(map[string]DataType)
	for attribute, dataType := range schemaInput.AttributesToCreateOrUpdate {
		createOrUpdateAttributes[attribute] = toProtoDataType[lang.FromDatabaseDataType[dataType]]
	}

	return &SchemaInput{
		Name:                       schemaInput.Name,
		AttributesToCreateOrUpdate: createOrUpdateAttributes,
		AttributesToDelete:         schemaInput.AttributesToDelete,
	}
}

func toProtoEntityInput(entityInput mutation.EntityInput) *EntityInput {
	createOrUpdateAttributes := make(map[string]*Value)
	for attribute, value := range entityInput.AttributesToCreateOrUpdate {
		protoDataType := toProtoDataType[lang.GetDataType(value)]
		protoValue := &Value{Type: protoDataType, Content: lang.String(value)}
		createOrUpdateAttributes[attribute] = protoValue
	}

	return &EntityInput{
		EntityID:                   entityInput.EntityID,
		SchemaName:                 entityInput.SchemaName,
		AttributesToCreateOrUpdate: createOrUpdateAttributes,
		AttributesToDelete:         entityInput.AttributesToDelete,
	}
}

func toProtoEntity(entity data.Entity) *Entity {
	protoAttributes := make(map[string]*Value)
	for attribute, value := range entity.Attributes {
		protoAttributes[attribute] = toProtoValue(value)
	}

	return &Entity{
		Id:         entity.ID,
		SchemaName: entity.SchemaName,
		Attributes: protoAttributes,
	}
}

func ToProtoEntities(entities []data.Entity) *Entities {
	protoEntities := make([]*Entity, 0)
	for _, entity := range entities {
		protoEntities = append(protoEntities, toProtoEntity(entity))
	}

	return &Entities{Entities: protoEntities}
}

func toProtoValue(value interface{}) *Value {
	return &Value{
		Type:    toProtoDataType[lang.GetDataType(value)],
		Content: lang.String(value),
	}
}

func ToProtoCommit(commit data.Commit) *Commit {
	return &Commit{
		CommittedTransactionId: commit.CommittedTransactionID,
		CommittedAt:            timestamppb.New(commit.CommittedAt),
	}
}
