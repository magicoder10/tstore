package mutation

import (
	"fmt"
	"time"

	"tstore/data"
	"tstore/history"
	"tstore/types"

	"golang.org/x/sync/errgroup"
)

const bufferSize = 500

type Mutator struct {
	dataStorage            *data.Storage
	transactionStorage     TransactionStorage
	entityIDGen            IDGen
	transactionIDGen       IDGen
	incomingTransactions   chan Transaction
	onTransactionProcessed chan uint64
}

func (m Mutator) CreateTransaction(transactionInput TransactionInput) error {
	ts := Transaction{
		ID:        m.transactionIDGen.NextID(),
		Mutations: transactionInput.Mutations,
	}
	err := m.transactionStorage.WriteTransaction(ts)
	if err != nil {
		return err
	}

	m.incomingTransactions <- ts
	return nil
}

func (m *Mutator) Start() {
	go func() {
		for transaction := range m.incomingTransactions {
			err := m.commitTransaction(transaction)
			if err != nil {
				fmt.Printf("fail to commit transaction: transaction=%v error=%v\n", transaction.ID, err)
				m.rollbackTransaction(transaction.ID)
			}

			go func(transaction Transaction) {
				m.onTransactionProcessed <- transaction.ID
			}(transaction)
		}
	}()
}

func (m *Mutator) commitTransaction(transaction Transaction) error {
	err := m.transactionStorage.WriteTransactionLog(TransactionStartLogLine{
		TransactionID: transaction.ID,
	})
	if err != nil {
		return err
	}

	errGroup := errgroup.Group{}
	for _, mutations := range transaction.Mutations {
		// apply mutation for different schemas in parallel
		mutations := mutations
		errGroup.Go(func() error {
			for _, mutation := range mutations {
				err := m.commitMutation(transaction.ID, mutation)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	err = errGroup.Wait()
	if err != nil {
		return err
	}

	commit := data.Commit{
		CommittedTransactionID: transaction.ID,
		CommittedAt:            time.Now(),
	}
	return m.dataStorage.AppendCommit(commit)
}

func (m *Mutator) rollbackTransaction(transactionID uint64) {
	m.dataStorage.EntityHistories.RemoveVersion(transactionID)
	m.dataStorage.SchemaHistories.RemoveVersion(transactionID)
}

func (m *Mutator) commitMutation(transactionID uint64, mutation data.Mutation) error {
	switch mutation.Type {
	case data.CreateSchemaMutation:
		return m.commitCreateSchemaMutation(transactionID, mutation)
	case data.DeleteSchemaMutation:
		return m.commitDeleteSchemaMutation(transactionID, mutation)
	case data.CreateSchemaAttributesMutation:
		return m.commitCreateSchemaAttributeMutation(transactionID, mutation)
	case data.DeleteSchemaAttributesMutation:
		return m.commitDeleteSchemaAttributesMutation(transactionID, mutation)
	case data.CreateEntityMutation:
		return m.commitCreateEntityMutation(transactionID, mutation)
	case data.DeleteEntityMutation:
		return m.commitDeleteEntityMutation(transactionID, mutation)
	case data.CreateEntityAttributesMutation:
		return m.commitCreateEntityAttributesMutation(transactionID, mutation)
	case data.DeleteEntityAttributesMutation:
		return m.commitDeleteEntityAttributesMutation(transactionID, mutation)
	case data.UpdateEntityAttributesMutation:
		return m.commitUpdateEntityAttributesMutation(transactionID, mutation)
	default:
		return fmt.Errorf("unknow mutation: %v", mutation)
	}
}

func (m *Mutator) commitCreateSchemaMutation(transactionID uint64, mutation data.Mutation) error {
	schemaName := mutation.SchemaInput.Name
	if _, ok := m.dataStorage.SchemaHistories.FindLatestValueAt(transactionID, schemaName); ok {
		return fmt.Errorf("schema already exist: name=%v", schemaName)
	}

	m.dataStorage.SchemaHistories.AddVersion(transactionID, schemaName, history.CreatedVersionStatus, mutation)
	return nil
}

func (m *Mutator) commitDeleteSchemaMutation(transactionID uint64, mutation data.Mutation) error {
	schemaName := mutation.SchemaInput.Name
	m.dataStorage.SchemaHistories.AddVersion(transactionID, schemaName, history.DeletedVersionStatus, data.Mutation{})
	return nil
}

func (m *Mutator) commitCreateSchemaAttributeMutation(transactionID uint64, mutation data.Mutation) error {
	schemaName := mutation.SchemaInput.Name
	currSchema, ok := m.dataStorage.SchemaHistories.FindLatestValueAt(transactionID, schemaName)
	if ok {
		for attribute := range mutation.SchemaInput.AttributesToCreateOrUpdate {
			if _, ok = currSchema.Attributes[attribute]; ok {
				return fmt.Errorf("schema attribute already exist: schema=%v, attribute=%v", schemaName, attribute)
			}
		}
	}

	m.dataStorage.SchemaHistories.AddVersion(transactionID, schemaName, history.UpdatedVersionStatus, mutation)
	return nil
}

func (m *Mutator) commitDeleteSchemaAttributesMutation(transactionID uint64, mutation data.Mutation) error {
	schemaName := mutation.SchemaInput.Name
	currSchema, ok := m.dataStorage.SchemaHistories.FindLatestValueAt(transactionID, schemaName)
	if !ok {
		return fmt.Errorf("schema not found: %s", schemaName)
	}

	attributes := make(map[string]data.Type)
	for _, attribute := range mutation.SchemaInput.AttributesToDelete {
		if _, ok = currSchema.Attributes[attribute]; !ok {
			return fmt.Errorf("schema attribute not found: schema=%v, attribute=%v", schemaName, attribute)
		}

		attributes[attribute] = currSchema.Attributes[attribute]
	}

	m.dataStorage.SchemaHistories.AddVersion(transactionID, schemaName, history.UpdatedVersionStatus, mutation)
	entities := m.dataStorage.EntityHistories.ListAllLatestValuesAt(transactionID)
	for entityID, entity := range entities {
		if entity.SchemaName != schemaName {
			continue
		}

		err := m.commitDeleteEntityAttributesMutation(transactionID, data.Mutation{
			Type: data.DeleteEntityAttributesMutation,
			EntityInput: data.EntityInput{
				EntityID:           entityID,
				SchemaName:         schemaName,
				AttributesToDelete: mutation.SchemaInput.AttributesToDelete,
			},
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (m Mutator) commitCreateEntityMutation(transactionID uint64, mutation data.Mutation) error {
	schemaName := mutation.EntityInput.SchemaName
	schema, ok := m.dataStorage.SchemaHistories.FindLatestValueAt(transactionID, schemaName)
	if !ok {
		return fmt.Errorf("schema not found: name=%v", schemaName)
	}

	entity := data.Entity{
		SchemaName: schemaName,
		Attributes: mutation.EntityInput.AttributesToCreateOrUpdate,
	}

	err := validateEntity(schema, entity)
	if err != nil {
		return err
	}

	entityID := m.entityIDGen.NextID()
	mutation.EntityInput.EntityID = entityID
	m.dataStorage.EntityHistories.AddVersion(transactionID, entityID, history.CreatedVersionStatus, mutation)
	return nil
}

func (m Mutator) commitDeleteEntityMutation(transactionID uint64, mutation data.Mutation) error {
	entityID := mutation.EntityInput.EntityID
	_, ok := m.dataStorage.EntityHistories.FindLatestValueAt(transactionID, entityID)
	if !ok {
		return fmt.Errorf("entity not found: %v", entityID)
	}
	m.dataStorage.EntityHistories.AddVersion(transactionID, entityID, history.DeletedVersionStatus, mutation)
	return nil
}

func (m Mutator) commitCreateEntityAttributesMutation(transactionID uint64, mutation data.Mutation) error {
	entityID := mutation.EntityInput.EntityID
	entity, ok := m.dataStorage.EntityHistories.FindLatestValueAt(transactionID, entityID)
	if !ok {
		return fmt.Errorf("entity not found: %v", entityID)
	}

	schema, ok := m.dataStorage.SchemaHistories.FindLatestValueAt(transactionID, entity.SchemaName)
	if !ok {
		return fmt.Errorf("schema not found: name=%v", entity.SchemaName)
	}

	attributes := make(map[string]interface{})
	for attribute, value := range mutation.EntityInput.AttributesToCreateOrUpdate {
		if _, ok = entity.Attributes[attribute]; ok {
			return fmt.Errorf("attribute already exist: entityID=%v, attribute=%v", entityID, attribute)
		}

		err := validateEntityAttribute(schema.Attributes[attribute], value)
		if err != nil {
			return err
		}

		attributes[attribute] = value
	}

	m.dataStorage.EntityHistories.AddVersion(transactionID, entityID, history.UpdatedVersionStatus, mutation)
	return nil
}

func (m Mutator) commitDeleteEntityAttributesMutation(transactionID uint64, mutation data.Mutation) error {
	entityID := mutation.EntityInput.EntityID
	entity, ok := m.dataStorage.EntityHistories.FindLatestValueAt(transactionID, entityID)
	if !ok {
		return fmt.Errorf("entity not found: %v", entityID)
	}

	attributes := make(map[string]interface{})
	for _, attribute := range mutation.SchemaInput.AttributesToDelete {
		if _, ok = entity.Attributes[attribute]; !ok {
			return fmt.Errorf("entity attribute not found: entity=%v, attribute=%v", entityID, attribute)
		}

		attributes[attribute] = entity.Attributes[attribute]
	}

	m.dataStorage.EntityHistories.AddVersion(transactionID, entityID, history.UpdatedVersionStatus, mutation)
	return nil
}

func (m Mutator) commitUpdateEntityAttributesMutation(transactionID uint64, mutation data.Mutation) error {
	entityID := mutation.EntityInput.EntityID
	entity, ok := m.dataStorage.EntityHistories.FindLatestValueAt(transactionID, entityID)
	if !ok {
		return fmt.Errorf("entity not found: %v", entityID)
	}

	schema, ok := m.dataStorage.SchemaHistories.FindLatestValueAt(transactionID, entity.SchemaName)
	if !ok {
		return fmt.Errorf("schema not found: name=%v", entity.SchemaName)
	}

	attributes := make(map[string]interface{})
	for attribute, value := range mutation.SchemaInput.AttributesToCreateOrUpdate {
		if _, ok = entity.Attributes[attribute]; !ok {
			return fmt.Errorf("entity attribute not found: entity=%v, attribute=%v", entityID, attribute)
		}

		err := validateEntityAttribute(schema.Attributes[attribute], value)
		if err != nil {
			return err
		}

		attributes[attribute] = value
	}

	m.dataStorage.EntityHistories.AddVersion(transactionID, entityID, history.UpdatedVersionStatus, mutation)
	return nil
}

func validateEntity(schema data.Schema, entity data.Entity) error {
	for attribute, value := range entity.Attributes {
		dataType, ok := schema.Attributes[attribute]
		if !ok {
			return fmt.Errorf(
				"attribute not found on schema: schema=%v entity=%v attribute=%v",
				schema.Name,
				entity.ID,
				attribute)
		}
		err := validateEntityAttribute(dataType, value)
		if err != nil {
			return err
		}
	}

	return nil
}

func validateEntityAttribute(dataType data.Type, value interface{}) error {
	switch value.(type) {
	case int8, int16, int, int64, uint8, uint16, uint32, uint64:
		if dataType != data.IntDataType {
			return fmt.Errorf("dataType mismatch: expected=%v actual=int", dataType)
		}
	case float32, float64:
		if dataType != data.DecimalDataType {
			return fmt.Errorf("dataType mismatch: expected=%v actual=float", dataType)
		}
	case bool:
		if dataType != data.BoolDataType {
			return fmt.Errorf("dataType mismatch: expected=%v actual=bool", dataType)
		}
	case string:
		if dataType != data.StringDataType {
			return fmt.Errorf("dataType mismatch: expected=%v actual=string", dataType)
		}
	case rune:
		if dataType != data.RuneDataType {
			return fmt.Errorf("dataType mismatch: expected=%v actual=rune", dataType)
		}
	case time.Time:
		if dataType != data.DatetimeDataType {
			return fmt.Errorf("dataType mismatch: expected=%v actual=time", dataType)
		}
	default:
		return fmt.Errorf("unsupported data type: value=%v", value)
	}

	return nil
}

func getMapKeys[Key types.Comparable, Value any](input map[Key]Value) []Key {
	keys := make([]Key, 0)
	for key := range input {
		keys = append(keys, key)
	}

	return keys
}

func NewMutator(
	dataStorage *data.Storage,
	dbName string,
) (*Mutator, error) {
	transactionStorage, err := NewTransactionStorage(dbName)
	if err != nil {
		return nil, err
	}

	entityIDGen, err := newIDGen(dbName, "entity")
	if err != nil {
		return nil, err
	}

	transactionIDGen, err := newIDGen(dbName, "transaction")
	if err != nil {
		return nil, err
	}

	return &Mutator{
		dataStorage:            dataStorage,
		transactionStorage:     transactionStorage,
		entityIDGen:            entityIDGen,
		transactionIDGen:       transactionIDGen,
		incomingTransactions:   make(chan Transaction, bufferSize),
		onTransactionProcessed: make(chan uint64),
	}, nil
}
