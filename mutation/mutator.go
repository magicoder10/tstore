package mutation

import (
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"
	"tstore/data"
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
				if err = m.rollbackTransaction(transaction.ID); err != nil {
					fmt.Printf("fail to undo transaction: transaction=%v\n", transaction)
				}
			}

			m.onTransactionProcessed <- transaction.ID
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

	commits, err := m.dataStorage.ReadAllCommits()
	if err != nil {
		commits = make([]data.Commit, 0)
	}
	commits = append(commits, commit)

	err = m.dataStorage.WriteAllCommits(commits)
	if err != nil {
		return err
	}

	return m.transactionStorage.WriteTransactionLog(TransactionCommittedLogLine{
		TransactionID: transaction.ID,
	})
}

func (m *Mutator) rollbackTransaction(transactionID uint64) error {
	// TODO: rollback transaction
	return m.transactionStorage.WriteTransactionLog(TransactionAbortedLogLine{
		TransactionID: transactionID,
	})
}

func (m *Mutator) commitMutation(transactionID uint64, mutation Mutation) error {
	switch mutation.Type {
	case CreateSchemaMutation:
		return m.commitCreateSchemaMutation(transactionID, mutation)
	case DeleteSchemaMutation:
		return m.commitDeleteSchemaMutation(transactionID, mutation)
	case CreateSchemaAttributes:
		return m.commitCreateSchemaAttributeMutation(transactionID, mutation)
	case DeleteSchemaAttributes:
		return m.commitDeleteSchemaAttributesMutation(transactionID, mutation)
	case CreateEntityMutation:
		return m.commitCreateEntityMutation(transactionID, mutation)
	case DeleteEntityMutation:
		return m.commitDeleteEntityMutation(transactionID, mutation)
	case CreateEntityAttributes:
		return m.commitCreateEntityAttributesMutation(transactionID, mutation)
	case DeleteEntityAttributes:
		return m.commitDeleteEntityAttributesMutation(transactionID, mutation)
	case UpdateEntityAttributes:
		return m.commitUpdateEntityAttributesMutation(transactionID, mutation)
	default:
		return fmt.Errorf("unknow mutation: %v", mutation)
	}
}

func (m *Mutator) commitCreateSchemaMutation(transactionID uint64, mutation Mutation) error {
	schemaName := mutation.SchemaInput.Name
	if _, err := m.dataStorage.ReadSchema(schemaName); err == nil {
		return fmt.Errorf("schema already exist: name=%v", schemaName)
	}

	err := m.transactionStorage.WriteTransactionLog(TransactionCreateSchemaLogLine{
		TransactionID: transactionID,
		MutationType:  mutation.Type,
		SchemaName:    schemaName,
	})
	if err != nil {
		return err
	}

	schema := data.Schema{
		Name:       schemaName,
		Attributes: mutation.SchemaInput.AttributesToCreateOrUpdate,
	}

	return m.dataStorage.WriteSchema(schema)
}

func (m *Mutator) commitDeleteSchemaMutation(transactionID uint64, mutation Mutation) error {
	schemaName := mutation.SchemaInput.Name
	schema, err := m.dataStorage.ReadSchema(schemaName)
	if err != nil {
		return err
	}

	err = m.transactionStorage.WriteTransactionLog(TransactionDeleteSchemaLogLine{
		TransactionID:  transactionID,
		MutationType:   mutation.Type,
		SchemaName:     schemaName,
		PrevAttributes: schema.Attributes,
	})
	if err != nil {
		return err
	}

	return m.dataStorage.DeleteSchema(schemaName)
}

func (m *Mutator) commitCreateSchemaAttributeMutation(transactionID uint64, mutation Mutation) error {
	schemaName := mutation.SchemaInput.Name
	currSchema, err := m.dataStorage.ReadSchema(schemaName)
	if err != nil {
		return err
	}

	for attribute := range mutation.SchemaInput.AttributesToCreateOrUpdate {
		if _, ok := currSchema.Attributes[attribute]; ok {
			return fmt.Errorf("schema attribute already exist: schema=%v, attribute=%v", schemaName, attribute)
		}
	}

	err = m.transactionStorage.WriteTransactionLog(TransactionCreateSchemaAttributesLogLine{
		TransactionID:     transactionID,
		MutationType:      mutation.Type,
		SchemaName:        schemaName,
		CreatedAttributes: getMapKeys(mutation.SchemaInput.AttributesToCreateOrUpdate),
	})
	if err != nil {
		return err
	}

	for attribute, dataType := range mutation.SchemaInput.AttributesToCreateOrUpdate {
		currSchema.Attributes[attribute] = dataType
	}

	return m.dataStorage.WriteSchema(currSchema)
}

func (m *Mutator) commitDeleteSchemaAttributesMutation(transactionID uint64, mutation Mutation) error {
	schemaName := mutation.SchemaInput.Name
	currSchema, err := m.dataStorage.ReadSchema(schemaName)
	if err != nil {
		return err
	}

	entities, err := m.dataStorage.ReadAllEntities()
	if err != nil {
		return err
	}

	entityIDs := make([]uint64, 0)
	for _, entity := range entities {
		if entity.SchemaName == schemaName {
			entityIDs = append(entityIDs, entity.ID)
		}
	}

	attributes := make(map[string]data.Type)
	for _, attribute := range mutation.SchemaInput.AttributesToDelete {
		if _, ok := currSchema.Attributes[attribute]; !ok {
			return fmt.Errorf("schema attribute not found: schema=%v, attribute=%v", schemaName, attribute)
		}

		attributes[attribute] = currSchema.Attributes[attribute]
	}

	err = m.transactionStorage.WriteTransactionLog(TransactionDeleteSchemaAttributesLogLine{
		TransactionID:  transactionID,
		MutationType:   mutation.Type,
		SchemaName:     schemaName,
		PrevAttributes: attributes,
	})
	if err != nil {
		return err
	}

	for _, attribute := range mutation.SchemaInput.AttributesToDelete {
		delete(currSchema.Attributes, attribute)
	}

	for _, entityID := range entityIDs {
		err = m.commitDeleteEntityAttributesMutation(transactionID, Mutation{
			Type: DeleteEntityAttributes,
			EntityInput: EntityInput{
				EntityID:           entityID,
				SchemaName:         schemaName,
				AttributesToDelete: mutation.SchemaInput.AttributesToDelete,
			},
		})
		if err != nil {
			return err
		}
	}

	return m.dataStorage.WriteSchema(currSchema)
}

func (m Mutator) commitCreateEntityMutation(transactionID uint64, mutation Mutation) error {
	schemaName := mutation.EntityInput.SchemaName
	schema, err := m.dataStorage.ReadSchema(schemaName)
	if err != nil {
		return err
	}

	entity := data.Entity{
		SchemaName: schemaName,
		Attributes: mutation.EntityInput.AttributesToCreateOrUpdate,
	}

	err = validateEntity(schema, entity)
	if err != nil {
		return err
	}

	entityID := m.entityIDGen.NextID()
	if _, err := m.dataStorage.ReadEntity(entityID); err == nil {
		return fmt.Errorf("entity already exist: id=%v", entityID)
	}

	entity.ID = entityID
	err = m.transactionStorage.WriteTransactionLog(TransactionCreateEntityLogLine{
		TransactionID: transactionID,
		MutationType:  mutation.Type,
		EntityID:      entityID,
	})
	if err != nil {
		return err
	}

	return m.dataStorage.WriteEntity(entity)
}

func (m Mutator) commitDeleteEntityMutation(transactionID uint64, mutation Mutation) error {
	entityID := mutation.EntityInput.EntityID
	entity, err := m.dataStorage.ReadEntity(entityID)
	if err != nil {
		return err
	}

	err = m.transactionStorage.WriteTransactionLog(TransactionDeleteEntityLogLine{
		TransactionID:  transactionID,
		MutationType:   mutation.Type,
		EntityID:       entityID,
		PrevAttributes: entity.Attributes,
	})
	if err != nil {
		return err
	}

	return m.dataStorage.DeleteEntity(entityID)
}

func (m Mutator) commitCreateEntityAttributesMutation(transactionID uint64, mutation Mutation) error {
	entityID := mutation.EntityInput.EntityID
	entity, err := m.dataStorage.ReadEntity(entityID)
	if err != nil {
		return err
	}

	schema, err := m.dataStorage.ReadSchema(entity.SchemaName)
	if err != nil {
		return err
	}

	attributes := make(map[string]interface{})

	for attribute, value := range mutation.EntityInput.AttributesToCreateOrUpdate {
		if _, ok := entity.Attributes[attribute]; ok {
			return fmt.Errorf("attribute already exist: entityID=%v, attribute=%v", entityID, attribute)
		}

		err := validateEntityAttribute(schema.Attributes[attribute], value)
		if err != nil {
			return err
		}

		attributes[attribute] = value
	}

	err = m.transactionStorage.WriteTransactionLog(TransactionCreateEntityAttributesLogLine{
		TransactionID:     transactionID,
		MutationType:      mutation.Type,
		EntityID:          entityID,
		CreatedAttributes: getMapKeys(mutation.EntityInput.AttributesToCreateOrUpdate),
	})
	if err != nil {
		return err
	}

	for attribute, attributeValue := range attributes {
		entity.Attributes[attribute] = attributeValue
	}

	return m.dataStorage.WriteEntity(entity)
}

func (m Mutator) commitDeleteEntityAttributesMutation(transactionID uint64, mutation Mutation) error {
	entityID := mutation.EntityInput.EntityID
	currEntity, err := m.dataStorage.ReadEntity(entityID)
	if err != nil {
		return err
	}

	attributes := make(map[string]interface{})
	for _, attribute := range mutation.SchemaInput.AttributesToDelete {
		if _, ok := currEntity.Attributes[attribute]; !ok {
			return fmt.Errorf("entity attribute not found: entity=%v, attribute=%v", entityID, attribute)
		}

		attributes[attribute] = currEntity.Attributes[attribute]
	}

	err = m.transactionStorage.WriteTransactionLog(TransactionDeleteEntityAttributesLogLine{
		TransactionID:  transactionID,
		MutationType:   mutation.Type,
		EntityID:       entityID,
		PrevAttributes: attributes,
	})
	if err != nil {
		return err
	}

	for _, attribute := range mutation.EntityInput.AttributesToDelete {
		delete(currEntity.Attributes, attribute)
	}

	return m.dataStorage.WriteEntity(currEntity)
}

func (m Mutator) commitUpdateEntityAttributesMutation(transactionID uint64, mutation Mutation) error {
	entityID := mutation.EntityInput.EntityID

	currEntity, err := m.dataStorage.ReadEntity(entityID)
	if err != nil {
		return err
	}

	currSchema, err := m.dataStorage.ReadSchema(currEntity.SchemaName)
	if err != nil {
		return err
	}

	attributes := make(map[string]interface{})
	for _, attribute := range mutation.SchemaInput.AttributesToDelete {
		if _, ok := currEntity.Attributes[attribute]; !ok {
			return fmt.Errorf("entity attribute not found: entity=%v, attribute=%v", entityID, attribute)
		}

		err := validateEntityAttribute(currSchema.Attributes[attribute], currEntity.Attributes[attribute])
		if err != nil {
			return err
		}

		attributes[attribute] = currEntity.Attributes[attribute]
	}

	err = m.transactionStorage.WriteTransactionLog(TransactionUpdateEntityAttributesLogLine{
		TransactionID:  transactionID,
		MutationType:   mutation.Type,
		EntityID:       entityID,
		PrevAttributes: attributes,
	})
	if err != nil {
		return err
	}

	for attribute, value := range mutation.EntityInput.AttributesToCreateOrUpdate {
		currEntity.Attributes[attribute] = value
	}

	return m.dataStorage.WriteEntity(currEntity)
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

func getMapKeys[Key data.Comparable, Value any](input map[Key]Value) []Key {
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
