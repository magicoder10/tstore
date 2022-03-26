package mutation

import (
	"fmt"
	"log"
	"time"

	"tstore/data"
	"tstore/history"

	"golang.org/x/sync/errgroup"
)

const bufferSize = 500

type Mutator struct {
	dataWithVersion        *data.WithVersion
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
				log.Printf("fail to commit transaction: transaction=%v error=%v\n", transaction.ID, err)
				err = m.rollbackTransaction(transaction.ID)
				if err != nil {
					log.Println(err)
				}
			}

			go func(transaction Transaction) {
				m.onTransactionProcessed <- transaction.ID
			}(transaction)
		}
	}()
}

func (m *Mutator) commitTransaction(transaction Transaction) error {
	log.Printf("[commitTransaction] %v\n", transaction)
	err := m.transactionStorage.WriteTransactionLog(TransactionStartLogLine{
		TransactionID: transaction.ID,
	})
	if err != nil {
		log.Println(err)
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
					log.Println(err)
					return err
				}
			}

			return nil
		})
	}

	err = errGroup.Wait()
	if err != nil {
		log.Println(err)
		return err
	}

	commit := data.Commit{
		CommittedTransactionID: transaction.ID,
		CommittedAt:            time.Now(),
	}
	return m.dataWithVersion.AppendCommit(commit)
}

func (m *Mutator) rollbackTransaction(transactionID uint64) error {
	_, err := m.dataWithVersion.EntityHistories.RemoveVersion(transactionID)
	if err != nil {
		log.Println(err)
		return err
	}

	_, err = m.dataWithVersion.SchemaHistories.RemoveVersion(transactionID)
	if err != nil {
		log.Println(err)
		return err
	}

	return err
}

func (m *Mutator) commitMutation(transactionID uint64, mutation data.Mutation) error {
	log.Printf("[commitMutation] transactionID=%v, mutation=%v\n", transactionID, mutation)
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
	_, exist, err := m.dataWithVersion.SchemaHistories.FindLatestValueAt(transactionID, schemaName)
	if err != nil {
		log.Println(err)
		return err
	}

	if exist {
		err = fmt.Errorf("schema already exist: name=%v", schemaName)
		log.Println(err)
		return err
	}

	_, err = m.dataWithVersion.SchemaHistories.AddVersion(transactionID, schemaName, history.CreatedVersionStatus, mutation)
	if err != nil {
		log.Println(err)
		return err
	}

	return err
}

func (m *Mutator) commitDeleteSchemaMutation(transactionID uint64, mutation data.Mutation) error {
	schemaName := mutation.SchemaInput.Name
	_, err := m.dataWithVersion.SchemaHistories.AddVersion(transactionID, schemaName, history.DeletedVersionStatus, data.Mutation{})
	if err != nil {
		log.Println(err)
		return err
	}

	return err
}

func (m *Mutator) commitCreateSchemaAttributeMutation(transactionID uint64, mutation data.Mutation) error {
	schemaName := mutation.SchemaInput.Name
	currSchema, exist, err := m.dataWithVersion.SchemaHistories.FindLatestValueAt(transactionID, schemaName)
	if err != nil {
		log.Println(err)
		return err
	}

	if exist {
		for attribute := range mutation.SchemaInput.AttributesToCreateOrUpdate {
			if _, exist = currSchema.Attributes[attribute]; exist {
				err = fmt.Errorf("schema attribute already exist: schema=%v, attribute=%v", schemaName, attribute)
				log.Println(err)
				return err
			}
		}
	}

	_, err = m.dataWithVersion.SchemaHistories.AddVersion(transactionID, schemaName, history.UpdatedVersionStatus, mutation)
	if err != nil {
		log.Println(err)
	}

	return err
}

func (m *Mutator) commitDeleteSchemaAttributesMutation(transactionID uint64, mutation data.Mutation) error {
	schemaName := mutation.SchemaInput.Name
	currSchema, exist, err := m.dataWithVersion.SchemaHistories.FindLatestValueAt(transactionID, schemaName)
	if err != nil {
		log.Println(err)
		return err
	}

	if !exist {
		return fmt.Errorf("schema not found: %s", schemaName)
	}

	attributes := make(map[string]data.Type)
	for _, attribute := range mutation.SchemaInput.AttributesToDelete {
		if _, exist = currSchema.Attributes[attribute]; !exist {
			err = fmt.Errorf("schema attribute not found: schema=%v, attribute=%v", schemaName, attribute)
			log.Println(err)
			return err
		}

		attributes[attribute] = currSchema.Attributes[attribute]
	}

	_, err = m.dataWithVersion.SchemaHistories.AddVersion(transactionID, schemaName, history.UpdatedVersionStatus, mutation)
	if err != nil {
		log.Println(err)
		return err
	}

	entities, _, err := m.dataWithVersion.EntityHistories.ListAllLatestValuesAt(transactionID)
	if err != nil {
		log.Println(err)
		return err
	}

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
			log.Println(err)
			return err
		}
	}

	return nil
}

func (m Mutator) commitCreateEntityMutation(transactionID uint64, mutation data.Mutation) error {
	schemaName := mutation.EntityInput.SchemaName
	schema, exist, err := m.dataWithVersion.SchemaHistories.FindLatestValueAt(transactionID, schemaName)
	if err != nil {
		log.Println(err)
		return err
	}

	if !exist {
		err = fmt.Errorf("schema not found: name=%v", schemaName)
		log.Println(err)
		return err
	}

	entity := data.Entity{
		SchemaName: schemaName,
		Attributes: mutation.EntityInput.AttributesToCreateOrUpdate,
	}

	err = validateEntity(schema, entity)
	if err != nil {
		log.Println(err)
		return err
	}

	entityID := m.entityIDGen.NextID()
	mutation.EntityInput.EntityID = entityID
	_, err = m.dataWithVersion.EntityHistories.AddVersion(transactionID, entityID, history.CreatedVersionStatus, mutation)
	if err != nil {
		log.Println(err)
	}

	return err
}

func (m Mutator) commitDeleteEntityMutation(transactionID uint64, mutation data.Mutation) error {
	entityID := mutation.EntityInput.EntityID
	_, exist, err := m.dataWithVersion.EntityHistories.FindLatestValueAt(transactionID, entityID)
	if err != nil {
		log.Println(err)
		return err
	}

	if !exist {
		err = fmt.Errorf("entity not found: %v", entityID)
		log.Println(err)
		return err
	}

	_, err = m.dataWithVersion.EntityHistories.AddVersion(transactionID, entityID, history.DeletedVersionStatus, mutation)
	if err != nil {
		log.Println(err)
	}

	return err
}

func (m Mutator) commitCreateEntityAttributesMutation(transactionID uint64, mutation data.Mutation) error {
	entityID := mutation.EntityInput.EntityID
	entity, exist, err := m.dataWithVersion.EntityHistories.FindLatestValueAt(transactionID, entityID)
	if err != nil {
		log.Println(err)
		return err
	}

	if !exist {
		err = fmt.Errorf("entity not found: %v", entityID)
		log.Println(err)
		return err
	}

	schema, exist, err := m.dataWithVersion.SchemaHistories.FindLatestValueAt(transactionID, entity.SchemaName)
	if err != nil {
		log.Println(err)
		return err
	}

	if !exist {
		err = fmt.Errorf("schema not found: name=%v", entity.SchemaName)
		log.Println(err)
		return err
	}

	attributes := make(map[string]interface{})
	for attribute, value := range mutation.EntityInput.AttributesToCreateOrUpdate {
		if _, exist = entity.Attributes[attribute]; exist {
			err = fmt.Errorf("attribute already exist: entityID=%v, attribute=%v", entityID, attribute)
			log.Println(err)
			return err
		}

		err = validateEntityAttribute(schema.Attributes[attribute], value)
		if err != nil {
			log.Println(err)
			return err
		}

		attributes[attribute] = value
	}

	_, err = m.dataWithVersion.EntityHistories.AddVersion(transactionID, entityID, history.UpdatedVersionStatus, mutation)
	if err != nil {
		log.Println(err)
	}

	return err
}

func (m Mutator) commitDeleteEntityAttributesMutation(transactionID uint64, mutation data.Mutation) error {
	entityID := mutation.EntityInput.EntityID
	entity, exist, err := m.dataWithVersion.EntityHistories.FindLatestValueAt(transactionID, entityID)
	if err != nil {
		log.Println(err)
		return err
	}

	if !exist {
		err = fmt.Errorf("entity not found: %v", entityID)
		log.Println(err)
		return err
	}

	attributes := make(map[string]interface{})
	for _, attribute := range mutation.SchemaInput.AttributesToDelete {
		if _, exist = entity.Attributes[attribute]; !exist {
			err = fmt.Errorf("entity attribute not found: entity=%v, attribute=%v", entityID, attribute)
			log.Println(err)
			return err
		}

		attributes[attribute] = entity.Attributes[attribute]
	}

	_, err = m.dataWithVersion.EntityHistories.AddVersion(transactionID, entityID, history.UpdatedVersionStatus, mutation)
	if err != nil {
		log.Println(err)
	}

	return err
}

func (m Mutator) commitUpdateEntityAttributesMutation(transactionID uint64, mutation data.Mutation) error {
	entityID := mutation.EntityInput.EntityID
	entity, exist, err := m.dataWithVersion.EntityHistories.FindLatestValueAt(transactionID, entityID)
	if err != nil {
		log.Println(err)
		return err
	}

	if !exist {
		return fmt.Errorf("entity not found: %v", entityID)
	}

	schema, exist, err := m.dataWithVersion.SchemaHistories.FindLatestValueAt(transactionID, entity.SchemaName)
	if err != nil {
		log.Println(err)
		return err
	}

	if !exist {
		err = fmt.Errorf("schema not found: name=%v", entity.SchemaName)
		log.Println(err)
		return err
	}

	attributes := make(map[string]interface{})
	for attribute, value := range mutation.SchemaInput.AttributesToCreateOrUpdate {
		if _, exist = entity.Attributes[attribute]; !exist {
			err = fmt.Errorf("entity attribute not found: entity=%v, attribute=%v", entityID, attribute)
			log.Println(err)
			return err
		}

		err = validateEntityAttribute(schema.Attributes[attribute], value)
		if err != nil {
			log.Println(err)
			return err
		}

		attributes[attribute] = value
	}

	_, err = m.dataWithVersion.EntityHistories.AddVersion(transactionID, entityID, history.UpdatedVersionStatus, mutation)
	if err != nil {
		log.Println(err)
	}

	return err
}

func validateEntity(schema data.Schema, entity data.Entity) error {
	for attribute, value := range entity.Attributes {
		dataType, ok := schema.Attributes[attribute]
		if !ok {
			err := fmt.Errorf(
				"attribute not found on schema: schema=%v entity=%v attribute=%v",
				schema.Name,
				entity.ID,
				attribute)
			log.Println(err)
			return err
		}

		err := validateEntityAttribute(dataType, value)
		if err != nil {
			log.Println(err)
			return err
		}
	}

	return nil
}

func validateEntityAttribute(dataType data.Type, value interface{}) error {
	switch value.(type) {
	case int8, int16, int, int64, uint8, uint16, uint32, uint64:
		if dataType != data.IntDataType {
			err := fmt.Errorf("dataType mismatch: expected=%v actual=int", dataType)
			if err != nil {
				log.Println(err)
				return err
			}
		}
	case float32, float64:
		if dataType != data.DecimalDataType {
			err := fmt.Errorf("dataType mismatch: expected=%v actual=float", dataType)
			if err != nil {
				log.Println(err)
				return err
			}
		}
	case bool:
		if dataType != data.BoolDataType {
			err := fmt.Errorf("dataType mismatch: expected=%v actual=bool", dataType)
			if err != nil {
				log.Println(err)
				return err
			}
		}
	case string:
		if dataType != data.StringDataType {
			err := fmt.Errorf("dataType mismatch: expected=%v actual=string", dataType)
			if err != nil {
				log.Println(err)
				return err
			}
		}
	case rune:
		if dataType != data.RuneDataType {
			err := fmt.Errorf("dataType mismatch: expected=%v actual=rune", dataType)
			if err != nil {
				log.Println(err)
				return err
			}
		}
	case time.Time:
		if dataType != data.DatetimeDataType {
			err := fmt.Errorf("dataType mismatch: expected=%v actual=time", dataType)
			if err != nil {
				log.Println(err)
				return err
			}
		}
	default:
		err := fmt.Errorf("unsupported data type: value=%v", value)
		if err != nil {
			log.Println(err)
			return err
		}
	}

	return nil
}

func NewMutator(
	dataWithVersion *data.WithVersion,
	dbName string,
) (*Mutator, error) {
	transactionStorage, err := NewTransactionStorage(dbName)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	entityIDGen, err := newIDGen(dbName, "entity")
	if err != nil {
		log.Println(err)
		return nil, err
	}

	transactionIDGen, err := newIDGen(dbName, "transaction")
	if err != nil {
		log.Println(err)
		return nil, err
	}

	return &Mutator{
		dataWithVersion:        dataWithVersion,
		transactionStorage:     transactionStorage,
		entityIDGen:            entityIDGen,
		transactionIDGen:       transactionIDGen,
		incomingTransactions:   make(chan Transaction, bufferSize),
		onTransactionProcessed: make(chan uint64),
	}, nil
}
