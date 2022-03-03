package main

import (
	"fmt"
	"time"

	"tstore/client"
	"tstore/data"
	"tstore/mutation"
	"tstore/query/lang"
)

func main() {
	cl, err := client.NewClient(client.Endpoint{
		Host: "",
		Port: 8001,
	})
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	dbName := "example"
	cl.CreateDatabase(dbName)

	databases, err := cl.ListDatabases()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Databases: %v\n", databases)

	schemaName := "user"
	transactions := []mutation.TransactionInput{
		{
			Mutations: map[string][]data.Mutation{
				schemaName: {
					{
						Type: data.CreateSchemaMutation,
						SchemaInput: data.SchemaInput{
							Name: schemaName,
							AttributesToCreateOrUpdate: map[string]data.Type{
								"firstName": data.StringDataType,
								"lastName":  data.StringDataType,
							},
						},
					},
					{
						Type: data.CreateEntityMutation,
						EntityInput: data.EntityInput{
							SchemaName: schemaName,
							AttributesToCreateOrUpdate: map[string]interface{}{
								"firstName": "Harry",
								"lastName":  "Potter",
							},
						},
					},
				},
			},
		},
		{
			Mutations: map[string][]data.Mutation{
				schemaName: {
					{
						Type: data.UpdateEntityAttributesMutation,
						EntityInput: data.EntityInput{
							EntityID:   1,
							SchemaName: schemaName,
							AttributesToCreateOrUpdate: map[string]interface{}{
								"lastName": "What",
							},
						},
					},
					{
						Type: data.CreateEntityMutation,
						EntityInput: data.EntityInput{
							SchemaName: schemaName,
							AttributesToCreateOrUpdate: map[string]interface{}{
								"firstName": "Tony",
								"lastName":  "Stark",
							},
						},
					},
				},
			},
		},
		{
			Mutations: map[string][]data.Mutation{
				schemaName: {
					{
						Type: data.CreateEntityMutation,
						EntityInput: data.EntityInput{
							SchemaName: schemaName,
							AttributesToCreateOrUpdate: map[string]interface{}{
								"firstName": "Princess",
								"lastName":  "Leia",
							},
						},
					},
				},
			},
		},
	}

	latestCommit, err := cl.GetLatestCommit(dbName)
	if err != nil {
		panic(err)
	}

	if latestCommit.CommittedTransactionID == 0 {
		for _, transaction := range transactions {
			err = cl.CreateTransaction(dbName, transaction)
			if err != nil {
				panic(err)
			}
		}

		<-time.After(1 * time.Second)
	}

	latestCommit, err = cl.GetLatestCommit(dbName)
	if err != nil {
		panic(err)
	}

	fmt.Printf("has latest commit: %v\n", latestCommit)
	transactionID := latestCommit.CommittedTransactionID

	for currTransID := uint64(0); currTransID <= transactionID; currTransID++ {
		fmt.Printf("Transaction ID: %v\n", currTransID)

		qu := lang.Find(
			lang.EqualTo(lang.SchemaAttribute, "user"),
		)
		entities, err := cl.QueryEntities(dbName, currTransID, qu)
		if err != nil {
			panic(err)
		}

		fmt.Println(entities)
		fmt.Println()
	}
}
