package main

import (
	"fmt"
	"time"

	"tstore/client"
	"tstore/data"
	"tstore/mutation"
	"tstore/query"
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
	err = cl.CreateDatabase(dbName)
	if err != nil {
		panic(err)
	}

	schemaName := "user"

	err = cl.CreateTransaction(dbName, mutation.TransactionInput{
		Mutations: map[string][]mutation.Mutation{
			schemaName: {
				{
					Type: mutation.CreateSchemaMutation,
					SchemaInput: mutation.SchemaInput{
						Name: schemaName,
						AttributesToCreateOrUpdate: map[string]data.Type{
							"firstName": data.StringDataType,
							"lastName":  data.StringDataType,
						},
					},
				},
				{
					Type: mutation.CreateEntityMutation,
					EntityInput: mutation.EntityInput{
						SchemaName: schemaName,
						AttributesToCreateOrUpdate: map[string]interface{}{
							"firstName": "Harry",
							"lastName":  "Potter",
						},
					},
				},
				{
					Type: mutation.CreateEntityMutation,
					EntityInput: mutation.EntityInput{
						SchemaName: schemaName,
						AttributesToCreateOrUpdate: map[string]interface{}{
							"firstName": "Tony",
							"lastName":  "Stark",
						},
					},
				},
				{
					Type: mutation.CreateEntityMutation,
					EntityInput: mutation.EntityInput{
						SchemaName: schemaName,
						AttributesToCreateOrUpdate: map[string]interface{}{
							"firstName": "Princess",
							"lastName":  "Leia",
						},
					},
				},
			},
		}})
	if err != nil {
		panic(err)
	}

	<-time.After(time.Second)

	var transactionID uint64
	latestCommit, err := cl.GetLatestCommit(dbName)
	if err != nil {
		fmt.Println("use default commit")
		transactionID = query.NoDataTransactionID
	} else {
		fmt.Printf("has latest commit: %v\n", latestCommit)
		transactionID = latestCommit.CommittedTransactionID
	}

	qu := lang.Find(lang.EqualTo(lang.SchemaAttribute, "user"))
	entities, err := cl.QueryEntities(dbName, transactionID, qu)
	if err != nil {
		panic(err)
	}

	fmt.Println(entities)
}
