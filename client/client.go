package client

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"tstore/data"
	"tstore/mutation"
	"tstore/proto"
	"tstore/query"
	"tstore/query/lang"
)

type Endpoint struct {
	Host string
	Port int
}

func (e Endpoint) String() string {
	return fmt.Sprintf("%s:%d", e.Host, e.Port)
}

type Client struct {
	grpcConn       *grpc.ClientConn
	databaseClient proto.DatabaseClient
}

var _ io.Closer = (*Client)(nil)

func (c *Client) CreateDatabase(name string) error {
	ctx := context.Background()
	_, err := c.databaseClient.CreateDatabase(ctx, &proto.CreateDatabaseRequest{Name: name})
	return err
}

func (c *Client) ListDatabases() ([]string, error) {
	ctx := context.Background()
	databases, err := c.databaseClient.ListAllDatabases(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}

	return databases.Databases, nil
}

func (c *Client) DeleteDatabase(name string) error {
	ctx := context.Background()
	_, err := c.databaseClient.DeleteDatabase(ctx, &proto.DeleteDatabaseRequest{Name: name})
	return err
}

func (c *Client) CreateTransaction(dbName string, transactionInput mutation.TransactionInput) error {
	ctx := context.Background()
	protoTransaction := proto.ToProtoTransaction(transactionInput)
	_, err := c.databaseClient.CreateTransaction(ctx, &proto.CreateTransactionRequest{
		DbName:      dbName,
		Transaction: protoTransaction,
	})
	return err
}

func (c *Client) GetLatestCommit(dbName string) (data.Commit, error) {
	ctx := context.Background()
	commit, err := c.databaseClient.GetLatestCommit(ctx, &proto.GetLatestCommitRequest{DbName: dbName})
	if err != nil {
		return data.Commit{}, err
	}

	return proto.FromProtoCommit(commit), nil
}

func (c *Client) QueryEntities(dbName string, transactionID uint64, collector lang.Collector) ([]data.Entity, error) {
	protoExpression := proto.ToProtoExpression(lang.Expression(collector))
	ctx := context.Background()
	entities, err := c.databaseClient.QueryEntitiesAtCommit(ctx, &proto.QueryAtCommitRequest{
		DbName:        dbName,
		TransactionId: transactionID,
		Query:         protoExpression,
	})
	if err != nil {
		return nil, err
	}

	return proto.FromProtoEntities(entities)
}

func (c *Client) QueryEntityGroups(dbName string, transactionID uint64, collector lang.GroupCollector) (query.Groups[data.Entity], error) {
	protoExpression := proto.ToProtoExpression(lang.Expression(collector))
	ctx := context.Background()
	groups, err := c.databaseClient.QueryEntityGroupsAtCommit(ctx, &proto.QueryAtCommitRequest{
		DbName:        dbName,
		TransactionId: transactionID,
		Query:         protoExpression,
	})
	if err != nil {
		return nil, err
	}

	return proto.FromProtoGroups(groups)
}

func (c *Client) Close() error {
	return c.grpcConn.Close()
}

func NewClient(endpoint Endpoint) (*Client, error) {
	conn, err := grpc.Dial(endpoint.String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &Client{
		grpcConn:       conn,
		databaseClient: proto.NewDatabaseClient(conn),
	}, nil
}
