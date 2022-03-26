package server

import (
	"context"
	"errors"
	"fmt"
	"net"

	"tstore/proto"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type GRPCServer struct {
	server Server
	proto.UnimplementedDatabaseServer
}

func (g GRPCServer) ListAllDatabases(ctx context.Context, empty *emptypb.Empty) (*proto.Databases, error) {
	databases, err := g.server.ListAllDatabases()
	return proto.ToProtoDatabases(databases), err
}

func (g GRPCServer) CreateDatabase(ctx context.Context, request *proto.CreateDatabaseRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, g.server.CreateDatabase(request.Name)
}

func (g GRPCServer) DeleteDatabase(ctx context.Context, request *proto.DeleteDatabaseRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, g.server.DeleteDatabase(request.Name)
}

func (g GRPCServer) CreateTransaction(ctx context.Context, request *proto.CreateTransactionRequest) (*emptypb.Empty, error) {
	transactionInput, err := proto.FromProtoTransactionInput(request.Transaction)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, g.server.CreateTransaction(request.DbName, transactionInput)
}

func (g GRPCServer) GetLatestCommit(ctx context.Context, request *proto.GetLatestCommitRequest) (*proto.Commit, error) {
	commit, err := g.server.GetLatestCommit(request.DbName)
	if err != nil {
		return nil, err
	}

	return proto.ToProtoCommit(commit), nil
}

func (g GRPCServer) QueryEntitiesAtCommit(ctx context.Context, request *proto.QueryAtCommitRequest) (*proto.Entities, error) {
	if request.Query == nil {
		return nil, errors.New("query can't be nil")
	}

	query := proto.FromProtoExpression(request.Query)
	entities, err := g.server.QueryEntitiesAtCommit(request.DbName, request.TransactionId, *query)
	if err != nil {
		return nil, err
	}

	return proto.ToProtoEntities(entities), nil
}

func (g GRPCServer) QueryGroupsAtCommit(ctx context.Context, request *proto.QueryAtCommitRequest) (*proto.Groups, error) {
	if request.Query == nil {
		return nil, errors.New("query can't be nil")
	}

	query := proto.FromProtoExpression(request.Query)
	groups, err := g.server.QueryEntityGroupsAtCommit(request.DbName, request.TransactionId, *query)
	if err != nil {
		return nil, err
	}

	protoGroups := make(map[string]*proto.Entities)
	for key, entities := range groups {
		protoGroups[key] = proto.ToProtoEntities(entities)
	}

	return &proto.Groups{Groups: protoGroups}, nil
}

var _ proto.DatabaseServer = (*GRPCServer)(nil)

func newGRPCServer() (*GRPCServer, error) {
	server, err := newServer()
	if err != nil {
		return nil, err
	}

	return &GRPCServer{
		server: server,
	}, nil
}

func StartGRPCServer(port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}

	grpcServer, err := newGRPCServer()
	if err != nil {
		return err
	}

	s := grpc.NewServer()
	proto.RegisterDatabaseServer(s, grpcServer)

	fmt.Printf("Server started at %d\n", port)
	if err = s.Serve(lis); err != nil {
		return err
	}

	return nil
}
