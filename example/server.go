package main

import (
	"tstore/server"
)

func main() {
	panic(server.StartGRPCServer(8001))
}
