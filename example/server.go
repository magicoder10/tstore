package main

import (
	"log"

	"tstore/server"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	panic(server.StartGRPCServer(8001))
}
