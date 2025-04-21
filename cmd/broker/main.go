package main

import (
	"log"
	"os"
)

func main() {
	srv := BuildServer() // wiring.go
	addr := ":8080"
	if v := os.Getenv("PORT"); v != "" {
		addr = ":" + v
	}
	log.Printf("MOM listening on %s", addr)
	log.Fatal(srv.Run(addr))
}
