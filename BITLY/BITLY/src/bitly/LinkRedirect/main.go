/*
	Gumball API in Go (Version 2)
	Process Order with Go Channels and Mutex
*/
	
package main

import (
	"os"
)

func main() {

	port := os.Getenv("PORT")
	if len(port) == 0 {
		port = "9090"
	}

	server := NewServer()
	server.Run(":" + port)
}
