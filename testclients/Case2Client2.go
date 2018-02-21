/*

A trivial client to illustrate how the kvservice library can be used
from an application in assignment 6 for UBC CS 416 2016 W2.

Usage:
go run client.go
*/

package main

// Expects kvservice.go to be in the ./kvservice/ dir, relative to
// this client.go file
import "./../kvservice"

import (
	"fmt"
	"bufio"
	"io"
	"os"
	"strings"
	// "time"
	"log"
)

func main() {
	args := os.Args[1:]

	// Missing command line args.
	if len(args) != 1 {
		fmt.Println("Usage: go run x.go [nodesFile]")
		return
	}

	var nodes []string

	pF, err := os.Open(args[0])
	checkError("nodesFile argument cannot be opened", err, true)
	bufIo := bufio.NewReader(pF)
	for {
		nodeIP, err := bufIo.ReadString('\n')
		nodeIP = strings.TrimSpace(nodeIP)
		fmt.Println("nodeIP: ", nodeIP)
		if err == io.EOF {
			break
		}
		nodes = append(nodes, nodeIP)		
		fmt.Println("nodes: ", nodes)
	}


	c := kvservice.NewConnection(nodes)
	fmt.Printf("NewConnection returned: %v\n", c)
	
	t, err := c.NewTX()
	fmt.Printf("NewTX returned: %v, %v\n", t, err)

	success, err := t.Put("a", "2")
	fmt.Printf("Get returned: %v, %v\n", success, err)

	success, txID, err := t.Commit(0)
	fmt.Printf("Commit returned: %v, %v,  %v\n", success, txID, err)
	
	c.Close()
}

func checkError(msg string, err error, exit bool) {
	if err != nil {
		log.Println(msg, err)
		if exit {
			os.Exit(-1)
		}
	}
}