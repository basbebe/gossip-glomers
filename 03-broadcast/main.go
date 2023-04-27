package main

import (
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// const (
// 	gossipNodesNr  = 8
// 	gossipInterval = time.Duration(500 * time.Millisecond)
// )

func main() {
	err := run()
	if err != nil {
		log.Fatal(err)
	}
}

func run() error {
	var (
		n     *maelstrom.Node
		state *gossipNode
		err   error
	)

	n = maelstrom.NewNode()

	state = NewGossipNode(n)

	registerHandles(state)

	go runGossip(state)

	err = n.Run()
	if err != nil {
		return err
	}

	return nil
}
