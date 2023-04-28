package main

import (
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	err := run()
	if err != nil {
		log.Fatal(err)
	}
}

func run() error {
	var (
		n          *maelstrom.Node
		gossipNode *gossipNode
		err        error
	)

	n = maelstrom.NewNode()

	gossipNode = NewGossipNode(n)

	registerHandles(gossipNode)

	go gossipNode.runGossip()

	err = n.Run()
	if err != nil {
		return err
	}

	return nil
}
