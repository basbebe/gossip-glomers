package main

import (
	"encoding/json"
	"log"

	uuid "github.com/gofrs/uuid/v5"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type nodeHandlerFunc func(maelstrom.Message) (any, error)

func main() {
	err := run()
	if err != nil {
		log.Fatal(err)
	}
}

func run() error {
	var (
		n   *maelstrom.Node
		err error
	)

	n = maelstrom.NewNode()

	registerHandles(n)

	err = n.Run()
	if err != nil {
		return err
	}

	return nil
}

func registerHandles(n *maelstrom.Node) {
	registerHandle(n, "generate", handleIDResponse)
}

func registerHandle(n *maelstrom.Node, typ string, fn nodeHandlerFunc) {
	n.Handle(typ, addHandle(n, typ, fn))
}

func addHandle(n *maelstrom.Node, typ string, fn nodeHandlerFunc) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		body, err := fn(msg)
		if err != nil {
			return err
		}

		return n.Reply(msg, body)
	}
}

func handleIDResponse(msg maelstrom.Message) (any, error) {
	var body map[string]any
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return nil, err
	}

	id, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}

	// Update the message type to return back.
	body["type"] = "generate_ok"
	body["id"] = id

	return body, nil
}
