package main

import (
	"encoding/json"
	"errors"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type nodeState struct {
	Node *maelstrom.Node

	valuesLock *sync.RWMutex
	values     []int

	topoLock *sync.RWMutex
	topology []string
}

func (s *nodeState) appendValue(n int) {
	s.valuesLock.Lock()
	defer s.valuesLock.Unlock()
	s.values = append(s.values, n)
}

func (s *nodeState) getValues() []int {
	s.valuesLock.RLock()
	defer s.valuesLock.RUnlock()
	newList := make([]int, len(s.values))
	copy(newList, s.values)
	return newList
}

func (s *nodeState) setTopology(topology map[string][]string) error {
	s.topoLock.Lock()
	defer s.topoLock.Unlock()
	if s.topology == nil {
		topo, ok := topology[s.Node.ID()]
		if !ok {
			return errors.New("could not set topology")
		}
		s.topology = topo
	}
	return nil
}

func main() {
	err := run()
	if err != nil {
		log.Fatal(err)
	}
}

func run() error {
	var (
		n     *maelstrom.Node
		state *nodeState
		err   error
	)

	n = maelstrom.NewNode()

	state = &nodeState{
		Node:       n,
		valuesLock: &sync.RWMutex{},
		topoLock:   &sync.RWMutex{},
	}

	registerHandles(state)

	err = n.Run()
	if err != nil {
		return err
	}

	return nil
}

func registerHandles(state *nodeState) {
	registerHandle(state, "broadcast", handleBroadcast)
	registerHandle(state, "read", handleRead)
	registerHandle(state, "topology", handleTopology)

}

func registerHandle(state *nodeState, typ string, fn func(*nodeState, maelstrom.Message) (any, error)) {
	h := state.Node.Handle

	handlerFunc := func(msg maelstrom.Message) error {
		resp, err := fn(state, msg)
		if err != nil {
			return err
		}

		return state.Node.Reply(msg, resp)
	}

	h(typ, handlerFunc)
}

func handleBroadcast(state *nodeState, msg maelstrom.Message) (any, error) {
	var body struct {
		Message int `json:"message"`
	}
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return nil, err
	}

	state.appendValue(body.Message)

	resp := map[string]string{
		"type": "broadcast_ok",
	}

	return resp, nil
}

func handleRead(state *nodeState, msg maelstrom.Message) (any, error) {
	var body map[string]any
	messages := state.getValues()

	body["type"] = "read_ok"
	body["messages"] = messages

	return body, nil
}

func handleTopology(state *nodeState, msg maelstrom.Message) (any, error) {
	var body struct {
		Topology map[string][]string `json:"topology"`
	}
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return nil, err
	}

	err = state.setTopology(body.Topology)
	if err != nil {
		return nil, err
	}

	resp := map[string]string{
		"type": "topology_ok",
	}

	return resp, nil
}
