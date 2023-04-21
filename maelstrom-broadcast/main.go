package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type store struct {
	mu   *sync.RWMutex
	list []int
}

func (s *store) append(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.list = append(s.list, n)
}

func (s *store) get() []int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	newList := make([]int, len(s.list))
	copy(newList, s.list)
	return newList
}

func main() {
	err := run()
	if err != nil {
		log.Fatal(err)
	}
}

func run() error {
	var (
		n    *maelstrom.Node
		seen *store
		err  error
	)

	n = maelstrom.NewNode()

	seen = &store{
		mu: &sync.RWMutex{},
	}

	registerHandles(n, seen)

	err = n.Run()
	if err != nil {
		return err
	}

	return nil
}

func registerHandles(n *maelstrom.Node, seen *store) {
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		resp, err := handleBroadcast(msg, seen)
		if err != nil {
			return err
		}
		return n.Reply(msg, resp)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		resp, err := handleRead(msg, seen)
		if err != nil {
			return err
		}
		return n.Reply(msg, resp)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		resp, err := handleTopology(n, msg)
		if err != nil {
			return err
		}
		return n.Reply(msg, resp)
	})

	// registerHandle(n, "broadcast", handleBroadcast)
}

func handleBroadcast(msg maelstrom.Message, seen *store) (any, error) {

	var body struct {
		Message int `json:"message"`
	}
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return nil, err
	}

	seen.append(body.Message)

	resp := map[string]string{
		"type": "broadcast_ok",
	}

	return resp, nil
}

func handleRead(msg maelstrom.Message, seen *store) (any, error) {
	var body map[string]any
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return nil, err
	}

	messages := seen.get()

	body["type"] = "read_ok"
	body["messages"] = messages

	return body, nil
}

func handleTopology(n *maelstrom.Node, msg maelstrom.Message) (any, error) {
	var body struct {
		Topology map[string][]string `json:"topology"`
	}
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return nil, err
	}

	id := n.ID()
	neighbours := n.NodeIDs()

	neighboursReq, ok := body.Topology[id]

	ok = compareTopology(id, neighboursReq, neighbours)
	if !ok {
		return nil, fmt.Errorf("topology not ok: got %s, have %s", neighboursReq, neighbours)
	}

	resp := map[string]string{
		"type": "topology_ok",
	}

	return resp, nil
}

func compareTopology(id string, t1 []string, t2 []string) bool {
	lenDiff := len(t1) - len(t2)
	if -1 > lenDiff || lenDiff > 1 {
		return false
	}

	if len(t1) == 0 && len(t2) == 0 {
		return true
	}

	sort.Strings(t1)
	sort.Strings(t2)

	var i, j int
	for i, j = 0, 0; i < len(t1) && j < len(t2); i, j = i+1, j+1 {
		if t2[j] == id {
			i--
			continue
		} else if t1[i] == id {
			j--
			continue
		}
		if t1[i] != t1[j] {
			return false
		}
	}

	if i < len(t1)-1 && j < len(t2) || i < len(t1) && j < len(t2)-1 {
		return false
	}

	return true
}
