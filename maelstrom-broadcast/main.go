package main

import (
	"encoding/json"
	// "errors"
	"fmt"
	// "strconv"

	// "fmt"
	"log"
	"sort"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type nodeHandlerFunc func(maelstrom.Message) (any, error)

type messageStore []int

var seen messageStore

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
	storeWrite := make(chan int)
	storeRead := make(chan (chan []int))

	go store(storeRead, storeWrite)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		resp, err := handleBroadcast(msg, storeWrite)
		if err != nil {
			return err
		}
		return n.Reply(msg, resp)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		resp, err := handleRead(msg, storeRead)
		if err != nil {
			return err
		}
		return n.Reply(msg, resp)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		resp, err := handleTopology(n, msg, storeRead)
		if err != nil {
			return err
		}
		return n.Reply(msg, resp)
	})

	// registerHandle(n, "broadcast", handleBroadcast)
}

func store(r <-chan (chan []int), w <-chan int) {
	var store []int

	for {
		select {
		case ch := <-r:
			ch <- store
			close(ch)

		case val := <-w:
			store = append(store, val)
		}
	}
}

func registerHandle(n *maelstrom.Node, typ string, fn nodeHandlerFunc) {
	n.Handle(typ, addHandle(n, typ, fn))
}

func addHandle(n *maelstrom.Node, typ string, fn nodeHandlerFunc) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		_, err := fn(msg)
		return err
	}
}

func addReplyHandle(n *maelstrom.Node, typ string, fn nodeHandlerFunc) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		body, err := fn(msg)
		if err != nil {
			return err
		}

		return n.Reply(msg, body)
	}
}

func handleBroadcast(msg maelstrom.Message, store chan<- int) (any, error) {

	// var body map[string]any
	var body struct {
		Message int `json:"message"`
	}
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return nil, err
	}

	store <- body.Message

	// switch message.(type) {
	// case int:
	// 	store <- message.(int)
	// case string:
	// 	num, err := strconv.Atoi(message.(string))
	// 	if err != nil {
	// 		return errors.New("message is string, not an integer")
	// 	}
	// 	store <- num
	// default:
	// 	fmt.Println()
	// 	return errors.New("message is not an integer")
	// }

	resp := map[string]string{
		"type": "broadcast_ok",
	}

	return resp, nil
}

func handleRead(msg maelstrom.Message, read chan<- (chan []int)) (any, error) {
	var body map[string]any
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return nil, err
	}

	resp := make(chan []int)

	read <- resp
	messages := <-resp

	body["type"] = "read_ok"
	body["messages"] = messages

	return body, nil
}

func handleTopology(n *maelstrom.Node, msg maelstrom.Message, read chan<- chan []int) (any, error) {
	var body struct {
		MsgID    int                 `json:"msg_id"`
		Typ      string              `json:"type"`
		Topology map[string][]string `json:"topology"`
	}
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return nil, err
	}
	// fmt.Println(body, body.Topology)

	// topology, ok := body.topology
	// if !ok {
	// 	return nil, errors.New("no topology received")
	// }

	// fmt.Printf("%t\n", topology)

	id := n.ID()
	neighbours := n.NodeIDs()

	neighboursReq, ok := body.Topology[id]

	ok = compareTopology(id, append(neighboursReq, id), neighbours)
	if !ok {
		return nil, fmt.Errorf("topology not ok, %s, %s, %s, %+v, id: %s", neighboursReq, neighbours, body.Topology, body, id)
	}

	resp := make(map[string]string)
	resp["type"] = "topology_ok"

	body.Typ = "topology_ok"

	return resp, nil
}

func compareTopology(id string, t1 []string, t2 []string) bool {
	if len(t1) != len(t2) {
		return false
	}

	if len(t1) == 0 && len(t2) == 0 {
		return true
	}

	sort.Strings(t1)
	sort.Strings(t2)

	for i, n1 := range t1 {
		if t2[i] != n1 {
			return false
		}
		if n1 == id {
			continue
		}
	}

	return true
}
