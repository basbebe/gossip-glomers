package main

import (
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func registerHandles(gn *gossipNode) {
	gn.handle("broadcast", handleBroadcast)
	gn.handle("read", handleRead)
	gn.handle("topology", handleTopology)
	gn.handle("gossip", handleGossip)
}
func handleBroadcast(gn *gossipNode, msg maelstrom.Message) error {
	var body struct {
		Message int `json:"message"`
	}
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return err
	}

	gn.appendValue(body.Message)

	resp := map[string]string{
		"type": "broadcast_ok",
	}

	return gn.Node.Reply(msg, resp)
}

func handleRead(gn *gossipNode, msg maelstrom.Message) error {
	messages := gn.getValueList()

	resp := map[string]any{
		"type":     "read_ok",
		"messages": messages,
	}

	return gn.Node.Reply(msg, resp)
}

func handleTopology(gn *gossipNode, msg maelstrom.Message) error {
	var body struct {
		Topology map[string][]string `json:"topology"`
	}
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return err
	}

	err = gn.setTopology(body.Topology)
	if err != nil {
		return err
	}

	resp := map[string]string{
		"type": "topology_ok",
	}

	return gn.Node.Reply(msg, resp)
}

func handleGossip(gn *gossipNode, msg maelstrom.Message) error {
	var body *gossipMessage
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return err
	}

	data, ok := gn.nodeStates[nodeID(msg.Src)]
	if !ok {
		panic("node state not found")
	}

	unchanged := gn.matchGossip(data, body)

	if unchanged || body.IsReply {
		return nil
	}

	return replyGossip(gn, nodeID(msg.Src), body.Messages)
}

func replyGossip(gn *gossipNode, node nodeID, messages []int) error {
	body := gn.deltaGossip(gn.nodeStates[node], messages)
	body.IsReply = true

	if len(body.Messages) == 0 && body.SentLocalVersion == body.Current {
		return nil
	}

	err := gn.Node.Send(string(node), body)
	if err != nil {
		return err
	}

	return nil
}