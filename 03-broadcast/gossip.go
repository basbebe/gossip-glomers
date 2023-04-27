package main

import (
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	gossipNodesNr  = 8
	gossipInterval = time.Duration(500 * time.Millisecond)
)

type nodeID string
type stateVersion int
type stateValue int
type stateSet map[int]stateVersion

type stateData struct {
	dataLock      *sync.RWMutex
	remoteVersion stateVersion
	localVersion  stateVersion
}

type gossipMessage struct {
	Type             string       `json:"type"`
	IsReply          bool         `json:"isReply"`
	Current          stateVersion `json:"current"`
	SentLocalVersion stateVersion `json:"sentLocalVersion"`
	GotRemoteVersion stateVersion `json:"gotRemoteVersion"`
	Messages         []int        `json:"messages"`
}

type gossipNode struct {
	Node *maelstrom.Node
	id   nodeID

	stateLock  *sync.RWMutex
	version    stateVersion
	values     stateSet
	nodeStates map[nodeID]*stateData

	topoLock *sync.RWMutex
	topology []nodeID
}

func NewGossipNode(gn *maelstrom.Node) *gossipNode {
	return &gossipNode{
		Node:       gn,
		stateLock:  &sync.RWMutex{},
		values:     make(stateSet),
		nodeStates: make(map[nodeID]*stateData),
		topoLock:   &sync.RWMutex{},
	}
}

func (gn *gossipNode) appendValue(v int) {
	gn.stateLock.Lock()
	defer gn.stateLock.Unlock()
	if _, exists := gn.values[v]; !exists {
		gn.version++
		gn.values[v] = gn.version
	}
}

func (gn *gossipNode) appendValueSlice(values []int) {
	var doInc bool = true
	var version stateVersion

	gn.stateLock.Lock()
	defer gn.stateLock.Unlock()

	for _, val := range values {
		if _, exists := gn.values[val]; !exists {
			if doInc {
				gn.version++
				version = gn.version
				doInc = false
			}
			gn.values[val] = version
		}
	}
}

func (gn *gossipNode) deltaGossip(data *stateData, omit []int) *gossipMessage {
	gn.stateLock.RLock()
	defer gn.stateLock.RUnlock()
	current := gn.version

	data.dataLock.RLock()
	defer data.dataLock.RUnlock()
	local, remote := data.localVersion, data.remoteVersion

	delta := []int{}
loop:
	for val, ver := range gn.values {
		if local < ver && ver <= current {
			for _, omit := range omit {
				if val == omit {
					continue loop
				}
			}
			delta = append(delta, int(val))
		}
	}

	msg := &gossipMessage{
		Type:             "gossip",
		Current:          current,
		SentLocalVersion: local,
		GotRemoteVersion: remote,
		Messages:         delta,
	}

	return msg
}

func (gn *gossipNode) matchGossip(data *stateData, msg *gossipMessage) bool {
	go gn.appendValueSlice(msg.Messages)

	data.dataLock.Lock()
	defer data.dataLock.Unlock()

	data.localVersion = msg.GotRemoteVersion
	if msg.SentLocalVersion > data.remoteVersion {
		return false
	}

	data.remoteVersion = msg.Current

	if msg.GotRemoteVersion != gn.version {
		return false
	}

	return true
}

func (gn *gossipNode) getValueList() []int {
	gn.stateLock.RLock()
	defer gn.stateLock.RUnlock()

	newStateList := make([]int, len(gn.values))
	i := 0
	for v := range gn.values {
		newStateList[i] = int(v)
		i++
	}

	return newStateList
}

func (gn *gossipNode) setTopology(topology map[string][]string) error {
	gn.topoLock.Lock()
	defer gn.topoLock.Unlock()

	if len(gn.nodeStates) == 0 {
		id := nodeID(gn.Node.ID())
		gn.id = id
		ids := gn.Node.NodeIDs()
		gn.topology = make([]nodeID, len(ids)-1)
		i := 0
		for _, node := range ids {
			if node == string(id) {
				continue
			}
			gn.nodeStates[nodeID(node)] = &stateData{
				dataLock: &sync.RWMutex{},
			}
			gn.topology[i] = nodeID(node)
			i++
		}
	}

	return nil
}

func registerHandles(gn *gossipNode) {
	gn.handle("broadcast", handleBroadcast)
	gn.handle("read", handleRead)
	gn.handle("topology", handleTopology)
	gn.handle("gossip", handleGossip)
}

func (gn *gossipNode) handle(typ string, fn func(*gossipNode, maelstrom.Message) error) {
	h := gn.Node.Handle
	handlerFunc := func(msg maelstrom.Message) error {
		return fn(gn, msg)
	}
	h(typ, handlerFunc)
}

func runGossip(gn *gossipNode) error {
	rand.Seed(time.Now().UnixNano())

	for {
		time.Sleep(gossipInterval)
		gossip(gn)
	}
}

func gossip(gn *gossipNode) {
	gn.topoLock.RLock()
	defer gn.topoLock.RUnlock()

	if len(gn.topology) < 1 {
		return
	}

	nodeSelection := make(map[nodeID]struct{}, gossipNodesNr)

	for i := 0; i < gossipNodesNr && i < len(gn.topology); i++ {
		randIndex := rand.Intn(len(gn.topology))
		node := gn.topology[randIndex]
		_, exists := nodeSelection[node]
		if exists {
			i--
			continue
		}
		go gossipSend(gn, node)
	}
}

func gossipSend(gn *gossipNode, node nodeID) error {
	body := gn.deltaGossip(gn.nodeStates[node], nil)
	body.Type = "gossip"

	if body.Current == body.SentLocalVersion {
		return nil
	}

	err := gn.Node.Send(string(node), body)
	if err != nil {
		return err
	}

	return nil
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
