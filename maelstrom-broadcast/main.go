package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	gossipNodesNr  = 8
	gossipInterval = time.Duration(200 * time.Millisecond)
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

type nodeState struct {
	Node *maelstrom.Node
	id   nodeID

	stateLock  *sync.RWMutex
	version    stateVersion
	nodeStates map[nodeID]*stateData
	values     stateSet

	topoLock *sync.RWMutex
	topology []nodeID
}

func NewNodeState(n *maelstrom.Node) *nodeState {
	return &nodeState{
		Node:       n,
		stateLock:  &sync.RWMutex{},
		nodeStates: make(map[nodeID]*stateData),
		values:     make(stateSet),
		topoLock:   &sync.RWMutex{},
	}
}

func (s *nodeState) appendValue(v int) {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	if _, exists := s.values[v]; !exists {
		s.version++
		s.values[v] = s.version
	}
}

func (s *nodeState) appendValueSlice(values []int) {
	var doInc bool = true
	var version stateVersion

	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	for _, val := range values {
		if _, exists := s.values[val]; !exists {
			if doInc {
				s.version++
				version = s.version
				doInc = false
			}
			s.values[val] = version
		}
	}
}

func (s *nodeState) deltaGossip(data *stateData, omit []int) *gossipMessage {
	s.stateLock.RLock()
	defer s.stateLock.RUnlock()
	current := s.version

	data.dataLock.RLock()
	defer data.dataLock.RUnlock()
	local, remote := data.localVersion, data.remoteVersion

	delta := []int{}
loop:
	for val, ver := range s.values {
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

func (s *nodeState) matchGossip(data *stateData, msg *gossipMessage) bool {
	go s.appendValueSlice(msg.Messages)

	data.dataLock.Lock()
	defer data.dataLock.Unlock()

	data.localVersion = msg.GotRemoteVersion
	if msg.SentLocalVersion > data.remoteVersion {
		return false
	}

	data.remoteVersion = msg.Current

	if msg.GotRemoteVersion != s.version {
		return false
	}

	return true
}

func (s *nodeState) getStateList() []int {
	s.stateLock.RLock()
	defer s.stateLock.RUnlock()

	newStateList := make([]int, len(s.values))
	i := 0
	for v := range s.values {
		newStateList[i] = int(v)
		i++
	}

	return newStateList
}

func (s *nodeState) setTopology(topology map[string][]string) error {
	s.topoLock.Lock()
	defer s.topoLock.Unlock()

	if len(s.nodeStates) == 0 {
		id := nodeID(s.Node.ID())
		s.id = id
		ids := s.Node.NodeIDs()
		s.topology = make([]nodeID, len(ids)-1)
		i := 0
		for _, node := range ids {
			if node == string(id) {
				continue
			}
			s.nodeStates[nodeID(node)] = &stateData{
				dataLock: &sync.RWMutex{},
			}
			s.topology[i] = nodeID(node)
			i++
		}
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

	state = NewNodeState(n)

	registerHandles(state)

	go runGossip(state)

	err = n.Run()
	if err != nil {
		return err
	}

	return nil
}

func registerHandles(state *nodeState) {
	registerHandleReply(state, "broadcast", handleBroadcast)
	registerHandleReply(state, "read", handleRead)
	registerHandleReply(state, "topology", handleTopology)
	registerHandle(state, "gossip", handleGossip)
}

func registerHandleReply(state *nodeState, typ string, fn func(*nodeState, maelstrom.Message) (any, error)) {
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

func registerHandle(state *nodeState, typ string, fn func(*nodeState, maelstrom.Message) error) {
	h := state.Node.Handle

	handlerFunc := func(msg maelstrom.Message) error {
		err := fn(state, msg)
		if err != nil {
			return err
		}

		return nil
	}

	h(typ, handlerFunc)
}

func runGossip(state *nodeState) error {
	rand.Seed(time.Now().UnixNano())

	for {
		time.Sleep(gossipInterval)
		gossip(state)
	}
}

func gossip(state *nodeState) {
	state.topoLock.RLock()
	defer state.topoLock.RUnlock()

	if len(state.topology) < 1 {
		return
	}

	nodeSelection := make(map[nodeID]struct{}, gossipNodesNr)

	for i := 0; i < gossipNodesNr && i < len(state.topology); i++ {
		randIndex := rand.Intn(len(state.topology))
		node := state.topology[randIndex]
		_, exists := nodeSelection[node]
		if exists {
			i--
			continue
		}
		go gossipSend(state, node)
	}
}

func gossipSend(state *nodeState, node nodeID) error {
	body := state.deltaGossip(state.nodeStates[node], nil)
	body.Type = "gossip"

	if body.Current == body.SentLocalVersion {
		return nil
	}

	err := state.Node.Send(string(node), body)
	if err != nil {
		return err
	}

	return nil
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
	messages := state.getStateList()

	body := map[string]any{
		"type":     "read_ok",
		"messages": messages,
	}

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

func handleGossip(state *nodeState, msg maelstrom.Message) error {
	var body *gossipMessage
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return err
	}

	data, ok := state.nodeStates[nodeID(msg.Src)]
	if !ok {
		panic("node state not found")
	}

	unchanged := state.matchGossip(data, body)

	if unchanged || body.IsReply {
		return nil
	}

	return replyGossip(state, nodeID(msg.Src), body.Messages)
}

func replyGossip(state *nodeState, node nodeID, messages []int) error {
	body := state.deltaGossip(state.nodeStates[node], messages)
	body.IsReply = true

	if len(body.Messages) == 0 && body.SentLocalVersion == body.Current {
		return nil
	}

	err := state.Node.Send(string(node), body)
	if err != nil {
		return err
	}

	return nil
}
