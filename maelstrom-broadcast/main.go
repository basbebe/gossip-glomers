package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	gossipNodesNr  = 8
	gossipInterval = time.Duration(100 * time.Millisecond)
)

var stderr = os.Stderr

type nodeID string
type stateVersion int
type stateValue int
type stateSet map[int]stateVersion
type stateData struct {
	// id nodeID

	// dataLock *sync.RWMutex
	remoteVersion stateVersion
	localVersion  stateVersion
}

func (s stateSet) add(val int, ver stateVersion) {
	s[val] = ver
}

func (s stateSet) exists(v int) bool {
	_, ok := s[v]
	return ok
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

	if !s.values.exists(v) {
		s.version++
		s.values.add(v, s.version)
	}
}

func (s *nodeState) deltaGossip(node nodeID, to stateVersion) *gossipMessage {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	data, ok := s.nodeStates[node]
	if !ok {
		data = &stateData{}
		s.nodeStates[node] = data
	}

	curr, local, remote := s.version, data.localVersion, data.remoteVersion
	if to == 0 {
		to = s.version
	}
	from := local

	delta := []int{}
	for val, ver := range s.values {
		if from < ver && ver <= to {
			delta = append(delta, int(val))
		}
	}

	msg := &gossipMessage{
		Type:             "gossip",
		Current:          curr,
		SentLocalVersion: local,
		GotRemoteVersion: remote,
		Messages:         delta,
	}

	return msg
}

func (s *nodeState) matchGossip(node nodeID, state []int, curr stateVersion, sentRemoteVer stateVersion, gotLocalVer stateVersion) (last stateVersion, unchanged bool) {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	data, ok := s.nodeStates[node]
	if !ok {
		data = &stateData{}
		s.nodeStates[node] = data
	}

	inc := true
	for _, val := range state {
		if !s.values.exists(val) {
			if inc {
				s.version++
				inc = false
			}
			s.values.add(val, s.version)
		}
	}

	data.localVersion = gotLocalVer

	if sentRemoteVer != data.remoteVersion {
		return 0, false
	}

	data.remoteVersion = curr
	if gotLocalVer != s.version {
		return 0, false
	}
	return 0, true
}

func (s *nodeState) updateGossipState(node nodeID, ver stateVersion) {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	data, ok := s.nodeStates[node]
	if !ok {
		data = &stateData{}
		s.nodeStates[node] = data
	}
	data.localVersion = ver
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
			s.nodeStates[nodeID(node)] = &stateData{}
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

	for i := 0; i < gossipNodesNr && i < len(state.topology); i++ {
		randIndex := rand.Intn(len(state.topology))
		node := state.topology[randIndex]
		go gossipSend(state, node)
	}
}

func gossipSend(state *nodeState, node nodeID) error {
	body := state.deltaGossip(node, 0)
	if node == "" {
		panic(state)
	}
	body.Type = "gossip"

	if body.Current == body.SentLocalVersion {
		return nil
	}

	err := state.Node.Send(string(node), body)
	if err != nil {
		fmt.Println(node)
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

	last, unchanged := state.matchGossip(nodeID(msg.Src), body.Messages, body.Current, body.SentLocalVersion, body.GotRemoteVersion)
	if unchanged {
		return nil
	}

	if body.IsReply {
		return nil
	}

	return replyGossip(state, nodeID(msg.Src), last)
}

func replyGossip(state *nodeState, node nodeID, lastRecv stateVersion) error {
	body := state.deltaGossip(node, lastRecv)

	body.IsReply = true

	err := state.Node.Send(string(node), body)
	if err != nil {
		return err
	}

	return nil
}
