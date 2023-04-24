package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	// "fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	gossipNodesNr  = 2
	gossipInterval = time.Duration(1000 * time.Millisecond)
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

// func (s stateSet) merge(s2 []int) {
// 	for _, val := range s2 {
// 		s.add(val)
// 	}
// }
// func (s stateSet) union2(s2 stateSet) {
// 	for val := range s2 {
// 		s.add(val)
// 	}
// }

type gossipMessage struct {
	Type             string       `json:"type"`
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
		// data =
	}

	curr, local, remote := s.version, data.localVersion, data.remoteVersion
	if to == 0 {
		to = s.version
	}
	from := local

	fmt.Fprintf(stderr, "DEBUG: Creating delta from %d to %d\n", from, to)

	delta := []int{}
	for val, ver := range s.values {
		if from < ver && ver <= to {
			delta = append(delta, int(val))
		}
	}
	fmt.Fprintf(stderr, "DEBUG: delta: %v\n", delta)
	fmt.Fprintf(stderr, "DEBUG: state: %+v\n", s.values)

	msg := &gossipMessage{
		Current:          curr,
		SentLocalVersion: local,
		GotRemoteVersion: remote,
		Messages:         delta,
	}

	return msg
}

// func (s *nodeState) diffGossip(node nodeID, state []int) {
// 	s.stateLock.Lock()
// 	defer s.stateLock.Unlock()

// 	s.nodeStates[node].merge(state)
// }

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
				// fmt.Fprintf(stderr, "DEBUG: Increasing version %d\n", s.version)
				s.version++
				// fmt.Fprintf(stderr, "DEBUG: Increased version %d\n", s.version)
				inc = false
			}
			s.values.add(val, s.version)
			// fmt.Fprintf(stderr, "DEBUG: adding %d with version %d\n", val, s.version)
		}
	}

	// fmt.Fprintf(stderr, "DEBUG: local - curr: %d, has: %d, have: %d\n", s.version, gotLocalVer, int(data.localVersion))
	data.localVersion = gotLocalVer

	if sentRemoteVer != data.remoteVersion {
		// data.Received = 0
		fmt.Fprintf(stderr, "DEBUG: fail. remote has: %d, have: %d\n", sentRemoteVer, int(data.remoteVersion))
		// data.Sent = 0
		return 0, false
	}
	fmt.Fprintf(stderr, "DEBUG: remote - has: %d, have: %d\n", sentRemoteVer, int(data.remoteVersion))
	data.remoteVersion = curr
	// data.Received = curr
	if gotLocalVer != s.version {
		return 0, false
	}
	return 0, true
}

// func (s *nodeState) resetGossip(node nodeID) {
// 	s.stateLock.Lock()
// 	defer s.stateLock.Unlock()

// 	s.nodeStates[node] = &stateData{}
// }

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

	// state, ok := s.nodeStates[s.id]
	// if !ok {
	// 	panic("ah")
	// }
	// fmt.Println(s, s.nodeStates, state)

	newStateList := make([]int, len(s.values))
	i := 0
	for v := range s.values {
		newStateList[i] = int(v)
		i++
	}

	return newStateList
}

// func (s *nodeState) getIDState(id nodeID) stateSet {
// 	s.stateLock.RLock()
// 	defer s.stateLock.RUnlock()

// 	state := s.nodeStates[id]
// 	newState := make(stateSet, len(state))
// 	for v := range state {
// 		newState.add(v)
// 	}

// 	return newState
// }

func (s *nodeState) setTopology(topology map[string][]string) error {
	s.topoLock.Lock()
	defer s.topoLock.Unlock()

	if len(s.nodeStates) == 0 {
		s.id = nodeID(s.Node.ID())
	}
	id := s.Node.ID()

	_, ok := s.nodeStates[s.id]
	if !ok {
		s.nodeStates[s.id] = &stateData{}
	}

	topo, ok := topology[id]
	if !ok {
		return errors.New("could not set topology")
	}

	s.topology = s.topology[:0]
	for _, id := range topo {
		s.topology = append(s.topology, nodeID(id))
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
	registerHandle(state, "gossip_reply", handleGossipReply)
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

	for i := 0; i < gossipNodesNr; i++ {
		node := state.topology[rand.Intn(len(state.topology))]
		id := state.id
		go gossipSend(state, id, node)
	}
}

func gossipSend(state *nodeState, id nodeID, node nodeID) error {
	// data, ok := state
	body := state.deltaGossip(node, 0)
	// if len(delta) == 0 || curr == sent {
	// 	return nil
	// }
	body.Type = "gossip"

	if body.Current == body.SentLocalVersion {
		return nil
	}

	err := state.Node.Send(string(node), body)
	if err != nil {
		return err
	}

	// state.updateGossipState(node, body.Current)

	// state.diffGossip(node, delta)

	// return state.Node.RPC(id, body, nil)
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
		// state.resetGossip(nodeID(msg.Src))
	}

	return replyGossip(state, nodeID(msg.Src), last)
	// return nil
}

func replyGossip(state *nodeState, node nodeID, lastRecv stateVersion) error {
	body := state.deltaGossip(node, lastRecv)

	body.Type = "gossip_reply"

	err := state.Node.Send(string(node), body)
	if err != nil {
		return err
	}

	// state.updateGossipState(node, body.Current)
	return nil
}

func handleGossipReply(state *nodeState, msg maelstrom.Message) error {
	var body *gossipMessage
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return err
	}

	_, ok := state.matchGossip(nodeID(msg.Src), body.Messages, body.Current, body.SentLocalVersion, body.GotRemoteVersion)
	if !ok {
		// state.resetGossip(nodeID(msg.Src))
	}

	// state.updateGossipState(nodeID(msg.Src), body.Current)

	return nil
}
