package main

import (
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	gossipNodesNr  = 2
	gossipInterval = time.Duration(500 * time.Millisecond)
)

type nodeID string
type stateSet map[int]struct{}

func (s stateSet) add(v int) {
	s[v] = struct{}{}
}

func (s stateSet) exists(v int) bool {
	_, ok := s[v]
	return ok
}

func (s stateSet) merge(s2 []int) {
	for _, val := range s2 {
		s.add(val)
	}
}
func (s stateSet) union2(s2 stateSet) {
	for val := range s2 {
		s.add(val)
	}
}

type nodeState struct {
	Node *maelstrom.Node
	id   nodeID

	stateLock  *sync.RWMutex
	nodeStates map[nodeID]stateSet

	topoLock *sync.RWMutex
	topology []nodeID
}

func NewNodeState(n *maelstrom.Node) *nodeState {
	return &nodeState{
		Node:       n,
		stateLock:  &sync.RWMutex{},
		nodeStates: make(map[nodeID]stateSet),
		topoLock:   &sync.RWMutex{},
	}
}

func (s *nodeState) appendValue(n int) {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	s.nodeStates[s.id].add(n)
}

func (s *nodeState) deltaGossip(node nodeID) (have int, assume int, delta []int) {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	deltaState, ok := s.nodeStates[node]
	if !ok {
		s.nodeStates[node] = make(stateSet)
		deltaState = s.nodeStates[node]
	}

	for val := range s.nodeStates[s.id] {
		if !deltaState.exists(val) {
			delta = append(delta, val)
		}
	}
	// fmt.Println("delta: ", delta)

	return len(s.nodeStates[s.id]), len(s.nodeStates[node]), delta
}

func (s *nodeState) diffGossip(node nodeID, state []int) {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	s.nodeStates[node].merge(state)
}

func (s *nodeState) matchGossip(node nodeID, state []int) {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	s.topoLock.Lock()
	id := s.id
	s.topoLock.Unlock()

	ownState := s.nodeStates[id]

	ownState.merge(state)
	_, ok := s.nodeStates[node]
	if !ok {
		s.nodeStates[node] = make(stateSet)
	}
	s.nodeStates[node].merge(state)

	return
}

func (s *nodeState) getStateList() []int {
	s.stateLock.RLock()
	defer s.stateLock.RUnlock()

	state, ok := s.nodeStates[s.id]
	if !ok {
		panic("ah")
	}
	// fmt.Println(s, s.nodeStates, state)

	newStateList := make([]int, len(state))
	i := 0
	for v := range state {
		newStateList[i] = v
		i++
	}

	return newStateList
}

func (s *nodeState) getIDState(id nodeID) stateSet {
	s.stateLock.RLock()
	defer s.stateLock.RUnlock()

	state := s.nodeStates[id]
	newState := make(stateSet, len(state))
	for v := range state {
		newState.add(v)
	}

	return newState
}

func (s *nodeState) setTopology(topology map[string][]string) error {
	s.topoLock.Lock()
	defer s.topoLock.Unlock()

	if len(s.nodeStates) == 0 {
		s.id = nodeID(s.Node.ID())
	}
	id := s.Node.ID()

	states, ok := s.nodeStates[s.id]
	if states == nil || !ok {
		s.nodeStates[s.id] = make(stateSet)
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
	have, assume, delta := state.deltaGossip(node)
	if len(delta) == 0 {
		return nil
	}
	body := map[string]any{
		"type":       "gossip",
		"source":     id,
		"haveSize":   have,
		"assumeSize": assume,
		"messages":   delta,
	}
	err := state.Node.Send(string(node), body)
	if err != nil {
		return err
	}

	state.diffGossip(node, delta)

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
	var body struct {
		Source     nodeID `json:"source"`
		HaveSize   int    `json:"haveSize"`
		AssumeSize int    `json:"assumeSize"`
		Messages   []int  `json:"messages"`
	}
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return err
	}
	// fmt.Printf("got gossip: %+v, %+v", msg, body)

	state.matchGossip(body.Source, body.Messages)

	return replyGossip(state, body.Source, body.AssumeSize)
	// return nil
}

func replyGossip(state *nodeState, node nodeID, assumed int) error {
	have, assume, delta := state.deltaGossip(node)

	if len(delta) < 1 {
		return nil
	}

	if assumed < have {

	}

	body := map[string]any{
		"type":       "gossip_reply",
		"source":     state.id,
		"haveSize":   have,
		"assumeSize": assume,
		"messages":   delta,
	}

	err := state.Node.Send(string(node), body)
	if err != nil {
		return err
	}
	// fmt.Printf("sent response to %s: %+v", node, body)

	return nil
}

func handleGossipReply(state *nodeState, msg maelstrom.Message) error {
	var body struct {
		Source     nodeID `json:"source"`
		HaveSize   int    `json:"haveSize"`
		AssumeSize int    `json:"assumeSize"`
		Messages   []int  `json:"messages"`
	}
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return err
	}
	// fmt.Printf("got response: %+v", body)

	state.matchGossip(body.Source, body.Messages)

	return nil
}
