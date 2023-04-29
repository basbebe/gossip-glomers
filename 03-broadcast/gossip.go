package main

import (
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
type stateVersion uint
type versionedValues map[int]*version

type version struct {
	nr     stateVersion
	values []int
	prev   *version
	next   *version
}

type stateData struct {
	dataLock      *sync.RWMutex
	remoteVersion stateVersion
	localVersion  *version
}

type gossipMessage struct {
	Type             string       `json:"type"`
	IsReply          bool         `json:"isReply"`
	Current          stateVersion `json:"current"`
	SentLocalVersion stateVersion `json:"sentLocalVersion"`
	GotRemoteVersion stateVersion `json:"gotRemoteVersion"`
	Messages         []int        `json:"messages"`
}

type gossipData struct {
	gossipLock  *sync.RWMutex
	clusterData map[nodeID]*stateData
	nodeList    []nodeID
}

type gossipNode struct {
	Node *maelstrom.Node
	id   nodeID

	stateLock *sync.RWMutex
	version   *version
	values    versionedValues

	gossipData
}

func NewGossipNode(gn *maelstrom.Node) *gossipNode {
	return &gossipNode{
		Node:      gn,
		stateLock: &sync.RWMutex{},
		version:   &version{nr: 0},
		values:    make(versionedValues),
		gossipData: gossipData{
			gossipLock:  &sync.RWMutex{},
			clusterData: make(map[nodeID]*stateData),
		},
	}
}

func (gn *gossipNode) appendValue(val int) {
	gn.stateLock.Lock()
	defer gn.stateLock.Unlock()
	_, exists := gn.values[val]
	if exists {
		return
	}
	newVersion := &version{
		nr:     gn.version.nr + 1,
		values: []int{val},
		prev:   gn.version,
	}
	gn.values[val] = newVersion
	gn.version.next = newVersion
	gn.version = newVersion
}

func (gn *gossipNode) appendValueSlice(values []int) {
	gn.stateLock.Lock()
	defer gn.stateLock.Unlock()
	var (
		doInc bool = true
		// version    stateVersion
		newVersion *version
	)
	for _, val := range values {
		_, exists := gn.values[val]
		if exists {
			continue
		}
		if doInc {
			newVersion = &version{
				nr:   gn.version.nr + 1,
				prev: gn.version,
			}
		}
		newVersion.values = append(newVersion.values, val)
		gn.values[val] = newVersion
	}
	if doInc == true {
		return
	}
	gn.version.next = newVersion
	gn.version = newVersion
}

func (gn *gossipNode) deltaGossip(data *stateData, omit []int) *gossipMessage {
	gn.stateLock.RLock()
	defer gn.stateLock.RUnlock()
	gn.gossipLock.RLock()
	defer gn.gossipLock.RUnlock()
	data.dataLock.RLock()
	defer data.dataLock.RUnlock()
	var (
		current = gn.version.nr
		local   = data.localVersion
		remote  = data.remoteVersion
		omitMap = make(map[int]struct{}, len(omit))
		delta   []int
	)
	for _, v := range omit {
		omitMap[v] = struct{}{}
	}
	for curr := local.next; curr != nil; curr = curr.next {
		for _, val := range curr.values {
			_, exists := omitMap[val]
			if exists {
				continue
			}
			delta = append(delta, val)
		}
	}
	msg := &gossipMessage{
		Type:             "gossip",
		Current:          current,
		SentLocalVersion: local.nr,
		GotRemoteVersion: remote,
		Messages:         delta,
	}
	return msg
}

func (gn *gossipNode) mergeGossip(data *stateData, msg *gossipMessage) bool {
	gn.appendValueSlice(msg.Messages)
	gn.gossipLock.RLock()
	defer gn.gossipLock.RUnlock()
	data.dataLock.Lock()
	defer data.dataLock.Unlock()
	for curr := gn.version; curr.nr > msg.GotRemoteVersion; curr = curr.prev {
		if curr.nr == msg.GotRemoteVersion {
			data.localVersion = curr
		}
	}
	if msg.SentLocalVersion > data.remoteVersion {
		return false
	}
	data.remoteVersion = msg.Current
	if msg.GotRemoteVersion != gn.version.nr {
		return false
	}
	return true
}

func (gn *gossipNode) getValueList() []int {
	gn.stateLock.RLock()
	defer gn.stateLock.RUnlock()
	var (
		newStateList = make([]int, len(gn.values))
		i            = 0
	)
	for v := range gn.values {
		newStateList[i] = int(v)
		i++
	}
	return newStateList
}

func (gn *gossipNode) setTopology(topology map[string][]string) error {
	gn.gossipLock.Lock()
	defer gn.gossipLock.Unlock()
	if len(gn.clusterData) > 0 {
		return nil
	}
	var (
		id    = nodeID(gn.Node.ID())
		ids   = gn.Node.NodeIDs()
		nodes = make([]nodeID, len(ids)-1)
		i     = 0
	)
	gn.id = id
	for _, node := range ids {
		if node == string(id) {
			continue
		}
		gn.clusterData[nodeID(node)] = &stateData{
			dataLock:     &sync.RWMutex{},
			localVersion: gn.version,
		}
		nodes[i] = nodeID(node)
		i++
	}
	gn.nodeList = nodes
	return nil
}

func (gn *gossipNode) handle(typ string, fn func(*gossipNode, maelstrom.Message) error) {
	h := gn.Node.Handle
	handlerFunc := func(msg maelstrom.Message) error {
		return fn(gn, msg)
	}
	h(typ, handlerFunc)
}

func (gn *gossipNode) runGossip() error {
	rand.Seed(time.Now().UnixNano())
	for {
		time.Sleep(gossipInterval)
		gn.gossip()
	}
}

func (gn *gossipNode) gossip() {
	gn.gossipLock.RLock()
	defer gn.gossipLock.RUnlock()
	if len(gn.nodeList) < 1 {
		return
	}
	nodeSelection := make(map[nodeID]struct{}, gossipNodesNr)
	for i := 0; i < gossipNodesNr && i < len(gn.nodeList); i++ {
		randIndex := rand.Intn(len(gn.nodeList))
		node := gn.nodeList[randIndex]
		_, exists := nodeSelection[node]
		if exists {
			i--
			continue
		}
		go gn.sendGossip(node)
	}
}

func (gn *gossipNode) sendGossip(node nodeID) error {
	body := gn.deltaGossip(gn.clusterData[node], nil)
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
