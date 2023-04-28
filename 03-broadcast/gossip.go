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
type stateVersion int
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

type gossipData struct {
	gossipLock      *sync.RWMutex
	clusterData     map[nodeID]*stateData
	versionedValues map[stateVersion]*[]int
	nodeList        []nodeID
}

func (gd *gossipData) addVersion(version stateVersion, values *[]int) {
	gd.gossipLock.Lock()
	defer gd.gossipLock.Unlock()
	gd.versionedValues[version] = values
}

func (gd *gossipData) discardStaleVersions(version stateVersion) {
	gd.gossipLock.Lock()
	defer gd.gossipLock.Unlock()
	for v := range gd.versionedValues {
		if v >= version {
			continue
		}
		delete(gd.versionedValues, v)
	}
}

type gossipNode struct {
	Node *maelstrom.Node
	id   nodeID

	stateLock *sync.RWMutex
	version   stateVersion
	values    stateSet

	gossipData
}

func NewGossipNode(gn *maelstrom.Node) *gossipNode {
	return &gossipNode{
		Node:      gn,
		stateLock: &sync.RWMutex{},
		values:    make(stateSet),
		gossipData: gossipData{
			clusterData:     make(map[nodeID]*stateData),
			gossipLock:      &sync.RWMutex{},
			versionedValues: make(map[stateVersion]*[]int),
		},
	}
}

func (gn *gossipNode) appendValue(v int) {
	gn.stateLock.Lock()
	defer gn.stateLock.Unlock()
	_, exists := gn.values[v]
	if exists {
		return
	}
	gn.version++
	gn.values[v] = gn.version
	gn.gossipData.addVersion(gn.version, &[]int{v})
}

func (gn *gossipNode) appendValueSlice(values []int) {
	gn.stateLock.Lock()
	defer gn.stateLock.Unlock()
	var (
		doInc     bool = true
		version   stateVersion
		newValues []int
	)
	for _, val := range values {
		_, exists := gn.values[val]
		if exists {
			continue
		}
		if doInc {
			gn.version++
			version = gn.version
			doInc = false
		}
		gn.values[val] = version
		newValues = append(newValues, val)
	}
	if doInc == true {
		return
	}
	gn.gossipData.addVersion(version, &newValues)
}

func (gn *gossipNode) deltaGossip(data *stateData, omit []int) *gossipMessage {
	gn.stateLock.RLock()
	defer gn.stateLock.RUnlock()
	gn.gossipLock.RLock()
	defer gn.gossipLock.RUnlock()
	data.dataLock.RLock()
	defer data.dataLock.RUnlock()
	var (
		current = gn.version
		local   = data.localVersion
		remote  = data.remoteVersion
		delta   []int
	)
loop:
	for val, ver := range gn.values {
		if ver <= local {
			continue
		}
		for _, omit := range omit {
			if val == omit {
				continue loop
			}
		}
		delta = append(delta, int(val))
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
	gn.gossipLock.RLock()
	defer gn.gossipLock.RUnlock()
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
			dataLock: &sync.RWMutex{},
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
