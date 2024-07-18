package distribution

import (
	"sort"
	"sync"

	"github.com/lafikl/consistent"
)

// Assigner locally assigns the owner for the key, see NodeFor and IsOwner methods. It is part of the Node.
//
// The hash ring/consistent hashing pattern is used to make the assigment,
// it is provided by the "consistent" package, see TestConsistentHashLib for more information.
type Assigner struct {
	nodeID string
	mutex  *sync.RWMutex
	nodes  *consistent.Consistent
}

func newAssigner(nodeID string) *Assigner {
	return &Assigner{
		nodeID: nodeID,
		mutex:  &sync.RWMutex{},
		nodes:  consistent.New(),
	}
}

// NodeID returns ID of the current node.
func (a *Assigner) NodeID() string {
	return a.nodeID
}

// Nodes method returns IDs of all known nodes.
func (a *Assigner) Nodes() []string {
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	out := a.nodes.Hosts()
	sort.Strings(out)
	return out
}

// NodesCount method returns count of known nodes.
func (a *Assigner) NodesCount() int {
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	return len(a.nodes.Hosts())
}

// NodeFor returns ID of the key's owner node.
// The consistent.ErrNoHosts may occur if there is no node in the list.
func (a *Assigner) NodeFor(key string) (string, error) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	return a.nodes.Get(key)
}

// MustGetNodeFor returns ID of the key's owner node.
// The method panic if there is no node in the list.
func (a *Assigner) MustGetNodeFor(key string) string {
	node, err := a.NodeFor(key)
	if err != nil {
		panic(err)
	}
	return node
}

// IsOwner method returns true, if the node is owner of the key.
// The consistent.ErrNoHosts may occur if there is no node in the list.
func (a *Assigner) IsOwner(key string) (bool, error) {
	node, err := a.NodeFor(key)
	if err != nil {
		return false, err
	}
	return node == a.nodeID, nil
}

// MustCheckIsOwner method returns true, if the node is owner of the key.
// The method panic if there is no node in the list.
func (a *Assigner) MustCheckIsOwner(key string) bool {
	is, err := a.IsOwner(key)
	if err != nil {
		panic(err)
	}
	return is
}

// HasNode returns true if the nodeID is known.
func (a *Assigner) HasNode(nodeID string) bool {
	for _, v := range a.Nodes() {
		if v == nodeID {
			return true
		}
	}
	return false
}

func (a *Assigner) clone() *Assigner {
	clone := newAssigner(a.nodeID)
	for _, nodeID := range a.Nodes() {
		clone.addNode(nodeID)
	}
	return clone
}

// lock acquires write lock for resetNodes, addNode, removeNode operations,
// it provides the ability to make multiple changes atomically.
func (a *Assigner) lock() {
	a.mutex.Lock()
}

// unlock releases write lock for resetNodes, addNode, removeNode operations.
func (a *Assigner) unlock() {
	a.mutex.Unlock()
}

func (a *Assigner) resetNodes() {
	a.nodes = consistent.New()
}

func (a *Assigner) addNode(nodeID string) {
	a.nodes.Add(nodeID)
}

func (a *Assigner) removeNode(nodeID string) bool {
	return a.nodes.Remove(nodeID)
}
