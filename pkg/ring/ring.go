package ring

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"sort"
	"sync"

)

// type Ring interface {
// 	GetNode(key string) string
// 	AddNode(name, addr string)
// 	RemoveNode(name string)
// 	Nodes() []string
// }

const VirtualNodes = 150

type VNode struct {
	Hash uint32
	Name string
}

type Ring struct {
	mu     sync.RWMutex
	vnodes []VNode
	nodes  map[string]struct{}
}

func New() *Ring {
	return &Ring{
		nodes: make(map[string]struct{}),
	}
}

func HashKey(key string) uint32 {
	h := fnv.New32a()
	_, err := h.Write([]byte(key))
	if err != nil {
		slog.Error("Error converting")
	}
	return h.Sum32()
}

// AddNode

func (r *Ring) AddNode(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, err := r.nodes[name]; err {
		return
	}

	r.nodes[name] = struct{}{}

	for i := 0; i < VirtualNodes; i++ {
		vkey := fmt.Sprintf("%s#%d", name, i)
		vnode := VNode{
			Hash: HashKey(vkey),
			Name: name,
		}

		r.vnodes = append(r.vnodes, vnode)

	}

	sort.Slice(r.vnodes, func(i, j int) bool {
		return r.vnodes[i].Hash < r.vnodes[j].Hash
	})
}

// Remove Node - gracefull

func (r *Ring) RemoveNode(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.nodes, name)

	filtered := r.vnodes[:0]
	for _, vn := range r.vnodes {
		if vn.Name != name {
			filtered = append(filtered, vn)
		}
	}

	r.vnodes = filtered
}

//Get Node

func (r *Ring) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return ""
	}

	h := HashKey(key)

	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].Hash >= h
	})

	if idx == len(r.vnodes) {
		idx = 0
	}

	return r.vnodes[idx].Name
}

func (r *Ring) Nodes() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	names := make([]string, 0, len(r.nodes))

	for name := range r.nodes {
		names = append(names, name)
	}

	sort.Strings(names)
	return names
}

// Size of the ring

func (r *Ring) Size() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.nodes)
}

func (r *Ring) GetN(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 || n <= 0 {
		return nil
	}

	h := HashKey(key)

	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].Hash >= h
	})

	seen := make(map[string]bool)
	result := make([]string, 0, n)

	for len(result) < n && len(result) < len(r.nodes) {
		name := r.vnodes[idx%len(r.vnodes)].Name
		if !seen[name] {
			seen[name] = true
			result = append(result, name)
		}
		idx++
	}
	return result
}
