package ring

import (
	"fmt"
	"testing"
)

// Test 1 ===== GetNode on Empty Ring ================

func TestNodeGetNode_EmptyRing(t *testing.T) {
	r := New()
	got := r.GetNode("Key1")
	if got != "" {
		t.Errorf("empty ring: want '', got %q", got)
	}
}

//Test 2 ====== Single node owns everything ==============

func TestGetNode_SingleNode(t *testing.T) {
	r := New()
	r.AddNode("NodeA")
	keys := []string{"user:1", "cart:abc", "order:1"}
	for _, key := range keys {
		got := r.GetNode(key)
		if got != "NodeA" {
			t.Errorf("key %s: want NodeA, got %d", key, got)
		}
	}
}

//Test 3 ================= Consistency if same key always returns same node =====================

func TestGetNode_Consistency(t *testing.T) {
	r := New()
	r.AddNode("NodeA")
	r.AddNode("NodeB")
	r.AddNode("NodeC")

	key := "user:john"

	first := r.GetNode(key)

	for i := 0; i < 100; i++ {
		got := r.GetNode(key)
		if got != first {
			t.Errorf("iteration %d: want %q, got %q", i, first, got)
		}
	}
}

// Test 4 =============== Remove Node ======================

func Test_RemoveNode(t *testing.T) {
	r := New()
	r.AddNode("NodeA")
	r.AddNode("NodeB")
	r.AddNode("NodeC")

	r.RemoveNode("NodeB")

	if r.Size() != 2 {
		t.Errorf("want 2 nodes, got %d", r.Size())
	}

	// All keys should now route to NodeA or NodeC only

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key:%d", i)
		node := r.GetNode(key)
		if node == "NodeB" {
			t.Errorf("key %q routed to removed NodeB", key)
		}
	}
}
