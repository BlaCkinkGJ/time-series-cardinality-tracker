package router_test

import (
	"fmt"
	"testing"

	"github.com/yourorg/cardinality-tracker/internal/router"
)

func TestResolve_Deterministic(t *testing.T) {
	r := router.New()
	r.AddNode("node1:9090")
	r.AddNode("node2:9090")
	r.AddNode("node3:9090")

	first := r.Resolve("my-series-id")
	for i := 0; i < 100; i++ {
		if got := r.Resolve("my-series-id"); got != first {
			t.Fatalf("non-deterministic: got %s want %s", got, first)
		}
	}
}

func TestResolve_Distribution(t *testing.T) {
	r := router.New()
	r.AddNode("node1:9090")
	r.AddNode("node2:9090")
	r.AddNode("node3:9090")
	counts := map[string]int{}
	for i := 0; i < 10000; i++ {
		counts[r.Resolve(fmt.Sprintf("series-%d", i))]++
	}
	// Each node should get 20%–46% (ideally 33%)
	for addr, c := range counts {
		pct := float64(c) / 100
		if pct < 20 || pct > 46 {
			t.Errorf("node %s got %.1f%% — uneven distribution", addr, pct)
		}
	}
}

func TestRemoveNode(t *testing.T) {
	r := router.New()
	r.AddNode("node1:9090")
	r.AddNode("node2:9090")
	r.RemoveNode("node2:9090")
	if got := r.Resolve("any-key"); got != "node1:9090" {
		t.Fatalf("after remove, want node1, got %s", got)
	}
}
