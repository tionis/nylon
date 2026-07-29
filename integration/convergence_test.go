//go:build integration

package integration

import (
	"context"
	"log/slog"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/encodeous/nylon/core"
	"github.com/encodeous/nylon/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

type routeSnapshot struct {
	route state.SelRoute
	found bool
}

func selectedRoute(n *core.Nylon, prefix netip.Prefix) (state.SelRoute, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	snapshot, err := core.NewDispatchFuture(n, func() (routeSnapshot, error) {
		route, found := n.RouterState.Routes[prefix]
		return routeSnapshot{route: route, found: found}, nil
	}).Await(ctx)
	return snapshot.route, snapshot.found, err
}

func TestOptimalConvergence(t *testing.T) {
	defer goleak.VerifyNone(t)
	tunables := state.DefaultRouterTunables()
	tunables.ProbeDelay /= 3 // 3x faster
	tunables.RouteUpdateDelay /= 3
	tunables.MinimumConfidenceWindow /= 5

	vh := &VirtualHarness{}
	vh.LogLevel = new(slog.LevelError)
	vh.Tunables = &tunables
	a1 := "192.168.1.1:1234"
	vh.NewNode("a", "10.0.0.1/32")
	b1 := "192.168.1.2:1234"
	vh.NewNode("b", "10.0.0.2/32")
	c1 := "192.168.1.3:1234"
	vh.NewNode("c", "10.0.0.3/32")
	vh.Central.Graph = []string{
		"a, b, c",
	}
	vh.Endpoints = map[string]state.NodeId{
		a1: "a",
		b1: "b",
		c1: "c",
	}
	// a <-10-> b
	vh.AddLink(a1, b1).WithMetricLatency(10*time.Millisecond, 0)
	vh.AddLink(b1, a1).WithMetricLatency(10*time.Millisecond, 0)

	// c <-50-> a
	vh.AddLink(a1, c1).WithMetricLatency(50*time.Millisecond, 0)
	vh.AddLink(c1, a1).WithMetricLatency(50*time.Millisecond, 0)

	errs := vh.Start()
	defer vh.Stop()

	a := vh.Nylons[vh.IndexOf("a")].Load()
	cPrefix := netip.MustParsePrefix("10.0.0.3/32")
	require.Eventually(t, func() bool {
		route, found, err := selectedRoute(a, cPrefix)
		return err == nil && found && route.Nh == "c" && route.Metric == 100_005
	}, 30*time.Second, 25*time.Millisecond, "A never selected the initial direct route to C")

	initial, found, err := selectedRoute(a, cPrefix)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, state.NodeId("c"), initial.Nh)
	assert.Equal(t, state.NodeId("c"), initial.NodeId)
	assert.Equal(t, uint32(100_005), initial.Metric)

	// Add the faster a <-10-> b <-10-> c path.
	vh.AddLink(b1, c1).WithMetricLatency(10*time.Millisecond, 0)
	vh.AddLink(c1, b1).WithMetricLatency(10*time.Millisecond, 0)

	require.Eventually(t, func() bool {
		route, found, err := selectedRoute(a, cPrefix)
		return err == nil && found && route.Nh == "b" && route.Metric == 40_010
	}, 30*time.Second, 25*time.Millisecond, "A never switched to the lower-cost route through B")

	converged, found, err := selectedRoute(a, cPrefix)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, state.NodeId("b"), converged.Nh)
	assert.Equal(t, state.NodeId("c"), converged.NodeId)
	assert.Equal(t, uint32(40_010), converged.Metric)

	transited := make(chan struct{})
	arrived := make(chan struct{})
	var transitOnce, arrivalOnce sync.Once
	vh.Net.TransitHandler = func(node state.NodeId, src, dst netip.Addr, data []byte) bool {
		if node == "b" && src.String() == "10.0.0.1" && dst.String() == "10.0.0.3" && len(data) != 0 && data[0] == 222 {
			transitOnce.Do(func() { close(transited) })
		}
		return false
	}
	vh.Net.SelfHandler = func(node state.NodeId, src, dst netip.Addr, data []byte) bool {
		if node == "c" && src.String() == "10.0.0.1" && dst.String() == "10.0.0.3" && len(data) != 0 && data[0] == 222 {
			arrivalOnce.Do(func() { close(arrived) })
		}
		return true
	}

	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for transited != nil || arrived != nil {
		select {
		case <-transited:
			transited = nil
		case <-arrived:
			arrived = nil
		case <-ticker.C:
			vh.Net.Send("a", "10.0.0.1", "10.0.0.3", []byte{222}, 64)
		case err := <-errs:
			t.Fatal(err)
		case <-timeout.C:
			t.Fatal("selected route did not carry traffic through B to C")
		}
	}
}
