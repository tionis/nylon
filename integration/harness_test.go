//go:build integration

package integration

import (
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/encodeous/nylon/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	harnessA1 = "192.0.2.1:1234"
	harnessA2 = "192.0.2.2:1234"
	harnessB1 = "192.0.2.3:1234"
	harnessC1 = "192.0.2.4:1234"
)

func newLatencyHarness() *VirtualHarness {
	return &VirtualHarness{
		Endpoints: map[string]state.NodeId{
			harnessA1: "a",
			harnessA2: "a",
			harnessB1: "b",
			harnessC1: "c",
		},
	}
}

func TestProbeLatency(t *testing.T) {
	remote := netip.MustParseAddrPort(harnessB1)

	t.Run("symmetric", func(t *testing.T) {
		vh := newLatencyHarness()
		vh.AddLink(harnessA1, harnessB1).WithMetricLatency(10*time.Millisecond, 0)
		vh.AddLink(harnessB1, harnessA1).WithMetricLatency(10*time.Millisecond, 0)

		latency, found := vh.probeLatency("a", "b", remote)
		require.True(t, found)
		assert.Equal(t, 20*time.Millisecond, latency)
	})

	t.Run("asymmetric", func(t *testing.T) {
		vh := newLatencyHarness()
		vh.AddLink(harnessA1, harnessB1).WithMetricLatency(10*time.Millisecond, 0)
		vh.AddLink(harnessB1, harnessA1).WithMetricLatency(30*time.Millisecond, 0)

		latency, found := vh.probeLatency("a", "b", remote)
		require.True(t, found)
		assert.Equal(t, 40*time.Millisecond, latency)
	})

	t.Run("zero latency", func(t *testing.T) {
		vh := newLatencyHarness()
		vh.AddLink(harnessA1, harnessB1)
		vh.AddLink(harnessB1, harnessA1)

		latency, found := vh.probeLatency("a", "b", remote)
		require.True(t, found)
		assert.Zero(t, latency)
	})

	t.Run("missing reverse link", func(t *testing.T) {
		vh := newLatencyHarness()
		vh.AddLink(harnessA1, harnessB1).WithMetricLatency(10*time.Millisecond, 0)

		_, found := vh.probeLatency("a", "b", remote)
		assert.False(t, found)
	})

	t.Run("wrong peer", func(t *testing.T) {
		vh := newLatencyHarness()
		vh.AddLink(harnessA1, harnessB1)
		vh.AddLink(harnessB1, harnessA1)

		_, found := vh.probeLatency("a", "c", remote)
		assert.False(t, found)
	})

	t.Run("unrelated links ignored", func(t *testing.T) {
		vh := newLatencyHarness()
		vh.AddLink(harnessA1, harnessC1).WithMetricLatency(time.Second, 0)
		vh.AddLink(harnessC1, harnessA1).WithMetricLatency(time.Second, 0)
		vh.AddLink(harnessA1, harnessB1).WithMetricLatency(12*time.Millisecond, 0)
		vh.AddLink(harnessB1, harnessA1).WithMetricLatency(18*time.Millisecond, 0)

		latency, found := vh.probeLatency("a", "b", remote)
		require.True(t, found)
		assert.Equal(t, 30*time.Millisecond, latency)
	})

	t.Run("deterministic local endpoint", func(t *testing.T) {
		vh := newLatencyHarness()
		// Add the non-selected endpoint first to ensure link insertion order
		// cannot change which local endpoint is measured.
		vh.AddLink(harnessA2, harnessB1).WithMetricLatency(100*time.Millisecond, 0)
		vh.AddLink(harnessB1, harnessA2).WithMetricLatency(100*time.Millisecond, 0)
		vh.AddLink(harnessA1, harnessB1).WithMetricLatency(7*time.Millisecond, 0)
		vh.AddLink(harnessB1, harnessA1).WithMetricLatency(11*time.Millisecond, 0)

		latency, found := vh.probeLatency("a", "b", remote)
		require.True(t, found)
		assert.Equal(t, 18*time.Millisecond, latency)
	})

	t.Run("first duplicate link wins", func(t *testing.T) {
		vh := newLatencyHarness()
		vh.AddLink(harnessA1, harnessB1).WithMetricLatency(5*time.Millisecond, 0)
		vh.AddLink(harnessA1, harnessB1).WithMetricLatency(50*time.Millisecond, 0)
		vh.AddLink(harnessB1, harnessA1).WithMetricLatency(9*time.Millisecond, 0)
		vh.AddLink(harnessB1, harnessA1).WithMetricLatency(90*time.Millisecond, 0)

		latency, found := vh.probeLatency("a", "b", remote)
		require.True(t, found)
		assert.Equal(t, 14*time.Millisecond, latency)
	})

	t.Run("injected jitter samples", func(t *testing.T) {
		vh := newLatencyHarness()
		vh.AddLink(harnessA1, harnessB1).
			WithMetricLatency(10*time.Millisecond, 4*time.Millisecond).
			withMetricJitterSampler(func() float64 { return 0.25 })
		vh.AddLink(harnessB1, harnessA1).
			WithMetricLatency(20*time.Millisecond, 8*time.Millisecond).
			withMetricJitterSampler(func() float64 { return 0.75 })

		latency, found := vh.probeLatency("a", "b", remote)
		require.True(t, found)
		assert.Equal(t, 37*time.Millisecond, latency)
	})
}

func TestProbeLatencyConcurrentUpdate(t *testing.T) {
	vh := newLatencyHarness()
	outbound := vh.AddLink(harnessA1, harnessB1).WithMetricLatency(time.Millisecond, 0)
	inbound := vh.AddLink(harnessB1, harnessA1).WithMetricLatency(time.Millisecond, 0)
	remote := netip.MustParseAddrPort(harnessB1)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 1_000 {
			outbound.WithMetricLatency(time.Duration(i+1)*time.Microsecond, 0)
			inbound.WithMetricLatency(time.Duration(i+1)*time.Microsecond, 0)
		}
	}()
	go func() {
		defer wg.Done()
		for range 1_000 {
			latency, found := vh.probeLatency("a", "b", remote)
			assert.True(t, found)
			assert.GreaterOrEqual(t, latency, 2*time.Microsecond)
		}
	}()
	wg.Wait()
}

func TestMetricAndDeliveryLatencyAreIndependent(t *testing.T) {
	vh := newLatencyHarness()
	link := vh.AddLink(harnessA1, harnessB1).
		WithMetricLatency(10*time.Millisecond, 4*time.Millisecond).
		WithDeliveryLatency(20*time.Millisecond, 8*time.Millisecond).
		withMetricJitterSampler(func() float64 { return 0.25 }).
		withDeliveryJitterSampler(func() float64 { return 0.75 })

	assert.Equal(t, 11*time.Millisecond, link.sampledMetricLatency())
	deliveryLatency, packetLoss := link.deliveryConditions()
	assert.Equal(t, 26*time.Millisecond, deliveryLatency)
	assert.Zero(t, packetLoss)
}
