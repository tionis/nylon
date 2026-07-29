package core

import (
	"net/netip"
	"testing"
	"time"

	"github.com/encodeous/nylon/state"
	"github.com/stretchr/testify/assert"
)

func TestResolveProbeLatency(t *testing.T) {
	peer := state.NodeId("peer")
	endpoint := netip.MustParseAddrPort("192.0.2.1:1234")
	measured := 17 * time.Millisecond

	tests := []struct {
		name   string
		aux    map[string]any
		expect time.Duration
	}{
		{
			name:   "no auxiliary configuration",
			expect: measured,
		},
		{
			name: "no override",
			aux: map[string]any{
				"unrelated": true,
			},
			expect: measured,
		},
		{
			name: "override declines",
			aux: map[string]any{
				ProbeLatencyOverrideAuxKey: ProbeLatencyOverride(
					func(gotPeer state.NodeId, gotEndpoint netip.AddrPort) (time.Duration, bool) {
						assert.Equal(t, peer, gotPeer)
						assert.Equal(t, endpoint, gotEndpoint)
						return 0, false
					},
				),
			},
			expect: measured,
		},
		{
			name: "override supplies synthetic latency",
			aux: map[string]any{
				ProbeLatencyOverrideAuxKey: ProbeLatencyOverride(
					func(gotPeer state.NodeId, gotEndpoint netip.AddrPort) (time.Duration, bool) {
						assert.Equal(t, peer, gotPeer)
						assert.Equal(t, endpoint, gotEndpoint)
						return 42 * time.Millisecond, true
					},
				),
			},
			expect: 42 * time.Millisecond,
		},
		{
			name: "zero synthetic latency is valid",
			aux: map[string]any{
				ProbeLatencyOverrideAuxKey: ProbeLatencyOverride(
					func(state.NodeId, netip.AddrPort) (time.Duration, bool) {
						return 0, true
					},
				),
			},
			expect: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &Nylon{AuxConfig: tt.aux}
			assert.Equal(t, tt.expect, n.resolveProbeLatency(peer, endpoint, measured))
		})
	}
}
