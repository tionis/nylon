//go:build integration

package integration

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/netip"
	"runtime/pprof"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/encodeous/nylon/core"
	"github.com/encodeous/nylon/polyamide/conn"
	"github.com/encodeous/nylon/polyamide/conn/bindtest"
	"github.com/encodeous/nylon/polyamide/device"
	"github.com/encodeous/nylon/polyamide/tun"
	"github.com/encodeous/nylon/polyamide/tun/tuntest"
	"github.com/encodeous/nylon/state"
)

type VirtualLink struct {
	mu sync.Mutex

	Edge state.Pair[bindtest.ChannelEndpoint2, bindtest.ChannelEndpoint2]

	metricLatency   time.Duration
	metricJitter    time.Duration
	deliveryLatency time.Duration
	deliveryJitter  time.Duration
	packetLoss      float64

	metricJitterSample   func() float64
	deliveryJitterSample func() float64
}

func jitteredDuration(latency, jitter time.Duration, sample func() float64) time.Duration {
	if jitter <= 0 {
		return latency
	}
	return latency + time.Duration(sample()*float64(jitter))
}

func (v *VirtualLink) deliveryConditions() (time.Duration, float64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return jitteredDuration(v.deliveryLatency, v.deliveryJitter, v.deliveryJitterSample), v.packetLoss
}

func (v *VirtualLink) sampledMetricLatency() time.Duration {
	v.mu.Lock()
	defer v.mu.Unlock()
	return jitteredDuration(v.metricLatency, v.metricJitter, v.metricJitterSample)
}

func (v *VirtualLink) simulate(pkt []byte, len int, from, to bindtest.ChannelEndpoint2, i *InMemoryNetwork) {
	//fmt.Printf("begin send: %s -> %s\n", from.DstToString(), to.DstToString())
	deliveryLatency, packetLoss := v.deliveryConditions()
	if rand.Float64() < packetLoss {
		// drop
		//fmt.Printf("dropped send: %s -> %s\n", from.DstToString(), to.DstToString())
		return
	}

	toIdx := i.cfg.IndexOf(i.cfg.Endpoints[to.DstToString()])
	send := func() {
		err := i.binds[toIdx].Send([][]byte{pkt[:len]}, from)
		if err != nil && !errors.Is(err, net.ErrClosed) {
			panic(err)
		}
	}
	if deliveryLatency <= 0 {
		send()
		return
	}
	go func() {
		timer := time.NewTimer(deliveryLatency)
		defer timer.Stop()
		select {
		case <-i.cfg.Context.Done():
		case <-timer.C:
			send()
		}
	}()
}

// WithMetricLatency sets the one-way cost reported by probes without delaying
// packet delivery.
func (v *VirtualLink) WithMetricLatency(latency, jitter time.Duration) *VirtualLink {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.metricLatency = latency
	v.metricJitter = jitter
	return v
}

// WithDeliveryLatency delays actual packets. Most routing tests should use
// WithMetricLatency instead so they remain fast and deterministic.
func (v *VirtualLink) WithDeliveryLatency(latency, jitter time.Duration) *VirtualLink {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.deliveryLatency = latency
	v.deliveryJitter = jitter
	return v
}

func (v *VirtualLink) WithPacketLoss(loss float64) *VirtualLink {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.packetLoss = loss
	return v
}

func (v *VirtualLink) withMetricJitterSampler(sample func() float64) *VirtualLink {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.metricJitterSample = sample
	return v
}

func (v *VirtualLink) withDeliveryJitterSampler(sample func() float64) *VirtualLink {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.deliveryJitterSample = sample
	return v
}

type VirtualHarness struct {
	Central          state.CentralCfg
	Context          context.Context
	Cancel           context.CancelCauseFunc
	Local            []state.LocalCfg
	Net              *InMemoryNetwork
	Nylons           []atomic.Pointer[core.Nylon]
	Links            []*VirtualLink
	linksMu          sync.RWMutex
	nextLinkID       uint64
	Endpoints        map[string]state.NodeId
	UntrackedRouting bool
	LogLevel         *slog.Level
	Tunables         *state.RouterTunables
}

func (v *VirtualHarness) IndexOf(id state.NodeId) int {
	return slices.IndexFunc(v.Central.Routers, func(cfg state.RouterCfg) bool {
		return cfg.Id == id
	})
}

func (v *VirtualHarness) NewNode(id state.NodeId, virtPrefix string) {
	privKey := state.GenerateKey()
	locCfg := state.LocalCfg{
		Key:              privKey,
		Id:               id,
		Port:             25565,
		NoNetConfigure:   true,
		UseSystemRouting: !v.UntrackedRouting,
	}
	ncfg := state.RouterCfg{
		NodeCfg: state.NodeCfg{
			Id:     id,
			PubKey: privKey.Pubkey(),
			Prefixes: []state.PrefixHealthWrapper{
				{
					&state.StaticPrefixHealth{
						Prefix: netip.MustParsePrefix(virtPrefix),
						Metric: 0,
					},
				},
			},
		},
	}
	v.Central.Routers = append(v.Central.Routers, ncfg)
	v.Local = append(v.Local, locCfg)
}

func (v *VirtualHarness) AddLink(from, to string) *VirtualLink {
	v.linksMu.Lock()
	linkID := v.nextLinkID
	v.nextLinkID++
	metricRNG := rand.New(rand.NewPCG(linkID, 0x6d6574726963))
	deliveryRNG := rand.New(rand.NewPCG(linkID, 0x64656c6976657279))
	link := &VirtualLink{
		Edge: state.Pair[bindtest.ChannelEndpoint2, bindtest.ChannelEndpoint2]{
			V1: bindtest.ChannelEndpoint2(netip.MustParseAddrPort(from)),
			V2: bindtest.ChannelEndpoint2(netip.MustParseAddrPort(to)),
		},
		metricJitterSample:   metricRNG.Float64,
		deliveryJitterSample: deliveryRNG.Float64,
	}
	v.Links = append(v.Links, link)
	v.linksMu.Unlock()
	return link
}

func (v *VirtualHarness) endpointForNode(node state.NodeId) (bindtest.ChannelEndpoint2, bool) {
	endpoints := make([]string, 0)
	for endpoint, owner := range v.Endpoints {
		if owner == node {
			endpoints = append(endpoints, endpoint)
		}
	}
	if len(endpoints) == 0 {
		return bindtest.ChannelEndpoint2(netip.AddrPort{}), false
	}
	slices.Sort(endpoints)
	return bindtest.ChannelEndpoint2(netip.MustParseAddrPort(endpoints[0])), true
}

func (v *VirtualHarness) probeLatency(local, peer state.NodeId, remote netip.AddrPort) (time.Duration, bool) {
	if v.Endpoints[remote.String()] != peer {
		return 0, false
	}
	localEndpoint, ok := v.endpointForNode(local)
	if !ok {
		return 0, false
	}

	v.linksMu.RLock()
	defer v.linksMu.RUnlock()

	var outbound, inbound *VirtualLink
	for _, link := range v.Links {
		from := link.Edge.V1.DstIPPort()
		to := link.Edge.V2.DstIPPort()
		if outbound == nil && from == localEndpoint.DstIPPort() && to == remote {
			outbound = link
		}
		if inbound == nil && from == remote && to == localEndpoint.DstIPPort() {
			inbound = link
		}
	}
	if outbound == nil || inbound == nil {
		return 0, false
	}
	return outbound.sampledMetricLatency() + inbound.sampledMetricLatency(), true
}

func (v *VirtualHarness) Start() chan error {
	ctx, cancel := context.WithCancelCause(context.Background())
	v.Context = ctx
	v.Cancel = cancel
	v.Nylons = make([]atomic.Pointer[core.Nylon], len(v.Central.Routers))
	errChan := make(chan error, 128) // a large number so we dont get blocked
	vn := &InMemoryNetwork{}
	v.Net = vn
	vn.cfg = v
	nodes := len(v.Central.Routers)
	vn.virtTun = make([]*tuntest.ChannelTUN, nodes)
	vn.binds = make([]conn.Bind, nodes)
	vn.readyCond = sync.NewCond(&sync.Mutex{})
	// pick the first endpoint specified for each node
	vn.EpOutMapping = func(curNode state.NodeId, to bindtest.ChannelEndpoint2) bindtest.ChannelEndpoint2 {
		endpoint, ok := v.endpointForNode(curNode)
		if ok {
			return endpoint
		}
		panic(fmt.Sprintf("no endpoint found for node %v", curNode))
	}
	for e, n := range v.Endpoints {
		idx := v.IndexOf(n)
		v.Central.Routers[idx].Endpoints = append(v.Central.Routers[idx].Endpoints, e)
	}
	if v.LogLevel == nil {
		v.LogLevel = new(slog.LevelDebug)
	}
	startDelay := 0 * time.Millisecond
	for idx, rt := range v.Central.Routers {
		sd := startDelay
		go func() {
			time.Sleep(sd)
			labels := pprof.Labels("nylon node", string(rt.Id))
			n, err := core.NewNylon(v.Central, v.Local[idx], *v.LogLevel, "", map[string]any{
				"vnet": vn,
				core.ProbeLatencyOverrideAuxKey: core.ProbeLatencyOverride(
					func(peer state.NodeId, endpoint netip.AddrPort) (time.Duration, bool) {
						return v.probeLatency(rt.Id, peer, endpoint)
					},
				),
			}, state.NylonOptions{DBG_log_wireguard: true}, v.Tunables)
			if err != nil {
				errChan <- err
				return
			}
			v.Nylons[idx].Store(n)
			pprof.Do(context.Background(), labels, func(_ context.Context) {
				cErr := n.Start()
				if cErr != nil {
					errChan <- cErr
					return
				}
			})
		}()
		startDelay += time.Millisecond * 500 // add a tiny delay so they don't try to handshake at the exact same time
	}
	// wait for all routers to start
	for {
		started := true
		for idx, _ := range v.Central.Routers {
			if v.Nylons[idx].Load() == nil {
				started = false
				break
			}
		}
		if started {
			break
		}
		select {
		case <-ctx.Done():
			return errChan
		case <-time.After(time.Millisecond * 50):
		case err := <-errChan:
			errChan <- err
			return errChan
		}
	}
	v.Net.Ready()
	return errChan
}

func (v *VirtualHarness) Stop() {
	println("Stopping VirtualHarness")
	v.Cancel(fmt.Errorf("stopping harness"))
	for idx, _ := range v.Central.Routers {
		v.Nylons[idx].Load().Stop()
	}
	v.Net.Stop()
	println("Stopped VirtualHarness")
}

type PacketFilter func(node state.NodeId, src, dst netip.Addr, data []byte) bool // return true to intercept, and stop the rest of the stack
func (h PacketFilter) TryApply(node state.NodeId, src, dst netip.Addr, data []byte) bool {
	if h == nil {
		return false
	}
	return h(node, src, dst, data)
}

type OutMapping func(curNode state.NodeId, to bindtest.ChannelEndpoint2) bindtest.ChannelEndpoint2

type InMemoryNetwork struct {
	sync.Mutex
	cfg            *VirtualHarness
	binds          []conn.Bind
	virtTun        []*tuntest.ChannelTUN
	SelfHandler    PacketFilter // packet filter for handling packets destined for the current node
	TransitHandler PacketFilter // packet filter for handling packets passing through the current node
	EpOutMapping   OutMapping
	ready          atomic.Bool
	readyCond      *sync.Cond
}

func (i *InMemoryNetwork) WaitForReady() {
	i.readyCond.L.Lock()
	defer i.readyCond.L.Unlock()
	for !i.ready.Load() {
		i.readyCond.Wait()
	}
}

func (i *InMemoryNetwork) Ready() {
	i.Lock()
	defer i.Unlock()
	i.ready.Store(true)
	i.readyCond.Broadcast()
}

func (i *InMemoryNetwork) virtualRouteTable(node state.NodeId, src, dst netip.Addr, data []byte, pkt []byte) bool {
	curCfg := i.cfg.Central.GetNode(node)
	if pkt[8] == 0 { // handle self if ttl is 0 as well
		if i.SelfHandler.TryApply(node, src, dst, data) {
			return true
		}
	}
	for _, prefix := range curCfg.Prefixes {
		if prefix.GetPrefix().Contains(dst) {
			if i.SelfHandler.TryApply(node, src, dst, data) {
				return true
			}
		}
	}
	curIdx := i.cfg.IndexOf(node)

	if !i.TransitHandler.TryApply(node, src, dst, data) {
		// default routing behaviour
		for _, n := range i.cfg.Central.Routers {
			if node == n.Id {
				continue
			}
			for _, prefix := range n.Prefixes {
				if prefix.GetPrefix().Contains(dst) {
					// route to this node
					select {
					case i.virtTun[curIdx].Outbound <- pkt: // send back into our tun to get routed by WireGuard/Polyamide
						return true
					default:
						fmt.Printf("%s's tun is not ready to accept data\n", n.Id)
						return true
					}
				}
			}
		}
		return false
	}
	return true
}

func (i *InMemoryNetwork) virtualInternet(pkt []byte, len int, from, to bindtest.ChannelEndpoint2) {
	// simulate network conditions
	i.cfg.linksMu.RLock()
	idx := slices.IndexFunc(i.cfg.Links, func(link *VirtualLink) bool {
		return link.Edge.V1 == from && link.Edge.V2 == to
	})
	var link *VirtualLink
	if idx != -1 {
		link = i.cfg.Links[idx]
	}
	i.cfg.linksMu.RUnlock()
	if link == nil {
		return // no connection, dropped packet
	}
	link.simulate(pkt, len, from, to, i)
}

func (i *InMemoryNetwork) Bind(node state.NodeId) conn.Bind {
	i.Lock()
	defer i.Unlock()
	epSendMapping := func(to bindtest.ChannelEndpoint2) bindtest.ChannelEndpoint2 {
		return i.EpOutMapping(node, to)
	}
	bp := bindtest.NewChannelBind2()

	remote := bp[0] // simulate an actual port opening
	open, _, err := remote.Open(0)
	if err != nil {
		return nil
	}
	numId := i.cfg.IndexOf(node)
	i.binds[numId] = remote
	go func() {
		// bind listener routine for packets sent from this node
		bufSize := remote.BatchSize()
		pktBuf := make([][]byte, bufSize)
		pktBuf[0] = make([]byte, device.MaxMessageSize)
		lenBuf := make([]int, bufSize)
		epBuf := make([]conn.Endpoint, bufSize)
		i.WaitForReady()
		for {
			for _, recv := range open {
				n, err := recv(pktBuf, lenBuf, epBuf)
				if err != nil {
					if !errors.Is(err, net.ErrClosed) {
						panic(err)
					}
					return
				}
				for pi := range n {
					if lenBuf[pi] == 0 {
						continue
					}
					toIp := epBuf[pi].(bindtest.ChannelEndpoint2)
					fromIp := epSendMapping(toIp)
					i.virtualInternet(slices.Clone(pktBuf[pi]), lenBuf[pi], fromIp, toIp)
				}
			}
		}
	}()
	return bp[1]
}

func (i *InMemoryNetwork) Tun(node state.NodeId) tun.Device {
	i.Lock()
	defer i.Unlock()
	const (
		ipv4Size = 20
	)
	bt := tuntest.NewChannelTUN()

	numId := slices.IndexFunc(i.cfg.Central.Routers, func(cfg state.RouterCfg) bool {
		return cfg.Id == node
	})

	i.virtTun[numId] = bt
	go func() {
		i.WaitForReady()
		for {
			select {
			case <-i.cfg.Context.Done():
				return
			case pkt := <-bt.Inbound:
				// wireguard doesn't actually verify checksum for ip
				ip := pkt[0:ipv4Size]
				if pkt[0]>>4 != 4 {
					panic("unexpected packet, not ipv4")
				}
				var src, dst netip.Addr
				err := src.UnmarshalBinary(ip[12:16])
				if err != nil {
					panic(err)
				}
				err = dst.UnmarshalBinary(ip[16:20])
				if err != nil {
					panic(err)
				}
				if !i.virtualRouteTable(node, src, dst, pkt[ipv4Size:], pkt) {
					panic(fmt.Sprintf("unhandled packet src: %v, dst: %v", src, dst))
				}
			}
		}
	}()
	return bt.TUN()
}

func (i *InMemoryNetwork) Send(node state.NodeId, src, dst string, pkt []byte, ttl byte) {
	const (
		ipv4Size = 20
	)
	numId := i.cfg.IndexOf(node)
	ipPkt := append(make([]byte, ipv4Size), pkt...)
	ip := ipPkt[0:ipv4Size]
	ip[0] = 4 << 4
	binary.BigEndian.PutUint16(ip[2:4], uint16(ipv4Size+len(pkt)))
	ip[8] = ttl
	copy(ipPkt[12:16], netip.MustParseAddr(src).AsSlice())
	copy(ipPkt[16:20], netip.MustParseAddr(dst).AsSlice())
	i.virtTun[numId].Outbound <- ipPkt
}

func (i *InMemoryNetwork) Stop() {
	for _, d := range i.binds {
		if d != nil {
			err := d.Close()
			if err != nil {
				panic(err)
			}
		}
	}
}
