package proxy

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/robinbraemer/event"
	"github.com/stretchr/testify/require"
	"go.minekube.com/gate/pkg/edition/java/config"
	"go.minekube.com/gate/pkg/edition/java/profile"
	"go.minekube.com/gate/pkg/edition/java/proto/packet"
	"go.minekube.com/gate/pkg/edition/java/proto/version"
	"go.minekube.com/gate/pkg/gate/proto"
	"go.minekube.com/gate/pkg/util/uuid"
)

func registryProxy(t *testing.T, online, kick bool) *Proxy {
	t.Helper()
	cfg := config.DefaultConfig
	cfg.OnlineMode, cfg.OnlineModeKickExistingPlayers = online, kick
	p, err := New(Options{Config: &cfg})
	require.NoError(t, err)
	return p
}

type registryDisconnectConn struct {
	*testMinecraftConn
	closed func()
}

func (c *registryDisconnectConn) Close() error { c.closed(); return nil }

func TestScopedOnlineDuplicateEviction(t *testing.T) {
	p := registryProxy(t, false, true) // per-connection Online overrides global offline
	id := uuid.New()
	old := registryPlayer("a", id, "Player", true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	old.MinecraftConn = &registryDisconnectConn{testMinecraftConn: &testMinecraftConn{ctx: ctx}, closed: func() {
		cancel()
		// Teardown is deliberately delayed until after replacement registration.
	}}
	other := registryPlayer("b", id, "Player", true)
	require.True(t, p.registerConnection(old))
	require.True(t, p.registerConnection(other))
	replacement := registryPlayer("a", id, "Player", true)
	require.True(t, p.canRegisterConnection(replacement))
	require.True(t, p.registerConnection(replacement))
	require.True(t, old.disconnectDueToDuplicateConnection.Load())
	require.False(t, other.disconnectDueToDuplicateConnection.Load())
	require.False(t, p.unregisterConnection(old))
	require.Same(t, replacement, p.PlayerInScope("a", id))
	require.Same(t, other, p.PlayerInScope("b", id))
	// A different authenticated UUID cannot overwrite a same-name entry.
	require.False(t, p.registerConnection(registryPlayer("a", uuid.New(), "player", true)))
}

func TestRegistryConcurrentDuplicateAdmission(t *testing.T) {
	for _, online := range []bool{false, true} {
		p := registryProxy(t, online, false)
		id := uuid.New()
		start := make(chan struct{})
		var winners atomic.Int32
		var wg sync.WaitGroup
		for range 32 {
			wg.Go(func() {
				<-start
				candidate := registryPlayer("scope", id, "Player", online)
				if p.registerConnection(candidate) {
					winners.Add(1)
				} else {
					p.unregisterConnection(candidate)
				}
			})
		}
		close(start) // release does not depend on worker scheduling
		wg.Wait()
		require.Equal(t, int32(1), winners.Load())
		require.Equal(t, 1, p.PlayerCount())
		require.NotNil(t, p.PlayerInScope("scope", id))
		require.NotNil(t, p.PlayerByNameInScope("scope", "player"))
	}
}

func TestRegistryScopeConcurrentSeal(t *testing.T) {
	e := newPreLoginEvent(nil, "Player", uuid.Nil)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() { e.SetRegistryScope("scope"); e.RegistryScope() })
	}
	captured := e.sealRegistryScope()
	wg.Wait()
	require.Equal(t, captured, e.RegistryScope())
	require.False(t, e.SetRegistryScope("replacement"))
}

func TestScopedBungeePlayerViews(t *testing.T) {
	p := registryProxy(t, false, false)
	id := uuid.New()
	a := registryPlayer("a", id, "Player", false)
	b := registryPlayer("b", id, "Player", false)
	require.True(t, p.registerConnection(a))
	require.True(t, p.registerConnection(b))
	adapter := &bungeeMessageResponderAdapter{player: a, Proxy: p}
	require.Same(t, a, adapter.PlayerByName("player"))
	require.Equal(t, 1, adapter.PlayerCount())
	require.Len(t, adapter.Players(), 1)
	require.Same(t, a, adapter.Players()[0])
	list := newPlayers()
	list.add(a, b)
	server := &bungeeServer{proxy: p, scope: "a", s: &registeredServer{players: list}}
	require.Equal(t, 1, server.PlayerCount())
	require.Len(t, server.Players(), 1)
	require.Same(t, a, server.Players()[0])
}

// Real protocol login through HandleConn, without a backend or external auth.
// Hold after LoginSuccess to prove simultaneous sessions and untouched identity.
func TestRegistryScopesThroughWireLogin(t *testing.T) {
	cfg := config.DefaultConfig
	cfg.OnlineMode = false
	cfg.OnlineModeKickExistingPlayers = false
	cfg.Compression.Threshold = -1
	cfg.Forwarding.Mode = config.NoneForwardingMode
	events := event.New()
	p, err := New(Options{Config: &cfg, EventMgr: events})
	require.NoError(t, err)
	require.NoError(t, p.init())
	var preLogin []*PreLoginEvent
	event.Subscribe(events, 0, func(e *PreLoginEvent) {
		scope := e.Conn().VirtualHost().String()
		if !e.SetRegistryScope(scope) {
			e.Deny(alreadyConnected)
			return
		}
		e.SetVirtualHost(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 25565})
		preLogin = append(preLogin, e)
	})
	joined := make(chan Player, 2)
	hold := make(chan struct{})
	defer close(hold)
	event.Subscribe(events, 0, func(e *PostLoginEvent) {
		joined <- e.Player()
		select {
		case <-hold:
		case <-time.After(10 * time.Second):
		}
	})
	connect := func(host string) Player {
		t.Helper()
		client, server := net.Pipe()
		done := make(chan struct{})
		go func() { defer close(done); p.HandleConn(server) }()
		t.Cleanup(func() {
			_ = client.Close()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("HandleConn did not exit")
			}
		})
		require.NoError(t, client.SetDeadline(time.Now().Add(5*time.Second)))
		require.NoError(t, writeTunnelHandshake(client, host, 25565, int(version.Minecraft_1_20.Protocol)))
		require.NoError(t, writeServerLogin(client, "SamePlayer"))
		packetID, data, err := readPacket(client)
		require.NoError(t, err)
		require.Equal(t, 2, packetID, "expected LoginSuccess")
		var success packet.ServerLoginSuccess
		require.NoError(t, success.Decode(&proto.PacketContext{Protocol: version.Minecraft_1_20.Protocol}, bytes.NewReader(data)))
		select {
		case player := <-joined:
			require.Equal(t, player.ID(), success.UUID)
			require.Equal(t, player.Username(), success.Username)
			return player
		case <-time.After(5 * time.Second):
			t.Fatal("PostLogin not reached")
			return nil
		}
	}
	a, b := connect("scope-a.example"), connect("scope-b.example")
	require.Equal(t, a.ID(), b.ID())
	require.Equal(t, "SamePlayer", a.Username())
	require.Equal(t, a.Username(), b.Username())
	require.NotEqual(t, PlayerRegistryScope(a), PlayerRegistryScope(b))
	require.Equal(t, 2, p.PlayerCount())
	require.True(t, a.Active())
	require.True(t, b.Active())
	for _, e := range preLogin {
		require.False(t, e.SetRegistryScope("late"))
	}
	require.Same(t, a, p.PlayerInScope(PlayerRegistryScope(a), a.ID()))
	require.Same(t, b, p.PlayerInScope(PlayerRegistryScope(b), b.ID()))
}

func registryPlayer(scope string, id uuid.UUID, name string, online bool) *connectedPlayer {
	return &connectedPlayer{registryScope: scope, profile: &profile.GameProfile{ID: id, Name: name}, onlineMode: online}
}

func TestRegistryScopeImmutable(t *testing.T) {
	for _, scope := range []string{"", "tenant-a/workload-a"} {
		e := newPreLoginEvent(nil, "Player", uuid.Nil)
		require.True(t, e.SetRegistryScope(scope))
		require.False(t, e.SetRegistryScope("replacement"))
		require.Equal(t, scope, e.sealRegistryScope())
		require.False(t, e.SetRegistryScope("late"))
		require.Equal(t, scope, e.RegistryScope())
	}
	e := newPreLoginEvent(nil, "Player", uuid.Nil)
	require.Empty(t, e.sealRegistryScope())
	require.False(t, e.SetRegistryScope("late"))
}

func TestRegistryScopeSeparatesSameIdentity(t *testing.T) {
	for _, online := range []bool{false, true} {
		for _, kick := range []bool{false, true} {
			t.Run(fmt.Sprintf("online=%v/kick=%v", online, kick), func(t *testing.T) {
				p := registryProxy(t, online, kick)
				id := uuid.New()
				var clients []*connectedPlayer
				for _, scope := range []string{"", "tenant-a/a", "tenant-a/b", "tenant-b/a"} {
					client := registryPlayer(scope, id, "Player", online)
					require.True(t, p.canRegisterConnection(client))
					require.True(t, p.registerConnection(client))
					require.Same(t, client, p.PlayerInScope(scope, id))
					require.Same(t, client, p.PlayerByNameInScope(scope, "pLaYeR"))
					require.Equal(t, []Player{client}, p.PlayersInScope(scope))
					require.Equal(t, scope, PlayerRegistryScope(client))
					require.Equal(t, id, client.ID())
					require.Equal(t, "Player", client.Username())
					clients = append(clients, client)
				}
				require.Equal(t, 4, p.PlayerCount())
				require.Len(t, p.Players(), 4)
				require.Same(t, clients[0], p.Player(id))
				require.Same(t, clients[0], p.PlayerByName("player"))
				require.Nil(t, p.PlayerInScope("unknown", id))
				require.Nil(t, p.PlayerByNameInScope("unknown", "player"))
				require.True(t, p.unregisterConnection(clients[1]))
				require.False(t, p.unregisterConnection(clients[1]))
				require.Same(t, clients[2], p.PlayerInScope("tenant-a/b", id))
				require.Equal(t, 3, p.PlayerCount())
			})
		}
	}
}

func TestScopedOfflineDuplicatesCannotEvict(t *testing.T) {
	p := registryProxy(t, true, true) // global policy must not authenticate an offline connection
	id := uuid.New()
	existing := registryPlayer("scope", id, "Player", false)
	require.True(t, p.registerConnection(existing))
	for _, candidate := range []*connectedPlayer{
		registryPlayer("scope", id, "Other", false),
		registryPlayer("scope", uuid.New(), "player", false),
	} {
		require.False(t, p.canRegisterConnection(candidate))
		require.False(t, p.registerConnection(candidate))
		require.False(t, p.unregisterConnection(candidate))
		require.Same(t, existing, p.PlayerInScope("scope", id))
		require.Same(t, existing, p.PlayerByNameInScope("scope", "Player"))
	}
}

func TestRegistryOldCleanupPreservesReplacement(t *testing.T) {
	p := registryProxy(t, false, false)
	id := uuid.New()
	old := registryPlayer("scope", id, "Player", false)
	require.True(t, p.registerConnection(old))
	require.True(t, p.unregisterConnection(old))
	next := registryPlayer("scope", id, "Player", false)
	require.True(t, p.registerConnection(next))
	require.False(t, p.unregisterConnection(old))
	require.Same(t, next, p.PlayerInScope("scope", id))
	require.Same(t, next, p.PlayerByNameInScope("scope", "Player"))
}

func TestRegistryConcurrentScopesAndEnumeration(t *testing.T) {
	p := registryProxy(t, false, false)
	backend := newPlayers()
	id := uuid.New()
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			client := registryPlayer(fmt.Sprint(i), id, "Player", false)
			for range 30 {
				if !p.registerConnection(client) {
					t.Error("independent registration rejected")
					return
				}
				backend.add(client)
				p.Players()
				p.PlayersInScope(client.registryScope)
				backend.Range(func(Player) bool { return true })
				backend.remove(client)
				p.unregisterConnection(client)
			}
		})
	}
	wg.Wait() // no ordering-dependent release; each worker has finite independent work
	require.Zero(t, p.PlayerCount())
	require.Zero(t, backend.Len())
}

func TestBackendPlayersKeepDistinctConnections(t *testing.T) {
	list := newPlayers()
	id := uuid.New()
	a := registryPlayer("a", id, "Player", true)
	b := registryPlayer("b", id, "Player", true)
	replacement := registryPlayer("a", id, "Player", true)
	list.add(a, b, replacement)
	require.Equal(t, 3, list.Len())
	list.remove(a)
	require.ElementsMatch(t, []Player{b, replacement}, PlayersToSlice[Player](list))
	list.Range(func(p Player) bool { list.remove(p.(*connectedPlayer)); return true })
	require.Zero(t, list.Len())
}
