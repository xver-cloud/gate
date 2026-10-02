package lite

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	"go.minekube.com/gate/pkg/edition/java/lite/config"
	"go.minekube.com/gate/pkg/edition/java/proto/packet"
	"go.minekube.com/gate/pkg/gate/proto"
)

func TestPipeCancellationAfterHalfClose(t *testing.T) {
	client, gateClient := tcpPair(t)
	gateBackend, backend := tcpPair(t)
	t.Cleanup(func() { _ = client.Close(); _ = gateClient.Close(); _ = gateBackend.Close(); _ = backend.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { pipeContext(ctx, logr.Discard(), gateClient, gateBackend); close(done) }()
	require.NoError(t, client.(*net.TCPConn).CloseWrite())
	require.NoError(t, backend.SetReadDeadline(time.Now().Add(time.Second)))
	_, err := io.ReadAll(backend)
	require.NoError(t, err)
	// Backend has seen client EOF but deliberately leaves its write half open.
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancel did not release both tunnel workers")
	}
}

func TestStatusReadBoundedByDialRouteDeadline(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		_, _ = io.Copy(io.Discard, conn) // accept handshake/status, never respond
	}()
	conn, err := dialRoute(context.Background(), 100*time.Millisecond, listener.Addr(), &config.Route{}, listener.Addr().String(), &packet.Handshake{}, &proto.PacketContext{Payload: []byte{0}}, false)
	require.NoError(t, err)
	defer conn.Close()
	started := time.Now()
	_, err = fetchStatus(logr.Discard(), conn, proto.Protocol(765), &proto.PacketContext{Payload: []byte{0}})
	require.Error(t, err)
	require.Less(t, time.Since(started), time.Second)
	_ = conn.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("status timeout did not close backend")
	}
}
