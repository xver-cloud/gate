package lite

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	"go.minekube.com/gate/pkg/edition/java/lite/config"
	"go.minekube.com/gate/pkg/edition/java/proto/packet"
	"go.minekube.com/gate/pkg/edition/java/proto/util"
	"go.minekube.com/gate/pkg/gate/proto"
)

func TestForwardConnPreservesWireAndCancelsAfterHalfClose(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	client, gate := tcpPair(t)
	defer client.Close()
	defer gate.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handshake := &packet.Handshake{ProtocolVersion: 765, ServerAddress: "raw.example.com\x00FML2\x00", Port: 65535, NextStatus: 2}
	var payload bytes.Buffer
	require.NoError(t, util.WriteVarInt(&payload, 0))
	require.NoError(t, handshake.Encode(nil, &payload))
	done := make(chan struct{})
	go func() {
		ForwardConn(ctx, time.Second, []config.Route{{Host: []string{"raw.example.com"}, Backend: []string{listener.Addr().String()}}}, logr.Discard(), gate, handshake, &proto.PacketContext{Payload: payload.Bytes(), Protocol: 765}, NewLite().StrategyManager())
		close(done)
	}()
	var backend net.Conn
	select {
	case backend = <-accepted:
	case <-time.After(time.Second):
		t.Fatal("backend not contacted")
	}
	defer backend.Close()
	require.NoError(t, backend.SetDeadline(time.Now().Add(2*time.Second)))
	size, err := util.ReadVarInt(backend)
	require.NoError(t, err)
	got := make([]byte, size)
	_, err = io.ReadFull(backend, got)
	require.NoError(t, err)
	require.Equal(t, payload.Bytes(), got)
	_, err = client.Write([]byte("opaque"))
	require.NoError(t, err)
	got = make([]byte, 6)
	_, err = io.ReadFull(backend, got)
	require.NoError(t, err)
	require.Equal(t, []byte("opaque"), got)
	require.NoError(t, client.(*net.TCPConn).CloseWrite())
	_, err = io.ReadAll(backend)
	require.NoError(t, err)
	// Keep the backend write-half open; cancellation must release its read.
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("raw forwarding did not cancel")
	}
}
