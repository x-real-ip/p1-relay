// p1-relay reads a DSMR/P1 telegram stream once over TCP (for example from
// an ESP running esp-link) and relays the raw bytes to as many downstream
// TCP consumers as needed, such as dsmr-reader and the Home Assistant DSMR
// integration.
//
// The upstream device accepts only a single TCP connection at a time. This
// program is that one connection, and acts as a TCP server towards all
// downstream consumers, which are therefore independent of each other.
//
// Two design decisions worth knowing about:
//
//   - New clients are only made live at the next telegram start ('/').
//     Without this, a client connecting mid-telegram receives a partial
//     telegram, which downstream parsers report as a CRC error. Already
//     connected clients keep receiving bytes unchanged and immediately, so
//     this costs no latency for the running stream.
//
//   - Every client has its own buffered channel and writer goroutine, so the
//     upstream read loop never blocks on a slow consumer. If a client's
//     buffer fills up, only that client is disconnected.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ------------------------------------------------------------------ state

// state tracks only what the health endpoint needs.
type state struct {
	upstreamConnected atomic.Int64 // 0/1
	lastDataUnix      atomic.Int64
}

// ------------------------------------------------------------ broadcaster

type client struct {
	conn net.Conn
	ch   chan []byte
	// synced stays false while the client is still waiting for the next
	// telegram boundary; it receives no data until then.
	synced bool
	once   sync.Once
}

func (c *client) close() {
	c.once.Do(func() {
		close(c.ch)
		_ = c.conn.Close()
	})
}

type broadcaster struct {
	mu      sync.Mutex
	clients map[*client]struct{}
	bufSize int
}

func newBroadcaster(bufSize int) *broadcaster {
	return &broadcaster{
		clients: make(map[*client]struct{}),
		bufSize: bufSize,
	}
}

func (b *broadcaster) add(conn net.Conn) *client {
	c := &client{
		conn: conn,
		ch:   make(chan []byte, b.bufSize),
	}
	b.mu.Lock()
	b.clients[c] = struct{}{}
	n := len(b.clients)
	b.mu.Unlock()

	log.Printf("client connected: %s, waiting for telegram boundary (total=%d)",
		conn.RemoteAddr(), n)

	// Dedicated writer goroutine: the upstream read loop never waits on it.
	go func() {
		for chunk := range c.ch {
			_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if _, err := conn.Write(chunk); err != nil {
				b.remove(c, fmt.Sprintf("write error: %v", err))
				return
			}
		}
	}()
	return c
}

func (b *broadcaster) remove(c *client, reason string) {
	b.mu.Lock()
	_, present := b.clients[c]
	delete(b.clients, c)
	n := len(b.clients)
	b.mu.Unlock()

	if !present {
		return
	}
	c.close()
	log.Printf("client disconnected (%s) (total=%d)", reason, n)
}

// send queues a chunk for a single client. It never blocks: if the buffer is
// full the client is hopelessly behind for a real-time stream, so we drop it.
// It is free to reconnect and will resync cleanly.
func (b *broadcaster) send(c *client, chunk []byte) {
	select {
	case c.ch <- chunk:
	default:
		b.remove(c, "write buffer full")
	}
}

// broadcast distributes an upstream chunk across all clients.
//
// Clients that are already synced receive the chunk unchanged. Clients still
// waiting for a telegram boundary receive data starting at the first '/' in
// the chunk, and are synced from then on.
func (b *broadcaster) broadcast(chunk []byte) {
	b.mu.Lock()
	active := make([]*client, 0, len(b.clients))
	pending := make([]*client, 0)
	for c := range b.clients {
		if c.synced {
			active = append(active, c)
		} else {
			pending = append(pending, c)
		}
	}
	b.mu.Unlock()

	for _, c := range active {
		b.send(c, chunk)
	}

	if len(pending) == 0 {
		return
	}
	idx := bytes.IndexByte(chunk, '/')
	if idx < 0 {
		return // no telegram start yet; pending clients keep waiting
	}
	tail := chunk[idx:]
	b.mu.Lock()
	for _, c := range pending {
		if _, still := b.clients[c]; still {
			c.synced = true
		}
	}
	b.mu.Unlock()
	for _, c := range pending {
		b.send(c, tail)
	}
}

func (b *broadcaster) closeAll() {
	b.mu.Lock()
	all := make([]*client, 0, len(b.clients))
	for c := range b.clients {
		all = append(all, c)
	}
	b.clients = make(map[*client]struct{})
	b.mu.Unlock()
	for _, c := range all {
		c.close()
	}
}

// ------------------------------------------------------------------- env

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}

// ------------------------------------------------------------------ main

func main() {
	var (
		upstreamHost   = envStr("UPSTREAM_HOST", "")
		upstreamPort   = envStr("UPSTREAM_PORT", "23")
		listenAddr     = envStr("LISTEN_ADDR", ":8888")
		healthAddr     = envStr("HEALTH_ADDR", ":9090")
		dialTimeout    = time.Duration(envInt("DIAL_TIMEOUT_SECONDS", 10)) * time.Second
		idleTimeout    = time.Duration(envInt("UPSTREAM_IDLE_TIMEOUT_SECONDS", 90)) * time.Second
		backoffMin     = time.Duration(envInt("RECONNECT_DELAY_SECONDS", 2)) * time.Second
		backoffMax     = time.Duration(envInt("RECONNECT_MAX_DELAY_SECONDS", 60)) * time.Second
		clientBufChunk = envInt("CLIENT_BUFFER_CHUNKS", 256)
		staleAfter     = time.Duration(envInt("HEALTH_STALE_SECONDS", 120)) * time.Second
	)

	if upstreamHost == "" {
		log.Fatal("UPSTREAM_HOST is required (IP or hostname of the esp-link P1 reader)")
	}
	upstreamAddr := net.JoinHostPort(upstreamHost, upstreamPort)

	st := &state{}
	b := newBroadcaster(clientBufChunk)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", listenAddr)
	if err != nil {
		log.Fatalf("cannot listen on %s: %v", listenAddr, err)
	}
	log.Printf("listening on %s, upstream=%s, health=%s", listenAddr, upstreamAddr, healthAddr)

	go acceptLoop(ln, b)

	// /healthz reflects the actual upstream state, not merely whether the
	// listening socket is open.
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		last := st.lastDataUnix.Load()
		stale := last > 0 && time.Since(time.Unix(last, 0)) > staleAfter
		if st.upstreamConnected.Load() != 1 || stale {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, "unhealthy: connected=%d stale=%v\n", st.upstreamConnected.Load(), stale)
			return
		}
		fmt.Fprintln(w, "ok")
	})
	healthSrv := &http.Server{Addr: healthAddr, Handler: mux}
	go func() {
		if err := healthSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("health server stopped: %v", err)
		}
	}()

	go upstreamLoop(ctx, upstreamAddr, dialTimeout, idleTimeout, backoffMin, backoffMax, b, st)

	<-ctx.Done()
	log.Print("shutting down...")
	_ = ln.Close()
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = healthSrv.Shutdown(shutCtx)
	b.closeAll()
}

func acceptLoop(ln net.Listener, b *broadcaster) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return // listener closed during shutdown
		}
		c := b.add(conn)

		// Downstream consumers send nothing themselves; we read only to
		// detect a disconnect promptly.
		go func(c *client, conn net.Conn) {
			buf := make([]byte, 256)
			for {
				if _, err := conn.Read(buf); err != nil {
					b.remove(c, "connection closed")
					return
				}
			}
		}(c, conn)
	}
}

func upstreamLoop(
	ctx context.Context,
	addr string,
	dialTimeout, idleTimeout, backoffMin, backoffMax time.Duration,
	b *broadcaster,
	st *state,
) {
	backoff := backoffMin
	for {
		if ctx.Err() != nil {
			return
		}
		err := readUpstream(ctx, addr, dialTimeout, idleTimeout, b, st)
		st.upstreamConnected.Store(0)
		if ctx.Err() != nil {
			return
		}
		log.Printf("upstream lost: %v (retrying in %s)", err, backoff)

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		// Exponential backoff with a ceiling, so a meter or wifi network that
		// stays down does not cause a reconnect storm.
		backoff *= 2
		if backoff > backoffMax {
			backoff = backoffMax
		}
	}
}

func readUpstream(
	ctx context.Context,
	addr string,
	dialTimeout, idleTimeout time.Duration,
	b *broadcaster,
	st *state,
) error {
	d := net.Dialer{Timeout: dialTimeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	log.Printf("connected to upstream %s", addr)
	st.upstreamConnected.Store(1)

	// Unblocks a pending Read when the process is shutting down.
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	buf := make([]byte, 4096)
	for {
		if idleTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(idleTimeout))
		}
		n, err := conn.Read(buf)
		if n > 0 {
			// Copy: buf is reused, while the chunk is handed to channels that
			// read it only later.
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			st.lastDataUnix.Store(time.Now().Unix())
			b.broadcast(chunk)
		}
		if err != nil {
			return err
		}
	}
}
