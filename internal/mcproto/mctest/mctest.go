// Package mctest provides a fake Minecraft server for in-process tests: it
// answers status pings only once marked ready (like a real server whose tick
// loop has not started yet) and hands login connections to the test.
package mctest

import (
	"bufio"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"lcm/internal/mcproto"
	"lcm/internal/proxyproto"
)

// Server is a fake Minecraft server.
type Server struct {
	ln            net.Listener
	proxyProtocol bool
	ready         atomic.Bool
	// MOTD is the description in status replies.
	MOTD string
	// Logins receives every login/transfer connection.
	Logins chan *Login
	// StatusAnswered counts status requests answered.
	StatusAnswered atomic.Int64
	// StatusRefused counts status connections closed because not ready.
	StatusRefused atomic.Int64

	mu    sync.Mutex
	conns map[net.Conn]bool
}

// Login is one login connection after the server read Handshake and Login Start.
type Login struct {
	Conn      net.Conn
	Handshake mcproto.Handshake
	Name      string
	// Raw is every byte the server read after the PROXY header: exactly the
	// client's Handshake + Login Start if nothing else was sent.
	Raw []byte
	// R reads whatever the client sends next.
	R *bufio.Reader
}

// Start listens on 127.0.0.1 and serves until Close. proxyProtocol expects
// a PROXY v2 header first on every connection.
func Start(proxyProtocol bool) (*Server, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	s := &Server{ln: ln, proxyProtocol: proxyProtocol, MOTD: "REAL SERVER",
		Logins: make(chan *Login, 16), conns: map[net.Conn]bool{}}
	go s.serve()
	return s, nil
}

// Addr is the listen address.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// SetReady makes status pings succeed (true) or be refused (false).
func (s *Server) SetReady(v bool) { s.ready.Store(v) }

// Close stops the server and closes its connections.
func (s *Server) Close() {
	s.ln.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.conns {
		c.Close()
	}
}

func (s *Server) serve() {
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns[c] = true
		s.mu.Unlock()
		go s.handle(c)
	}
}

func (s *Server) handle(c net.Conn) {
	c.SetReadDeadline(time.Now().Add(10 * time.Second))
	br := bufio.NewReader(c)
	if s.proxyProtocol {
		if _, err := proxyproto.Read(br); err != nil {
			c.Close()
			return
		}
	}
	rd := mcproto.NewReader(br, 1<<16)
	id, body, err := rd.ReadPacket()
	if err != nil || id != 0 {
		c.Close()
		return
	}
	hs, err := mcproto.ParseHandshake(body)
	if err != nil {
		c.Close()
		return
	}
	if hs.Intent == mcproto.IntentStatus {
		defer c.Close()
		if !s.ready.Load() {
			s.StatusRefused.Add(1)
			return
		}
		for {
			id, body, err := rd.ReadPacket()
			if err != nil {
				return
			}
			switch id {
			case 0:
				s.StatusAnswered.Add(1)
				c.Write(mcproto.StatusResponse(mcproto.NewStatus(s.MOTD, "Fake 1.21", hs.Protocol)))
			case 1:
				c.Write(mcproto.AppendPacket(nil, 1, body))
				return
			}
		}
	}
	id, body, err = rd.ReadPacket()
	if err != nil || id != 0 {
		c.Close()
		return
	}
	name, _ := mcproto.ParseLoginStartName(body)
	c.SetReadDeadline(time.Time{})
	rest := bufio.NewReader(&prefixReader{buf: append([]byte(nil), rd.Buffered()...), r: br})
	s.Logins <- &Login{Conn: c, Handshake: hs, Name: name, Raw: rd.Raw()[:len(rd.Raw())-len(rd.Buffered())], R: rest}
}

type prefixReader struct {
	buf []byte
	r   *bufio.Reader
}

func (p *prefixReader) Read(b []byte) (int, error) {
	if len(p.buf) > 0 {
		n := copy(b, p.buf)
		p.buf = p.buf[n:]
		return n, nil
	}
	return p.r.Read(b)
}
