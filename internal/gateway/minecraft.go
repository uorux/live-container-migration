package gateway

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"lcm/internal/mcproto"
	"lcm/internal/tunnel"
	"lcm/internal/wire"
)

// MinecraftConfig enables Minecraft-aware handling of the first packets of
// each player connection (docs/on-demand.md §5.1). After those, the stream is
// plain L4 again: bytes are forwarded unchanged in both directions.
type MinecraftConfig struct {
	// MOTD is the server list description shown while no relay is connected.
	MOTD string
	// VersionName is the version string in that status reply; the protocol
	// number is echoed from the client's handshake.
	VersionName string
	// HoldInterval is how often a held login gets the no-op Set Compression
	// packet. It must stay well under the client's 30 s read timeout.
	HoldInterval time.Duration
	// MaxHold is how long a login may wait for the server's first byte before
	// the player gets a Login Disconnect with HoldMessage.
	MaxHold time.Duration
	// LegacyMaxHold caps the hold for 1.7.x clients (protocol < 47), which
	// cannot be kept alive in login.
	LegacyMaxHold time.Duration
	HoldMessage   string
	// SniffTimeout bounds reading the handshake (and Login Start) and a
	// locally answered status exchange.
	SniffTimeout time.Duration
	// Menu sends players whose hostname names no backend to the server menu
	// (menu.go) instead of the default backend.
	Menu bool
	// MenuTitle heads the menu and its server list entry.
	MenuTitle string
	// Labels are display names for backends in the menu.
	Labels map[string]string
	// Whitelist, if set, decides who may wake (and be held for) an asleep
	// backend; others get the vanilla "not white-listed" disconnect.
	Whitelist *Whitelist
}

// notWhitelisted is the vanilla server's message.
const notWhitelisted = "You are not white-listed on this server!"

func (m *MinecraftConfig) defaults() {
	if m.MOTD == "" {
		m.MOTD = "Server is starting, please wait..."
	}
	if m.VersionName == "" {
		m.VersionName = "starting"
	}
	if m.HoldInterval <= 0 {
		m.HoldInterval = 10 * time.Second
	}
	if m.MaxHold <= 0 {
		m.MaxHold = 5 * time.Minute
	}
	if m.LegacyMaxHold <= 0 {
		m.LegacyMaxHold = 25 * time.Second
	}
	if m.HoldMessage == "" {
		m.HoldMessage = "The server did not start in time. Please try again later."
	}
	if m.SniffTimeout <= 0 {
		m.SniffTimeout = 5 * time.Second
	}
	if m.MenuTitle == "" {
		m.MenuTitle = "Minecraft servers"
	}
}

// motd fills in {backend} (the server the player asked for).
func motd(tmpl string, b *Backend) string { return strings.ReplaceAll(tmpl, "{backend}", b.Name) }

// maxSniffPacket bounds the handshake and Login Start we buffer.
const maxSniffPacket = 32 << 10

// handleMinecraft classifies a new player connection by its first packet(s).
func (g *Gateway) handleMinecraft(c net.Conn, b *Backend) {
	mc := g.cfg.Minecraft
	log := g.log.With("src", c.RemoteAddr().String())
	c.SetReadDeadline(time.Now().Add(mc.SniffTimeout))
	rd := mcproto.NewReader(c, maxSniffPacket)
	if err := rd.Fill(); err != nil {
		log.Debug("player sent nothing", "err", err)
		c.Close()
		return
	}
	if rd.Raw()[0] == 0xfe {
		// Possibly a pre-1.7 ping; a 1.3-and-older client sends only 0xFE and
		// waits, so give it a moment to send more before deciding.
		if len(rd.Raw()) < 3 {
			c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
			for len(rd.Raw()) < 3 && rd.Fill() == nil {
			}
			c.SetReadDeadline(time.Now().Add(mc.SniffTimeout))
		}
		if mcproto.IsLegacyPing(rd.Raw()) {
			if b.Session().Connected() {
				g.addStream(b, &replayConn{Conn: c, prefix: rd.Raw()}, "legacy-status", log)
				return
			}
			log.Debug("legacy ping answered locally")
			c.SetWriteDeadline(time.Now().Add(mc.SniffTimeout))
			if _, err := c.Write(mcproto.LegacyKick(127, mc.VersionName, motd(mc.MOTD, b), 0, 0)); err == nil {
				g.statusAnswered(b)
			}
			gracefulClose(c)
			return
		}
	}
	id, body, err := rd.ReadPacket()
	var hs mcproto.Handshake
	if err == nil && id != 0x00 {
		err = errors.New("first packet is not a handshake")
	}
	if err == nil {
		hs, err = mcproto.ParseHandshake(body)
	}
	if err == nil && (hs.Intent < mcproto.IntentStatus || hs.Intent > mcproto.IntentTransfer) {
		err = errors.New("bad handshake intent")
	}
	if err != nil {
		log.Info("not a Minecraft connection; closing", "err", err)
		abortClose(c)
		return
	}
	b, matched := g.route(hs.Host)
	menu := mc.Menu && !matched
	if menu {
		log = log.With("protocol", hs.Protocol, "host", mcproto.CleanHost(hs.Host), "backend", "(menu)")
	} else {
		log = log.With("protocol", hs.Protocol, "host", mcproto.CleanHost(hs.Host), "backend", b.Name)
	}

	if hs.Intent == mcproto.IntentStatus && menu {
		go g.refreshAll(wire.Open{Src: addrPort(c.RemoteAddr()), Dst: addrPort(c.LocalAddr())}, hs.Protocol, time.Second)
		g.answerStatus(c, rd, func() any { return g.menuStatus(hs.Protocol) }, nil)
		return
	}
	if hs.Intent == mcproto.IntentStatus {
		if b.Session().Connected() {
			c.SetReadDeadline(time.Time{})
			g.addStream(b, &replayConn{Conn: c, prefix: rd.Raw()}, "status", log)
			return
		}
		g.answerStatus(c, rd, func() any { return mcproto.NewStatus(motd(mc.MOTD, b), mc.VersionName, hs.Protocol) },
			func() { g.statusAnswered(b) })
		return
	}

	// Login or transfer: read Login Start too (for the player name).
	id, body, err = rd.ReadPacket()
	var name string
	if err == nil && id != 0x00 {
		err = errors.New("second packet is not Login Start")
	}
	if err == nil {
		name, err = mcproto.ParseLoginStartName(body)
	}
	if err != nil {
		log.Info("bad Login Start; closing", "err", err)
		abortClose(c)
		return
	}
	log = log.With("player", name)
	raw := rd.Raw()
	if menu && hs.Intent == mcproto.IntentTransfer {
		if tb, prefix := g.menuArrival(c, rd, hs, body, log); tb != nil {
			b, raw, menu = tb, prefix, false
			log = log.With("backend", b.Name)
		}
	}
	if menu {
		g.serveMenu(c, rd, hs, body, log)
		return
	}
	if !b.Session().Connected() && !mc.Whitelist.Allowed(b.Name, name, loginUUID(body, len(mcproto.AppendString(nil, name)))) {
		log.Info("not whitelisted; refused without waking", "backend", b.Name)
		g.metrics.conns.Inc(b.Name, resultNotWhitelisted)
		c.SetWriteDeadline(time.Now().Add(5 * time.Second))
		c.Write(mcproto.LoginDisconnect(notWhitelisted))
		gracefulClose(c)
		return
	}
	c.SetReadDeadline(time.Time{})

	keepalive := hs.Protocol >= mcproto.Protocol47
	limit := mc.MaxHold
	if !keepalive {
		limit = min(limit, mc.LegacyMaxHold)
	}
	hc := newHoldConn(c, raw)
	st := g.addStream(b, hc, "login", log)
	go g.hold(hc, st, b, keepalive, limit, log.With("stream", st.ID()))
}

// answerStatus serves the server list ping from the gateway (while no relay
// is connected, or for the menu): the status document doc returns, then
// Pong. answered, if set, is called once the document is sent.
func (g *Gateway) answerStatus(c net.Conn, rd *mcproto.Reader, doc func() any, answered func()) {
	mc := g.cfg.Minecraft
	defer gracefulClose(c)
	c.SetDeadline(time.Now().Add(mc.SniffTimeout))
	for {
		id, body, err := rd.ReadPacket()
		if err != nil {
			return
		}
		switch {
		case id == 0x00 && len(body) == 0:
			if _, err := c.Write(mcproto.StatusResponse(doc())); err != nil {
				return
			}
			if answered != nil {
				answered()
			}
		case id == 0x01 && len(body) == 8:
			c.Write(mcproto.AppendPacket(nil, 0x01, body))
			return
		default:
			return
		}
	}
}

func (g *Gateway) statusAnswered(b *Backend) {
	b.statusAnswered.Add(1)
	g.metrics.statusResp.Inc(b.Name)
}

// hold keeps a login stream alive until the server's first byte arrives
// (or the client goes away), and kicks the player after limit.
func (g *Gateway) hold(hc *holdConn, st *tunnel.Stream, b *Backend, keepalive bool, limit time.Duration, log *slog.Logger) {
	mc := g.cfg.Minecraft
	b.held.Add(1)
	defer b.held.Add(-1)
	start := time.Now()
	var tick <-chan time.Time
	if keepalive {
		t := time.NewTicker(mc.HoldInterval)
		defer t.Stop()
		tick = t.C
	}
	kick := time.NewTimer(limit)
	defer kick.Stop()
	injected := 0
	for {
		select {
		case <-hc.released:
			if injected > 0 {
				log.Info("held player released", "held", time.Since(start).Round(time.Millisecond), "keepalives", injected)
			}
			return
		case <-tick:
			ok, err := hc.inject(mcproto.SetCompressionNoop)
			if !ok {
				continue // released meanwhile; the next select returns
			}
			if err != nil {
				log.Info("keepalive write failed; dropping player", "err", err)
				if st.Reset(wire.ResetLocal, "keepalive write failed") {
					g.metrics.conns.Inc(b.Name, resultResetByPlayer)
				}
				return
			}
			injected++
		case <-kick.C:
			if hc.expire(mcproto.LoginDisconnect(mc.HoldMessage)) {
				log.Info("hold limit reached; player disconnected", "held", time.Since(start).Round(time.Millisecond))
				if st.Reset(wire.ResetLocal, "hold limit reached") {
					g.metrics.conns.Inc(b.Name, resultHoldTimeout)
				}
			}
			return
		}
	}
}

// replayConn returns prefix from Read before reading the connection, so the
// bytes the gateway sniffed reach the server unchanged. It keeps the
// CloseWrite/SetLinger behaviour of the underlying TCP connection.
type replayConn struct {
	net.Conn
	prefix []byte // only touched by the (single) stream reader
}

func (r *replayConn) Read(p []byte) (int, error) {
	if len(r.prefix) > 0 {
		n := copy(p, r.prefix)
		r.prefix = r.prefix[n:]
		return n, nil
	}
	return r.Conn.Read(p)
}

func (r *replayConn) CloseWrite() error {
	if cw, ok := r.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return r.Conn.Close()
}

func (r *replayConn) SetLinger(sec int) error {
	if l, ok := r.Conn.(interface{ SetLinger(int) error }); ok {
		return l.SetLinger(sec)
	}
	return nil
}

var _ tunnel.TCPLike = (*replayConn)(nil)

// HandoffConn: movable once the sniffed prefix has been passed on (only the
// stream's reader touches prefix, and Freeze has stopped it).
func (r *replayConn) HandoffConn() (*net.TCPConn, bool) {
	if len(r.prefix) > 0 {
		return nil, false
	}
	if h, ok := r.Conn.(tunnel.Handoffer); ok {
		return h.HandoffConn()
	}
	tc, ok := r.Conn.(*net.TCPConn)
	return tc, ok
}

const (
	holdHolding int32 = iota // no server byte yet: injection allowed
	holdStarted              // the server owns the socket; never inject again
	holdExpired              // the gateway sent Login Disconnect; server bytes are refused
)

var errHoldExpired = errors.New("login hold expired")

// holdConn is the player socket of a login stream. Writes are serialised so
// an injected keepalive never interleaves with server bytes: the stream's
// first Write flips the state to started under mu, and injections happen
// only under mu while still holding. After that, server writes go straight
// to the socket.
type holdConn struct {
	replayConn
	mu       sync.Mutex
	state    atomic.Int32
	released chan struct{} // closed when the state leaves holding, or on Close
	relOnce  sync.Once
}

func newHoldConn(c net.Conn, prefix []byte) *holdConn {
	return &holdConn{replayConn: replayConn{Conn: c, prefix: prefix}, released: make(chan struct{})}
}

func (h *holdConn) release() { h.relOnce.Do(func() { close(h.released) }) }

// HandoffConn: a login is movable once the server owns the socket; a held
// one (keepalives injected by this gateway, server not up yet) is not.
func (h *holdConn) HandoffConn() (*net.TCPConn, bool) {
	if h.state.Load() != holdStarted {
		return nil, false
	}
	return h.replayConn.HandoffConn()
}

func (h *holdConn) Write(p []byte) (int, error) {
	if h.state.Load() != holdStarted {
		h.mu.Lock()
		switch h.state.Load() {
		case holdExpired:
			h.mu.Unlock()
			return 0, errHoldExpired
		case holdHolding:
			h.state.Store(holdStarted)
			h.release()
		}
		h.mu.Unlock()
	}
	return h.Conn.Write(p)
}

// inject writes pkt if no server byte has been written yet. ok is false if
// the hold is over.
func (h *holdConn) inject(pkt []byte) (ok bool, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.state.Load() != holdHolding {
		return false, nil
	}
	h.Conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err = h.Conn.Write(pkt)
	h.Conn.SetWriteDeadline(time.Time{})
	return true, err
}

// expire ends the hold with a final packet (Login Disconnect) if the server
// has not spoken yet; later server writes fail.
func (h *holdConn) expire(pkt []byte) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.state.Load() != holdHolding {
		return false
	}
	h.state.Store(holdExpired)
	h.release()
	h.Conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	h.Conn.Write(pkt)
	return true
}

// CloseWrite is the server's FIN: no keepalive may follow it.
func (h *holdConn) CloseWrite() error {
	h.mu.Lock()
	if h.state.Load() == holdHolding {
		h.state.Store(holdStarted)
	}
	h.release()
	h.mu.Unlock()
	return h.replayConn.CloseWrite()
}

// SetLinger is ignored after expiry so an abortive close cannot discard the
// Login Disconnect still in the send buffer.
func (h *holdConn) SetLinger(sec int) error {
	if h.state.Load() == holdExpired {
		return nil
	}
	return h.replayConn.SetLinger(sec)
}

func (h *holdConn) Close() error {
	h.release()
	if h.state.Load() == holdExpired {
		gracefulClose(h.Conn)
		return nil
	}
	return h.Conn.Close()
}

// gracefulClose half-closes c, drains input briefly so the kernel does not
// answer unread data with RST (discarding what we just wrote), then closes.
func gracefulClose(c net.Conn) {
	if cw, ok := c.(interface{ CloseWrite() error }); ok {
		cw.CloseWrite()
		c.SetReadDeadline(time.Now().Add(2 * time.Second))
		io.Copy(io.Discard, c)
	}
	c.Close()
}

func abortClose(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetLinger(0)
	}
	c.Close()
}
