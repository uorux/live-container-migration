package gateway

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"lcm/internal/mcproto"
	"lcm/internal/wire"
)

// The server menu (MinecraftConfig.Menu) replaces a lobby server: a player
// whose hostname names no backend is logged in by the gateway itself
// (offline mode; it grants nothing) and kept in the configuration phase
// with a dialog listing the servers. Picking one wakes it if it is asleep,
// waits until its relay is connected, then stores a cookie and transfers the
// client back to the address it used. The transferred connection (handshake
// intent 3) carries the cookie, so the gateway sends it to that backend.
// Needs a 1.21.6+ client (docs/on-demand.md "Server menu").

const (
	menuCookieKey    = "lcm:route"
	menuCookieMaxAge = 30 * time.Second
	menuJoinPrefix   = "lcm:join/"
	menuActionMenu   = "lcm:menu"
	menuActionQuit   = "lcm:quit"
	// menuMaxIdle disconnects a player who leaves the menu open.
	menuMaxIdle = 15 * time.Minute
	// serverInfoTTL is how long a backend's status ping result is reused.
	serverInfoTTL = 5 * time.Second
	// maxConfigPacket bounds what a client may send in the configuration
	// phase (modded clients send long channel lists).
	maxConfigPacket = 2 << 20
)

// Menu results, for lcm_gateway_menu_total.
const (
	menuShown       = "shown"       // a player got the menu
	menuTransferred = "transferred" // ... and was sent to a server
	menuQuit        = "quit"        // ... pressed Disconnect
	menuLeft        = "left"        // ... closed the connection
	menuTimeout     = "timeout"     // ... idled past menuMaxIdle
	menuUnsupported = "unsupported" // a client too old (or unknown) for dialogs got the address list
	menuArrived     = "arrived"     // a transferred player came back with a valid cookie
)

var menuResults = []string{menuShown, menuTransferred, menuQuit, menuLeft, menuTimeout, menuUnsupported, menuArrived}

// serverInfo is what the menu shows about a backend, from its last status ping.
type serverInfo struct {
	at              time.Time
	online, max     int
	version         string
	protocol        int32
	modded          bool
	descriptionText string
}

func (g *Gateway) label(b *Backend) string {
	if l := g.cfg.Minecraft.Labels[b.Name]; l != "" {
		return l
	}
	return b.Name
}

// menuBackends lists the backends in configuration order.
func (g *Gateway) menuBackends() []*Backend {
	var bs []*Backend
	for _, name := range g.order {
		if b := g.Backend(name); b != nil {
			bs = append(bs, b)
		}
	}
	return bs
}

// refreshInfo pings b through its tunnel if it is connected and the last
// result is stale. Concurrent callers share one ping.
func (g *Gateway) refreshInfo(b *Backend, open wire.Open, protocol int32) {
	if !b.Session().Connected() {
		return
	}
	if cur := b.info.Load(); cur != nil && time.Since(cur.at) < serverInfoTTL {
		return
	}
	if !b.infoBusy.CompareAndSwap(false, true) {
		return
	}
	defer b.infoBusy.Store(false)
	local, remote := net.Pipe()
	b.Session().AddStream(remote, open)
	defer local.Close()
	local.SetDeadline(time.Now().Add(2 * time.Second))
	doc, err := mcproto.QueryStatus(local, protocol, b.Name, 25565)
	if err != nil {
		g.log.Debug("menu: status ping failed", "backend", b.Name, "err", err)
		return
	}
	var st struct {
		Version struct {
			Name     string `json:"name"`
			Protocol int32  `json:"protocol"`
		} `json:"version"`
		Players struct {
			Online int `json:"online"`
			Max    int `json:"max"`
		} `json:"players"`
		ForgeData json.RawMessage `json:"forgeData"`
		ModInfo   json.RawMessage `json:"modinfo"`
		Modded    bool            `json:"isModded"`
	}
	if err := json.Unmarshal([]byte(doc), &st); err != nil {
		g.log.Debug("menu: bad status document", "backend", b.Name, "err", err)
		return
	}
	b.info.Store(&serverInfo{at: time.Now(), online: st.Players.Online, max: st.Players.Max,
		version: st.Version.Name, protocol: st.Version.Protocol,
		modded: st.Modded || len(st.ForgeData) > 0 || len(st.ModInfo) > 0})
}

// refreshAll pings every connected backend, waiting at most wait.
func (g *Gateway) refreshAll(open wire.Open, protocol int32, wait time.Duration) {
	var wg sync.WaitGroup
	for _, b := range g.menuBackends() {
		wg.Add(1)
		go func() { defer wg.Done(); g.refreshInfo(b, open, protocol) }()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(wait):
	}
}

// menuStatus is the server list entry for the menu's address.
func (g *Gateway) menuStatus(protocol int32) mcproto.StatusDoc {
	mc := g.cfg.Minecraft
	online, up := 0, 0
	bs := g.menuBackends()
	for _, b := range bs {
		if b.Session().Connected() {
			up++
			if in := b.info.Load(); in != nil {
				online += in.online
			}
		}
	}
	// The client's own protocol, so it shows no version warning; clients
	// that cannot show the menu see it marked incompatible.
	version := "1.21.6+"
	if _, ok := mcproto.MenuProtocolFor(protocol); !ok {
		version, protocol = "1.21.6 to 26.3", mcproto.MenuMinProtocol
	}
	motd := fmt.Sprintf("%s\n%d of %d servers up, %d playing", mc.MenuTitle, up, len(bs), online)
	doc := mcproto.NewStatus(motd, version, protocol)
	doc.Players.Online = online
	doc.Players.Max = online + 1
	return doc
}

// menuAddresses is the Login Disconnect text for clients that cannot show
// the menu: where to connect for each server instead.
func (g *Gateway) menuAddresses(hs mcproto.Handshake, why string) string {
	host := strings.ToLower(mcproto.CleanHost(hs.Host))
	port := ""
	if hs.Port != 25565 {
		port = ":" + strconv.Itoa(int(hs.Port))
	}
	var sb strings.Builder
	sb.WriteString(why)
	sb.WriteString("\n\nConnect to a server directly:\n")
	for _, b := range g.menuBackends() {
		addr := b.Name + "." + host + port
		for h, name := range g.cfg.Routes {
			if name == b.Name {
				addr = h + port
				break
			}
		}
		if l := g.label(b); l != b.Name {
			fmt.Fprintf(&sb, "\n%s: %s", l, addr)
		} else {
			fmt.Fprintf(&sb, "\n%s", addr)
		}
	}
	return sb.String()
}

// issueMenuCookie returns the cookie payload naming the chosen backend.
func issueMenuCookie(backend string) []byte {
	var n [8]byte
	rand.Read(n[:])
	return fmt.Appendf(nil, "v1|%s|%d|%s", backend, time.Now().Unix(), hex.EncodeToString(n[:]))
}

// redeemMenuCookie returns the backend a menu cookie names if it is fresh
// and has not been used on this gateway yet (so a player who later comes
// back to the menu address, e.g. through a server's /transfer, gets the
// menu again rather than the old choice).
func (g *Gateway) redeemMenuCookie(p []byte) (*Backend, error) {
	parts := strings.Split(string(p), "|")
	if len(parts) != 4 || parts[0] != "v1" {
		return nil, errors.New("not an lcm menu cookie")
	}
	ts, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return nil, errors.New("bad timestamp")
	}
	age := time.Since(time.Unix(ts, 0))
	if age > menuCookieMaxAge || age < -menuCookieMaxAge {
		return nil, fmt.Errorf("stale (%s old)", age.Round(time.Second))
	}
	g.cookieMu.Lock()
	defer g.cookieMu.Unlock()
	for n, at := range g.usedCookies {
		if time.Since(at) > 2*menuCookieMaxAge {
			delete(g.usedCookies, n)
		}
	}
	if _, used := g.usedCookies[parts[3]]; used {
		return nil, errors.New("already used")
	}
	b := g.Backend(parts[1])
	if b == nil {
		return nil, fmt.Errorf("unknown backend %q", parts[1])
	}
	g.usedCookies[parts[3]] = time.Now()
	return b, nil
}

// menuArrival handles a transferred connection to the menu's address: it
// asks for the menu cookie and, if the client has a valid one, returns the
// chosen backend and the bytes to replay to it (the handshake with intent 2,
// so the server needs no accepts-transfers, and Login Start).
func (g *Gateway) menuArrival(c net.Conn, rd *mcproto.Reader, hs mcproto.Handshake, loginStart []byte, log *slog.Logger) (*Backend, []byte) {
	mc := g.cfg.Minecraft
	c.SetDeadline(time.Now().Add(mc.SniffTimeout))
	defer c.SetDeadline(time.Time{})
	if _, err := c.Write(mcproto.CookieRequest(menuCookieKey)); err != nil {
		return nil, nil
	}
	for {
		id, body, err := rd.ReadPacket()
		if err != nil {
			log.Debug("menu: no cookie response", "err", err)
			return nil, nil
		}
		if id != mcproto.LoginCookieResponseID {
			continue
		}
		key, payload, err := mcproto.ParseCookieResponse(body)
		if err != nil || key != menuCookieKey {
			log.Debug("menu: bad cookie response", "err", err, "key", key)
			return nil, nil
		}
		if payload == nil {
			return nil, nil // transferred here by someone else: show the menu
		}
		b, err := g.redeemMenuCookie(payload)
		if err != nil {
			log.Info("menu: cookie ignored", "err", err)
			return nil, nil
		}
		hs.Intent = mcproto.IntentLogin
		prefix := mcproto.AppendHandshake(nil, hs)
		prefix = mcproto.AppendPacket(prefix, 0x00, loginStart)
		prefix = append(prefix, rd.Buffered()...)
		g.metrics.menu.Inc(menuArrived)
		return b, prefix
	}
}

// menuSession is one player in the menu. Only its own goroutine writes to c.
type menuSession struct {
	g     *Gateway
	c     net.Conn
	p     mcproto.MenuProtocol
	hs    mcproto.Handshake
	ls    mcproto.LoginStart
	open  wire.Open
	log   *slog.Logger
	shown []byte // the last dialog packet sent
}

// serveMenu runs the menu for a login to an address that names no backend.
func (g *Gateway) serveMenu(c net.Conn, rd *mcproto.Reader, hs mcproto.Handshake, loginStart []byte, log *slog.Logger) {
	mc := g.cfg.Minecraft
	defer gracefulClose(c)
	c.SetDeadline(time.Now().Add(mc.SniffTimeout))
	p, ok := mcproto.MenuProtocolFor(hs.Protocol)
	ls, err := mcproto.ParseLoginStart(loginStart)
	if !ok || err != nil {
		why := "The server menu needs Minecraft 1.21.6 or newer."
		if hs.Protocol > mcproto.MenuMaxProtocol {
			why = "This lcm-gateway doesn't know your Minecraft version yet, so it can't show the server menu."
			log.Warn("menu: client protocol newer than this gateway knows; update lcm", "max_known", mcproto.MenuMaxProtocol)
		}
		g.metrics.menu.Inc(menuUnsupported)
		c.Write(mcproto.LoginDisconnect(g.menuAddresses(hs, why)))
		return
	}
	if _, err := c.Write(p.LoginSuccess(ls.UUID, ls.Name)); err != nil {
		return
	}
	for {
		id, _, err := rd.ReadPacket()
		if err != nil {
			log.Debug("menu: no Login Acknowledged", "err", err)
			return
		}
		if id == mcproto.LoginAcknowledgedID {
			break
		}
	}
	c.SetDeadline(time.Time{})
	clicks := make(chan string, 4)
	gone := make(chan struct{})
	go func() {
		defer close(gone)
		// Continue after the login reader, which may hold bytes the client
		// sent right after Login Acknowledged.
		cr := mcproto.NewReader(io.MultiReader(bytes.NewReader(rd.Buffered()), c), maxConfigPacket)
		for {
			id, body, err := cr.ReadPacket()
			if err != nil {
				return
			}
			if id == mcproto.ConfigCustomClickID {
				if a, err := mcproto.ParseCustomClick(body); err == nil {
					select {
					case clicks <- a:
					default: // a player clicking faster than we answer
					}
				}
			}
			cr.Discard()
		}
	}()
	open := wire.Open{Src: addrPort(c.RemoteAddr()), Dst: addrPort(c.LocalAddr())}
	m := &menuSession{g: g, c: c, p: p, hs: hs, ls: ls, open: open, log: log}
	result := m.run(clicks, gone)
	g.metrics.menu.Inc(result)
	log.Info("menu: player left the menu", "result", result)
}

func (m *menuSession) send(pkt []byte) error {
	m.c.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := m.c.Write(pkt)
	return err
}

func (m *menuSession) showMenu(force bool) error {
	pkt := m.p.ShowDialog(m.g.menuDialog(m.hs.Protocol))
	if !force && string(pkt) == string(m.shown) {
		return nil
	}
	m.shown = pkt
	return m.send(pkt)
}

func (m *menuSession) showNotice(title, body string) error {
	pkt := m.p.ShowDialog(noticeDialog(title, body))
	m.shown = pkt
	return m.send(pkt)
}

// run is the menu's event loop; it returns a menu result.
func (m *menuSession) run(clicks <-chan string, gone <-chan struct{}) string {
	g, mc := m.g, m.g.cfg.Minecraft
	g.refreshAll(m.open, m.hs.Protocol, 1500*time.Millisecond)
	if m.showMenu(true) != nil {
		return menuLeft
	}
	g.metrics.menu.Inc(menuShown)
	m.log.Info("menu: shown")
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var (
		showing   = true    // the server list is on screen
		waiting   *Backend  // the server being started for this player
		waitSince time.Time // when waiting began
		lastNote  time.Time // when the "starting" notice was last updated
		lastKA    = time.Now()
		lastMenu  = time.Now()
		active    = time.Now()
		wakeErrs  = make(chan wakeResult, 1)
		refreshed = make(chan struct{}, 1)
	)
	for {
		select {
		case <-gone:
			return menuLeft
		case a := <-clicks:
			active = time.Now()
			switch {
			case a == menuActionQuit:
				m.send(mcproto.ConfigDisconnect(mcproto.Text("See you!")))
				return menuQuit
			case a == menuActionMenu:
				waiting, showing = nil, true
				g.refreshAll(m.open, m.hs.Protocol, time.Second)
				if m.showMenu(true) != nil {
					return menuLeft
				}
				lastMenu = time.Now()
			case strings.HasPrefix(a, menuJoinPrefix):
				b := g.Backend(strings.TrimPrefix(a, menuJoinPrefix))
				if b == nil {
					m.showMenu(true)
					continue
				}
				if b.Session().Connected() {
					return m.transfer(b)
				}
				showing = false
				if !mc.Whitelist.Allowed(b.Name, m.ls.Name, &m.ls.UUID) {
					m.log.Info("menu: not whitelisted; not starting", "backend", b.Name)
					g.metrics.conns.Inc(b.Name, resultNotWhitelisted)
					m.showNotice("Not whitelisted", "You aren't on the whitelist for "+g.label(b)+".")
					continue
				}
				if g.cfg.Wake == nil {
					m.showNotice(g.label(b)+" is offline", g.label(b)+" isn't running, and this gateway can't start servers.")
					continue
				}
				waiting, waitSince, lastNote = b, time.Now(), time.Now()
				m.log.Info("menu: starting server for player", "backend", b.Name)
				g.wake(b, func(err error) {
					select {
					case wakeErrs <- wakeResult{b, err}:
					default:
					}
				})
				if m.showNotice("Starting "+g.label(b), startingText(g.label(b), 0)) != nil {
					return menuLeft
				}
			}
		case r := <-wakeErrs:
			if r.err != nil && waiting == r.b {
				m.log.Warn("menu: wake failed", "backend", waiting.Name, "err", r.err)
				m.showNotice("Couldn't start "+g.label(waiting), "The cluster couldn't start "+g.label(waiting)+". Try again later.")
				waiting = nil
			}
		case <-refreshed:
			if showing && m.showMenu(false) != nil {
				return menuLeft
			}
		case now := <-tick.C:
			if now.Sub(lastKA) >= mc.HoldInterval {
				if m.send(mcproto.ConfigKeepAlive(now.UnixMilli())) != nil {
					return menuLeft
				}
				lastKA = now
			}
			if now.Sub(active) > menuMaxIdle {
				m.send(mcproto.ConfigDisconnect(mcproto.Text("You were in the server menu for too long.")))
				return menuTimeout
			}
			switch {
			case waiting != nil && waiting.Session().Connected():
				return m.transfer(waiting)
			case waiting != nil && now.Sub(waitSince) > mc.MaxHold:
				m.log.Info("menu: server did not start in time", "backend", waiting.Name)
				m.showNotice(g.label(waiting)+" didn't start", g.label(waiting)+" didn't start in time. Try again later.")
				waiting = nil
			case waiting != nil && now.Sub(lastNote) >= 5*time.Second:
				m.showNotice("Starting "+g.label(waiting), startingText(g.label(waiting), now.Sub(waitSince)))
				lastNote = now
			case showing && now.Sub(lastMenu) >= 10*time.Second:
				// Keep the list fresh; it is resent only if something changed.
				lastMenu = now
				go func() {
					g.refreshAll(m.open, m.hs.Protocol, time.Second)
					select {
					case refreshed <- struct{}{}:
					default:
					}
				}()
			}
		}
	}
}

type wakeResult struct {
	b   *Backend
	err error
}

// transfer sends the player to b through the address they used.
func (m *menuSession) transfer(b *Backend) string {
	host := mcproto.CleanHost(m.hs.Host)
	if host == "" {
		host = addrPort(m.c.LocalAddr()).Addr().String()
	}
	m.send(m.p.StoreCookie(menuCookieKey, issueMenuCookie(b.Name)))
	m.send(m.p.Transfer(host, int(m.hs.Port)))
	m.g.metrics.menuTransfers.Inc(b.Name)
	m.log.Info("menu: transferring player", "backend", b.Name, "to", net.JoinHostPort(host, strconv.Itoa(int(m.hs.Port))))
	return menuTransferred
}

func startingText(label string, waited time.Duration) string {
	s := label + " was asleep and is starting. Big modpacks can take a few minutes; you'll join automatically when it's up."
	if waited >= time.Second {
		s += fmt.Sprintf("\n\nWaiting for %d s", int(waited.Seconds()))
	}
	return s
}

// menuDialog is the server list (a multi_action dialog; see
// net.minecraft.server.dialog). Buttons send "lcm:join/<backend>".
func (g *Gateway) menuDialog(protocol int32) mcproto.Compound {
	mc := g.cfg.Minecraft
	var buttons mcproto.List
	for _, b := range g.menuBackends() {
		label, tip := g.menuEntry(b, protocol)
		buttons = append(buttons, mcproto.Compound{
			mcproto.F("label", label),
			mcproto.F("tooltip", mcproto.Text(tip)),
			mcproto.F("width", mcproto.Int(300)),
			mcproto.F("action", mcproto.Compound{mcproto.F("type", mcproto.String("minecraft:custom")), mcproto.F("id", mcproto.String(menuJoinPrefix+b.Name))}),
		})
	}
	body := "Pick a server. A sleeping server starts when you pick it, and you join once it's up."
	return mcproto.Compound{
		mcproto.F("type", mcproto.String("minecraft:multi_action")),
		mcproto.F("title", mcproto.Text(mc.MenuTitle)),
		mcproto.F("body", mcproto.List{mcproto.Compound{mcproto.F("type", mcproto.String("minecraft:plain_message")), mcproto.F("contents", mcproto.Text(body)), mcproto.F("width", mcproto.Int(300))}}),
		mcproto.F("can_close_with_escape", mcproto.Bool(false)),
		mcproto.F("pause", mcproto.Bool(false)),
		mcproto.F("after_action", mcproto.String("wait_for_response")),
		mcproto.F("actions", buttons),
		mcproto.F("exit_action", mcproto.Compound{
			mcproto.F("label", mcproto.Text("Disconnect")),
			mcproto.F("width", mcproto.Int(150)),
			mcproto.F("action", mcproto.Compound{mcproto.F("type", mcproto.String("minecraft:custom")), mcproto.F("id", mcproto.String(menuActionQuit))}),
		}),
		mcproto.F("columns", mcproto.Int(1)),
	}
}

// menuEntry is a server's button text and tooltip.
func (g *Gateway) menuEntry(b *Backend, protocol int32) (mcproto.Compound, string) {
	name := mcproto.Styled(g.label(b), "", true)
	in := b.info.Load()
	var tip []string
	if in != nil && in.version != "" {
		tip = append(tip, "Version: "+in.version)
	}
	if in != nil && in.modded {
		tip = append(tip, "Modded: you need the modpack installed")
	}
	var state mcproto.Compound
	switch {
	case b.Session().Connected() && in != nil:
		state = mcproto.Styled(fmt.Sprintf("  %d playing", in.online), "green", false)
		if in.protocol != protocol && in.protocol > 0 {
			state = mcproto.Styled(fmt.Sprintf("  %d playing, %s", in.online, in.version), "yellow", false)
			tip = append(tip, "Your Minecraft version differs from the server's")
		}
	case b.Session().Connected():
		state = mcproto.Styled("  up", "green", false)
	case g.cfg.Wake != nil && time.Since(time.Unix(0, b.lastWake.Load())) < 2*time.Minute:
		state = mcproto.Styled("  starting", "gold", false)
		tip = append(tip, "Starting; pick it to join when it's up")
	case g.cfg.Wake != nil:
		state = mcproto.Styled("  asleep", "gray", false)
		tip = append(tip, "Asleep; pick it to start it")
	default:
		state = mcproto.Styled("  offline", "red", false)
	}
	if len(tip) == 0 {
		tip = append(tip, b.Name)
	}
	return mcproto.Join(name, state), strings.Join(tip, "\n")
}

// noticeDialog shows a message with a button back to the menu.
func noticeDialog(title, body string) mcproto.Compound {
	return mcproto.Compound{
		mcproto.F("type", mcproto.String("minecraft:notice")),
		mcproto.F("title", mcproto.Text(title)),
		mcproto.F("body", mcproto.List{mcproto.Compound{mcproto.F("type", mcproto.String("minecraft:plain_message")), mcproto.F("contents", mcproto.Text(body)), mcproto.F("width", mcproto.Int(300))}}),
		mcproto.F("can_close_with_escape", mcproto.Bool(false)),
		mcproto.F("pause", mcproto.Bool(false)),
		mcproto.F("after_action", mcproto.String("wait_for_response")),
		mcproto.F("action", mcproto.Compound{
			mcproto.F("label", mcproto.Text("Back to the server list")),
			mcproto.F("width", mcproto.Int(200)),
			mcproto.F("action", mcproto.Compound{mcproto.F("type", mcproto.String("minecraft:custom")), mcproto.F("id", mcproto.String(menuActionMenu))}),
		}),
	}
}
