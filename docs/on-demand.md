# On-demand servers: wake on join, hold the player, stop when idle

Goal: a Minecraft server pod exists only while someone wants to play. A player who joins a stopped
server is not timed out while the server boots, even a modded pack that needs 1–5 minutes. The
server stops after it has been empty for a while, and none of this conflicts with live migration
([design.md](design.md)).

Researched and tested 2026-10-22. Versions are as of that date. The proof script is
[tools/holdproxy/holdproxy.js](../tools/holdproxy/holdproxy.js).

## TL;DR

- **Recommendation: make `lcm-gateway` Minecraft-aware for the first few packets only, and hold
  joining players in the *login* phase while the backend boots.** The gateway already queues a
  player's bytes while no relay is attached. The missing piece is keeping the client alive. Every
  Java client from 1.8 to 26.3 times out after 30 s without inbound bytes, and it has no other login
  timer. So until the backend sends its first byte, the gateway sends a 7-byte **Login "Set
  Compression" packet with threshold −1** every 10 s. When compression is off, the client treats
  that packet as a no-op. The client sends no reply, so nothing has to be filtered out of the byte
  stream. Once the backend is up, the gateway forwards the buffered Handshake and Login Start and
  steps out of the way: plain L4 and byte-exact, so encryption, modern forwarding, Forge/NeoForge
  negotiation and migration all work unchanged.
- **Tested with real clients:**
  - Vanilla 1.21.11: held 180 s, then joined.
  - NeoForge 1.21.1 with a content mod: held 150 s, then joined a NeoForge server.
  - Forge 1.12.2: held 120 s, then the full FML handshake ran and it joined.
  - Without the keepalive, both vanilla 1.21.11 and Forge 1.12.2 disconnect at exactly 30.0 s.
- **A lobby or limbo world does not work for modded clients in general.** Every standalone limbo is
  a vanilla server:
  - A NeoForge client whose mods have required network channels refuses to join a vanilla server.
  - Forge 1.13–1.20.1 cannot be moved from a vanilla server to a Forge server without a client
    reset mod.
  - The only lobby that works for every modded client is a server running the same modpack, which
    costs most of what we are trying to save. The login hold replaces the lobby. The player sees
    "Logging in…" plus a "starting" MOTD in the server list. A lobby remains an option for
    vanilla/Paper/Fabric only, via the Transfer packet (§6.3).
- **Wake and idle stop belong next to the gateway, not in a StatefulSet scaler.** Servers are bare
  pods that change name on every migration. A small lifecycle loop creates the pod from a template on
  the first login, and deletes it (SIGTERM, then `stop`, then the world is saved) once the gateway
  has counted zero players for N minutes. It skips that step while a migration holds the backend's
  Lease.
- **Later option:** "hibernate" with our own CRIU path instead of stopping. A modded wake then takes
  a restore of a few seconds instead of minutes.

## 1. The numbers that bound the design

| What | Value | Source |
|---|---|---|
| Client read timeout | **30 s without any inbound bytes**, in every phase, counted from TCP connect. Installed as Netty `ReadTimeoutHandler(30)` in the client channel initialiser. | Bytecode of the 26.3 client (`Connection$1`, `bipush 30`) and the 1.12.2 client (same). Measured: vanilla 1.21.11 and Forge 1.12.2 both closed at **30.0 s** after Login Start when the server sent nothing (§2). |
| Other client login timer | **None.** `ClientHandshakePacketListenerImpl` has no `tick()`, and `ConnectScreen.tick()` only ticks the connection. | 26.3 bytecode. Measured: 120–180 s holds succeeded (§2). |
| Client configuration-phase timer (1.20.2+) | None besides the 30 s read timeout. Keep Alive and Ping are valid there. | Velocity and vanilla source, via research. |
| Server login timeout | `MAX_TICKS_BEFORE_LOGIN = 600` (30 s at 20 TPS), "Took too long to log in". It counts from the *backend's* accept, so it doesn't matter if we only open the backend connection once the server is ready. | [yarn constants](https://maven.fabricmc.net/docs/yarn-1.21.1+build.3/constant-values.html) |
| Server read timeout, Paper keepalive | 30 s and 30 s | [research/networking-and-minecraft.md](research/networking-and-minecraft.md) §4 |
| Modded boot time | 1–5 min (given) | – |
| Hold keepalive interval we use | 10 s, giving 3× margin against the 30 s timeout | – |
| 1.7.10 and older | **Cannot be kept alive in login.** 1.7 has no Set Compression and no plugin query; the clientbound login packets are only Disconnect, Encryption Request and Login Success. Hold at most about 25 s, then send a kick with a message. | Protocol |

Packets that keep a client alive in the login phase (clientbound, before encryption):

| Packet | Versions | Client reply | Caveat |
|---|---|---|---|
| **Set Compression, threshold −1** (`06 03 ff ff ff ff 0f`) | 1.8 → 26.3 (id 0x03 throughout) | none | `setupCompression(<0)` removes compression handlers if present, so with compression off it is a no-op (26.3 and 1.12.2 bytecode). **node-minecraft-protocol/mineflayer mishandle it**: they enable zlib and then break, so test bots need the other mode. |
| Login Plugin Request / Custom Query, unknown channel | 1.13 → 26.3 | Custom Query Answer, empty | The gateway must swallow the answers, and must not splice until every outstanding query is answered: a vanilla server disconnects on an answer it never asked for (`DISCONNECT_UNEXPECTED_QUERY`). The client status text changes to "Negotiating…". Forge 1.13–1.20.1, NeoForge and Fabric API all fall through to the vanilla "not understood" reply ([Forge patch](https://github.com/MinecraftForge/MinecraftForge/blob/1.20.1/patches/minecraft/net/minecraft/client/multiplayer/ClientHandshakePacketListenerImpl.java.patch), [Fabric addon](https://github.com/FabricMC/fabric-api/blob/1.21.1/fabric-networking-api-v1/src/client/java/net/fabricmc/fabric/impl/networking/client/ClientLoginNetworkAddon.java); NeoForge doesn't patch the login listener). Prior art: [redoxide](https://github.com/samuelloranger/redoxide) does exactly this. |
| Cookie Request | 1.20.5 → 26.3 | Cookie Response | Same filtering problem. No advantage. |

## 2. Empirical verification

Setup:
- [holdproxy.js](../tools/holdproxy/holdproxy.js) models the proposed gateway behaviour. It reads
  the Handshake and Login Start, holds the client for H seconds while injecting a keepalive every
  10 s, then dials the real server, replays the two packets and splices the connection.
- Clients are real Mojang clients launched by [HeadlessMC](https://github.com/headlesshq/headlessmc)
  3.0.0-RC8 (`-lwjgl`, offline account, quick-play/`--server` join).
- Servers are real servers in offline mode, run locally (not on the lab nodes).

| Client | Server | Hold mode | Result |
|---|---|---|---|
| vanilla 1.21.11 (protocol 774) | – | nothing sent | client closed at **30.0 s** |
| vanilla 1.21.11 | vanilla 1.21.11 | Set Compression −1 every 10 s, **180 s** | first backend byte at 180.1 s; "Offline joined the game"; played until killed 30 s later |
| vanilla 1.21.11 | vanilla 1.21.11 | Custom Query `lcm:hold` every 10 s, 120 s (11 answers swallowed) | joined |
| NeoForge 21.1.264 (1.21.1, protocol 767) + Farmer's Delight 1.3.4 | NeoForge 21.1.264 + Farmer's Delight | Set Compression −1, **150 s** | joined at 150.1 s, played about 100 s until killed |
| Forge 14.23.5.2864 (1.12.2, protocol 340, host `127.0.0.1\0FML\0`) | – | nothing sent | client closed at **30.0 s** |
| Forge 1.12.2 | Forge 1.12.2 | Set Compression −1, 120 s | "Server protocol version 2 … Client side modded connection established" (full FML\|HS handshake with registry injection), joined |
| mineflayer 4.39 / minecraft-protocol 1.68 | vanilla 1.21.11 | Set Compression −1 | **breaks** with zlib "incorrect header check": nmp bug, it treats −1 as "enable" |
| mineflayer 4.39 | vanilla 1.21.11 | Custom Query, 45 s | joined. nmp has no login read timeout, so it can't measure the real client's limit. **Don't use mineflayer as a stand-in for timeout behaviour.** |

Also observed: the 1.12.2 client sends a status ping right before it logs in. The gateway must
answer status requests locally while the backend is down, or the join stalls.

## 3. Findings: existing tools

| Tool (version, date) | Wake on join / idle stop | What the client gets while the server boots | Modded support | Caveats |
|---|---|---|---|---|
| [itzg/mc-router](https://github.com/itzg/mc-router) v1.49.0, 2026-10-20 (Go, MIT) | `-auto-scale-up`/`-auto-scale-down` (default after 10 m). **StatefulSets only**: it scales `spec.replicas` 0→1, and the StatefulSet name must equal `serviceName` ([k8s.go](https://github.com/itzg/mc-router/blob/main/server/k8s.go)). Also supports Docker and a webhook scaler. Only a login wakes the server, not a ping. | Reads Handshake and Login Start, then **holds the TCP connection silently** while TCP-dialling the backend (`autoScaleWaitTimeout`, default 60 s). The README itself says more than 30 s is pointless because the client times out. Optional fake "asleep" and "loading" MOTDs. ([connector.go](https://github.com/itzg/mc-router/blob/main/server/connector.go)) | Any loader works (it strips `\0FML…`, then passes bytes through) | No keepalive, so a cold boot over 30 s means the player has to rejoin. "Ready" means the TCP port accepts, but vanilla and Forge bind the port *before* the world loads. The StatefulSet model conflicts with our migration pods. PROXY protocol both ways. |
| [lazymc](https://github.com/timvisee/lazymc) v0.2.11, 2024-03-16 (Rust, GPL-3) | Wraps one server process; `sleep_after=60`; can stop via SIGTERM or RCON, or `freeze_process` (SIGSTOP). No Kubernetes. | Join methods ([lazymc.toml](https://github.com/timvisee/lazymc/blob/master/res/lazymc.toml)): **hold** (silent, `timeout=25`, [hold.rs](https://github.com/timvisee/lazymc/blob/master/src/join/hold.rs)), **kick** (message), **forward**, **lobby** (fake void world, then teleport) | Lobby is "highly experimental", **offline mode only, MC 1.16.3–1.17.1**, "may work with Forge, not FTB" ([lobby docs](https://github.com/timvisee/lazymc/blob/master/docs/join-method-lobby.md)). Open issues: startup never detected on Forge/Fabric 1.19–1.20.1 (#85, #86, #89, #91) | v0.2.11 needs a 1.20.3+ server ([#65](https://github.com/timvisee/lazymc/issues/65)). Fake status has no `forgeData`. Can send PROXY v2 but not receive it. A `transfer` join method exists only as an unmerged PR ([#110](https://github.com/timvisee/lazymc/issues/110)). |
| [Gate](https://github.com/minekube/gate) v0.74.51, 2026-10-21 (Go, Apache-2) | None | Lite mode: host routing, ping cache, per-route fallback MOTD when the backend is offline ([lite](https://github.com/minekube/gate/blob/master/.web/docs/guide/lite.md)) | Full proxy: relays Forge 1.13–1.20.1 `fml:loginwrapper` ([forge_login_relay.go](https://github.com/minekube/gate/blob/master/pkg/edition/java/proxy/forge_login_relay.go)); 1.20.2+ through the configuration phase; legacy Forge "limited" ([modded-servers](https://github.com/minekube/gate/blob/master/.web/docs/guide/modded-servers.md)). Lite is passthrough, so any loader. | No wake feature. Supports Transfer (`TransferToHost`). |
| [Infrarust](https://github.com/Shadowner/Infrarust) v2.0.0-beta.4, 2026-10-18 (Rust, AGPL-3) | Server manager: local process, Pterodactyl or Crafty; `shutdown_after` ([docs](https://github.com/Shadowner/Infrarust/blob/main/docs/v2/guide/server-management.md)). No Kubernetes or Docker lifecycle. | `server_wake` limbo with a title screen (intercepted modes, `online-mode=false`), or a silent login-screen hold in passthrough mode | Its docs say a limbo before the first server makes Forge 1.13–1.20.1 fail ([modded-servers](https://github.com/Shadowner/Infrarust/blob/main/docs/v2/guide/modded-servers.md)) | Beta |
| [msh](https://github.com/gekware/minecraft-server-hibernation) v2.5.1, 2026-04-16 (Go) | Process wrapper, hibernate after empty | Kick: "Server start command issued. Please wait…" | Any (kick happens before mods matter) | No PROXY protocol |
| [redoxide](https://github.com/samuelloranger/redoxide) | Process start | **Login Plugin Request keepalive every 10 s**, `startup_timeout_secs=120` | Any loader 1.13+ | Prior art for the keepalive-in-login idea. Small project. |
| Velocity [4.3.0 / 3.5.1](https://fill.papermc.io/v3/projects/velocity) (Java) | None built in. Plugins: [AutoServer](https://github.com/Artificial-720/AutoServer) (refuses while booting), [LimboAutoServer](https://github.com/Uxzylon/LimboAutoServer) (LimboAPI hold, 2 stars) | Whatever the plugin does | Legacy Forge 1.7–1.12 handled natively (FML\|HS reset `{-2,0}`, sent even on the first switch away from a vanilla server); Forge 1.13–1.20.1 needs [Ambassador](https://github.com/adde0109/Ambassador); Forge/NeoForge 1.20.2+ re-sync through the configuration phase (`Start Configuration` on switch) ([compat](https://docs.papermc.io/velocity/server-compatibility/)). Backends need [Proxy-Compatible-Forge](https://modrinth.com/mod/proxy-compatible-forge) or [FabricProxy-Lite](https://github.com/OKTW-Network/FabricProxy-Lite) (+ [CrossStitch](https://github.com/VelocityPowered/CrossStitch)). | Terminates the protocol, so it has to own online-mode auth, and backends switch to offline mode with forwarding. Modern forwarding refuses clients older than 1.13. Possible regression: NeoForge 26.2+ initialises client channels once per TCP connection ([PR #3415](https://github.com/neoforged/NeoForge/pull/3415)), so a vanilla→NeoForge switch inside one connection may keep the vanilla channel set (inferred, untested). |
| [Ambassador](https://github.com/adde0109/Ambassador) v1.4.5, 2024-12-18 | – | – | Forge **1.13–1.20.1 only**. Switching works by replaying FML login exchanges if the mods match, via the Client Reset Packet / ServerRedirect client mods, or as a last resort `enable-kick-reset` (the client reconnects). | Vanilla limbo → Forge 1.13–1.20.1 has nothing to replay, so it needs a client mod or a reconnect |
| Waterfall / BungeeCord | – | – | BungeeCord `forge_support` for 1.7–1.12 (sends the FML reset only when leaving a Forge server, so limbo→Forge works) | **Waterfall reached end of life 2024-03-26** ([announcement](https://papermc.io/news/announcing-the-end-of-life-of-waterfall)) |
| [PicoLimbo](https://github.com/Quozul/PicoLimbo) v1.15.0+mc26.3, 2026-10-10 (Rust) | – | Void-world limbo; 1.7.2 → 26.3; about 11 MB for 1,000 idle players; Velocity modern, Bungee and BungeeGuard forwarding; `/transfer` command (1.20.5+) | FAQ: "does not support modded clients". NeoForge 1.21.1 with content mods can't join ([#30](https://github.com/Quozul/PicoLimbo/issues/30)) without the client-side [Be Quiet Negotiator](https://modrinth.com/project/be-quiet-negotiator) mod | Best limbo for vanilla, Paper and Fabric |
| [NanoLimbo](https://github.com/Nan1t/NanoLimbo) v1.13.0, 2026-07-17 ([BoomEaro fork](https://github.com/BoomEaro/NanoLimbo) 1.14.0 adds 26.3) (Java) | – | Limbo, 1.7 → 26.x, forwarding modes | No Forge or NeoForge code | No outbound Transfer |
| [LOOHP/Limbo](https://github.com/LOOHP/Limbo) (Java) | – | One MC version at a time (26.3) | Status always claims `modinfo` FML, but it does no negotiation | – |
| [LimboAPI](https://github.com/Elytrium/LimboAPI) 1.1.26 / dev 2026-09 (Velocity plugin) | – | Virtual worlds inside Velocity | Open Forge 1.12.2 and 1.20.1 bugs ([#151](https://github.com/Elytrium/LimboAPI/issues/151), [#197](https://github.com/Elytrium/LimboAPI/issues/197)), no NeoForge handling | – |
| [Agones](https://github.com/googleforgames/agones) v1.61.0 | Fleet/allocation; no join trigger | – | – | Too heavy for what we need |

## 4. Why a lobby does not work for modded clients (and the Transfer packet doesn't fix it)

1. **Whether a modded client can enter a vanilla limbo at all depends on the loader:**
   - **NeoForge 1.20.2+:** the client checks the server during configuration and **disconnects
     itself** if any mod registers a non-optional payload, uses extended enums in serverbound
     networking, or adds custom feature flags ("…not running NeoForge, but you have mods that
     require it", [ClientNetworkRegistry](https://github.com/neoforged/NeoForge/blob/1.21.x/src/client/java/net/neoforged/neoforge/client/network/registration/ClientNetworkRegistry.java)).
     Most content packs hit at least one of these. NeoForge clients send no handshake marker, so a
     limbo can't even tell them apart.
   - **Forge 1.13–1.20.1:** allowed onto vanilla servers if every channel accepts `ALLOWVANILLA`.
   - **Forge 1.7–1.12:** a missing ServerHello aborts to a vanilla login, so it gets in.
   - **Fabric:** generally gets in.
2. **Getting from the limbo onto the modded server:**
   - **Proxy server switch (Velocity, Gate):**
     - Forge 1.13–1.20.1 negotiates mods in the *login* phase, which has already happened against
       the limbo. With nothing to replay, the client needs a reset mod or a reconnect (Ambassador,
       Gate).
     - Legacy Forge 1.7–1.12 works through the FML\|HS reset.
     - 1.20.2+ re-runs configuration. With NeoForge 26.2+ that may no longer re-negotiate channels
       (above).
   - **Transfer packet (1.20.5+, protocol 766):** clientbound in configuration and play. The
     client closes the connection and opens a **new** one with handshake intent 3, so all mod and
     registry negotiation runs fresh. That part is good. The target needs `accepts-transfers=true`,
     otherwise it rejects with "Server does not accept transfers"
     ([protocol](https://minecraft.wiki/w/Java_Edition_protocol/Packets), [server.properties](https://minecraft.wiki/w/Server.properties)).
     Known bug: a NeoForge transfer between IPv4 and IPv6 hosts fails on Linux
     ([#2467](https://github.com/neoforged/NeoForge/issues/2467)). But Transfer only helps once
     the client is *inside* a limbo, and step 1 is what blocks NeoForge. It also doesn't exist
     before 1.20.5, so there is nothing for 1.20.1 Forge or 1.12.2 packs.
3. **A proxy that holds a client in configuration (1.20.2+)** would also work: Keep Alive or Ping
   every 30 s or less, and no brand or feature packets for NeoForge. But it needs a
   protocol-terminating proxy, which means the proxy owns online-mode authentication and the
   backends need forwarding mods. That gains nothing over holding in login, which needs neither.
4. **The only lobby that every modded client accepts is a server running the same modpack.** For
   example, one always-on, small-view-distance instance per pack with a void world and
   `/transfer`, for 1.20.5+ only. That costs a full modded JVM (2–4 GiB) per pack, all the time.
   Not recommended unless several servers share a pack.

So for modded servers, the "lobby" becomes: the server list shows "Starting… (≈2 min)", and the join
screen shows "Logging in…" until the world appears. That works for all loaders from 1.8 on, without
any server or client mods.

## 5. Recommended architecture

```
                    ┌──────────────────────── lcm-gateway ─────────────────────────┐
player ──TCP──▶ LB ─┤ sniff Handshake (+ Login Start)                              │
                    │   status, backend down → answer locally ("Starting…" MOTD)   │
                    │   status, backend up   → stream (as today)                   │
                    │   login → stream queued (as today) + holdConn keepalives     │
                    │           + wake(backend) if no pod                          │
                    │ players(backend) = open login streams  → idle timer          │
                    └───────────────┬──────────────────────────────┬───────────────┘
                                    │ resumable tunnel (unchanged) │ k8s API (pods, Lease)
                                    ▼                              ▼
                     lcm-relay: dials the gateway only once    lifecycle loop: create pod from
                     the server answers a loopback status      template / delete when idle;
                     ping (readiness gate), then as today      skips while the migration Lease is held
```

### 5.1 Gateway: Minecraft mode (`-protocol minecraft`; the default stays `tcp`)

- **Sniff:** read at most about 2 KiB (handshake host up to 255 chars plus FML markers) with a 5 s
  deadline. Then:
  - First byte `0xFE` (legacy ping, ≤1.6 or old launchers): pass through, or answer with the
    legacy kick-style status.
  - Otherwise parse the VarInt-framed Handshake: protocol, host (strip `\0FML\0`, `\0FML2\0`,
    `\0FML3\0`, `\0FORGE…` before any host routing), port, and intent (1 status, 2 login,
    3 transfer).
  - For login and transfer, also read Login Start (only the player name matters, for logs and
    allow-lists).
  - Anything unparsable goes on as plain TCP. The gateway must never break a non-MC stream.
- **Replay:** the sniffed bytes must reach the backend unchanged. Wrap the `net.Conn` so `Read`
  returns the buffered prefix first, and hand it to `Session.AddStream` exactly as today. Nothing
  in the tunnel protocol changes.
- **Status while down:**
  - Answer status locally only when no relay session is connected; otherwise stream it as today,
    so the real MOTD, player count and `forgeData` show.
  - Cache the last real status JSON the backend returned. The gateway can parse the status
    response: it is plaintext and the first packet the server sends on a status stream.
  - While down, serve that cached JSON with `description` replaced ("Sleeping, join to start" or
    "Starting… 1m40s") and `players.online=0`, and answer Ping with Pong.
  - With no cache yet, use the configured version name and protocol.
  - Modded clients only use `forgeData`/`modinfo`/`isModded` for a tooltip, never to refuse the
    join, but serving the cached copy keeps the icon right.
- **Hold (`holdConn`):**
  - Wrap the player conn with a mutex and a `started` flag. The first `Write` from the stream's
    `writePump` (the first backend byte) sets `started` under the mutex.
  - A per-stream timer writes the 7-byte Set Compression −1 packet every 10 s, under the same
    mutex, while `!started`. Because injected packets are whole and only ever precede the first
    backend byte, the client sees a valid packet sequence.
  - About 30 lines, no change to `internal/tunnel`. Only for intent 2/3 with protocol ≥ 47 (1.8).
    For 1.7.x (protocol 4/5), no keepalive, and the hold limit is 25 s.
  - It also helps outside on-demand: a player who logs in during a migration blackout is now held
    past 30 s. Players already in play state are unaffected: their streams are encrypted and
    `started` is already true.
- **Hold limit:**
  - Configurable, default 10 min (the pod may be Pending for lack of memory, or the pack may be
    slow).
  - When the limit passes and `!started`, write a Login Disconnect (packet 0x00, JSON chat:
    "Server failed to start in time, try again later"), close, and drop the stream from the session.
  - The same path gives 1.7.x clients "Server is starting, rejoin in a minute" after 25 s.
- **Keepalive mode switch:** `-hold-keepalive setcompression|query|none`, default setcompression.
  Use `query` (Custom Query on channel `lcm:hold`, answers swallowed, splice only with no query
  outstanding) for test bots built on node-minecraft-protocol and for any client found to mishandle
  −1.

### 5.2 Relay: readiness gate

- On a **cold start**, the relay does not dial the gateway until the child answers a **status
  ping on loopback**.
- Vanilla, Paper, Forge and NeoForge bind the port before the world finishes loading, and status
  packets are processed by the main tick loop. So a status response means the server is joinable,
  and a TCP connect alone does not.
- If Paper runs with `proxies.proxy-protocol: true`, the probe sends a PROXY v2 `LOCAL` header
  first.
- Poll every 1 s. `status` on the control socket reports `"ready": false/true`.
- Streams queued in the gateway are opened when the session becomes NEW. The protocol already does
  this for streams that were never opened (tunnel-protocol.md, "Reconciliation").
- After a CRIU restore the relay is already past the gate, so migration is unaffected.

### 5.3 Lifecycle: wake and idle stop

- **Object model:**
  - A backend is `name` + pod template + settings.
  - Pods are bare pods labelled `lcm.rooty.dev/backend=<name>`, created with `generateName`,
    because migration renames pods.
  - "Running" means a non-terminating pod with that label exists. Phase 3a keeps the template as a
    file or ConfigMap mounted into the gateway; phase 3b makes it a small CRD reconciled by
    `lcm-controller`.
- **Mutual exclusion:**
  - Migration, stop and start all take the `coordination.k8s.io` Lease `lcm-backend-<name>`.
  - The migrate CLI/controller holds it from pre-dump to source deletion.
  - The idle stopper and waker skip, or wait, while someone else holds it.
  - Migration refuses to start on a pod that is terminating.
- **Wake** (on login, no relay session, no pod):
  - Take the Lease and create the pod. The world on CephFS (or RBD) attaches as usual.
  - If an old pod is still **terminating**, wait for it to be gone first. Two JVMs must never open
    one world: Minecraft's `session.lock` is advisory, and CephFS RWX doesn't stop it.
  - Players keep being held throughout.
- **Idle stop:**
  - `players(backend)` = open login streams in the gateway. Status streams don't count, and a
    detached tunnel doesn't zero the count: during a migration blackout the streams still exist.
  - Stop when the count has been 0 for `idle-after` (default 10 min, or 5 min for Paper), the pod
    has been Ready for at least `min-uptime` (5 min), and the Lease is free.
  - Delete the pod with `terminationGracePeriodSeconds` ≥ 120. kubelet sends SIGTERM to
    `lcm-relay` (PID 1), which forwards it to the child. The itzg image's
    [mc-server-runner](https://github.com/itzg/mc-server-runner) answers by sending `stop` and
    waiting `-stop-duration`; other servers' shutdown hooks also save. RCON isn't needed.
  - The relay sends a FINAL GOAWAY when the child exits.
  - A login that arrives during shutdown is held, and the wake path creates the next pod once the
    old one is gone.
- **Drains and node maintenance:** an empty server can simply be stopped instead of migrated,
  because it wakes on the next join.

### 5.4 Fallbacks

- **B. Kick with a message instead of holding** (`-hold-keepalive none`, `-hold-limit 25s`). It
  works for every client and version. With an auto-reconnect mod such as
  [Auto Reconnect Reforged](https://modrinth.com/mod/autoreconnectrf) (1.21 → 26.3) it is almost
  invisible. Use it if some client turns out to mishandle the injected packets, and always for 1.7.x.
- **C. Off-the-shelf in front of the gateway:** mc-router with `-auto-scale-webhook-url` pointing
  at our waker. It's cheaper to build, but it inherits the silent 30 s hold, its readiness check
  only tests that the TCP port accepts, and it adds a hop. It's only worth it if we decide the
  gateway must stay protocol-agnostic.
- **D. Real lobby for non-modded servers** (Paper, vanilla, Fabric without required channels, all
  1.20.5+): PicoLimbo behind the gateway as a second backend. While the real backend boots, the
  gateway routes the login to the limbo instead of holding it. When the backend is ready, the limbo
  sends `/transfer` to the public address, and the client reconnects to the gateway, which now
  streams to the real backend (`accepts-transfers=true` there). This keeps the gateway L4.
  Optional polish, not needed for modded.

### 5.5 Optional later: CRIU hibernate instead of stop

Instead of deleting an idle server, checkpoint it with the existing agent path to a dump that is
kept rather than streamed, then delete the pod. Wake recreates the pod with the restore annotation,
so `lcm-runc` restores on create.

- **Gain:** a modded wake goes from minutes to roughly the restore time. Expect on the order of
  10 s for a multi-GiB heap, to be measured.
- **Hibernating is easier than migrating:** an idle server has no player TCP state, and the relay is
  quiesced anyway.
- **Costs:** disk for images (heap-sized, kept per backend). A restore may only land on a
  CPU-compatible node.
- **When to fall back to a cold boot:**
  - The world files changed while it slept (CRIU checks file sizes, so the restore fails).
  - The server's jar or mods changed (invalidate the image on any template change).
  - The image is older than N days.
- Mods that hold external connections (Discord bridges, databases) break here just as they do in
  migration.
- Wall-clock time jumps, but monotonic time doesn't (time namespace), so the watchdog and tick
  catch-up stay quiet.

## 6. Phased implementation plan

### Implementation status (2026-10-22)

The gateway hold (phase 1), the relay readiness gate (phase 2), wake/idle stop (phase 3, built on
LivePods rather than in the gateway; see "Wake and idle stop" below) and hostname routing (phase 4)
are implemented. Code: [internal/mcproto](../internal/mcproto) (VarInt, framing, handshake,
status, Login Disconnect, legacy ping), [internal/gateway/minecraft.go](../internal/gateway/minecraft.go),
`waitReady` in [internal/relay/relay.go](../internal/relay/relay.go).

`lcm-gateway -minecraft` (env `LCM_MINECRAFT=true`; off by default, and off means exactly the old
pure-L4 path):

| Flag | Default | Meaning |
|---|---|---|
| `-minecraft` | off | sniff the first packets of each player connection |
| `-motd` | `Server is starting, please wait...` | status description while no relay is connected (env `LCM_MOTD`) |
| `-status-version` | `starting` | `version.name` of that reply; `version.protocol` echoes the client's handshake |
| `-hold-interval` | 10s | Set Compression −1 (`06 03 ff ff ff ff 0f`) period for held logins |
| `-hold-max` | 5m | then Login Disconnect with `-hold-message`; 1.7.x (protocol < 47) gets no keepalive and at most 25 s |

Behaviour, and where it differs from §5.1:

- Handshake (and Login Start for intents 2/3) are read with a 5 s deadline and replayed byte-exact
  through a wrapper around the player socket; nothing in `internal/tunnel` changed except that
  `closeLocal` accepts any `TCPLike` (`CloseWrite` + `SetLinger`), so the wrapper keeps the
  graceful/abortive close behaviour.
- **Status:** answered locally (status + pong) only while the backend session is not connected,
  otherwise streamed to the real server. No cached status JSON yet, so during a migration blackout
  the server list briefly shows the configured MOTD.
- **Login:** every login/transfer stream goes through `holdConn`. The first server write flips it to
  "started" under the write mutex; injections happen only under that mutex while not started, so
  they never interleave with server bytes and never follow one (or the server's FIN). Established
  streams in play state are therefore never touched by an outage. A login that arrives during a
  migration blackout is held like one that arrives during boot.
- **Hold limit:** Login Disconnect, graceful close (no RST that could drop it), and the stream is
  reset (removed if the relay never saw it). A failed keepalive write drops the stream too.
- **Legacy ping** (0xFE, vanilla's detection rule): answered with a `§1` kick-style reply while down,
  streamed while up. **Anything that is not a valid handshake is closed** (not passed through as
  §5.1 suggested).
- `-hold-keepalive query|none` is not implemented (setcompression only). Held logins show up as
  `held` in the gateway's `/status` and in `lcm_gateway_held_players` /
  `lcm_gateway_player_connections_total{result="hold_timeout"}` on `/metrics`.

`lcm-relay` readiness (phase 2): before the first tunnel connection of a fresh process it polls
`-upstream` every `-ready-interval` (1 s): a TCP connect by default (`-wait-ready`, on), or with
`-minecraft-ready` (env `LCM_MINECRAFT_READY=true`) a full status ping (PROXY v2 `LOCAL` header first
when `-proxy-protocol` is on). The check runs inside the connect attempt, so `quiesce` cancels it
cleanly. The passed state lives in memory, so a CRIU-restored relay never re-waits. `status` on the
control socket reports `"ready"` and the state `waiting-for-server`.

Tests: `internal/mcproto` (VarInt vectors, framing, handshake capture, legacy reply),
`internal/gateway/minecraft_test.go` (in-process gateway + relay + fake server from
`internal/mcproto/mctest`: keepalives at the configured interval while the server is not ready, then
byte-exact replay and server bytes with no injection afterwards; local status then passthrough;
quiesce mid-stream gets no injection while a new login during the outage is held; hold limit
Disconnect; 1.7.x; garbage and legacy ping; `-minecraft` off stays L4),
`internal/relay/ready_test.go` (gate, no re-probe after passing, quiesce while waiting). Not yet
done: the e2e run with a real client (HeadlessMC) against the gateway binary.

#### Wake and idle stop (phase 3, as built)

An on-demand LivePod (`spec.onDemand: {backend: atm10, idleAfter: 10m}`) replaces AutoServer:

- **Idle:** the LivePod controller (leader in lcm-agent) asks the pod's lcm-relay, through its node's
  agent (`GET /v1/relay?container=`), how many player connections it carries over *all* gateways
  (one per ingress). After `idleAfter` with none (and no migration holding the LivePod) it sets
  `status.asleep` and deletes the pod; the server gets SIGTERM and its
  `terminationGracePeriodSeconds` to save. An unanswered query counts as busy.
- **Wake:** a gateway started with `-wake-url http://<agent>:7790[,…] -wake-token-file F` that gets a
  login (or, in L4 mode, any connection) for a backend whose relay isn't connected calls
  `POST /v1/wake/<backend>` (at most every 15 s per backend), holds the player, and the controller
  creates the pod once any old one is fully gone. Server-list pings don't wake (like AutoServer) and
  get the `-motd` (`{backend} is starting…`). Gateways outside the cluster use the agents' wake-only
  token (`services.lcm.wakeTokenFile`), which can't call anything else. Only agents that run the
  LivePod controller (`services.lcm.livePods.enable`) can wake; the others answer 503, and the
  gateway tries the next URL. List at least the controller agents. A failed wake doesn't count
  toward the 15 s limit.
- Status: `kubectl get livepod` shows phase `Asleep` / `Sleeping` / `Waking`; metrics
  `lcm_livepod_running{phase}` and `lcm_gateway_wakes_total{backend,result}`;
  `LcmLivePodNotRunning` ignores asleep LivePods.

Lab (Fabric 1.21.10, idleAfter 1m): asleep 1 min after the last player left, world saved
("All dimensions are saved"); a login while asleep was held (one keepalive) and got Login Success
after 18.4 s of cold boot; 3 bots kept it awake for 75 s, including a migration (1.18 s blackout,
0 disconnects).

#### Hostname routing (phase 4, as built)

`lcm-gateway -minecraft -backend lobby -backends atm9,atm10,…` serves many servers from one
listener: `<backend>.<any domain>` goes to that backend (the forced-hosts scheme), `-route
host=backend` adds exact names, anything else goes to `-backend`. Forge markers, case and a
trailing dot are ignored. Each server's relay presents its name (`lcm-relay -backend atm10`).

#### Server menu (replaces the lobby, as built)

`lcm-gateway -minecraft -menu -menu-title "rooty.dev" -label "atm9=All the Mods 9,…"` turns the
main address into a server picker. Players who connect with a hostname that names no backend
(`mc.example.com`, or a bare IP) get the menu instead of the default backend; `<backend>.<domain>`
and `-route` names still go straight to their server, as before. No lobby server runs.

1. The gateway logs the player in itself: offline mode, no encryption. Nothing in the menu needs
   to know who the player is, and each server still does its own online-mode login after the
   transfer.
2. It keeps the client in the configuration phase and shows a dialog: one button per backend with
   its state (`3 playing`, `asleep`, `starting`, `offline`) and a tooltip with the version (from a
   status ping through the tunnel, cached 5 s). It sends a Keep Alive every `-hold-interval`.
3. When the player picks an asleep server, the gateway calls the waker (the same `-wake-url` path
   a direct login uses) and shows "Starting …" with a counter. It gives up after `-hold-max`.
4. Once the backend's relay is connected (with `-minecraft-ready`, that means the server answers
   pings), the gateway stores a cookie (`lcm:route`, naming the backend, valid once for 30 s) and
   sends Transfer to the address the player used.
5. The client reconnects with handshake intent 3. The gateway asks for the cookie and sends the
   connection to that backend, rewriting the intent to 2 (login). The backend therefore needs no
   `accepts-transfers=true`. It sees an ordinary login, including Forge markers.

Limits:
- **Clients:** 1.21.6+ (dialogs; protocols 771–777, i.e. up to 26.3). Older clients that connect
  to the menu address get a disconnect listing each server's direct address. A newer protocol gets
  the same list, and the gateway logs a warning to update lcm, because packet IDs move between
  releases (26.3 shifted Store Cookie, Transfer and Show Dialog by one).
- **Vanilla clients only:** a modded client can open the menu, but the server it picks still
  decides whether it may join (see §4). Modded players should keep using the pack's hostname.
- **No in-game switching:** going back to the menu means reconnecting to the main address. Because
  cookies are used once, an old choice is not replayed.
- **Server list:** the menu address shows `<title>`, "N of M servers up, P playing" and the
  client's own version (1.21.6+). Older clients see it as incompatible.

How it was checked:
- Packet IDs come from each release's data generator report (`packets.json`, 1.21.6 to 26.3).
- Packet layouts were read from the decompiled 1.21.6, 1.21.10, 26.1.2 and 26.3 code, client
  included. In particular, the configuration-phase Show Dialog carries the dialog inline as NBT,
  and the client sends dialog clicks in the configuration phase.
- The dialogs the gateway sends were loaded as a data pack into vanilla 1.21.6, 1.21.8, 1.21.10,
  1.21.11, 26.1.2, 26.2 and 26.3 servers, which reject any dialog that doesn't parse. A broken
  control dialog was rejected.
- Unit tests cover the bytes, and e2e tests cover menu, pick, wake, transfer and cookie arrival
  with the fake server.
- Lab, with a real vanilla 1.21.10 client (portablemc, Xvfb, Mesa llvmpipe, clicks with
  xdotool), on gateway2:
  - The menu rendered, showing the asleep Fabric LivePod.
  - The click woke it, and the notice counted the wait.
  - The gateway sent the Transfer 19.5 s after the click.
  - The client came back with the cookie, and "LabBot joined the game" on the Fabric server.
  - A wake that failed (agent without the controller) showed "Couldn't start …" with a way back to
    the list.

| Phase | Work | Reuses | Done when |
|---|---|---|---|
| 0 (done) | Prove the login hold with real clients | – | §2 |
| 1 (done, see above) | Gateway `-minecraft`: sniff and replay wrapper, local status while down, `holdConn` keepalive, hold limit with Login Disconnect, 1.7.x path, metrics (`held`, `players`). Unit tests with a fake client: packet bytes, no injection after the first backend byte, the replay is exact. | `ServePlayers`, `AddStream`, the session queues unopened streams | A cold `lcm-relay` that starts 3 min late still gets the queued player in; e2e covers this with an nmp bot in `query` mode, plus a manual HeadlessMC run in `setcompression` mode |
| 2 (done, see above) | Relay readiness gate (loopback status probe, with PROXY v2 LOCAL when configured), `ready` in `status` | relay dial loop | No RESET for players who arrive during boot |
| 3a | In-gateway lifecycle for its single backend: pod template file, wake on login, idle stop, Lease, wait for termination before wake. RBAC: pods create/list/watch/delete, leases. Lab first in namespace `ondemand`. | client-go (already in go.mod) | Join a stopped Paper server, then a modded server (1–5 min); stops after idle; `lcm migrate` during play is not interrupted by the idle stopper, and the stopper waits for the Lease |
| 3b | Move lifecycle into `lcm-controller` with a small `MinecraftServer` CRD (template, hostnames, idle settings); the gateway calls `wake` and reports player counts | phase 3a code | Several backends |
| 4 | Host-based routing to several backends from one gateway (handshake host → backend; the session map already exists) | `backends` map | One LB IP for all servers |
| 5 | CRIU hibernate (§5.5), with fallback to a cold boot | agent pre-dump/dump, `lcm-runc` restore-on-create | Modded wake ≤ 20 s |
| 4b (done, see "Server menu") | Lobby replacement: a dialog menu in the gateway for 1.21.6+ clients. It wakes the picked server, then transfers the player there with a one-time cookie. | phase 4 routing, waker | Pick an asleep server from the menu and join it |

## 7. Risks and open points

- **Unusual clients:** the Set Compression −1 trick is verified on vanilla 1.21.11 and on the 26.3
  and 1.12.2 bytecode, NeoForge 1.21.1 and Forge 1.12.2. Still to try: Fabric 1.21.x, Forge
  1.20.1, and a 1.7.10 Forge client for the kick path. Third-party clients and launchers (Lunar,
  Feather, Bedrock via Geyser) are untested. The `query` mode is the per-backend escape hatch.
- **UX:** the player sees "Logging in…" for minutes with no progress text, and may cancel. Nothing
  can display text in the login phase except a disconnect. The server-list MOTD carries the ETA.
  Cancelling is harmless, because the server keeps booting.
- **Online mode:** unaffected. The client contacts Mojang's session server only after the backend's
  Encryption Request, which comes after the hold.
- **Readiness on huge packs:** some packs accept status pings before the spawn area is ready. The
  backend then handles login slowly, but its own timers start only on accept, so that's fine.
- **Insufficient node memory:** the wake pod stays Pending and the hold limit kicks the player.
  Expose this in the MOTD ("waiting for capacity").
- **Two writers on one world:** prevented only by "wait until the old pod is gone" and the Lease.
  Make that a hard invariant in code and tests.
