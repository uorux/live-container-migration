#!/usr/bin/env node
// mcbot: N offline-mode mineflayer bots that keep "playing" on a Minecraft server and log, as JSON
// lines on stdout, what a connected player would experience: receive gaps (freezes), ping RTT,
// keepalives, chat echo ordering/loss, server position corrections, disconnects and reconnects.
// A final {"event":"summary",...} line says pass/fail; exit code 1 on any disconnect, 2 on any
// other failure (failed initial connect, chat lost/out of order, gap over --fail-gap-ms).
'use strict'

const { parseArgs } = require('node:util')
const { performance } = require('node:perf_hooks')
const mineflayer = require('mineflayer')

const { values: opt } = parseArgs({
  options: {
    host: { type: 'string', default: '127.0.0.1' },
    port: { type: 'string', default: '25565' },
    count: { type: 'string', default: '1' },
    prefix: { type: 'string', default: 'bot' },
    duration: { type: 'string', default: '60' }, // seconds, counted from all_connected
    version: { type: 'string', default: '1.21.10' },
    'gap-ms': { type: 'string', default: '500' }, // log receive gaps longer than this
    'probe-ms': { type: 'string', default: '200' }, // ping_request interval, 0 = off
    'chat-ms': { type: 'string', default: '10000' },
    'stats-ms': { type: 'string', default: '10000' },
    'stagger-ms': { type: 'string', default: '4200' }, // > bukkit.yml connection-throttle (4000)
    'read-timeout-ms': { type: 'string', default: '30000' }, // vanilla client ReadTimeoutHandler(30)
    'fail-gap-ms': { type: 'string', default: '0' }, // 0 = gaps never fail the run
    'no-move': { type: 'boolean', default: false },
    'no-reconnect': { type: 'boolean', default: false },
    help: { type: 'boolean', short: 'h', default: false }
  }
})

if (opt.help) {
  console.error(`usage: mcbot.js --host H --port P --count N --prefix NAME --duration SECONDS
  [--version 1.21.10] [--gap-ms 500] [--probe-ms 200] [--chat-ms 10000] [--stats-ms 10000]
  [--stagger-ms 4200] [--read-timeout-ms 30000] [--fail-gap-ms 0] [--no-move] [--no-reconnect]`)
  process.exit(0)
}

const num = (k) => {
  const v = Number(opt[k])
  if (!Number.isFinite(v) || v < 0) { console.error(`bad --${k}: ${opt[k]}`); process.exit(64) }
  return v
}
const HOST = opt.host
const PORT = num('port')
const COUNT = num('count')
const DURATION_MS = num('duration') * 1000
const GAP_MS = num('gap-ms')
const PROBE_MS = num('probe-ms')
const CHAT_MS = num('chat-ms')
const STATS_MS = num('stats-ms')
const STAGGER_MS = num('stagger-ms')
const READ_TIMEOUT_MS = num('read-timeout-ms')
const FAIL_GAP_MS = num('fail-gap-ms')
const MOVE = !opt['no-move']
const RECONNECT = !opt['no-reconnect']
const CHAT_GRACE_MS = 3000
const KEEPALIVE_LATE_MS = 2000

// Monotonic ms clock anchored to wall time, so durations are immune to clock steps.
const T0_WALL = Date.now()
const T0_MONO = performance.now()
const now = () => performance.now() - T0_MONO
const iso = (mono) => new Date(T0_WALL + mono).toISOString()

function emit (obj) {
  const t = now()
  process.stdout.write(JSON.stringify({ t: iso(t), ms: Math.round(t), ...obj }) + '\n')
}

// ---- event loop stall tracking: a stalled client also looks like a receive gap ----
const stalls = [] // {start, end}
{
  const TICK = 50
  let last = now()
  setInterval(() => {
    const t = now()
    const lag = t - last - TICK
    if (lag > 100) {
      stalls.push({ start: last + TICK, end: t })
      if (stalls.length > 1000) stalls.shift()
      emit({ event: 'loop_stall', stall_ms: Math.round(lag) })
    }
    last = t
  }, TICK).unref()
}
function stallOverlap (start, end) {
  let s = 0
  for (const st of stalls) s += Math.max(0, Math.min(end, st.end) - Math.max(start, st.start))
  return Math.round(s)
}

// ---- per-bot state, persists across reconnects ----
function newState (name) {
  return {
    name,
    bot: null,
    spawned: false,
    everSpawned: false,
    initialConnectFailed: false,
    connectAttempts: 0,
    disconnects: 0,
    reconnects: 0,
    disconnectedAt: null,
    kickReason: null,
    lastError: null,
    // receive gaps
    lastRx: null,
    gaps: 0,
    maxGapMs: 0,
    // probes
    probeSeq: 0,
    probeSent: new Map(),
    rttWindow: [],
    rttMax: 0,
    rttCount: 0,
    rttSum: 0,
    // keepalive
    lastKeepAlive: null,
    keepAlives: 0,
    maxKeepAliveIntervalMs: 0,
    // chat
    chatSeq: 0,
    chatPending: new Map(), // seq -> mono send time
    chatSent: 0,
    chatEchoed: 0,
    chatMaxEchoed: 0,
    chatOutOfOrder: 0,
    chatDuplicate: 0,
    chatMaxLatencyMs: 0,
    echoedSet: new Set(),
    othersLast: new Map(), // sender -> last seq seen
    othersSeen: 0,
    othersMissing: 0,
    othersOutOfOrder: 0,
    // position
    forcedMoves: 0,
    positionJumps: 0,
    lastPos: null,
    lastPosT: null,
    home: null,
    timers: []
  }
}

const bots = []
let shuttingDown = false
let chatStopped = false
let allConnectedAt = null

const CHAT_RE = /mcbot (\S+) seq (\d+)/

function connect (st, isReconnect) {
  st.connectAttempts++
  const tStart = now()
  const marks = {}
  emit({ bot: st.name, event: isReconnect ? 'reconnect_attempt' : 'connect_attempt', attempt: st.connectAttempts })

  const bot = mineflayer.createBot({
    host: HOST,
    port: PORT,
    username: st.name,
    auth: 'offline',
    version: opt.version,
    keepAlive: false, // we answer keepalives ourselves to observe them and emulate the vanilla timeout
    hideErrors: true,
    viewDistance: 'tiny',
    logErrors: false
  })
  st.bot = bot
  st.spawned = false
  st.kickReason = null
  st.lastError = null
  st.lastKeepAlive = null
  st.probeSent.clear()
  const client = bot._client

  client.on('connect', () => {
    marks.tcp_ms = Math.round(now() - tStart)
    client.socket.on('data', () => onRx(st))
  })
  client.on('login', () => { marks.login_ms = Math.round(now() - tStart) })
  client.on('state', (s) => { if (s === 'configuration') marks.config_ms = Math.round(now() - tStart) })

  client.on('keep_alive', (p) => {
    client.write('keep_alive', { keepAliveId: p.keepAliveId })
    const t = now()
    const interval = st.lastKeepAlive == null ? null : Math.round(t - st.lastKeepAlive)
    st.lastKeepAlive = t
    st.keepAlives++
    if (interval != null && interval > st.maxKeepAliveIntervalMs) st.maxKeepAliveIntervalMs = interval
    // Paper 1.21.x sends a keepalive about every second; only log unusually late ones. The RTT the
    // server measures from them is reported back in the tab list (stats.server_ping_ms).
    if (interval != null && interval > KEEPALIVE_LATE_MS) emit({ bot: st.name, event: 'keepalive_late', interval_ms: interval })
  })
  // server-initiated ping (not normally sent by Paper); answer like vanilla
  client.on('ping', (p) => { try { client.write('pong', { id: p.id }) } catch {} })
  client.on('ping_response', (p) => onPong(st, p))

  bot.once('spawn', () => {
    st.spawned = true
    const t = now()
    st.lastRx = t
    st.lastPos = bot.entity.position.clone()
    st.lastPosT = t
    if (!st.home) st.home = bot.entity.position.clone()
    const ev = {
      bot: st.name,
      event: isReconnect ? 'reconnected' : 'connected',
      connect_ms: Math.round(t - tStart),
      ...marks,
      pos: fmtPos(bot.entity.position)
    }
    if (isReconnect && st.disconnectedAt != null) ev.downtime_ms = Math.round(t - st.disconnectedAt)
    emit(ev)
    if (isReconnect) st.reconnects++
    st.everSpawned = true
    // a reconnect resets the baseline for other bots' sequence numbers
    st.othersLast.clear()
    startPlaying(st)
    maybeAllConnected()
  })

  bot.on('forcedMove', () => {
    if (!st.spawned) return
    const p = bot.entity.position
    const from = st.lastPos
    st.forcedMoves++
    emit({
      bot: st.name,
      event: 'server_teleport',
      to: fmtPos(p),
      dist: from ? round2(from.distanceTo(p)) : null
    })
    st.lastPos = p.clone()
    st.lastPosT = now()
  })

  bot.on('messagestr', (str) => onChat(st, str))

  bot.on('kicked', (reason, loggedIn) => {
    st.kickReason = kickText(bot, reason)
    emit({ bot: st.name, event: 'kicked', reason: st.kickReason, logged_in: loggedIn })
  })
  bot.on('error', (err) => {
    st.lastError = String(err && err.message ? err.message : err)
    emit({ bot: st.name, event: 'error', error: st.lastError })
  })
  bot.once('end', (reason) => onEnd(st, bot, reason))
}

function onRx (st) {
  const t = now()
  if (st.spawned && st.lastRx != null) {
    const gap = t - st.lastRx
    if (gap > GAP_MS) {
      st.gaps++
      if (gap > st.maxGapMs) st.maxGapMs = gap
      emit({
        bot: st.name,
        event: 'gap',
        start: iso(st.lastRx),
        gap_ms: Math.round(gap),
        client_stall_ms: stallOverlap(st.lastRx, t)
      })
    }
  }
  st.lastRx = t
}

function onPong (st, p) {
  const v = p.id
  const id = typeof v === 'bigint' ? Number(v) : (Array.isArray(v) || v?.length === 2) ? (v[0] * 2 ** 32 + (v[1] >>> 0)) : Number(v)
  const sent = st.probeSent.get(id)
  if (sent == null) return
  st.probeSent.delete(id)
  const rtt = now() - sent
  st.rttWindow.push(rtt)
  st.rttCount++
  st.rttSum += rtt
  if (rtt > st.rttMax) st.rttMax = rtt
}

function onChat (st, str) {
  const m = CHAT_RE.exec(str)
  if (!m) return
  const sender = m[1]
  const seq = Number(m[2])
  if (sender === st.name) {
    if (st.echoedSet.has(seq)) {
      st.chatDuplicate++
      emit({ bot: st.name, event: 'chat_duplicate', seq })
      return
    }
    st.echoedSet.add(seq)
    const sent = st.chatPending.get(seq)
    st.chatPending.delete(seq)
    st.chatEchoed++
    const inOrder = seq > st.chatMaxEchoed
    if (!inOrder) st.chatOutOfOrder++
    st.chatMaxEchoed = Math.max(st.chatMaxEchoed, seq)
    const latency = sent == null ? null : Math.round(now() - sent)
    if (latency != null && latency > st.chatMaxLatencyMs) st.chatMaxLatencyMs = latency
    emit({ bot: st.name, event: 'chat_echo', seq, latency_ms: latency, in_order: inOrder })
  } else if (sender.startsWith(opt.prefix)) {
    st.othersSeen++
    const last = st.othersLast.get(sender)
    if (last != null) {
      if (seq > last + 1) {
        st.othersMissing += seq - last - 1
        emit({ bot: st.name, event: 'chat_others_gap', sender, expected: last + 1, got: seq })
      } else if (seq <= last) {
        st.othersOutOfOrder++
        emit({ bot: st.name, event: 'chat_others_out_of_order', sender, last, got: seq })
      }
    }
    if (last == null || seq > last) st.othersLast.set(sender, seq)
  }
}

function startPlaying (st) {
  const bot = st.bot
  const every = (ms, fn) => { if (ms > 0) st.timers.push(setInterval(fn, ms)) }

  // RTT probes; also guarantee steady server->client traffic so gaps mean a real stall
  every(PROBE_MS, () => {
    if (!st.spawned) return
    const id = ++st.probeSeq
    st.probeSent.set(id, now())
    if (st.probeSent.size > 2000) st.probeSent.delete(st.probeSent.keys().next().value)
    try { bot._client.write('ping_request', { id: [0, id] }) } catch {}
  })

  // client read timeout like vanilla: no bytes at all for READ_TIMEOUT_MS -> drop the connection
  every(1000, () => {
    if (st.spawned && READ_TIMEOUT_MS > 0 && now() - st.lastRx > READ_TIMEOUT_MS) {
      emit({ bot: st.name, event: 'read_timeout', silent_ms: Math.round(now() - st.lastRx) })
      st.lastError = 'read timeout'
      bot._client.end('readTimeout')
    }
  })

  // chat with sequence numbers; first message soon after spawn, then every CHAT_MS
  const sendChat = () => {
    if (!st.spawned || shuttingDown || chatStopped) return
    const seq = ++st.chatSeq
    st.chatPending.set(seq, now())
    st.chatSent++
    try { bot.chat(`mcbot ${st.name} seq ${seq}`) } catch (e) { emit({ bot: st.name, event: 'error', error: 'chat: ' + e.message }) }
  }
  st.timers.push(setTimeout(sendChat, 1000 + Math.random() * 2000))
  every(CHAT_MS, sendChat)

  // position continuity: client-side position must not jump between checks
  every(500, () => {
    if (!st.spawned || !bot.entity) return
    const t = now()
    const p = bot.entity.position
    if (st.lastPos) {
      const dt = (t - st.lastPosT) / 1000
      const dxz = Math.hypot(p.x - st.lastPos.x, p.z - st.lastPos.z)
      const dy = Math.abs(p.y - st.lastPos.y)
      // walking/sprinting is < 6 b/s; allow slack for physics catch-up after a client stall
      if (dxz > Math.max(3, 12 * dt) || dy > Math.max(10, 40 * dt)) {
        st.positionJumps++
        emit({ bot: st.name, event: 'position_jump', from: fmtPos(st.lastPos), to: fmtPos(p), dt_s: round2(dt) })
      }
    }
    st.lastPos = p.clone()
    st.lastPosT = t
  })

  every(STATS_MS, () => {
    if (!st.spawned) return
    const w = st.rttWindow.sort((a, b) => a - b)
    st.rttWindow = []
    const q = (f) => w.length ? Math.round(w[Math.min(w.length - 1, Math.floor(f * w.length))]) : null
    emit({
      bot: st.name,
      event: 'stats',
      pos: fmtPos(bot.entity.position),
      rtt_n: w.length,
      rtt_p50_ms: q(0.5),
      rtt_p99_ms: q(0.99),
      rtt_max_ms: w.length ? Math.round(w[w.length - 1]) : null,
      probes_unanswered: st.probeSent.size,
      server_ping_ms: bot.player ? bot.player.ping : null,
      keepalives: st.keepAlives,
      gaps: st.gaps
    })
  })

  if (MOVE) wander(st)
}

// Small random walks around the spawn point with the occasional jump.
function wander (st) {
  const bot = st.bot
  const step = () => {
    if (!st.spawned || shuttingDown) return
    bot.clearControlStates()
    const r = Math.random()
    let ms = 400 + Math.random() * 1200
    if (r < 0.2) {
      // stand still briefly
    } else {
      const p = bot.entity.position
      let yaw
      if (st.home && Math.hypot(p.x - st.home.x, p.z - st.home.z) > 6) {
        // mineflayer yaw: 0 faces -Z, increasing counter-clockwise
        yaw = Math.atan2(-(st.home.x - p.x), -(st.home.z - p.z))
      } else {
        yaw = Math.random() * 2 * Math.PI - Math.PI
      }
      bot.look(yaw, 0, true).catch(() => {})
      bot.setControlState('forward', true)
      if (Math.random() < 0.3) bot.setControlState('jump', true)
      if (Math.random() < 0.15) { bot.setControlState('forward', false); bot.setControlState('jump', true); ms = 300 }
    }
    st.timers.push(setTimeout(step, ms))
  }
  step()
}

function stopPlaying (st) {
  for (const t of st.timers) { clearInterval(t); clearTimeout(t) }
  st.timers = []
  try { st.bot && st.bot.clearControlStates() } catch {}
}

function onEnd (st, bot, reason) {
  if (st.bot !== bot) return
  const wasSpawned = st.spawned
  st.spawned = false
  stopPlaying(st)
  if (shuttingDown) return
  const t = now()
  if (wasSpawned) {
    st.disconnects++
    st.disconnectedAt = t
    emit({
      bot: st.name,
      event: 'disconnect',
      reason: String(reason),
      kick_reason: st.kickReason,
      error: st.lastError,
      since_last_rx_ms: st.lastRx == null ? null : Math.round(t - st.lastRx)
    })
  } else {
    emit({ bot: st.name, event: 'connect_failed', reason: String(reason), kick_reason: st.kickReason, error: st.lastError })
  }
  const throttled = /throttl/i.test(st.kickReason || '')
  if (!st.everSpawned) {
    // initial connect: retry a throttled/refused login a few times, but that is not a "reconnect"
    if (st.connectAttempts < 5) {
      setTimeout(() => connect(st, false), throttled ? 4500 : 2000)
    } else {
      st.initialConnectFailed = true
      maybeAllConnected()
    }
    return
  }
  if (RECONNECT) {
    const n = st.connectAttempts
    setTimeout(() => { if (!shuttingDown) connect(st, true) }, Math.min(10000, 1000 * 2 ** Math.min(n, 4)))
  }
}

function maybeAllConnected () {
  if (allConnectedAt != null) return
  if (!bots.every((b) => b.everSpawned || b.initialConnectFailed)) return
  allConnectedAt = now()
  emit({ event: 'all_connected', connected: bots.filter((b) => b.everSpawned).length, count: COUNT })
  setTimeout(() => finish('duration'), DURATION_MS)
}

let finished = false
async function finish (why) {
  if (finished) return
  finished = true
  emit({ event: 'finishing', why })
  // stop sending new chat but give in-flight messages a moment to echo
  chatStopped = true
  await new Promise((r) => setTimeout(r, why === 'duration' ? CHAT_GRACE_MS : 500))
  shuttingDown = true
  for (const st of bots) stopPlaying(st)
  let s = {
    bots: COUNT,
    connected: 0,
    connect_failures: 0,
    disconnects: 0,
    reconnects: 0,
    max_gap_ms: 0,
    gaps: 0,
    rtt_avg_ms: null,
    rtt_max_ms: 0,
    max_keepalive_interval_ms: 0,
    chat_sent: 0,
    chat_echoed: 0,
    chat_lost: 0,
    chat_out_of_order: 0,
    chat_duplicate: 0,
    chat_max_latency_ms: 0,
    chat_others_missing: 0,
    chat_others_out_of_order: 0,
    server_teleports: 0,
    position_jumps: 0,
    client_max_stall_ms: Math.round(stalls.reduce((m, x) => Math.max(m, x.end - x.start), 0))
  }
  let rttSum = 0; let rttN = 0
  for (const st of bots) {
    const lost = [...st.chatPending.keys()].sort((a, b) => a - b)
    emit({
      bot: st.name,
      event: 'bot_summary',
      connected: st.everSpawned,
      disconnects: st.disconnects,
      reconnects: st.reconnects,
      gaps: st.gaps,
      max_gap_ms: Math.round(st.maxGapMs),
      rtt_avg_ms: st.rttCount ? Math.round(st.rttSum / st.rttCount) : null,
      rtt_max_ms: Math.round(st.rttMax),
      keepalives: st.keepAlives,
      max_keepalive_interval_ms: st.maxKeepAliveIntervalMs,
      chat_sent: st.chatSent,
      chat_echoed: st.chatEchoed,
      chat_lost: lost.length,
      chat_lost_seqs: lost,
      chat_out_of_order: st.chatOutOfOrder,
      chat_duplicate: st.chatDuplicate,
      chat_max_latency_ms: st.chatMaxLatencyMs,
      chat_others_seen: st.othersSeen,
      chat_others_missing: st.othersMissing,
      chat_others_out_of_order: st.othersOutOfOrder,
      server_teleports: st.forcedMoves,
      position_jumps: st.positionJumps
    })
    if (st.everSpawned) s.connected++
    if (st.initialConnectFailed || !st.everSpawned) s.connect_failures++
    s.disconnects += st.disconnects
    s.reconnects += st.reconnects
    s.gaps += st.gaps
    s.max_gap_ms = Math.max(s.max_gap_ms, Math.round(st.maxGapMs))
    rttSum += st.rttSum; rttN += st.rttCount
    s.rtt_max_ms = Math.max(s.rtt_max_ms, Math.round(st.rttMax))
    s.max_keepalive_interval_ms = Math.max(s.max_keepalive_interval_ms, st.maxKeepAliveIntervalMs)
    s.chat_sent += st.chatSent
    s.chat_echoed += st.chatEchoed
    s.chat_lost += lost.length
    s.chat_out_of_order += st.chatOutOfOrder
    s.chat_duplicate += st.chatDuplicate
    s.chat_max_latency_ms = Math.max(s.chat_max_latency_ms, st.chatMaxLatencyMs)
    s.chat_others_missing += st.othersMissing
    s.chat_others_out_of_order += st.othersOutOfOrder
    s.server_teleports += st.forcedMoves
    s.position_jumps += st.positionJumps
  }
  s.rtt_avg_ms = rttN ? Math.round(rttSum / rttN) : null
  const failures = []
  if (s.disconnects > 0) failures.push('disconnect')
  if (s.connect_failures > 0) failures.push('connect_failed')
  if (s.chat_lost > 0) failures.push('chat_lost')
  if (s.chat_out_of_order > 0 || s.chat_duplicate > 0) failures.push('chat_order')
  if (s.chat_others_missing > 0 || s.chat_others_out_of_order > 0) failures.push('chat_others')
  if (FAIL_GAP_MS > 0 && s.max_gap_ms > FAIL_GAP_MS) failures.push('gap')
  if (why !== 'duration') failures.push('interrupted')
  s = { event: 'summary', ...s, pass: failures.length === 0, failures }
  emit(s)
  for (const st of bots) { try { st.bot && st.bot.quit() } catch {} }
  setTimeout(() => process.exit(s.disconnects > 0 ? 1 : s.pass ? 0 : 2), 300)
}

process.on('SIGINT', () => finish('SIGINT'))
process.on('SIGTERM', () => finish('SIGTERM'))
process.on('uncaughtException', (e) => {
  emit({ event: 'error', error: 'uncaught: ' + (e && e.stack ? e.stack : String(e)) })
})

// Kick reasons arrive as JSON text or (1.20.3+) NBT text components; flatten to plain text.
function kickText (bot, reason) {
  if (typeof reason === 'string') {
    try { reason = JSON.parse(reason) } catch { return reason }
  }
  try {
    const nbt = require('prismarine-nbt')
    const plain = reason && reason.type && reason.value !== undefined ? nbt.simplify(reason) : reason
    if (typeof plain === 'string') return plain
    return require('prismarine-chat')(bot.registry).fromNotch(plain).toString()
  } catch {
    return JSON.stringify(reason)
  }
}

function fmtPos (p) { return p ? [round2(p.x), round2(p.y), round2(p.z)] : null }
function round2 (x) { return Math.round(x * 100) / 100 }

emit({
  event: 'start',
  host: HOST,
  port: PORT,
  count: COUNT,
  prefix: opt.prefix,
  duration_s: DURATION_MS / 1000,
  version: opt.version,
  mineflayer: require('mineflayer/package.json').version
})
for (let i = 0; i < COUNT; i++) {
  const st = newState(`${opt.prefix}${i + 1}`)
  bots.push(st)
  setTimeout(() => connect(st, false), i * STAGGER_MS)
}
if (COUNT === 0) maybeAllConnected()
