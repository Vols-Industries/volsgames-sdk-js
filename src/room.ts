import { type Kind, type Link, type Sink, webSocket, webTransport } from './link.ts'
import { type ServerMessage, type Target, code, concat, decodeServer, encodeClient, frame, pack, parseDatagram, reason, seqNewer } from './proto.ts'

export interface Endpoints {
  quic?: string
  webtransport?: string
  websocket?: string | null
  cert_hash?: string | null
}

export interface Grant {
  token: string
  endpoints: Endpoints
}

export type GrantSource = Grant | (() => Promise<Grant>)

export interface Options {
  transport?: 'auto' | Kind
  fallbackMs?: number
  welcomeMs?: number
  reconnectMs?: number
  WebSocket?: typeof WebSocket
  WebTransport?: typeof WebTransport
}

export interface SendOptions {
  to?: Target
  reliable?: boolean
}

export interface Closed {
  reason?: number
  code?: number
  message?: string
}

export interface Events {
  data: [from: number, payload: Uint8Array]
  joined: [player: number, name: string]
  left: [player: number, reason: number]
  reconnecting: []
  reconnected: []
  close: [closed: Closed]
}

export class VolsError extends Error {
  readonly code: number
  constructor(code: number, message: string) {
    super(message)
    this.code = code
  }
}

const fatal = new Set<number>([code.badToken, code.noRoom, code.full, code.wrongGame])
const utf8 = new TextEncoder()
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

function deadline<T>(p: Promise<T>, ms: number, what: string): Promise<T> {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`${what} timed out`)), ms)
    p.then(
      (v) => {
        clearTimeout(t)
        resolve(v)
      },
      (e) => {
        clearTimeout(t)
        reject(e)
      },
    )
  })
}

async function dial(ep: Endpoints, o: Options, sink: Sink): Promise<Link> {
  const mode = o.transport ?? 'auto'
  const WT = o.WebTransport ?? globalThis.WebTransport
  const WS = o.WebSocket ?? globalThis.WebSocket
  if (mode !== 'websocket' && WT && ep.webtransport) {
    const stop = new AbortController()
    const guarded: Sink = {
      record: (b) => stop.signal.aborted || sink.record(b),
      datagram: (d) => stop.signal.aborted || sink.datagram(d),
      closed: () => stop.signal.aborted || sink.closed(),
    }
    const attempt = webTransport(ep.webtransport, ep.cert_hash ?? undefined, guarded, WT, stop.signal)
    try {
      return mode === 'webtransport' ? await attempt : await deadline(attempt, o.fallbackMs ?? 2000, 'webtransport')
    } catch (e) {
      stop.abort()
      attempt.catch(() => {})
      if (mode === 'webtransport' || !ep.websocket || !WS) throw e
    }
  }
  if (mode === 'webtransport' || !ep.websocket || !WS) throw new Error('no usable transport')
  return webSocket(ep.websocket, sink, WS)
}

export class Room {
  player = 0
  room = ''
  tickRate = 0
  transport: Kind = 'websocket'
  readonly players = new Map<number, string>()

  private readonly source: GrantSource
  private readonly options: Options
  private state: 'opening' | 'open' | 'reconnecting' | 'closed' = 'opening'
  private link?: Link
  private gen = 0
  private seq = 0
  private lastSeq = -1
  private offset = 0
  private lastPing = 0
  private leaving = false
  private bye?: Closed
  private closed?: Closed
  private welcome?: { resolve: () => void; reject: (e: unknown) => void }
  private reliableQueue: Uint8Array[] = []
  private unreliableQueue: Uint8Array[] = []
  private scheduled = false
  private readonly pings = new Map<number, (rtt: number) => void>()
  private readonly listeners = new Map<keyof Events, Set<(...a: unknown[]) => void>>()

  private constructor(source: GrantSource, options: Options) {
    this.source = source
    this.options = options
  }

  static async connect(source: GrantSource, options: Options = {}): Promise<Room> {
    const room = new Room(source, options)
    await room.open(await room.grant())
    return room
  }

  on<K extends keyof Events>(event: K, fn: (...a: Events[K]) => void): () => void {
    let set = this.listeners.get(event)
    if (!set) this.listeners.set(event, (set = new Set()))
    const f = fn as (...a: unknown[]) => void
    set.add(f)
    return () => set.delete(f)
  }

  send(payload: Uint8Array | string, { to = 'others', reliable = true }: SendOptions = {}) {
    const bytes = typeof payload === 'string' ? utf8.encode(payload) : payload
    this.queue(encodeClient({ kind: 'data', target: to, payload: bytes }), reliable)
  }

  subscribe(group: number) {
    this.queue(encodeClient({ kind: 'subscribe', group }), true)
  }

  unsubscribe(group: number) {
    this.queue(encodeClient({ kind: 'unsubscribe', group }), true)
  }

  ping(timeoutMs = 3000): Promise<number> {
    const clientTime = (this.lastPing = Math.max(Math.round(performance.now() * 1000), this.lastPing + 1))
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => {
        this.pings.delete(clientTime)
        reject(new Error('ping timed out'))
      }, timeoutMs)
      this.pings.set(clientTime, (rtt) => {
        clearTimeout(t)
        resolve(rtt)
      })
      this.queue(encodeClient({ kind: 'ping', clientTime }), false)
    })
  }

  serverNow() {
    return Date.now() + this.offset
  }

  leave(): Promise<Closed> {
    return new Promise((resolve) => {
      if (this.closed) return resolve(this.closed)
      this.on('close', resolve)
      this.leaving = true
      if (this.state !== 'open') return this.finish({ reason: reason.left })
      this.queue(encodeClient({ kind: 'leave' }), true)
      const t = setTimeout(() => this.finish({ reason: reason.left }), 2000)
      this.on('close', () => clearTimeout(t))
    })
  }

  private emit<K extends keyof Events>(event: K, ...a: Events[K]) {
    for (const fn of this.listeners.get(event) ?? []) fn(...a)
  }

  private grant(): Promise<Grant> {
    return typeof this.source === 'function' ? this.source() : Promise.resolve(this.source)
  }

  private async open(grant: Grant) {
    const gen = ++this.gen
    const sink: Sink = {
      record: (b) => gen === this.gen && this.onRecord(b),
      datagram: (d) => gen === this.gen && this.onDatagram(d),
      closed: () => gen === this.gen && this.onClosed(),
    }
    const link = await dial(grant.endpoints, this.options, sink)
    if (gen !== this.gen) {
      link.close()
      throw new Error('closed')
    }
    this.link = link
    this.transport = link.kind
    this.lastSeq = -1
    const welcomed = new Promise<void>((resolve, reject) => (this.welcome = { resolve, reject }))
    link.reliable(encodeClient({ kind: 'hello', token: grant.token }))
    try {
      await deadline(welcomed, this.options.welcomeMs ?? 10_000, 'welcome')
    } catch (e) {
      this.welcome = undefined
      link.close()
      throw e
    }
  }

  private queue(rec: Uint8Array, reliable: boolean) {
    if (this.state === 'closed' || (!reliable && this.state !== 'open')) return
    ;(reliable ? this.reliableQueue : this.unreliableQueue).push(rec)
    if (this.scheduled) return
    this.scheduled = true
    queueMicrotask(() => this.flush())
  }

  private flush() {
    this.scheduled = false
    const link = this.link
    if (!link || this.state !== 'open') return
    const { batches, oversize } = pack(this.unreliableQueue, link.maxDatagram - 2)
    const reliable = this.reliableQueue.concat(oversize)
    this.unreliableQueue = []
    this.reliableQueue = []
    if (reliable.length) link.reliable(concat(reliable))
    for (const b of batches) {
      this.seq = (this.seq + 1) & 0xffff
      link.unreliable(frame(this.seq, b))
    }
  }

  private onDatagram(d: Uint8Array) {
    try {
      const { seq, records } = parseDatagram(d)
      if (this.lastSeq >= 0 && !seqNewer(seq, this.lastSeq)) return
      this.lastSeq = seq
      for (const r of records) this.onRecord(r)
    } catch {
      return
    }
  }

  private onRecord(body: Uint8Array) {
    let m: ServerMessage
    try {
      m = decodeServer(body)
    } catch {
      return
    }
    switch (m.kind) {
      case 'welcome':
        return this.onWelcome(m)
      case 'joined':
        this.players.set(m.player, m.name)
        return this.emit('joined', m.player, m.name)
      case 'left':
        this.players.delete(m.player)
        return this.emit('left', m.player, m.reason)
      case 'data':
        return this.emit('data', m.from, m.payload)
      case 'pong': {
        const done = this.pings.get(m.clientTime)
        if (!done) return
        this.pings.delete(m.clientTime)
        const rtt = (performance.now() * 1000 - m.clientTime) / 1000
        this.offset = m.serverTime + rtt / 2 - Date.now()
        return done(rtt)
      }
      case 'error':
        if (this.welcome) this.welcome.reject(new VolsError(m.code, m.message))
        this.welcome = undefined
        this.bye = { code: m.code, message: m.message }
        return
      case 'bye':
        this.bye = { reason: m.reason }
        return
    }
  }

  private onWelcome(m: Extract<ServerMessage, { kind: 'welcome' }>) {
    const before = new Map(this.players)
    const again = this.state === 'reconnecting'
    this.player = m.player
    this.room = m.room
    this.tickRate = m.tickRate
    this.offset = m.serverTime - Date.now()
    this.players.clear()
    for (const [id, name] of m.players) this.players.set(id, name)
    this.state = 'open'
    this.bye = undefined
    if (again) {
      for (const id of before.keys()) if (!this.players.has(id)) this.emit('left', id, reason.timeout)
      for (const [id, name] of this.players) if (!before.has(id)) this.emit('joined', id, name)
    }
    this.welcome?.resolve()
    this.welcome = undefined
    this.flush()
  }

  private onClosed() {
    this.link = undefined
    if (this.welcome) {
      this.welcome.reject(this.bye?.code ? new VolsError(this.bye.code, this.bye.message ?? '') : new Error('connection closed'))
      this.welcome = undefined
      return
    }
    if (this.state !== 'open') return
    if (this.bye || this.leaving) return this.finish(this.bye ?? { reason: reason.left })
    void this.recover()
  }

  private async recover() {
    this.state = 'reconnecting'
    this.emit('reconnecting')
    const until = Date.now() + (this.options.reconnectMs ?? 20_000)
    for (let wait = 100; Date.now() + wait < until; wait = Math.min(wait * 2, 2000)) {
      await sleep(wait)
      if (this.closed) return
      let grant: Grant
      try {
        grant = await this.grant()
      } catch (e) {
        return this.finish({ message: e instanceof Error ? e.message : String(e) })
      }
      if (this.closed) return
      try {
        await this.open(grant)
        return this.emit('reconnected')
      } catch (e) {
        if (this.closed) return
        if (e instanceof VolsError && (fatal.has(e.code) || typeof this.source !== 'function')) return this.finish({ code: e.code, message: e.message })
      }
    }
    this.finish({ reason: reason.timeout })
  }

  private finish(c: Closed) {
    if (this.state === 'closed') return
    this.state = 'closed'
    this.closed = c
    this.gen++
    this.link?.close()
    this.link = undefined
    this.reliableQueue = []
    this.unreliableQueue = []
    this.emit('close', c)
  }
}

export const connect = Room.connect
