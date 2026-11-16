export const VERSION = 0
export const DATAGRAM_BUDGET = 1150

export const reason = { left: 0, timeout: 1, kicked: 2, roomClosed: 3, replaced: 4, slow: 5, limit: 6 } as const
export const code = { badToken: 1, expired: 2, noRoom: 3, full: 4, protocol: 5, helloTimeout: 6, wrongGame: 7 } as const

export type Target = 'all' | 'others' | { player: number } | { group: number }

export type ClientMessage =
  | { kind: 'hello'; token: string }
  | { kind: 'data'; target: Target; payload: Uint8Array }
  | { kind: 'ping'; clientTime: number }
  | { kind: 'subscribe'; group: number }
  | { kind: 'unsubscribe'; group: number }
  | { kind: 'leave' }

export type ServerMessage =
  | { kind: 'welcome'; player: number; room: string; tickRate: number; serverTime: number; reconnected: boolean; players: [number, string][] }
  | { kind: 'joined'; player: number; name: string }
  | { kind: 'left'; player: number; reason: number }
  | { kind: 'data'; from: number; payload: Uint8Array }
  | { kind: 'pong'; clientTime: number; serverTime: number }
  | { kind: 'error'; code: number; message: string }
  | { kind: 'bye'; reason: number }

export class ProtocolError extends Error {}

const utf8 = new TextEncoder()
const text = new TextDecoder('utf-8', { fatal: true })

class Writer {
  buf = new Uint8Array(64)
  view = new DataView(this.buf.buffer)
  len = 2

  grow(n: number) {
    if (this.len + n <= this.buf.length) return
    const next = new Uint8Array(Math.max(this.buf.length * 2, this.len + n))
    next.set(this.buf)
    this.buf = next
    this.view = new DataView(next.buffer)
  }
  u8(v: number) {
    this.grow(1)
    this.buf[this.len++] = v
  }
  u16(v: number) {
    this.grow(2)
    this.view.setUint16(this.len, v, true)
    this.len += 2
  }
  u32(v: number) {
    this.grow(4)
    this.view.setUint32(this.len, v, true)
    this.len += 4
  }
  u64(v: number) {
    this.grow(8)
    this.view.setBigUint64(this.len, BigInt(v), true)
    this.len += 8
  }
  varint(v: number) {
    while (v >= 0x80) {
      this.u8((v % 0x80) | 0x80)
      v = Math.floor(v / 0x80)
    }
    this.u8(v)
  }
  bytes(v: Uint8Array) {
    this.varint(v.length)
    this.grow(v.length)
    this.buf.set(v, this.len)
    this.len += v.length
  }
  record() {
    const n = this.len - 2
    if (n > 0xffff) throw new ProtocolError('record too long')
    this.view.setUint16(0, n, true)
    return this.buf.subarray(0, this.len)
  }
}

class Reader {
  at = 0
  buf: Uint8Array
  view: DataView

  constructor(buf: Uint8Array) {
    this.buf = buf
    this.view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength)
  }
  need(n: number) {
    if (this.at + n > this.buf.length) throw new ProtocolError('short')
  }
  u8() {
    this.need(1)
    return this.buf[this.at++]
  }
  u16() {
    this.need(2)
    this.at += 2
    return this.view.getUint16(this.at - 2, true)
  }
  u32() {
    this.need(4)
    this.at += 4
    return this.view.getUint32(this.at - 4, true)
  }
  u64() {
    this.need(8)
    this.at += 8
    return Number(this.view.getBigUint64(this.at - 8, true))
  }
  varint() {
    let v = 0
    for (let mul = 1, i = 0; i < 8; i++, mul *= 0x80) {
      const b = this.u8()
      v += (b & 0x7f) * mul
      if (!(b & 0x80)) return v
    }
    throw new ProtocolError('varint')
  }
  bytes() {
    const n = this.varint()
    this.need(n)
    this.at += n
    return this.buf.subarray(this.at - n, this.at)
  }
  str() {
    const b = this.bytes()
    try {
      return text.decode(b)
    } catch {
      throw new ProtocolError('utf-8')
    }
  }
  finish() {
    if (this.at !== this.buf.length) throw new ProtocolError('trailing bytes')
  }
}

export function encodeClient(m: ClientMessage): Uint8Array {
  const w = new Writer()
  switch (m.kind) {
    case 'hello':
      w.u8(1)
      w.u8(VERSION)
      w.bytes(utf8.encode(m.token))
      break
    case 'data':
      w.u8(2)
      if (m.target === 'all') w.u8(0)
      else if (m.target === 'others') w.u8(1)
      else if ('player' in m.target) {
        w.u8(2)
        w.u32(m.target.player)
      } else {
        w.u8(3)
        w.u32(m.target.group)
      }
      w.bytes(m.payload)
      break
    case 'ping':
      w.u8(3)
      w.u64(m.clientTime)
      break
    case 'subscribe':
      w.u8(4)
      w.u32(m.group)
      break
    case 'unsubscribe':
      w.u8(5)
      w.u32(m.group)
      break
    case 'leave':
      w.u8(6)
      break
  }
  return w.record()
}

export function decodeServer(body: Uint8Array): ServerMessage {
  const r = new Reader(body)
  let m: ServerMessage
  switch (r.u8()) {
    case 0x81: {
      if (r.u8() !== VERSION) throw new ProtocolError('version')
      const player = r.u32()
      const room = r.str()
      const tickRate = r.u16()
      const serverTime = r.u64()
      const reconnected = r.u8() !== 0
      const n = r.varint()
      if (n > 65_536) throw new ProtocolError('too many players')
      const players: [number, string][] = []
      for (let i = 0; i < n; i++) players.push([r.u32(), r.str()])
      m = { kind: 'welcome', player, room, tickRate, serverTime, reconnected, players }
      break
    }
    case 0x82:
      m = { kind: 'joined', player: r.u32(), name: r.str() }
      break
    case 0x83:
      m = { kind: 'left', player: r.u32(), reason: r.u8() }
      break
    case 0x84:
      m = { kind: 'data', from: r.u32(), payload: r.bytes() }
      break
    case 0x85:
      m = { kind: 'pong', clientTime: r.u64(), serverTime: r.u64() }
      break
    case 0x86:
      m = { kind: 'error', code: r.u16(), message: r.str() }
      break
    case 0x87:
      m = { kind: 'bye', reason: r.u8() }
      break
    default:
      throw new ProtocolError('unknown kind')
  }
  r.finish()
  return m
}

export class StreamBuffer {
  buf = new Uint8Array(0)

  push(chunk: Uint8Array) {
    const next = new Uint8Array(this.buf.length + chunk.length)
    next.set(this.buf)
    next.set(chunk, this.buf.length)
    this.buf = next
  }
  pop(): Uint8Array | undefined {
    if (this.buf.length < 2) return
    const n = this.buf[0] | (this.buf[1] << 8)
    if (this.buf.length < 2 + n) return
    const r = this.buf.subarray(2, 2 + n)
    this.buf = this.buf.subarray(2 + n)
    return r
  }
}

export function parseDatagram(d: Uint8Array): { seq: number; records: Uint8Array[] } {
  if (d.length < 2) throw new ProtocolError('short')
  const records: Uint8Array[] = []
  let at = 2
  while (at < d.length) {
    if (at + 2 > d.length) throw new ProtocolError('short')
    const n = d[at] | (d[at + 1] << 8)
    if (at + 2 + n > d.length) throw new ProtocolError('short')
    records.push(d.subarray(at + 2, at + 2 + n))
    at += 2 + n
  }
  return { seq: d[0] | (d[1] << 8), records }
}

export function frame(seq: number, body: Uint8Array): Uint8Array {
  const out = new Uint8Array(body.length + 2)
  out[0] = seq & 0xff
  out[1] = seq >> 8
  out.set(body, 2)
  return out
}

export function seqNewer(a: number, b: number) {
  return a !== b && ((a - b) & 0xffff) < 0x8000
}

export function concat(parts: Uint8Array[]): Uint8Array {
  if (parts.length === 1) return parts[0]
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0))
  let at = 0
  for (const p of parts) {
    out.set(p, at)
    at += p.length
  }
  return out
}

export function pack(records: Uint8Array[], budget: number): { batches: Uint8Array[]; oversize: Uint8Array[] } {
  const batches: Uint8Array[] = []
  const oversize: Uint8Array[] = []
  let group: Uint8Array[] = []
  let size = 0
  for (const r of records) {
    if (r.length > budget) {
      oversize.push(r)
      continue
    }
    if (size + r.length > budget) {
      batches.push(concat(group))
      group = []
      size = 0
    }
    group.push(r)
    size += r.length
  }
  if (group.length) batches.push(concat(group))
  return { batches, oversize }
}
