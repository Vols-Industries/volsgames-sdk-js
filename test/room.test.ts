import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { test } from 'node:test'
import { type Target, encodeClient, frame, parseDatagram, reason } from '../src/proto.ts'
import { type Grant, type Options, Room, VolsError, connect } from '../src/room.ts'

const golden = new Map(
  readFileSync(new URL('golden.txt', import.meta.url), 'utf8')
    .trim()
    .split('\n')
    .map((l) => {
      const [side, name, hex] = l.split(' ')
      return [`${side} ${name}` as string, new Uint8Array(Buffer.from(hex, 'hex'))] as const
    }),
)
const g = (k: string) => golden.get(k)!
const byeLeft = new Uint8Array([2, 0, 0x87, reason.left])
const hex = (b: Uint8Array) => Buffer.from(b).toString('hex')
const tick = () => new Promise((r) => setTimeout(r, 0))
async function until(cond: () => boolean, ms = 2000) {
  const end = Date.now() + ms
  while (!cond()) {
    if (Date.now() > end) throw new Error('condition not met in time')
    await new Promise((r) => setTimeout(r, 5))
  }
}

class FakeSocket {
  static sockets: FakeSocket[] = []
  static refuse = false
  binaryType = ''
  readyState = 0
  sent: Uint8Array[] = []
  onopen?: () => void
  onclose?: () => void
  onmessage?: (e: { data: ArrayBuffer }) => void

  constructor() {
    FakeSocket.sockets.push(this)
    const refuse = FakeSocket.refuse
    queueMicrotask(() => {
      if (refuse) return this.close()
      this.readyState = 1
      this.onopen?.()
    })
  }
  send(b: Uint8Array) {
    this.sent.push(b.slice())
  }
  close() {
    if (this.readyState === 3) return
    this.readyState = 3
    queueMicrotask(() => this.onclose?.())
  }
  serve(lane: number, bytes: Uint8Array) {
    const out = new Uint8Array(bytes.length + 1)
    out[0] = lane
    out.set(bytes, 1)
    this.onmessage?.({ data: out.buffer })
  }
}

const grant: Grant = { token: 't', endpoints: { websocket: 'ws://fake' } }
const opts: Options = { WebSocket: FakeSocket as unknown as typeof WebSocket, transport: 'websocket' }
const lane0 = (rec: Uint8Array) => hex(new Uint8Array([0, ...rec]))

async function joined(source: Grant | (() => Promise<Grant>) = grant, o: Options = opts): Promise<[Room, FakeSocket]> {
  FakeSocket.sockets = []
  FakeSocket.refuse = false
  const p = connect(source, o)
  await until(() => FakeSocket.sockets[0]?.sent.length === 1)
  const s = FakeSocket.sockets[0]
  assert.equal(hex(s.sent[0]), lane0(encodeClient({ kind: 'hello', token: 't' })))
  s.serve(0, g('s welcome'))
  return [await p, s]
}

test('hello first, welcome fills the room, events flow, stale datagrams drop', async () => {
  const [room, s] = await joined()
  assert.equal(room.player, 3)
  assert.equal(room.room, 'eu-1')
  assert.equal(room.tickRate, 60)
  assert.equal(room.host, 1)
  assert.equal(room.transport, 'websocket')
  assert.deepEqual([...room.players], [
    [1, 'ann'],
    [3, 'Eric'],
  ])
  assert.ok(Math.abs(room.serverNow() - 1793549111000) < 1000)
  const events: string[] = []
  room.on('joined', (p, n) => events.push(`joined ${p} ${n}`))
  room.on('left', (p, r) => events.push(`left ${p} ${r}`))
  room.on('data', (f, d) => events.push(`data ${f} ${d.length}`))
  s.serve(0, g('s joined'))
  s.serve(0, new Uint8Array([1, 0, 0x99]))
  s.serve(1, frame(5, g('s data')))
  s.serve(1, frame(4, g('s data')))
  s.serve(1, frame(5, g('s data')))
  s.serve(1, frame(6, new Uint8Array([...g('s data'), ...g('s left')])))
  assert.deepEqual(events, ['joined 2 bob', 'data 2 130', 'data 2 130', 'left 2 1'])
  assert.equal(room.players.has(2), false)
})

test('sends coalesce per tick and oversize unreliable payloads go reliable', async () => {
  const [room, s] = await joined()
  room.send('a', { reliable: false, to: 'all' })
  room.send('b', { reliable: false, to: { player: 1 } })
  room.send('c', { reliable: false, to: { group: 2 } })
  room.send(new Uint8Array(1500), { reliable: false })
  room.send('r1')
  room.subscribe(4)
  room.unsubscribe(4)
  assert.equal(s.sent.length, 1)
  await tick()
  assert.equal(s.sent.length, 3)
  const enc = new TextEncoder()
  const data = (p: string | Uint8Array, target: Target) => encodeClient({ kind: 'data', target, payload: typeof p === 'string' ? enc.encode(p) : p })
  const reliable = [data('r1', 'others'), encodeClient({ kind: 'subscribe', group: 4 }), encodeClient({ kind: 'unsubscribe', group: 4 }), data(new Uint8Array(1500), 'others')]
  assert.equal(hex(s.sent[1]), lane0(new Uint8Array(reliable.flatMap((r) => [...r]))))
  assert.equal(s.sent[2][0], 1)
  const d = parseDatagram(s.sent[2].subarray(1))
  assert.equal(d.seq, 1)
  assert.deepEqual(d.records.map(hex), [data('a', 'all'), data('b', { player: 1 }), data('c', { group: 2 })].map((r) => hex(r.subarray(2))))
})

test('a refusal before the welcome rejects with its code', async () => {
  FakeSocket.sockets = []
  const p = connect(grant, opts)
  await until(() => FakeSocket.sockets[0]?.sent.length === 1)
  FakeSocket.sockets[0].serve(0, g('s error'))
  FakeSocket.sockets[0].close()
  await assert.rejects(p, (e) => e instanceof VolsError && e.code === 4 && e.message === 'full')
})

test('bye ends the room without reconnecting', async () => {
  const [room, s] = await joined()
  const closed = new Promise((r) => room.on('close', r))
  let reconnecting = false
  room.on('reconnecting', () => (reconnecting = true))
  s.serve(0, g('s bye'))
  s.close()
  assert.deepEqual(await closed, { reason: reason.replaced })
  assert.equal(reconnecting, false)
  assert.equal(FakeSocket.sockets.length, 1)
  room.send('ignored')
  await tick()
  assert.equal(s.sent.length, 1)
})

test('a lost connection reconnects with a fresh grant, replays queued sends and diffs players', async () => {
  let calls = 0
  const [room, s] = await joined(async () => (calls++, grant))
  s.serve(0, g('s joined'))
  const events: string[] = []
  room.on('reconnecting', () => events.push('reconnecting'))
  room.on('reconnected', () => events.push('reconnected'))
  room.on('left', (p, r) => events.push(`left ${p} ${r}`))
  s.close()
  await until(() => events.length === 1)
  room.send('queued')
  room.send('dropped', { reliable: false })
  await until(() => FakeSocket.sockets[1]?.sent.length === 1)
  const s2 = FakeSocket.sockets[1]
  s2.serve(0, g('s welcome'))
  await until(() => events.length === 3)
  assert.deepEqual(events, ['reconnecting', 'left 2 1', 'reconnected'])
  assert.equal(calls, 2)
  await tick()
  assert.equal(s2.sent.length, 2)
  assert.equal(hex(s2.sent[1]), lane0(encodeClient({ kind: 'data', target: 'others', payload: new TextEncoder().encode('queued') })))
  const left = room.leave()
  await tick()
  assert.equal(hex(s2.sent[2]), lane0(encodeClient({ kind: 'leave' })))
  s2.serve(0, byeLeft)
  s2.close()
  assert.deepEqual(await left, { reason: reason.left })
  assert.deepEqual(await room.leave(), { reason: reason.left })
})

test('reconnect stops on a fatal refusal', async () => {
  const [room, s] = await joined(async () => grant)
  const closed = new Promise((r) => room.on('close', r))
  s.close()
  await until(() => FakeSocket.sockets[1]?.sent.length === 1)
  FakeSocket.sockets[1].serve(0, new Uint8Array([...g('s error')].map((b, i) => (i === 3 ? 3 : b))))
  FakeSocket.sockets[1].close()
  assert.deepEqual(await closed, { code: 3, message: 'full' })
})

test('reconnect gives up when the window runs out', async () => {
  const [room, s] = await joined(grant, { ...opts, reconnectMs: 400 })
  const closed = new Promise((r) => room.on('close', r))
  FakeSocket.refuse = true
  s.close()
  assert.deepEqual(await closed, { reason: reason.timeout })
  assert.ok(FakeSocket.sockets.length >= 3)
})

test('auto falls back to websocket when webtransport stalls, and closes the stalled attempt', async () => {
  let shut = 0
  class StalledTransport {
    ready = new Promise(() => {})
    closed = new Promise(() => {})
    close() {
      shut++
    }
  }
  const [room] = await joined(
    { token: 't', endpoints: { webtransport: 'https://fake/', websocket: 'ws://fake' } },
    { ...opts, transport: 'auto', fallbackMs: 30, WebTransport: StalledTransport as unknown as typeof WebTransport },
  )
  assert.equal(room.transport, 'websocket')
  assert.equal(shut, 1)
  await assert.rejects(
    connect({ token: 't', endpoints: { webtransport: 'https://fake/' } }, { ...opts, transport: 'auto', fallbackMs: 30, WebTransport: StalledTransport as unknown as typeof WebTransport }),
    /timed out/,
  )
  await assert.rejects(connect({ token: 't', endpoints: {} }, opts), /no usable transport/)
})

test('a grant function that throws during reconnect ends the room with its message', async () => {
  let calls = 0
  const [room, s] = await joined(async () => {
    if (calls++) throw new Error('backend said no')
    return grant
  })
  const closed = new Promise((r) => room.on('close', r))
  s.close()
  assert.deepEqual(await closed, { message: 'backend said no' })
  assert.equal(FakeSocket.sockets.length, 1)
})

const text = (e: { value: Uint8Array } | undefined) => e && new TextDecoder().decode(e.value)
const withHost = (host: number) => g('s welcome').map((b, i, a) => (i === a.length - 5 ? host : b))

async function withState(source: Grant | (() => Promise<Grant>) = grant): Promise<[Room, FakeSocket]> {
  FakeSocket.sockets = []
  FakeSocket.refuse = false
  let done = false
  const p = connect(source, opts).then((r) => ((done = true), r))
  await until(() => FakeSocket.sockets[0]?.sent.length === 1)
  const s = FakeSocket.sockets[0]
  s.serve(0, g('s welcome_state'))
  s.serve(0, g('s state'))
  await tick()
  assert.equal(done, false)
  s.serve(0, g('s state_mine'))
  return [await p, s]
}

test('connect waits for the state after the welcome, writes resolve or reject in order', async () => {
  const [room, s] = await withState()
  assert.equal(text(room.state().get('phase')), 'play')
  assert.deepEqual({ ...room.state(3).get('ready'), value: undefined }, { value: undefined, rev: 8, by: 3 })
  assert.equal(room.state(2).size, 0)
  const events: string[] = []
  room.on('state', (k, e, p) => events.push(`${p} ${k} ${text(e)}`))
  room.on('host', (p) => events.push(`host ${p}`))
  const refused = room.set('flag', 'x')
  const mine = room.set('ready', '1', { mine: true, expect: 0 })
  await tick()
  assert.equal(hex(s.sent[1]), lane0(new Uint8Array([...encodeClient({ kind: 'set', scope: 0, key: 'flag', value: new TextEncoder().encode('x') }), ...g('c set_mine')])))
  s.serve(0, g('s refused'))
  await assert.rejects(refused, (e) => e instanceof VolsError && e.code === 8)
  s.serve(0, g('s state_mine'))
  assert.deepEqual(text(await mine), '1')
  const lost = room.set('later', new Uint8Array([1]))
  await tick()
  s.serve(0, g('s state'))
  s.serve(0, g('s state_delete'))
  s.serve(0, g('s host'))
  assert.equal(room.state().has('phase'), false)
  assert.equal(room.host, 1)
  assert.deepEqual(events, ['3 ready 1', '0 phase play', '0 phase undefined', 'host 1'])
  s.close()
  await assert.rejects(lost, /connection lost/)
  const unsent = room.set('p', 'q')
  assert.deepEqual(await room.leave(), { reason: reason.left })
  await assert.rejects(unsent, /room closed/)
  await assert.rejects(room.set('p', 'q'), /room closed/)
  await assert.rejects(room.set('', 'x'), (e) => e instanceof VolsError && e.code === 5)
  await assert.rejects(room.set('k'.repeat(65), 'x'), (e) => e instanceof VolsError && e.code === 10)
  await assert.rejects(room.set('k', new Uint8Array(8193)), (e) => e instanceof VolsError && e.code === 10)
})

test('a reconnect swaps in the server state, reports differences and sends queued writes', async () => {
  const [room, s] = await withState(async () => grant)
  const events: string[] = []
  room.on('state', (k, e, p) => events.push(`${p} ${k} ${text(e)}`))
  room.on('host', (p) => events.push(`host ${p}`))
  room.on('left', (p) => events.push(`left ${p}`))
  s.serve(0, g('s left'))
  s.close()
  await until(() => FakeSocket.sockets.length === 2)
  const queued = room.set('flag', null, { expect: 1234567890123 })
  await until(() => FakeSocket.sockets[1].sent.length === 1)
  const s2 = FakeSocket.sockets[1]
  s2.serve(0, withHost(3))
  await tick()
  assert.deepEqual(events, ['left 2', '0 phase undefined', '3 ready undefined', 'host 3'])
  assert.equal(room.host, 3)
  assert.equal(hex(s2.sent[1]), lane0(g('c set_delete')))
  s2.serve(0, g('s refused'))
  await assert.rejects(queued, (e) => e instanceof VolsError && e.code === 8)
  const left = room.leave()
  await tick()
  s2.serve(0, byeLeft)
  s2.close()
  assert.deepEqual(await left, { reason: reason.left })
})
