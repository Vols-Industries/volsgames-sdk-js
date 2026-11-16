import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { test } from 'node:test'
import { type ClientMessage, ProtocolError, StreamBuffer, code, decodeServer, encodeClient, frame, pack, parseDatagram, reason, seqNewer } from '../src/proto.ts'

const golden = new Map(
  readFileSync(new URL('golden.txt', import.meta.url), 'utf8')
    .trim()
    .split('\n')
    .map((l) => {
      const [side, name, hex] = l.split(' ')
      return [`${side} ${name}` as string, Buffer.from(hex, "hex")] as const
    }),
)
const g = (k: string) => new Uint8Array(golden.get(k)!)
const body = (k: string) => g(k).subarray(2)
const hex = (b: Uint8Array) => Buffer.from(b).toString('hex')

test('client messages match the Rust encoder byte for byte', () => {
  const cases: [string, ClientMessage][] = [
    ['hello', { kind: 'hello', token: 'a.b' }],
    ['data_all', { kind: 'data', target: 'all', payload: new TextEncoder().encode('xyz') }],
    ['data_others', { kind: 'data', target: 'others', payload: new Uint8Array() }],
    ['data_player', { kind: 'data', target: { player: 70000 }, payload: new Uint8Array(200).fill(7) }],
    ['data_group', { kind: 'data', target: { group: 5 }, payload: new TextEncoder().encode('g') }],
    ['ping', { kind: 'ping', clientTime: 1234567890123 }],
    ['subscribe', { kind: 'subscribe', group: 9 }],
    ['unsubscribe', { kind: 'unsubscribe', group: 9 }],
    ['leave', { kind: 'leave' }],
  ]
  for (const [name, m] of cases) assert.equal(hex(encodeClient(m)), hex(g(`c ${name}`)), name)
})

test('server messages from the Rust encoder decode', () => {
  assert.deepEqual(decodeServer(body('s welcome')), {
    kind: 'welcome',
    player: 3,
    room: 'eu-1',
    tickRate: 60,
    serverTime: 1793549111000,
    reconnected: true,
    players: [
      [1, 'ann'],
      [3, 'Eric'],
    ],
  })
  assert.deepEqual(decodeServer(body('s joined')), { kind: 'joined', player: 2, name: 'bob' })
  assert.deepEqual(decodeServer(body('s left')), { kind: 'left', player: 2, reason: reason.timeout })
  assert.deepEqual(decodeServer(body('s data')), { kind: 'data', from: 2, payload: new Uint8Array(130).fill(1) })
  assert.deepEqual(decodeServer(body('s pong')), { kind: 'pong', clientTime: 1234567890123, serverTime: 1793549111000 })
  assert.deepEqual(decodeServer(body('s error')), { kind: 'error', code: code.full, message: 'full' })
  assert.deepEqual(decodeServer(body('s bye')), { kind: 'bye', reason: reason.replaced })
})

test('malformed server messages throw', () => {
  const bad = [
    new Uint8Array([]),
    new Uint8Array([0x99]),
    new Uint8Array([0x87]),
    new Uint8Array([0x87, 4, 0]),
    new Uint8Array([0x81, 1]),
    new Uint8Array([0x82, 2, 0, 0, 0, 2, 0xff, 0xfe]),
    new Uint8Array([0x84, 2, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
  ]
  for (const b of bad) assert.throws(() => decodeServer(b), ProtocolError, hex(b))
})

test('stream buffer reassembles records split at every byte', () => {
  const all = Buffer.concat(['s welcome', 's data', 's bye'].map((k) => g(k)))
  const sb = new StreamBuffer()
  const out: string[] = []
  for (const byte of all) {
    sb.push(new Uint8Array([byte]))
    for (let r = sb.pop(); r; r = sb.pop()) out.push(decodeServer(r).kind)
  }
  assert.deepEqual(out, ['welcome', 'data', 'bye'])
  assert.equal(sb.buf.length, 0)
})

test('datagram framing, parsing and sequence order', () => {
  const recs = Buffer.concat([g('s joined'), g('s bye')])
  const d = parseDatagram(frame(65535, recs))
  assert.equal(d.seq, 65535)
  assert.deepEqual(
    d.records.map((r) => decodeServer(r).kind),
    ['joined', 'bye'],
  )
  assert.deepEqual(parseDatagram(frame(1, new Uint8Array())).records, [])
  assert.throws(() => parseDatagram(new Uint8Array([1])), ProtocolError)
  assert.throws(() => parseDatagram(frame(1, g('s joined').subarray(0, 5))), ProtocolError)
  assert.throws(() => parseDatagram(frame(1, new Uint8Array([9]))), ProtocolError)
  assert.ok(seqNewer(0, 65535))
  assert.ok(!seqNewer(65535, 0))
  assert.ok(seqNewer(10, 9))
  assert.ok(!seqNewer(9, 9))
  assert.ok(!seqNewer(9, 9 + 0x8000))
})

test('pack fills datagrams up to the budget and sets oversize records aside', () => {
  const r = (n: number) => new Uint8Array(n).fill(n % 256)
  const { batches, oversize } = pack([r(400), r(400), r(400), r(2000), r(100)], 1000)
  assert.deepEqual(
    batches.map((b) => b.length),
    [800, 500],
  )
  assert.deepEqual(
    oversize.map((b) => b.length),
    [2000],
  )
  assert.deepEqual(pack([], 1000), { batches: [], oversize: [] })
})

test('records longer than the u16 length refuse to encode', () => {
  assert.throws(() => encodeClient({ kind: 'data', target: 'all', payload: new Uint8Array(70_000) }), ProtocolError)
})
