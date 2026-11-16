import assert from 'node:assert/strict'
import { after, before, test } from 'node:test'
import { Control, ControlError, VolsError, code, connect, reason } from '../src/index.ts'
import { type Engine, engine, once, text, until } from './engine.ts'

let e: Engine
before(async () => {
  e = await engine()
})
after(() => e.stop())

const grant = (room: string, player: string) => e.control.token(room, { player, name: player })

test('control api: rooms, tokens, endpoints, errors', async () => {
  const v = await e.control.createRoom({ game: 'g', room: 'ctl', tick_rate: 60, max_players: 2 })
  assert.equal(v.tick_rate, 60)
  await assert.rejects(e.control.createRoom({ game: 'g', room: 'ctl' }), (x) => x instanceof ControlError && x.status === 409 && x.error === 'exists')
  await assert.rejects(new Control(e.http, 'wrong').rooms(), (x) => x instanceof ControlError && x.status === 401)
  assert.equal((await e.control.room('ctl')).max_players, 2)
  assert.ok((await e.control.rooms()).some((r) => r.room === 'ctl'))
  const t = await grant('ctl', 'p')
  assert.equal(t.room, 'ctl')
  assert.ok(t.exp > Date.now() / 1000)
  assert.deepEqual(await e.control.endpoints(), t.endpoints)
  assert.ok(t.endpoints.websocket && t.endpoints.webtransport && t.endpoints.cert_hash)
  await e.control.closeRoom('ctl')
  await assert.rejects(e.control.room('ctl'), (x) => x instanceof ControlError && x.status === 404)
})

test('relay between two clients on every target and lane', async () => {
  await e.control.createRoom({ game: 'g', room: 'relay' })
  const a = await connect(await grant('relay', 'alice'))
  assert.equal(a.transport, 'websocket')
  assert.equal(a.player, 1)
  assert.equal(a.room, 'relay')
  assert.equal(a.tickRate, 30)
  assert.deepEqual([...a.players], [[1, 'alice']])
  const seen = once(a, 'joined')
  const b = await connect(await grant('relay', 'bob'))
  assert.deepEqual(await seen, [2, 'bob'])
  assert.deepEqual([...b.players.keys()].sort(), [1, 2])

  const got = once(b, 'data')
  a.send('hello')
  assert.deepEqual(await got, [1, new TextEncoder().encode('hello')])

  const both = Promise.all([once(a, 'data'), once(b, 'data')])
  a.send(new Uint8Array([1, 2, 3]), { to: 'all', reliable: false })
  assert.deepEqual(await both, [
    [1, new Uint8Array([1, 2, 3])],
    [1, new Uint8Array([1, 2, 3])],
  ])

  const dm = once(a, 'data')
  b.send('dm', { to: { player: 1 } })
  assert.equal(text((await dm)[1]), 'dm')

  b.subscribe(7)
  await b.ping()
  const group = once(b, 'data')
  a.send('group', { to: { group: 7 }, reliable: false })
  assert.equal(text((await group)[1]), 'group')
  b.unsubscribe(7)
  await b.ping()
  const next = once(b, 'data')
  a.send('nobody', { to: { group: 7 } })
  a.send('after', { to: { player: 2 } })
  assert.equal(text((await next)[1]), 'after')

  const burst: string[] = []
  const off = b.on('data', (_, p) => burst.push(text(p)))
  for (let i = 0; i < 40; i++) a.send(`u${i}`, { reliable: false })
  await until(() => burst.length === 40)
  off()
  assert.deepEqual(burst, Array.from({ length: 40 }, (_, i) => `u${i}`))

  const rtt = await a.ping()
  assert.ok(rtt >= 0 && rtt < 1000)
  assert.ok(Math.abs(a.serverNow() - Date.now()) < 1000)

  const left = once(a, 'left')
  assert.deepEqual(await b.leave(), { reason: reason.left })
  assert.deepEqual(await left, [2, reason.left])
  assert.deepEqual([...a.players.keys()], [1])
  assert.deepEqual(await a.leave(), { reason: reason.left })
})

test('refusals and room close carry their codes', async () => {
  const endpoints = await e.control.endpoints()
  await assert.rejects(connect({ token: 'not.a-token', endpoints }), (x) => x instanceof VolsError && x.code === code.badToken)
  await e.control.createRoom({ game: 'g', room: 'tiny', max_players: 1 })
  const one = await connect(await grant('tiny', 'p1'))
  await assert.rejects(connect(await grant('tiny', 'p2')), (x) => x instanceof VolsError && x.code === code.full)
  const later = await grant('tiny', 'p3')
  const closed = once(one, 'close')
  await e.control.closeRoom('tiny')
  assert.deepEqual(await closed, [{ reason: reason.roomClosed }])
  await assert.rejects(connect(later), (x) => x instanceof VolsError && x.code === code.noRoom)
})

function tracked(): [typeof WebSocket, WebSocket[]] {
  const sockets: WebSocket[] = []
  class Tracked extends WebSocket {
    constructor(url: string | URL, protocols?: string | string[]) {
      super(url, protocols)
      sockets.push(this)
    }
  }
  return [Tracked, sockets]
}

test('a dropped connection resumes the same player without the peer noticing', async () => {
  await e.control.createRoom({ game: 'g', room: 'resume' })
  const [WS, sockets] = tracked()
  let grants = 0
  const a = await connect(async () => (grants++, grant('resume', 'alice')), { WebSocket: WS })
  const b = await connect(await grant('resume', 'bob'))
  let peerSaw = false
  b.on('left', () => (peerSaw = true))
  const back = once(a, 'reconnected')
  sockets[0].close()
  await back
  assert.equal(a.player, 1)
  assert.equal(grants, 2)
  assert.equal(sockets.length, 2)
  const got = once(b, 'data')
  a.send('back')
  assert.deepEqual(await got, [1, new TextEncoder().encode('back')])
  assert.equal(peerSaw, false)
  await a.leave()
  await b.leave()
})

test('reconnect ends on a refusal or when the grant function throws', async () => {
  await e.control.createRoom({ game: 'g', room: 'gone' })
  const [WS1, s1] = tracked()
  const [WS2, s2] = tracked()
  const fixed = await connect(await grant('gone', 'fixed'), { WebSocket: WS1 })
  const fresh = await connect(() => grant('gone', 'fresh'), { WebSocket: WS2 })
  const closed = Promise.all([once(fixed, 'close'), once(fresh, 'close')])
  const retrying = Promise.all([once(fixed, 'reconnecting'), once(fresh, 'reconnecting')])
  s1[0].close()
  s2[0].close()
  await retrying
  await e.control.closeRoom('gone')
  assert.deepEqual(await closed, [[{ code: code.noRoom, message: 'no such room' }], [{ message: '404 no_room: no such room' }]])
})

test('shared state, host handover and backend controls', async () => {
  await e.control.createRoom({ game: 'g', room: 'state', max_players: 3 })
  const [WS, sockets] = tracked()
  const a = await connect(() => grant('state', 'alice'), { WebSocket: WS })
  assert.equal(a.host, 1)
  const phase = await a.set('phase', 'lobby', { expect: 0 })
  assert.equal(phase?.rev, 1)
  await a.set('ready', 'yes', { mine: true })
  assert.deepEqual(await e.control.setState('state', { key: 'seed', value: '42' }), { scope: 0, key: 'seed', rev: 3 })

  const b = await connect(await grant('state', 'bob'))
  assert.equal(b.host, 1)
  assert.equal(text(b.state().get('phase')!.value), 'lobby')
  assert.equal(text(b.state(1).get('ready')!.value), 'yes')
  assert.equal(b.state().get('seed')!.by, 0)
  await assert.rejects(b.set('seed', 'x'), (x) => x instanceof VolsError && x.code === code.forbidden)
  await assert.rejects(b.set('phase', 'play', { expect: 0 }), (x) => x instanceof VolsError && x.code === code.conflict)
  const seen = once(a, 'state')
  assert.equal((await b.set('phase', 'play', { expect: phase!.rev }))?.rev, 4)
  const [key, entry, scope] = await seen
  assert.deepEqual([key, entry && text(entry.value), entry?.by, scope], ['phase', 'play', 2, 0])

  const view = await e.control.players('state')
  assert.equal(view.host, 1)
  assert.deepEqual(
    view.players.map((p) => [p.id, p.player, p.connected]),
    [
      [1, 'alice', true],
      [2, 'bob', true],
    ],
  )
  const st = await e.control.state('state')
  assert.equal(st.rev, 4)
  assert.equal(text(st.entries.find((x) => x.key === 'phase')!.value), 'play')
  await assert.rejects(e.control.setState('state', { key: 'phase', value: null, expect: 1 }), (x) => x instanceof ControlError && x.status === 409)

  const moved = once(b, 'host')
  const back = once(a, 'reconnected')
  sockets[0].close()
  assert.deepEqual(await moved, [2])
  await back
  assert.equal(a.host, 2)
  assert.equal(text(a.state().get('phase')!.value), 'play')

  await e.control.lock('state')
  await assert.rejects(connect(await grant('state', 'carol')), (x) => x instanceof VolsError && x.code === code.locked)
  const kicked = once(a, 'close')
  const gone = once(b, 'left')
  await e.control.kick('state', 'alice')
  assert.deepEqual(await kicked, [{ reason: reason.kicked }])
  assert.deepEqual(await gone, [1, reason.kicked])
  assert.equal(b.state(1).size, 0)
  assert.equal((await e.control.lock('state', false)).locked, false)
  await assert.rejects(connect(await grant('state', 'alice')), (x) => x instanceof VolsError && x.code === code.forbidden)
  await assert.rejects(e.control.kick('state', 'alice'), (x) => x instanceof ControlError && x.error === 'no_player')
  await b.leave()
})
