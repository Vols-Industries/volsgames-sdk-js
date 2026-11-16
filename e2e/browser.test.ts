import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { createServer } from 'node:http'
import type { AddressInfo } from 'node:net'
import { test } from 'node:test'
import { type Browser, chromium } from 'playwright-core'
import { type Grant, connect, reason } from '../src/index.ts'
import { engine, once, text } from './engine.ts'

type Sdk = typeof import('../src/index.ts')
type Page = { room: Awaited<ReturnType<Sdk['connect']>>; got: [number, string][] }

let browser: Browser | undefined
try {
  browser = await chromium.launch({ channel: 'chrome', headless: true })
} catch {
  browser = undefined
}

test('chrome over webtransport relays both lanes to a websocket peer, and falls back', { skip: !browser && !process.env.CI && 'Chrome not installed' }, async () => {
  const e = await engine()
  const dist = new URL('../dist/', import.meta.url)
  const site = createServer(async (req, res) => {
    if (req.url === '/') return res.writeHead(200, { 'content-type': 'text/html' }).end('<!doctype html><title>sdk</title>')
    const file = await readFile(new URL(`.${req.url!.replace(/^\/dist/, '')}`, dist)).catch(() => undefined)
    if (!file) return res.writeHead(404).end()
    res.writeHead(200, { 'content-type': 'text/javascript' }).end(file)
  })
  await new Promise<void>((r) => site.listen(0, '127.0.0.1', r))
  const page = await browser!.newPage()
  try {
    await page.goto(`http://127.0.0.1:${(site.address() as AddressInfo).port}/`)
    await e.control.createRoom({ game: 'g', room: 'web' })
    const kind = await page.evaluate(async (grant: Grant) => {
      const sdk: Sdk = await import(String('/dist/index.js'))
      const w = window as unknown as Page
      w.got = []
      w.room = await sdk.connect(grant, { transport: 'webtransport' })
      w.room.on('data', (from, p) => w.got.push([from, new TextDecoder().decode(p)]))
      return w.room.transport
    }, await e.control.token('web', { player: 'chrome' }))
    assert.equal(kind, 'webtransport')

    const peer = await connect(await e.control.token('web', { player: 'node' }))
    const fast = once(peer, 'data')
    await page.evaluate(() => (window as unknown as Page).room.send('datagram from chrome', { reliable: false }))
    const [from, payload] = await fast
    assert.equal(from, 1)
    assert.equal(text(payload), 'datagram from chrome')
    const sure = once(peer, 'data')
    await page.evaluate(() => (window as unknown as Page).room.send('stream from chrome'))
    assert.equal(text((await sure)[1]), 'stream from chrome')

    peer.send('stream to chrome')
    peer.send('datagram to chrome', { reliable: false })
    await page.waitForFunction(() => (window as unknown as Page).got.length === 2)
    const got = await page.evaluate(() => (window as unknown as Page).got)
    assert.deepEqual(got.sort(), [
      [2, 'datagram to chrome'],
      [2, 'stream to chrome'],
    ])
    const rtt = await page.evaluate(() => (window as unknown as Page).room.ping())
    assert.ok(rtt >= 0 && rtt < 1000)

    const left = once(peer, 'left')
    await page.evaluate(() => (window as unknown as Page).room.leave())
    assert.deepEqual(await left, [1, reason.left])

    const fallback = await page.evaluate(async (grant: Grant) => {
      const sdk: Sdk = await import(String('/dist/index.js'))
      const room = await sdk.connect({ ...grant, endpoints: { ...grant.endpoints, webtransport: 'https://127.0.0.1:9/' } }, { fallbackMs: 500 })
      const kind = room.transport
      await room.leave()
      return kind
    }, await e.control.token('web', { player: 'fallback' }))
    assert.equal(fallback, 'websocket')
    await peer.leave()
  } finally {
    await page.close()
    await browser!.close()
    site.close()
    e.stop()
  }
})
