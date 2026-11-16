import { spawn } from 'node:child_process'
import { createSocket } from 'node:dgram'
import { mkdtempSync, rmSync } from 'node:fs'
import { type AddressInfo, createServer } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Control, type Events, type Room } from '../src/index.ts'

const bin = process.env.VOLSGAMES_BIN ?? new URL('../../volsgames/target/debug/volsgames', import.meta.url).pathname

function tcpPort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const s = createServer()
    s.on('error', reject)
    s.listen(0, '127.0.0.1', () => {
      const { port } = s.address() as AddressInfo
      s.close(() => resolve(port))
    })
  })
}

function udpPort(): Promise<number> {
  return new Promise((resolve) => {
    const s = createSocket('udp4')
    s.bind(0, '127.0.0.1', () => {
      const { port } = s.address()
      s.close(() => resolve(port))
    })
  })
}

export interface Engine {
  control: Control
  http: string
  stop(): void
}

export async function engine(): Promise<Engine> {
  const [quic, ws, http] = [await udpPort(), await tcpPort(), await tcpPort()]
  const dir = mkdtempSync(join(tmpdir(), 'volsgames-'))
  const key = 'sdk-test-key-0123456789'
  const args = ['serve', '--quic', `127.0.0.1:${quic}`, '--ws', `127.0.0.1:${ws}`, '--http', `127.0.0.1:${http}`, '--api-key', key, '--state-dir', dir, '--workers', '2']
  const child = spawn(bin, args, { stdio: ['ignore', 'ignore', 'inherit'] })
  const url = `http://127.0.0.1:${http}`
  const stop = () => {
    child.kill()
    rmSync(dir, { recursive: true, force: true })
  }
  for (const end = Date.now() + 15_000; ; ) {
    if (await fetch(`${url}/healthz`).then((r) => r.ok, () => false)) break
    if (Date.now() > end || child.exitCode !== null) {
      stop()
      throw new Error(`engine did not start: ${bin}`)
    }
    await new Promise((r) => setTimeout(r, 50))
  }
  return { control: new Control(url, key), http: url, stop }
}

export function once<K extends keyof Events>(room: Room, event: K): Promise<Events[K]> {
  return new Promise((resolve) => {
    const off = room.on(event, (...a) => {
      off()
      resolve(a)
    })
  })
}

export async function until(cond: () => boolean, ms = 5000) {
  for (const end = Date.now() + ms; !cond(); await new Promise((r) => setTimeout(r, 5))) {
    if (Date.now() > end) throw new Error('condition not met in time')
  }
}

export const text = (b: Uint8Array) => new TextDecoder().decode(b)
