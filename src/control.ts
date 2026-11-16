import type { Endpoints, Grant } from './room.ts'

export interface RoomSpec {
  game: string
  room?: string
  tick_rate?: number
  max_players?: number
  reconnect_grace_ms?: number
  empty_ttl_ms?: number
  flush_on_tick?: boolean
  max_state_bytes?: number
}

export interface RoomView extends Required<RoomSpec> {
  room: string
  players: number
  created_ms: number
  locked: boolean
  node?: string
}

export interface PlayerView {
  id: number
  player: string
  name: string
  connected: boolean
}

export interface PlayersView {
  host: number | null
  players: PlayerView[]
}

export interface StateEntry {
  scope: number
  key: string
  value: Uint8Array
  rev: number
  by: number
}

export interface StateView {
  rev: number
  entries: StateEntry[]
}

export interface StateWrite {
  key: string
  value: Uint8Array | string | null
  player?: string
  expect?: number
}

export interface Written {
  scope: number
  key: string
  rev: number
}

export interface PlayerSpec {
  player: string
  name?: string
  ttl_s?: number
}

export interface TokenGrant extends Grant {
  exp: number
  room: string
  node?: string
}

export interface NodeView {
  url: string
  up: boolean
  rooms: number
  players: number
}

const utf8 = new TextEncoder()

function toBase64(b: Uint8Array) {
  let s = ''
  for (const x of b) s += String.fromCharCode(x)
  return btoa(s)
}

function fromBase64(s: string) {
  return Uint8Array.from(atob(s), (c) => c.charCodeAt(0))
}

export class ControlError extends Error {
  readonly status: number
  readonly error: string
  constructor(status: number, error: string, detail: string) {
    super(`${status} ${error}: ${detail}`)
    this.status = status
    this.error = error
  }
}

export class Control {
  private readonly base: string
  private readonly key: string
  private readonly fetch: typeof fetch

  constructor(url: string, apiKey: string, fetchImpl: typeof fetch = globalThis.fetch) {
    this.base = url.replace(/\/+$/, '')
    this.key = apiKey
    this.fetch = (input, init) => fetchImpl(input, init)
  }

  private async call<T>(method: string, path: string, body?: unknown): Promise<T> {
    const headers: Record<string, string> = { authorization: `Bearer ${this.key}` }
    if (body !== undefined) headers['content-type'] = 'application/json'
    const res = await this.fetch(this.base + path, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) })
    const text = await res.text()
    let json: { error?: string; detail?: string } | undefined
    try {
      json = text ? JSON.parse(text) : undefined
    } catch {
      json = undefined
    }
    if (!res.ok) throw new ControlError(res.status, json?.error ?? 'http', json?.detail ?? text)
    return json as T
  }

  createRoom(spec: RoomSpec) {
    return this.call<RoomView>('POST', '/v1/rooms', spec)
  }

  rooms() {
    return this.call<RoomView[]>('GET', '/v1/rooms')
  }

  room(id: string) {
    return this.call<RoomView>('GET', `/v1/rooms/${encodeURIComponent(id)}`)
  }

  async closeRoom(id: string) {
    await this.call<void>('DELETE', `/v1/rooms/${encodeURIComponent(id)}`)
  }

  lock(id: string, locked = true) {
    return this.call<RoomView>('PATCH', `/v1/rooms/${encodeURIComponent(id)}`, { locked })
  }

  players(room: string) {
    return this.call<PlayersView>('GET', `/v1/rooms/${encodeURIComponent(room)}/players`)
  }

  async kick(room: string, player: string) {
    await this.call<void>('DELETE', `/v1/rooms/${encodeURIComponent(room)}/players/${encodeURIComponent(player)}`)
  }

  async state(room: string): Promise<StateView> {
    const v = await this.call<{ rev: number; entries: (Omit<StateEntry, 'value'> & { value: string })[] }>('GET', `/v1/rooms/${encodeURIComponent(room)}/state`)
    return { rev: v.rev, entries: v.entries.map((e) => ({ ...e, value: fromBase64(e.value) })) }
  }

  setState(room: string, { key, value, player, expect }: StateWrite) {
    const bytes = typeof value === 'string' ? utf8.encode(value) : value
    return this.call<Written>('POST', `/v1/rooms/${encodeURIComponent(room)}/state`, { key, value: bytes && toBase64(bytes), player, expect })
  }

  token(room: string, spec: PlayerSpec) {
    return this.call<TokenGrant>('POST', `/v1/rooms/${encodeURIComponent(room)}/tokens`, spec)
  }

  endpoints() {
    return this.call<Endpoints>('GET', '/v1/endpoints')
  }

  nodes() {
    return this.call<NodeView[]>('GET', '/v1/nodes')
  }
}
