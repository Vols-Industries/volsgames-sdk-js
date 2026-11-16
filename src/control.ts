import type { Endpoints, Grant } from './room.ts'

export interface RoomSpec {
  game: string
  room?: string
  tick_rate?: number
  max_players?: number
  reconnect_grace_ms?: number
  empty_ttl_ms?: number
  flush_on_tick?: boolean
}

export interface RoomView extends Required<RoomSpec> {
  room: string
  players: number
  created_ms: number
  node?: string
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
