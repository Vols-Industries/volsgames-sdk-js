import { DATAGRAM_BUDGET, StreamBuffer } from './proto.ts'

export type Kind = 'webtransport' | 'websocket'

export interface Link {
  readonly kind: Kind
  readonly maxDatagram: number
  reliable(bytes: Uint8Array): void
  unreliable(frame: Uint8Array): void
  close(): void
}

export interface Sink {
  record(body: Uint8Array): void
  datagram(frame: Uint8Array): void
  closed(): void
}

function records(buf: StreamBuffer, sink: Sink) {
  for (let r = buf.pop(); r; r = buf.pop()) sink.record(r)
}

export function webSocket(url: string, sink: Sink, Ctor: typeof WebSocket): Promise<Link> {
  return new Promise((resolve, reject) => {
    const ws = new Ctor(url)
    ws.binaryType = 'arraybuffer'
    const buf = new StreamBuffer()
    let open = false
    const send = (lane: number, b: Uint8Array) => {
      if (ws.readyState !== 1) return
      const out = new Uint8Array(b.length + 1)
      out[0] = lane
      out.set(b, 1)
      ws.send(out)
    }
    ws.onopen = () => {
      open = true
      resolve({ kind: 'websocket', maxDatagram: DATAGRAM_BUDGET, reliable: (b) => send(0, b), unreliable: (f) => send(1, f), close: () => ws.close() })
    }
    ws.onclose = () => (open ? sink.closed() : reject(new Error(`websocket to ${url} failed`)))
    ws.onmessage = (e: MessageEvent) => {
      const b = new Uint8Array(e.data as ArrayBuffer)
      if (b[0] === 0) {
        buf.push(b.subarray(1))
        records(buf, sink)
      } else if (b[0] === 1) sink.datagram(b.subarray(1))
    }
  })
}

async function pump(stream: ReadableStream<Uint8Array>, fn: (chunk: Uint8Array) => void) {
  const reader = stream.getReader()
  try {
    for (;;) {
      const { value, done } = await reader.read()
      if (done) return
      fn(value)
    }
  } catch {
    return
  }
}

export async function webTransport(url: string, certHash: string | undefined, sink: Sink, Ctor: typeof WebTransport, signal?: AbortSignal): Promise<Link> {
  const options: WebTransportOptions = certHash ? { serverCertificateHashes: [{ algorithm: 'sha-256', value: Uint8Array.from(atob(certHash), (c) => c.charCodeAt(0)) }] } : {}
  const t = new Ctor(url, options)
  const shut = () => {
    try {
      t.close()
    } catch {
      return
    }
  }
  t.closed.catch(() => {})
  signal?.addEventListener('abort', shut)
  let bidi: WebTransportBidirectionalStream
  try {
    await t.ready
    bidi = await t.createBidirectionalStream()
  } catch (e) {
    shut()
    throw e
  }
  const writer = bidi.writable.getWriter()
  const datagrams = t.datagrams.writable.getWriter()
  const buf = new StreamBuffer()
  let ended = false
  const end = () => {
    if (ended) return
    ended = true
    sink.closed()
  }
  void pump(bidi.readable, (chunk) => {
    buf.push(chunk)
    records(buf, sink)
  }).then(end)
  void pump(t.datagrams.readable, (d) => sink.datagram(d))
  t.closed.then(end, end)
  return {
    kind: 'webtransport',
    maxDatagram: Math.min(t.datagrams.maxDatagramSize || DATAGRAM_BUDGET, DATAGRAM_BUDGET),
    reliable: (b) => void writer.write(b).catch(end),
    unreliable: (f) => void datagrams.write(f).catch(() => {}),
    close: () => {
      shut()
      end()
    },
  }
}
