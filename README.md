# volsgames-sdk-js

Client SDK for VolsGames: WebTransport with WebSocket fallback, rooms, relay, reconnect.
Browsers and Node 22+, no runtime dependencies.

```
npm install github:Vols-Industries/volsgames-sdk-js
```

```js
import { connect } from '@volsgames/sdk'

const room = await connect(await fetch('/join').then((r) => r.json()))
room.on('data', (from, payload) => {})
room.send('hello', { to: 'others', reliable: false })
```

Tests: `npm test`; `npm run e2e` runs against `../volsgames/target/debug/volsgames` or `VOLSGAMES_BIN`.

Documentation: https://volsgames.com/docs#sdk

Licence: Apache-2.0, see LICENSE.
