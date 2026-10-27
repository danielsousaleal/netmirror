# netmirror UI

Next.js App Router dashboard for the netmirror HTTP gateway.

## Development

Start the C gateway on port 8080, then:

```bash
npm install
npm run dev
```

Open [http://localhost:3000](http://localhost:3000). The dashboard polls
`/api/health` every five seconds and shows the gateway/node status.

## API proxy

`next.config.ts` rewrites `/api/*` to `http://localhost:8080/*`, so browser
requests are same-origin during development. Override the target with
`NETMIRROR_GATEWAY`, for example:

```bash
NETMIRROR_GATEWAY=http://127.0.0.1:8080 npm run dev
```

The gateway also sends permissive CORS headers for direct access.

## Build

```bash
npm run build
npm run start
```
