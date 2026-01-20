# FX Ticker gRPC Proxy

This proxy connects to the Match-Trade gRPC quotations stream and exposes it as
a WebSocket JSON feed for the browser shortcode.

## Setup

1. Install dependencies:

```bash
cd proxy
npm install
```

2. Set environment variables:

```bash
setx BROKER_SYSTEM_UUID "70c5bd6d-14f5-4c8e-b6cf-6b4be3df0d9c"
setx BROKER_AUTH_TOKEN "PJEcEjqaSx3EQwtI0XYymAJuu6610mqGEWuYXbg_hmE="
setx BROKER_GRPC_HOST "grpc-broker-api-demo.match-trader.com:443"
setx PROXY_PORT "8787"
```

3. Start the proxy:

```bash
npm start
```

## WebSocket usage

Connect with instruments in the query string:

```
ws://localhost:8787?instruments=EURUSD,GBPUSD,USDJPY
```

Optional params:
- `group`
- `throttlingMs`
- `smartThrottling`
