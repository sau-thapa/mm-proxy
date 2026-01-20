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
setx BROKER_SYSTEM_UUID "<SYSTEM_UUID>"
setx BROKER_AUTH_TOKEN "<AUTH>"
setx BROKER_GRPC_HOST "<GRPC_HOST>"
setx PROXY_PORT "<POST>"
```

3. Start the proxy:

```bash
npm start
```

## WebSocket usage

Connect with instruments in the query string:

```
ws://<GRPC_HOST>?instruments=x,y,z
```

Optional params:
- `group`
- `throttlingMs`
- `smartThrottling`
