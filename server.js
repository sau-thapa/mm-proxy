const http = require("http");
const path = require("path");
const url = require("url");
const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
const WebSocket = require("ws");

require("dotenv").config();

const PORT = process.env.PROXY_PORT || 8787;
const GRPC_HOST = process.env.BROKER_GRPC_HOST || "grpc-broker-api-demo.match-trader.com:443";
const SYSTEM_UUID = process.env.BROKER_SYSTEM_UUID;
const AUTH_TOKEN = process.env.BROKER_AUTH_TOKEN;

if (!SYSTEM_UUID || !AUTH_TOKEN) {
  console.error("Missing BROKER_SYSTEM_UUID or BROKER_AUTH_TOKEN.");
  process.exit(1);
}

const PROTO_PATH = path.join(__dirname, "proto", "broker_api.proto");
const packageDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});

const grpcPackage = grpc.loadPackageDefinition(packageDef).com.matchtrade.broker_api.grpc;
const grpcClient = new grpcPackage.QuotationsServiceExternal(
  GRPC_HOST,
  grpc.credentials.createSsl()
);

function normalizeToken(token) {
  if (!token) return "";
  return token.toLowerCase().startsWith("bearer ") ? token : "Bearer " + token;
}

function parseBool(value) {
  if (value === undefined) return undefined;
  return value === "true" || value === "1";
}

function parseInstruments(value) {
  if (!value) return [];
  return value
    .split(",")
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
}

const server = http.createServer((req, res) => {
  res.writeHead(200, { "content-type": "text/plain" });
  res.end("FX ticker gRPC proxy is running.");
});

const wss = new WebSocket.Server({ server });

wss.on("connection", (ws, req) => {
  const parsedUrl = url.parse(req.url || "", true);
  const query = parsedUrl.query || {};
  const instruments = parseInstruments(query.instruments);

  if (!instruments.length) {
    ws.close(1008, "Missing instruments list.");
    return;
  }

  const request = {
    systemUuid: SYSTEM_UUID,
    instruments: instruments
  };

  if (query.group) {
    request.group = String(query.group);
  }
  if (query.throttlingMs) {
    const throttlingMs = parseInt(query.throttlingMs, 10);
    if (!Number.isNaN(throttlingMs)) {
      request.throttlingMs = throttlingMs;
    }
  }
  if (query.smartThrottling !== undefined) {
    request.smartThrottling = parseBool(String(query.smartThrottling));
  }

  const metadata = new grpc.Metadata();
  metadata.set("authorization", normalizeToken(AUTH_TOKEN));

  const call = grpcClient.getQuotationsWithMarkupStream(request, metadata);

  const send = (payload) => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(payload));
    }
  };

  call.on("data", (response) => {
    if (response.quotation) {
      const quotation = response.quotation;
      send({
        type: "quotation",
        symbol: quotation.symbol,
        bidPrice: quotation.bidPrice,
        askPrice: quotation.askPrice,
        timestampInMillis: quotation.timestampInMillis,
        dailyStatistics: quotation.dailyStatistics || null
      });
    } else if (response.heartbeat) {
      send({ type: "heartbeat" });
    }
  });

  call.on("error", (err) => {
    const details = err && err.details ? err.details : "Unknown error";
    const code = err && typeof err.code !== "undefined" ? err.code : "unknown";
    console.error("gRPC stream error:", { code, details });
    send({ type: "error", message: "gRPC stream error.", code: code, details: details });
    ws.close(1011, "gRPC stream error.");
  });

  call.on("end", () => {
    ws.close(1000, "gRPC stream ended.");
  });

  ws.on("close", () => {
    call.cancel();
  });
});

server.listen(PORT, () => {
  console.log(`Proxy listening on port ${PORT}`);
});
