const http = require("http");
const fs = require("fs");
const fsp = require("fs/promises");
const path = require("path");
const url = require("url");
const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
const WebSocket = require("ws");

require("dotenv").config();

const PORT = process.env.PORT || process.env.PROXY_PORT || 8787;
const GRPC_HOST = process.env.BROKER_GRPC_HOST || "grpc-broker-api-demo.match-trader.com:443";
const SYSTEM_UUID = process.env.BROKER_SYSTEM_UUID;
const AUTH_TOKEN = process.env.BROKER_AUTH_TOKEN;
const CACHE_FILE_PATH = process.env.CACHE_FILE_PATH || path.join(__dirname, "data", "quote-cache.json");
const CACHE_FLUSH_MS = Math.max(500, parseInt(process.env.CACHE_FLUSH_MS, 10) || 5000);

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

const quoteCache = new Map();
let cacheFlushTimer = null;
let cacheDirty = false;

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

function getCacheKey(group) {
  return group ? String(group) : "";
}

function cacheQuote(group, quotation) {
  if (!quotation || !quotation.symbol) return;
  const key = getCacheKey(group);
  if (!quoteCache.has(key)) {
    quoteCache.set(key, new Map());
  }
  const groupCache = quoteCache.get(key);
  groupCache.set(quotation.symbol, quotation);
  cacheDirty = true;
}

function getCachedQuote(group, symbol) {
  const groupCache = quoteCache.get(getCacheKey(group));
  if (!groupCache) return null;
  return groupCache.get(symbol) || null;
}

function serializeCache() {
  const groups = {};
  quoteCache.forEach((groupCache, groupKey) => {
    const sorted = Array.from(groupCache.values()).sort((a, b) => {
      const ta = a && a.timestampInMillis ? Number(a.timestampInMillis) : 0;
      const tb = b && b.timestampInMillis ? Number(b.timestampInMillis) : 0;
      return tb - ta;
    });
    groups[groupKey] = sorted.slice(0, 500);
  });
  return { groups };
}

function hydrateCacheFromFile() {
  try {
    if (!fs.existsSync(CACHE_FILE_PATH)) return;
    const raw = fs.readFileSync(CACHE_FILE_PATH, "utf8");
    if (!raw) return;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || !parsed.groups) return;
    Object.keys(parsed.groups).forEach((groupKey) => {
      const entries = parsed.groups[groupKey];
      if (!Array.isArray(entries)) return;
      const map = new Map();
      entries.forEach((quote) => {
        if (!quote || !quote.symbol) return;
        map.set(quote.symbol, quote);
      });
      if (map.size) {
        quoteCache.set(groupKey, map);
      }
    });
  } catch (err) {
    console.warn("Failed to load cache file:", err && err.message ? err.message : String(err));
  }
}

async function flushCacheToFile() {
  if (!cacheDirty) return;
  cacheDirty = false;
  try {
    await fsp.mkdir(path.dirname(CACHE_FILE_PATH), { recursive: true });
    const payload = JSON.stringify(serializeCache());
    await fsp.writeFile(CACHE_FILE_PATH, payload, "utf8");
  } catch (err) {
    console.warn("Failed to write cache file:", err && err.message ? err.message : String(err));
  }
}

function scheduleCacheFlush() {
  if (cacheFlushTimer) return;
  cacheFlushTimer = setInterval(() => {
    flushCacheToFile();
  }, CACHE_FLUSH_MS);
}

function collectSnapshot(instruments, group, timeoutMs) {
  return new Promise((resolve) => {
    const result = new Map();
    instruments.forEach((symbol) => {
      const cached = getCachedQuote(group, symbol);
      if (cached) {
        result.set(symbol, cached);
      }
    });

    if (result.size === instruments.length) {
      resolve({
        quotes: Array.from(result.values()),
        missing: [],
        source: "cache"
      });
      return;
    }

    const request = {
      systemUuid: SYSTEM_UUID,
      instruments: instruments
    };
    if (group) {
      request.group = String(group);
    }

    const metadata = new grpc.Metadata();
    metadata.set("authorization", normalizeToken(AUTH_TOKEN));

    const call = grpcClient.getQuotationsWithMarkupStream(request, metadata);

    const timer = setTimeout(() => {
      call.cancel();
      resolve({
        quotes: Array.from(result.values()),
        missing: instruments.filter((symbol) => !result.has(symbol)),
        source: "stream-timeout"
      });
    }, timeoutMs);

    call.on("data", (response) => {
      if (!response.quotation) return;
      const quotation = response.quotation;
      result.set(quotation.symbol, quotation);
      cacheQuote(group, quotation);
      if (result.size === instruments.length) {
        clearTimeout(timer);
        call.cancel();
        resolve({
          quotes: Array.from(result.values()),
          missing: [],
          source: "stream"
        });
      }
    });

    call.on("error", () => {
      clearTimeout(timer);
      resolve({
        quotes: Array.from(result.values()),
        missing: instruments.filter((symbol) => !result.has(symbol)),
        source: "stream-error"
      });
    });

    call.on("end", () => {
      clearTimeout(timer);
      resolve({
        quotes: Array.from(result.values()),
        missing: instruments.filter((symbol) => !result.has(symbol)),
        source: "stream-end"
      });
    });
  });
}

const server = http.createServer(async (req, res) => {
  const parsedUrl = url.parse(req.url || "", true);
  const pathname = parsedUrl.pathname || "/";

  if (pathname === "/snapshot") {
    const query = parsedUrl.query || {};
    const instruments = parseInstruments(query.instruments);
    const group = query.group ? String(query.group) : "";
    const timeoutMs = Math.max(500, Math.min(5000, parseInt(query.timeoutMs, 10) || 1500));

    res.setHeader("access-control-allow-origin", "*");
    res.setHeader("content-type", "application/json");

    if (!instruments.length) {
      res.statusCode = 400;
      res.end(JSON.stringify({ error: "Missing instruments list." }));
      return;
    }

    try {
      const snapshot = await collectSnapshot(instruments, group, timeoutMs);
      res.statusCode = 200;
      res.end(JSON.stringify(snapshot));
    } catch (err) {
      res.statusCode = 500;
      res.end(JSON.stringify({ error: "Snapshot failed." }));
    }
    return;
  }

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
      cacheQuote(request.group, quotation);
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

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Proxy listening on port ${PORT}`);
});

hydrateCacheFromFile();
scheduleCacheFlush();
