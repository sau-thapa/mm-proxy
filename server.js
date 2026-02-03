const http = require("http");
const https = require("https");
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
const WP_CACHE_URL = process.env.WP_CACHE_URL || "";
const WP_CACHE_TOKEN = process.env.WP_CACHE_TOKEN || "";
const WP_CACHE_FLUSH_MS = Math.max(500, parseInt(process.env.WP_CACHE_FLUSH_MS, 10) || 2000);

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
const pendingWpUpdates = new Map();
let wpFlushTimer = null;

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

function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (chunk) => {
      data += chunk;
      if (data.length > 1_000_000) {
        reject(new Error("Payload too large."));
        req.destroy();
      }
    });
    req.on("end", () => {
      if (!data) {
        resolve(null);
        return;
      }
      try {
        resolve(JSON.parse(data));
      } catch (err) {
        reject(err);
      }
    });
  });
}

function requestJson(method, targetUrl, body) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(targetUrl);
    const payload = body ? JSON.stringify(body) : null;
    const isHttps = parsed.protocol === "https:";
    const client = isHttps ? https : http;

    const options = {
      method: method,
      hostname: parsed.hostname,
      port: parsed.port || (isHttps ? 443 : 80),
      path: parsed.pathname + parsed.search,
      headers: {
        "accept": "application/json"
      }
    };

    if (payload) {
      options.headers["content-type"] = "application/json";
      options.headers["content-length"] = Buffer.byteLength(payload);
    }

    const req = client.request(options, (res) => {
      let data = "";
      res.on("data", (chunk) => {
        data += chunk;
      });
      res.on("end", () => {
        if (!data) {
          resolve({ status: res.statusCode, json: null });
          return;
        }
        try {
          resolve({ status: res.statusCode, json: JSON.parse(data) });
        } catch (err) {
          resolve({ status: res.statusCode, json: null });
        }
      });
    });

    req.on("error", reject);
    if (payload) {
      req.write(payload);
    }
    req.end();
  });
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
}

function getCachedQuote(group, symbol) {
  const groupCache = quoteCache.get(getCacheKey(group));
  if (!groupCache) return null;
  return groupCache.get(symbol) || null;
}

function enqueueWpUpdate(group, quotation) {
  if (!WP_CACHE_URL || !WP_CACHE_TOKEN || !quotation || !quotation.symbol) return;
  const key = getCacheKey(group);
  if (!pendingWpUpdates.has(key)) {
    pendingWpUpdates.set(key, new Map());
  }
  pendingWpUpdates.get(key).set(quotation.symbol, quotation);
}

async function flushWpUpdates() {
  if (!WP_CACHE_URL || !WP_CACHE_TOKEN || pendingWpUpdates.size === 0) return;
  const batches = Array.from(pendingWpUpdates.entries());
  pendingWpUpdates.clear();

  await Promise.all(batches.map(async ([groupKey, quotesMap]) => {
    const payload = {
      token: WP_CACHE_TOKEN,
      group: groupKey || "",
      quotes: Array.from(quotesMap.values())
    };
    try {
      const response = await requestJson("POST", WP_CACHE_URL, payload);
      if (!response || response.status < 200 || response.status >= 300) {
        console.warn("[WP Cache] POST failed:", {
          status: response ? response.status : "no-response",
          group: groupKey || "",
          count: payload.quotes.length
        });
      } else {
        console.log("[WP Cache] POST ok:", {
          status: response.status,
          group: groupKey || "",
          count: payload.quotes.length
        });
      }
    } catch (err) {
      console.warn("[WP Cache] POST error:", {
        group: groupKey || "",
        count: payload.quotes.length,
        message: err && err.message ? err.message : String(err)
      });
    }
  }));
}

function scheduleWpFlush() {
  if (!WP_CACHE_URL || !WP_CACHE_TOKEN) return;
  if (wpFlushTimer) return;
  wpFlushTimer = setInterval(() => {
    flushWpUpdates();
  }, WP_CACHE_FLUSH_MS);
}

async function hydrateFromWpCache(instruments, group, result) {
  if (!WP_CACHE_URL) return result;
  let query = "instruments=" + encodeURIComponent(instruments.join(","));
  if (group) {
    query += "&group=" + encodeURIComponent(group);
  }
  if (WP_CACHE_TOKEN) {
    query += "&token=" + encodeURIComponent(WP_CACHE_TOKEN);
  }
  const targetUrl = WP_CACHE_URL.indexOf("?") === -1 ?
    WP_CACHE_URL + "?" + query :
    WP_CACHE_URL + "&" + query;

  try {
    const response = await requestJson("GET", targetUrl, null);
    if (response.status !== 200 || !response.json || !Array.isArray(response.json.quotes)) {
      return result;
    }
    response.json.quotes.forEach((quote) => {
      if (!quote || !quote.symbol) return;
      result.set(quote.symbol, quote);
    });
  } catch (err) {
    // Ignore WP cache failures; fallback to stream.
  }

  return result;
}

function collectSnapshot(instruments, group, timeoutMs) {
  return new Promise(async (resolve) => {
    const result = new Map();
    instruments.forEach((symbol) => {
      const cached = getCachedQuote(group, symbol);
      if (cached) {
        result.set(symbol, cached);
      }
    });

    if (result.size < instruments.length) {
      await hydrateFromWpCache(instruments.filter((symbol) => !result.has(symbol)), group, result);
    }

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
      enqueueWpUpdate(request.group, quotation);
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

scheduleWpFlush();
