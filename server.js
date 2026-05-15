'use strict';

require('dotenv').config();

const express = require('express');
const cors = require('cors');
const Redis = require('ioredis');

const PORT = Number(process.env.PORT) || 3000;
const MAX_KEY_LEN = 512;
const MAX_VALUE_BYTES = 512 * 1024;

function log(stage, payload = {}) {
  const ts = new Date().toISOString();
  console.log(`[${ts}] [redis-api] ${stage}`, payload);
}

function requireEnv(name) {
  const v = process.env[name];
  if (!v || !String(v).trim()) {
    log('fatal_missing_env', { name });
    process.exit(1);
  }
  return String(v).trim();
}

const REDIS_HOST = requireEnv('REDIS_HOST');
requireEnv('REDIS_PASSWORD');

if (/^your-redis\.rds\.aliyuncs\.com$/i.test(REDIS_HOST)) {
  log('fatal_redis_host_placeholder', {
    REDIS_HOST,
    hint:
      '请把 .env 里的 REDIS_HOST 改成阿里云 Redis 控制台里的「连接地址」（形如 r-xxxx.redis.rds.aliyuncs.com），不要保留 .env.example 里的示例域名',
  });
  process.exit(1);
}

const REDIS_USERNAME = process.env.REDIS_USERNAME || 'default';

const tlsEnabled = String(process.env.REDIS_TLS || '')
  .toLowerCase()
  .trim();
const useTls = tlsEnabled === '1' || tlsEnabled === 'true' || tlsEnabled === 'yes';

const redis = new Redis({
  host: REDIS_HOST,
  port: Number(process.env.REDIS_PORT || 6379),
  username: REDIS_USERNAME,
  password: process.env.REDIS_PASSWORD,
  ...(useTls ? { tls: {} } : {}),
  lazyConnect: true,
  maxRetriesPerRequest: 2,
  enableReadyCheck: true,
});

redis.on('connect', () => log('redis_connect', {}));
redis.on('ready', () => log('redis_ready', {}));
redis.on('error', (err) => log('redis_error', { message: err.message }));
redis.on('close', () => log('redis_close', {}));

const app = express();
app.disable('x-powered-by');
app.use(
  cors({
    origin: true,
    methods: ['GET', 'HEAD', 'POST', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-API-Key'],
  })
);
app.use(express.json({ limit: '512kb' }));

const API_KEY = (process.env.API_KEY || '').trim();

function guardApi(req, res, next) {
  if (!API_KEY) return next();

  const header = req.get('authorization') || '';
  const bearer = /^Bearer\s+(.+)$/i.exec(header)?.[1];
  const apiKeyHeader = req.get('x-api-key');
  const token = bearer || apiKeyHeader;

  if (token !== API_KEY) {
    log('auth_denied', { path: req.path });
    res.status(401).json({ ok: false, error: 'unauthorized' });
    return;
  }
  next();
}

function validateKey(key) {
  if (typeof key !== 'string' || !key.trim()) {
    return { ok: false, error: 'invalid_key' };
  }
  const k = key.trim();
  if (k.length > MAX_KEY_LEN) {
    return { ok: false, error: 'key_too_long' };
  }
  return { ok: true, key: k };
}

app.get('/', (_req, res) => {
  res.status(200).json({
    ok: true,
    service: 'lingmao-redis-api',
    uptimeSec: Math.floor(process.uptime()),
  });
});

app.get('/health', async (_req, res) => {
  try {
    await redis.ping();
    res.status(200).json({ ok: true, redis: 'up' });
  } catch (e) {
    log('health_fail', { message: e.message });
    res.status(503).json({ ok: false, redis: 'down' });
  }
});

app.use('/api', guardApi);

app.post('/api/kv/get', async (req, res) => {
  const vk = validateKey(req.body?.key);
  if (!vk.ok) {
    res.status(400).json({ ok: false, error: vk.error });
    return;
  }
  try {
    const val = await redis.get(vk.key);
    log('kv_get', { key: vk.key, hit: val !== null });
    res.status(200).json({ ok: true, exists: val !== null, value: val });
  } catch (e) {
    log('kv_get_error', { key: vk.key, message: e.message });
    res.status(500).json({ ok: false, error: 'redis_error' });
  }
});

app.post('/api/kv/set', async (req, res) => {
  const vk = validateKey(req.body?.key);
  if (!vk.ok) {
    res.status(400).json({ ok: false, error: vk.error });
    return;
  }
  const raw = req.body?.value;
  let valueStr;
  if (raw === undefined || raw === null) {
    valueStr = '';
  } else if (typeof raw === 'string') {
    valueStr = raw;
  } else {
    try {
      valueStr = JSON.stringify(raw);
    } catch {
      res.status(400).json({ ok: false, error: 'invalid_value' });
      return;
    }
  }
  const bytes = Buffer.byteLength(valueStr, 'utf8');
  if (bytes > MAX_VALUE_BYTES) {
    res.status(413).json({ ok: false, error: 'value_too_large' });
    return;
  }

  const ttlRaw = req.body?.ttlSeconds ?? req.body?.ttl;
  let ttlSeconds;
  if (ttlRaw !== undefined && ttlRaw !== null && ttlRaw !== '') {
    ttlSeconds = Number(ttlRaw);
    if (
      !Number.isFinite(ttlSeconds) ||
      ttlSeconds !== Math.floor(ttlSeconds) ||
      ttlSeconds <= 0
    ) {
      res.status(400).json({ ok: false, error: 'invalid_ttl' });
      return;
    }
  }

  try {
    if (ttlSeconds) {
      await redis.set(vk.key, valueStr, 'EX', ttlSeconds);
      log('kv_set', { key: vk.key, ttlSeconds, bytes });
    } else {
      await redis.set(vk.key, valueStr);
      log('kv_set', { key: vk.key, ttlSeconds: null, bytes });
    }
    res.status(200).json({ ok: true });
  } catch (e) {
    log('kv_set_error', { key: vk.key, message: e.message });
    res.status(500).json({ ok: false, error: 'redis_error' });
  }
});

async function handleKvDelete(req, res) {
  const vk = validateKey(req.body?.key);
  if (!vk.ok) {
    res.status(400).json({ ok: false, error: vk.error });
    return;
  }
  try {
    const removed = await redis.del(vk.key);
    log('kv_del', { key: vk.key, removed });
    res.status(200).json({ ok: true, removed });
  } catch (e) {
    log('kv_del_error', { key: vk.key, message: e.message });
    res.status(500).json({ ok: false, error: 'redis_error' });
  }
}

app.delete('/api/kv/delete', handleKvDelete);
app.post('/api/kv/delete', handleKvDelete);

app.use((_req, res) => res.status(404).json({ ok: false, error: 'not_found' }));

(async function main() {
  const pwdChars = String(process.env.REDIS_PASSWORD || '').length;
  log('bootstrap', {
    port: PORT,
    useTls,
    redisUsername: REDIS_USERNAME,
    redisPasswordChars: pwdChars,
  });
  await redis.connect();
  app.listen(PORT, () => log('listening', { port: PORT }));
})().catch((e) => {
  log('fatal', { message: e.message });
  process.exit(1);
});
