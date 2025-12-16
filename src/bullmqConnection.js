function parseBool(value) {
  if (typeof value !== 'string') return false;
  const v = value.trim().toLowerCase();
  return v === 'true' || v === '1' || v === 'yes' || v === 'y';
}

function parseIntEnv(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

export const redisDisabled = parseBool(process.env.REDIS_DISABLED);

export function getQueuePrefix() {
  return undefined;
}

export function resolveRedisUrlFromEnv() {
  const explicitUrl = typeof process.env.REDIS_URL === 'string' ? process.env.REDIS_URL.trim() : '';
  const url = explicitUrl || undefined;
  if (!url) return undefined;

  if (process.env.NODE_ENV !== 'development') {
    const lower = url.toLowerCase();
    if (lower.includes('localhost') || lower.includes('127.0.0.1')) {
      throw new Error('[bullmqConnection] Refusing to use localhost Redis outside development');
    }
  }

  return url;
}

export function getRedisTargetForLogs() {
  const url = resolveRedisUrlFromEnv();
  if (!url) return 'missing';

  try {
    const u = new URL(url);
    const port = u.port || (u.protocol === 'rediss:' ? '6380' : '6379');
    return `${u.hostname}:${port}${u.protocol === 'rediss:' ? ' (tls)' : ''}`;
  } catch {
    return 'url';
  }
}

let connectionSingleton;

export function getBullMqConnection() {
  if (redisDisabled) return null;
  if (connectionSingleton) return connectionSingleton;

  const url = resolveRedisUrlFromEnv();
  if (!url) {
    throw new Error('[bullmqConnection] Redis is enabled but no REDIS_URL provided');
  }

  connectionSingleton = {
    url,
  };

  return connectionSingleton;
}
