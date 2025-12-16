import { redisEnabled } from './redisAvailability.js';
import { resolveRedisUrlFromEnv, getRedisTargetForLogs } from './bullmqConnection.js';

let client;
let isConnecting = false;

export async function getRedisClient() {
  if (!redisEnabled) {
    return null;
  }

  if (client && client.isOpen) {
    return client;
  }

  if (!client) {
    const redisUrl = resolveRedisUrlFromEnv();
    if (!redisUrl) {
      throw new Error('[redisClient] Redis is enabled but no REDIS_URL or REDIS_HOST provided');
    }

    const { createClient } = await import('redis');

    client = createClient({
      url: redisUrl,
      socket: {
        connectTimeout: 10_000,
        reconnectStrategy: (retries) => {
          // Exponential backoff with cap
          const delay = Math.min(30_000, 200 * 2 ** Math.max(0, retries - 1));
          return delay;
        },
      },
    });

    client.on('error', (err) => {
      console.error('[redisClient] Redis Client Error:', err);
    });

    client.on('reconnecting', () => {
      console.warn('[redisClient] Redis reconnecting...', { target: getRedisTargetForLogs() });
    });

    client.on('ready', () => {
      console.log('[redisClient] Redis ready');
    });
  }

  if (!client.isOpen && !isConnecting) {
    isConnecting = true;
    try {
      await client.connect();
      console.log('[redisClient] Connected to Redis', { target: getRedisTargetForLogs() });
    } catch (err) {
      console.error('[redisClient] Failed to connect to Redis:', err);
      throw err;
    } finally {
      isConnecting = false;
    }
  }

  return client;
}
