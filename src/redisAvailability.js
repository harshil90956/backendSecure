import { getRedisTargetForLogs, redisDisabled } from './bullmqConnection.js';

export const redisMode = process.env.REDIS_MODE;
export const redisEnabled = !redisDisabled;

export function logBootDiagnostics() {
  console.log(`[BOOT] REDIS_DISABLED = ${redisDisabled}`);
  console.log(`[BOOT] redisEnabled = ${redisEnabled}`);
  console.log(`[BOOT] REDIS_MODE = ${process.env.REDIS_MODE}`);

  if (!redisEnabled) {
    console.log('[BOOT] redisTarget = disabled');
    return;
  }

  console.log(`[BOOT] redisTarget = ${getRedisTargetForLogs()}`);
}

export function assertRedisModeDefinedInDev() {
  if (process.env.NODE_ENV === 'development' && typeof process.env.REDIS_DISABLED === 'undefined') {
    // Keep this strict in dev so you never accidentally enqueue jobs to production Redis.
    throw new Error('[BOOT] REDIS_DISABLED is required when NODE_ENV=development (set true for local w/o Redis)');
  }
}

export function warnRedisDisabled(action) {
  console.warn(`[queue] skipped (${action})`);
}
