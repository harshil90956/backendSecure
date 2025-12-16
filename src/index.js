import express from 'express';
import mongoose from 'mongoose';
import cors from 'cors';
import bcrypt from 'bcryptjs';
import User from './models/User.js';
import { ipSecurity, checkLoginAttempts, checkIPWhitelist } from './middleware/ipSecurity.js';
import { redisEnabled } from './redisAvailability.js';
import { getBullMqConnection, getQueuePrefix, getRedisTargetForLogs } from './bullmqConnection.js';
import { OUTPUT_PDF_QUEUE_NAME, MERGE_PDF_QUEUE_NAME } from '../queues/outputPdfQueue.js';

const app = express();
const PORT = process.env.PORT || 4000;

app.use(cors());
app.use(express.json({ limit: '900mb' }));
app.use(express.urlencoded({ extended: true, limit: '900mb' }));

app.use(ipSecurity);
app.use(checkLoginAttempts);

const authRoutes = (await import('./routes/auth.js')).default;
app.use('/api/auth', authRoutes);

app.use(checkIPWhitelist);
const securityRoutes = (await import('./routes/security.js')).default;
const adminRoutes = (await import('./routes/admin.js')).default;
const adminUsersRoutes = (await import('./routes/adminUsers.js')).default;
const docsRoutes = (await import('./routes/docs.js')).default;
const pdfRoutes = (await import('./routes/pdfRoutes.js')).default;

app.use('/api/security', securityRoutes);
app.use('/api/admin', adminRoutes);
app.use('/api/admin', adminUsersRoutes);
app.use('/api/docs', docsRoutes);
app.use('/api', pdfRoutes);

// Health check
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok' });
});

// Simple Redis connectivity test route (Phase 3)
app.get('/api/redis-test', async (req, res) => {
  try {
    if (!redisEnabled) {
      return res.status(503).json({ status: 'disabled', message: 'Redis is disabled' });
    }

    const { getRedisClient } = await import('./redisClient.js');
    const client = await getRedisClient();

    if (!client) {
      return res.status(503).json({ status: 'disabled', message: 'Redis is disabled' });
    }
    const key = 'redis-test-key';
    const value = `ok-${Date.now()}`;

    await client.set(key, value, { EX: 60 });
    const readBack = await client.get(key);

    res.json({
      status: 'ok',
      written: value,
      readBack,
      usingUrl: !!process.env.REDIS_URL,
    });
  } catch (err) {
    console.error('[index] /api/redis-test failed:', err);
    res.status(500).json({ status: 'error', message: 'Redis test failed' });
  }
});

// BullMQ queue health: lets you verify if jobs are waiting/active/failed and whether prefix matches.
app.get('/api/queue-health', async (req, res) => {
  try {
    if (!redisEnabled) {
      return res.status(503).json({ status: 'disabled', message: 'Queues are disabled (REDIS_DISABLED=true)' });
    }

    const connection = getBullMqConnection();

    const { Queue } = await import('bullmq');
    const baseOpts = { connection };

    const outputQueue = new Queue(OUTPUT_PDF_QUEUE_NAME, baseOpts);
    const mergeQueue = new Queue(MERGE_PDF_QUEUE_NAME, baseOpts);

    const [outputCounts, mergeCounts] = await Promise.all([
      outputQueue.getJobCounts('waiting', 'active', 'completed', 'failed', 'delayed', 'paused'),
      mergeQueue.getJobCounts('waiting', 'active', 'completed', 'failed', 'delayed', 'paused'),
    ]);

    await Promise.all([outputQueue.close(), mergeQueue.close()]);

    return res.json({
      status: 'ok',
      redisTarget: getRedisTargetForLogs(),
      prefix: null,
      queues: {
        [OUTPUT_PDF_QUEUE_NAME]: outputCounts,
        [MERGE_PDF_QUEUE_NAME]: mergeCounts,
      },
    });
  } catch (err) {
    console.error('[index] /api/queue-health failed:', err);
    return res.status(500).json({ status: 'error', message: 'Queue health failed' });
  }
});

app.get('/api/queue-failures', async (req, res) => {
  try {
    const debugEnabled =
      process.env.WORKER_DEBUG === '1' ||
      process.env.WORKER_DEBUG === 'true' ||
      process.env.WORKER_DEBUG === 'yes';

    const ip = req.ip;
    const isLocalhost =
      ip === '127.0.0.1' ||
      ip === '::1' ||
      ip === '::ffff:127.0.0.1' ||
      (typeof ip === 'string' && ip.startsWith('::ffff:127.'));

    if (!debugEnabled && process.env.NODE_ENV === 'production' && !isLocalhost) {
      return res.status(404).json({ status: 'not_found' });
    }

    if (!redisEnabled) {
      return res.status(503).json({ status: 'disabled', message: 'Queues are disabled (REDIS_DISABLED=true)' });
    }

    const connection = getBullMqConnection();
    const { Queue } = await import('bullmq');

    const limit = Math.max(1, Math.min(50, Number(req.query.limit || 10)));
    const outputQueue = new Queue(OUTPUT_PDF_QUEUE_NAME, { connection });
    const mergeQueue = new Queue(MERGE_PDF_QUEUE_NAME, { connection });

    const [outputFailedJobs, mergeFailedJobs] = await Promise.all([
      outputQueue.getJobs(['failed'], 0, limit - 1, true),
      mergeQueue.getJobs(['failed'], 0, limit - 1, true),
    ]);

    const serialize = (job) => {
      if (!job) return null;
      return {
        id: job.id,
        name: job.name,
        queue: job.queueName,
        attemptsMade: job.attemptsMade,
        failedReason: job.failedReason,
        stacktrace: job.stacktrace,
        timestamp: job.timestamp,
        processedOn: job.processedOn,
        finishedOn: job.finishedOn,
        data: job.data,
      };
    };

    await Promise.all([outputQueue.close(), mergeQueue.close()]);

    return res.json({
      status: 'ok',
      redisTarget: getRedisTargetForLogs(),
      limit,
      queues: {
        [OUTPUT_PDF_QUEUE_NAME]: outputFailedJobs.map(serialize).filter(Boolean),
        [MERGE_PDF_QUEUE_NAME]: mergeFailedJobs.map(serialize).filter(Boolean),
      },
    });
  } catch (err) {
    console.error('[index] /api/queue-failures failed:', err);
    return res.status(500).json({ status: 'error', message: 'Queue failures failed' });
  }
});

async function ensureAdminUser() {
  const seedEnabledRaw = typeof process.env.SEED_ADMIN === 'string' ? process.env.SEED_ADMIN.trim().toLowerCase() : '';
  const seedEnabled = seedEnabledRaw === 'true' || seedEnabledRaw === '1' || seedEnabledRaw === 'yes' || seedEnabledRaw === 'y';

  if (!seedEnabled) {
    return;
  }

  const adminEmail = typeof process.env.ADMIN_EMAIL === 'string' ? process.env.ADMIN_EMAIL.trim() : '';
  const adminPassword = typeof process.env.ADMIN_PASSWORD === 'string' ? process.env.ADMIN_PASSWORD : '';

  if (!adminEmail || !adminPassword) {
    console.error('Admin seeding enabled but ADMIN_EMAIL / ADMIN_PASSWORD missing');
    return;
  }

  const existing = await User.findOne({ email: adminEmail.toLowerCase() });
  if (existing) {
    
    return;
  }

  const passwordHash = await bcrypt.hash(adminPassword, 10);

  await User.create({
    email: adminEmail.toLowerCase(),
    passwordHash,
    role: 'admin',
  });

 
}

async function start() {
  try {
    const mongoUri = typeof process.env.MONGO_URI === 'string' ? process.env.MONGO_URI.trim() : '';

    if (!mongoUri) {
      if (process.env.NODE_ENV === 'development') {
        console.error('MONGO_URI is not set in environment (required)');
      } else {
        console.error('MONGO_URI is not set in environment (required in production)');
      }
      process.exit(1);
    }

    await mongoose.connect(mongoUri);
   

    await ensureAdminUser();

    const server = app.listen(PORT, () => {
      console.log(`Backend listening on port ${PORT}`);
    });

    // Handle low-level client connection errors like ECONNRESET gracefully
    server.on('clientError', (err, socket) => {
      if (err && (err.code === 'ECONNRESET' || err.code === 'EPIPE')) {
        try {
          socket.destroy();
        } catch (_) {
          // ignore
        }
        return;
      }

      console.error('HTTP client error:', err);
      try {
        socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
      } catch (_) {
        // ignore
      }
    });
  } catch (err) {
    console.error('Failed to start backend', err);
    process.exit(1);
  }
}

start();
