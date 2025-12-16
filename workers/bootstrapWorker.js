import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

dotenv.config();

const envPath = path.resolve(__dirname, '../.env');
if (fs.existsSync(envPath)) {
  dotenv.config({ path: envPath, override: true });
}

process.on('unhandledRejection', (reason) => {
  console.error('[bootstrapWorker] unhandledRejection', reason);
  process.exit(1);
});

process.on('uncaughtException', (err) => {
  console.error('[bootstrapWorker] uncaughtException', err);
  process.exit(1);
});

const { logBootDiagnostics } = await import('../src/redisAvailability.js');
console.log('[bootstrapWorker] starting');
logBootDiagnostics();

try {
  console.log('[bootstrapWorker] importing pdfWorker');
  await import('./pdfWorker.js');
  console.log('[bootstrapWorker] pdfWorker imported');
} catch (err) {
  console.error('[bootstrapWorker] failed to start pdfWorker', err);
  process.exit(1);
}
