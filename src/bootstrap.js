import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

dotenv.config({ path: path.resolve(__dirname, '../.env'), override: true });

if (!process.env.NODE_ENV) {
  process.env.NODE_ENV = 'development';
}

const { assertRedisModeDefinedInDev, logBootDiagnostics } = await import('./redisAvailability.js');
assertRedisModeDefinedInDev();
logBootDiagnostics();

await import('./index.js');
