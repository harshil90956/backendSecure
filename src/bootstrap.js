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

if (!process.env.NODE_ENV) {
  process.env.NODE_ENV = 'development';
}

const { assertRedisModeDefinedInDev, logBootDiagnostics } = await import('./redisAvailability.js');
assertRedisModeDefinedInDev();
logBootDiagnostics();

await import('./index.js');
