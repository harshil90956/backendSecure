import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

// Load environment variables from backend/.env
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, '../.env') });

const { outputPdfQueue, mergePdfQueue, OUTPUT_PDF_QUEUE_NAME, MERGE_PDF_QUEUE_NAME } = await import(
  '../queues/outputPdfQueue.js'
);

async function clearQueue(queue, name) {
  try {
    if (!queue) {
      console.log(`\n[clearQueues] Queue is missing, skipping: ${name}`);
      return;
    }

    console.log(`\n[clearQueues] Starting obliterate for queue: ${name}`);

    await queue.obliterate({ force: true });

    console.log(`[clearQueues] Successfully obliterated queue: ${name}`);
  } catch (err) {
    console.error(`[clearQueues] Failed to clear queue ${name}:`, err);
  }
}

async function main() {
  console.log('[clearQueues] Starting');

  await clearQueue(outputPdfQueue, OUTPUT_PDF_QUEUE_NAME);
  await clearQueue(mergePdfQueue, MERGE_PDF_QUEUE_NAME);

  console.log('\n[clearQueues] Done. Exiting.');
  process.exit(0);
}

main().catch((err) => {
  console.error('[clearQueues] Unexpected error in main:', err);
  process.exit(1);
});
