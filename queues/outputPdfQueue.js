// queues/outputPdfQueue.js
import dotenv from "dotenv";
dotenv.config();

import { Queue } from "bullmq";

if (!process.env.REDIS_URL) {
  console.error("❌ REDIS_URL missing");
  process.exit(1);
}

export const connection = {
  url: process.env.REDIS_URL,
};

/**
 * 🔥 IMPORTANT
 * ONLY camelCase queue names
 */
export const OUTPUT_PDF_QUEUE_NAME = "outputPdfQueue";
export const MERGE_PDF_QUEUE_NAME  = "mergePdfQueue";

export const outputPdfQueue = new Queue(OUTPUT_PDF_QUEUE_NAME, {
  connection,
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: "exponential", delay: 3000 },
    removeOnComplete: true,
    removeOnFail: false,
  },
});

export const mergePdfQueue = new Queue(MERGE_PDF_QUEUE_NAME, {
  connection,
  defaultJobOptions: {
    attempts: 1,
    backoff: { type: "fixed", delay: 5000 },
    removeOnComplete: true,
    removeOnFail: true,
  },
});

console.log("📦 Output Queue:", OUTPUT_PDF_QUEUE_NAME);
console.log("📦 Merge Queue :", MERGE_PDF_QUEUE_NAME);
console.log("🔗 Redis URL  :", process.env.REDIS_URL);
