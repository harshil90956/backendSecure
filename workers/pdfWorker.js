// workers/pdfWorker.js
import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";
import fs from "fs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ENV: load from process env (Railway/Render/etc.), then optionally override from local ../.env if present.
dotenv.config();
const envPath = path.resolve(__dirname, "../.env");
if (fs.existsSync(envPath)) {
  dotenv.config({ path: envPath, override: true });
}

import mongoose from "mongoose";
import crypto from "crypto";
import { Worker } from "bullmq";
import { PDFDocument } from "pdf-lib";
import { GetObjectCommand } from "@aws-sdk/client-s3";

import {
  connection,
  mergePdfQueue,
} from "../queues/outputPdfQueue.js";

import { generateOutputPdfBuffer } from "../src/pdf/generateOutputPdf.js";
import { s3, uploadToS3 } from "../src/services/s3.js";
import Document from "../src/models/Document.js";
import DocumentAccess from "../src/models/DocumentAccess.js";
import DocumentJobs from "../src/models/DocumentJobs.js";

// 🔥 DEBUG
const debug =
  process.env.WORKER_DEBUG === "1" ||
  process.env.WORKER_DEBUG === "true";

const log = (...a) => debug && console.log(...a);

// --------------------------------------------------
// Mongo
// --------------------------------------------------
async function connectMongo() {
  if (!process.env.MONGO_URI) {
    console.error("❌ MONGO_URI missing");
    process.exit(1);
  }

  await mongoose.connect(process.env.MONGO_URI, {
    maxPoolSize: 20,
    connectTimeoutMS: 10000,
  });

  console.log("🟢 Mongo connected");
}

// --------------------------------------------------
// Resolve s3:// images
// --------------------------------------------------
async function resolveS3ImagesInLayout(layout) {
  if (!process.env.AWS_S3_BUCKET) return layout;

  const items = Array.isArray(layout?.items) ? layout.items : [];
  if (!items.some(i => i?.src?.startsWith("s3://"))) return layout;

  const cache = new Map();

  const resolved = await Promise.all(
    items.map(async (item) => {
      if (!item?.src?.startsWith("s3://")) return item;

      const key = item.src.replace("s3://", "");
      if (!cache.has(key)) {
        const res = await s3.send(
          new GetObjectCommand({
            Bucket: process.env.AWS_S3_BUCKET,
            Key: key,
          })
        );

        const chunks = [];
        for await (const c of res.Body) chunks.push(c);
        const buf = Buffer.concat(chunks);

        const type = res.ContentType || "image/png";
        cache.set(
          key,
          `data:${type};base64,${buf.toString("base64")}`
        );
      }

      return { ...item, src: cache.get(key) };
    })
  );

  return { ...layout, items: resolved };
}

// --------------------------------------------------
// START
// --------------------------------------------------
async function start() {
  console.log("🚀 PDF Worker booting...");
  await connectMongo();

  // ==================================================
  // RENDER WORKER
  // ==================================================
  new Worker(
    "outputPdfQueue", // 🔥 HARD-CODED (NO MISMATCH)
    async (job) => {
      const { jobId, pageLayout, pageIndex } = job.data;
      if (!jobId) return;

      try {
        log("▶ render start", jobId, pageIndex);

        const jobDoc = await DocumentJobs.findById(jobId);
        if (!jobDoc) return;

        const layout = await resolveS3ImagesInLayout(pageLayout);
        const pdf = await generateOutputPdfBuffer([layout]);
        if (!pdf?.length) throw new Error("Empty PDF");

        const { key } = await uploadToS3(
          pdf,
          "application/pdf",
          "generated/pages/"
        );

        const updated = await DocumentJobs.findByIdAndUpdate(
          jobId,
          {
            $inc: { completedPages: 1 },
            $push: { pageArtifacts: { key, pageIndex } },
            $set: { stage: "rendering", status: "processing" },
          },
          { new: true }
        );

        if (
          updated.completedPages >= updated.totalPages &&
          updated.totalPages > 0
        ) {
          await mergePdfQueue.add(
            "mergeJob",
            { jobId },
            { jobId: `${jobId}-merge` }
          );
        }

      } catch (err) {
        console.error("❌ render error", err.message);
        await DocumentJobs.findByIdAndUpdate(jobId, {
          $set: { status: "failed", stage: "failed" },
        });
        throw err;
      }
    },
    { connection, concurrency: 4 }
  );

  // ==================================================
  // MERGE WORKER
  // ==================================================
  new Worker(
    "mergePdfQueue",
    async (job) => {
      const { jobId } = job.data;
      if (!jobId) return;

      try {
        log("▶ merge start", jobId);
        const jobDoc = await DocumentJobs.findById(jobId);
        if (!jobDoc) {
          log("[merge] jobDoc not found", jobId);
          return;
        }

        if (jobDoc.outputDocumentId || jobDoc.stage === "completed") {
          log("[merge] already completed, skipping", {
            jobId,
            outputDocumentId: jobDoc.outputDocumentId?.toString?.(),
            stage: jobDoc.stage,
          });
          return;
        }

        // Best-effort mark as merging so UI matches reality.
        await DocumentJobs.findByIdAndUpdate(jobId, {
          $set: { status: "processing", stage: "merging" },
        }).catch(() => null);

        const merged = await PDFDocument.create();
        const pages = [...jobDoc.pageArtifacts].sort(
          (a, b) => a.pageIndex - b.pageIndex
        );

        log("[merge] pageArtifacts", { jobId, count: pages.length });

        for (const p of pages) {
          log("[merge] downloading page", { jobId, pageIndex: p.pageIndex, key: p.key });
          const res = await s3.send(
            new GetObjectCommand({
              Bucket: process.env.AWS_S3_BUCKET,
              Key: p.key,
            })
          );
          const chunks = [];
          for await (const c of res.Body) chunks.push(c);
          const buf = Buffer.concat(chunks);
          log("[merge] downloaded bytes", { jobId, pageIndex: p.pageIndex, bytes: buf.length });

          const pdf = await PDFDocument.load(buf);
          const copied = await merged.copyPages(
            pdf,
            pdf.getPageIndices()
          );
          copied.forEach(pg => merged.addPage(pg));
        }

        const finalPdf = Buffer.from(await merged.save());
        log("[merge] final pdf bytes", { jobId, bytes: finalPdf.length });
        const { key, url } = await uploadToS3(
          finalPdf,
          "application/pdf",
          "generated/output/"
        );

        log("[merge] uploaded output", { jobId, key });

        const totalPrintsRaw = jobDoc.assignedQuota;
        const totalPrintsNum = Number(totalPrintsRaw ?? 0);
        const totalPrints = Number.isFinite(totalPrintsNum) ? totalPrintsNum : 0;

        const doc = await Document.create({
          title: "Generated Output",
          fileKey: key,
          fileUrl: url,
          totalPrints,
          mimeType: "application/pdf",
          documentType: "generated-output",
          createdBy: jobDoc.createdBy,
        });

        const access = await DocumentAccess.findOneAndUpdate(
          { userId: jobDoc.userId, documentId: doc._id },
          {
            userId: jobDoc.userId,
            documentId: doc._id,
            assignedQuota: Number(jobDoc.assignedQuota),
            usedPrints: 0,
            sessionToken: crypto.randomBytes(32).toString("hex"),
          },
          { upsert: true, new: true }
        );

        await DocumentJobs.findByIdAndUpdate(jobId, {
          $set: {
            status: "completed",
            stage: "completed",
            outputDocumentId: doc._id,
          },
        });

        log("✅ merge completed", { jobId, outputDocumentId: doc._id?.toString?.() });

      } catch (err) {
        console.error("❌ merge error", {
          jobId,
          message: err && err.message,
          name: err && err.name,
          stack: err && err.stack,
        });
        await DocumentJobs.findByIdAndUpdate(jobId, {
          $set: { status: "failed", stage: "failed" },
        });
        throw err;
      }
    },
    { connection, concurrency: 1 }
  );
}

start().catch((e) => {
  console.error("❌ Worker crashed", e);
  process.exit(1);
});
