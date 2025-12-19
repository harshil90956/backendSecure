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
import { GetObjectCommand, HeadObjectCommand } from "@aws-sdk/client-s3";

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

const dbg = (scope, jobId, ...a) => {
  if (!debug) return;
  const prefix = typeof jobId === "string" && jobId.length ? `[${scope}] ${jobId}` : `[${scope}]`;
  console.log(prefix, ...a);
};

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

const A4_WIDTH_MM = 210;
const A4_HEIGHT_MM = 297;
const PT_PER_MM = 72 / 25.4;
const PX_TO_PT = 72 / 96;

const A4_WIDTH_PT = A4_WIDTH_MM * PT_PER_MM;
const A4_HEIGHT_PT = A4_HEIGHT_MM * PT_PER_MM;
const TICKETS_PER_PAGE = 4;
const TICKET_HEIGHT_MM = A4_HEIGHT_MM / TICKETS_PER_PAGE;
const TICKET_HEIGHT_PT = TICKET_HEIGHT_MM * PT_PER_MM;

const svgServiceUrl =
  process.env.SVG_TO_PDF_SERVICE_URL ||
  process.env.SVG_TO_PDF_SERVICE ||
  process.env.SVG_TO_PDF_URL ||
  "http://localhost:3000";

const vectorEnabled =
  process.env.ENABLE_VECTOR_OUTPUT === "1" ||
  process.env.ENABLE_VECTOR_OUTPUT === "true" ||
  process.env.ENABLE_VECTOR_OUTPUT === "yes";

const hasFetchStack =
  typeof fetch === "function" &&
  typeof FormData === "function" &&
  typeof Blob === "function" &&
  typeof AbortController === "function";

const vectorEnabledEffective = vectorEnabled && hasFetchStack;

async function downloadS3Buffer(key) {
  if (!process.env.AWS_S3_BUCKET) {
    throw new Error("AWS_S3_BUCKET missing in worker env");
  }

  const res = await s3.send(
    new GetObjectCommand({
      Bucket: process.env.AWS_S3_BUCKET,
      Key: key,
    })
  );

  const chunks = [];
  for await (const c of res.Body) chunks.push(c);
  return Buffer.concat(chunks);
}

async function convertSvgBufferToCmykPdf(svgBuffer, cropPercent) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 120000);

  try {
    const form = new FormData();
    form.append(
      "file",
      new Blob([svgBuffer], { type: "image/svg+xml" }),
      "input.svg"
    );
    if (cropPercent) {
      form.append("crop", JSON.stringify(cropPercent));
    }

    const res = await fetch(`${svgServiceUrl}/svg-to-pdf-cmyk`, {
      method: "POST",
      body: form,
      signal: controller.signal,
    });

    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(`svg-to-pdf-service error (${res.status}): ${text}`);
    }

    const ab = await res.arrayBuffer();
    return Buffer.from(ab);
  } finally {
    clearTimeout(timeoutId);
  }
}

function extractDocumentIdFromTemplateKey(templateKey) {
  if (typeof templateKey !== "string") return null;
  if (!templateKey.startsWith("document:")) return null;
  const id = templateKey.slice("document:".length).trim();
  return id || null;
}

async function renderVectorLayoutToPdfBuffer(layout) {
  const items = Array.isArray(layout?.items) ? layout.items : [];
  const templateItem = items.find((i) => i && i.kind === "svgTemplate");
  if (!templateItem) {
    throw new Error("vector layout missing svgTemplate");
  }

  const docId = extractDocumentIdFromTemplateKey(templateItem.templateKey);
  if (!docId) {
    throw new Error("vector templateKey missing document id");
  }

  const srcDoc = await Document.findById(docId);
  if (!srcDoc) {
    throw new Error("source document not found");
  }

  const mimeType = typeof srcDoc.mimeType === "string" ? srcDoc.mimeType : "";
  const isSvg =
    mimeType.includes("svg") ||
    (typeof srcDoc.fileKey === "string" && srcDoc.fileKey.toLowerCase().endsWith(".svg"));

  if (!isSvg) {
    throw new Error("vector mode currently requires SVG source");
  }

  const svgBuffer = await downloadS3Buffer(srcDoc.fileKey);
  if (!svgBuffer?.length) {
    throw new Error("empty SVG buffer");
  }

  const cropPercent = templateItem.ticketRegion || null;
  const ticketPdfBuffer = await convertSvgBufferToCmykPdf(svgBuffer, cropPercent);

  const ticketPdf = await PDFDocument.load(ticketPdfBuffer);
  const [embeddedTicket] = await ticketPdf.embedPages([ticketPdf.getPage(0)]);

  const out = await PDFDocument.create();
  const page = out.addPage([A4_WIDTH_PT, A4_HEIGHT_PT]);

  const scaleX = A4_WIDTH_PT / embeddedTicket.width;
  const scaleY = TICKET_HEIGHT_PT / embeddedTicket.height;
  const scale = Math.min(scaleX, scaleY);
  const drawW = embeddedTicket.width * scale;
  const drawH = embeddedTicket.height * scale;
  const x = (A4_WIDTH_PT - drawW) / 2;

  for (let i = 0; i < TICKETS_PER_PAGE; i += 1) {
    const ticketBottomYpt = A4_HEIGHT_PT - (i + 1) * TICKET_HEIGHT_PT;
    page.drawPage(embeddedTicket, {
      x,
      y: ticketBottomYpt + (TICKET_HEIGHT_PT - drawH) / 2,
      xScale: scale,
      yScale: scale,
    });
  }

  for (const item of items) {
    if (!item || item.kind !== "text") continue;

    const rawText = typeof item.text === "string" ? item.text : "";
    if (!rawText) continue;

    const unit = item.unit === 'mm' || layout?.unit === 'mm' ? 'mm' : 'px';
    const xRaw = Number(item.x) || 0;
    const yRaw = Number(item.y) || 0;
    const sizeRaw = Number(item.fontSize) || 12;

    const xPt = unit === 'mm' ? xRaw * PT_PER_MM : xRaw * PX_TO_PT;
    const yPt = A4_HEIGHT_PT - (unit === 'mm' ? yRaw * PT_PER_MM : yRaw * PX_TO_PT);
    const sizePt = unit === 'mm' ? sizeRaw * PT_PER_MM : sizeRaw * PX_TO_PT;

    page.drawText(rawText, {
      x: xPt,
      y: yPt,
      size: sizePt,
    });
  }

  return Buffer.from(await out.save());
}

async function validatePdfIsA4(pdfBuffer) {
  const pdfDoc = await PDFDocument.load(pdfBuffer);
  const pages = pdfDoc.getPages();
  const p0 = pages && pages.length ? pages[0] : null;
  if (!p0) return { ok: false, reason: 'missing_page' };

  const w = p0.getWidth();
  const h = p0.getHeight();

  const eps = 0.05;
  const okPortrait = Math.abs(w - A4_WIDTH_PT) <= eps && Math.abs(h - A4_HEIGHT_PT) <= eps;
  const okLandscape = Math.abs(w - A4_HEIGHT_PT) <= eps && Math.abs(h - A4_WIDTH_PT) <= eps;
  const ok = okPortrait || okLandscape;
  return { ok, widthPt: w, heightPt: h, eps };
}

// --------------------------------------------------
// START
// --------------------------------------------------
async function start() {
  console.log("🚀 PDF Worker booting...");
  await connectMongo();

  try {
    dbg("boot", "", "build", {
      railwayCommit: process.env.RAILWAY_GIT_COMMIT_SHA,
      commitSha: process.env.COMMIT_SHA,
      nodeEnv: process.env.NODE_ENV,
    });

    const tp = Document?.schema?.path?.("totalPrints");
    dbg("boot", "", "Document.totalPrints schema", {
      required: tp?.isRequired === true || tp?.options?.required === true,
      hasDefault: tp?.options?.default !== undefined,
      default: tp?.options?.default,
      min: tp?.options?.min,
    });
  } catch (e) {
    dbg("boot", "", "boot debug failed", {
      message: e && e.message,
      name: e && e.name,
    });
  }

  // ==================================================
  // RENDER WORKER
  // ==================================================
  new Worker(
    "outputPdfQueue", // 🔥 HARD-CODED (NO MISMATCH)
    async (job) => {
      const { jobId, pageLayout, pageIndex, layoutMode } = job.data;
      if (!jobId) return;

      try {
        log("▶ render start", jobId, pageIndex);

        const jobDoc = await DocumentJobs.findById(jobId);
        if (!jobDoc) return;

        if (jobDoc.outputDocumentId || jobDoc.stage === "completed") {
          dbg("render", jobId, "skip: already completed", {
            stage: jobDoc.stage,
            status: jobDoc.status,
            outputDocumentId: jobDoc.outputDocumentId?.toString?.(),
          });
          return;
        }
        if (jobDoc.stage === "merging") {
          dbg("render", jobId, "skip: already merging", {
            stage: jobDoc.stage,
            status: jobDoc.status,
          });
          return;
        }

        let pdf;
        if (layoutMode === "vector" && vectorEnabledEffective) {
          try {
            pdf = await renderVectorLayoutToPdfBuffer(pageLayout);
          } catch (e) {
            dbg("render", jobId, "vector render failed; falling back to raster", {
              pageIndex,
              message: e && e.message,
              name: e && e.name,
            });
            const layout = await resolveS3ImagesInLayout(pageLayout);
            pdf = await generateOutputPdfBuffer([layout]);
          }
        } else {
          const layout = await resolveS3ImagesInLayout(pageLayout);
          pdf = await generateOutputPdfBuffer([layout]);
        }
        if (!pdf?.length) throw new Error("Empty PDF");

        try {
          const v = await validatePdfIsA4(pdf);
          dbg('render', jobId, 'pdf_size_check', {
            pageIndex,
            ok: v?.ok === true,
            widthPt: v?.widthPt,
            heightPt: v?.heightPt,
            expectedWidthPt: A4_WIDTH_PT,
            expectedHeightPt: A4_HEIGHT_PT,
            eps: v?.eps,
          });
          if (!v || v.ok !== true) {
            throw new Error('Generated PDF is not exact A4');
          }
        } catch (e) {
          dbg('render', jobId, 'pdf_size_check_failed', {
            pageIndex,
            message: e && e.message,
          });
          throw e;
        }

        const { key } = await uploadToS3(
          pdf,
          "application/pdf",
          "generated/pages/"
        );

        const setFields = { status: "processing" };
        if (jobDoc.stage !== "merging" && jobDoc.stage !== "completed") {
          setFields.stage = "rendering";
        }

        const updated = await DocumentJobs.findByIdAndUpdate(
          jobId,
          {
            $inc: { completedPages: 1 },
            $push: { pageArtifacts: { key, pageIndex } },
            $set: setFields,
          },
          { new: true }
        );

        dbg("render", jobId, "page uploaded", {
          pageIndex,
          key,
          completedPages: updated?.completedPages,
          totalPages: updated?.totalPages,
          stage: updated?.stage,
          status: updated?.status,
        });

        if (
          updated.completedPages >= updated.totalPages &&
          updated.totalPages > 0
        ) {
          dbg("render", jobId, "render complete: attempting merge transition", {
            completedPages: updated.completedPages,
            totalPages: updated.totalPages,
            stage: updated.stage,
            status: updated.status,
          });

          const transitioned = await DocumentJobs.findOneAndUpdate(
            {
              _id: jobId,
              outputDocumentId: null,
              stage: { $in: ["pending", "rendering"] },
            },
            { $set: { status: "processing", stage: "merging" } },
            { new: true }
          );

          if (!transitioned) {
            dbg("render", jobId, "merge transition skipped (already transitioned or completed)");
            return;
          }

          try {
            await mergePdfQueue.add(
              "mergeJob",
              { jobId },
              {
                jobId: `${jobId}-merge`,
                attempts: 1,
                removeOnComplete: true,
                removeOnFail: true,
              }
            );
            dbg("render", jobId, "merge enqueued", { mergeJobId: `${jobId}-merge` });
          } catch (e) {
            const msg = e && e.message ? String(e.message) : "";
            if (msg.toLowerCase().includes("already exists")) {
              dbg("render", jobId, "merge enqueue ignored: jobId already exists", {
                mergeJobId: `${jobId}-merge`,
              });
              return;
            }
            throw e;
          }
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
        dbg("merge", jobId, "start", {
          bullmqJobId: job?.id,
          attemptsMade: job?.attemptsMade,
          data: job?.data,
        });

        const jobDoc = await DocumentJobs.findById(jobId);
        if (!jobDoc) {
          dbg("merge", jobId, "jobDoc not found");
          return;
        }

        dbg("merge", jobId, "job snapshot", {
          stage: jobDoc.stage,
          status: jobDoc.status,
          totalPages: jobDoc.totalPages,
          completedPages: jobDoc.completedPages,
          outputDocumentId: jobDoc.outputDocumentId?.toString?.(),
          assignedQuota: jobDoc.assignedQuota,
          artifacts: Array.isArray(jobDoc.pageArtifacts) ? jobDoc.pageArtifacts.length : 0,
          createdBy: jobDoc.createdBy?.toString?.(),
          userId: jobDoc.userId?.toString?.(),
        });

        if (jobDoc.outputDocumentId || jobDoc.stage === "completed") {
          dbg("merge", jobId, "already completed, skipping", {
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

        const totalPages = Number(jobDoc.totalPages ?? 0);
        const artifactPageIndexes = new Set(
          pages.map((p) => p?.pageIndex).filter((v) => Number.isInteger(v))
        );
        const missing = [];
        if (Number.isFinite(totalPages) && totalPages > 0) {
          for (let i = 0; i < totalPages; i += 1) {
            if (!artifactPageIndexes.has(i)) missing.push(i);
          }
        }

        dbg("merge", jobId, "artifacts validation", {
          artifactsSorted: pages.length,
          totalPages,
          uniqueArtifactPages: artifactPageIndexes.size,
          missingCount: missing.length,
          missing: missing.length ? missing.slice(0, 20) : [],
        });

        if (Number.isFinite(totalPages) && totalPages > 0 && missing.length) {
          throw new Error(
            `Missing rendered page artifacts for indexes: ${missing.slice(0, 50).join(",")}`
          );
        }

        for (const p of pages) {
          const pageIndex = p?.pageIndex;
          const key = p?.key;
          dbg("merge", jobId, "page start", { pageIndex, key });

          if (!process.env.AWS_S3_BUCKET) {
            throw new Error("AWS_S3_BUCKET missing in worker env");
          }

          try {
            const head = await s3
              .send(
                new HeadObjectCommand({
                  Bucket: process.env.AWS_S3_BUCKET,
                  Key: key,
                })
              )
              .catch(() => null);
            if (head) {
              dbg("merge", jobId, "page head", {
                pageIndex,
                key,
                contentLength: head.ContentLength,
                contentType: head.ContentType,
                lastModified: head.LastModified,
              });
            }
          } catch (e) {
            dbg("merge", jobId, "page head failed", {
              pageIndex,
              key,
              message: e && e.message,
            });
          }

          const res = await s3.send(
            new GetObjectCommand({
              Bucket: process.env.AWS_S3_BUCKET,
              Key: key,
            })
          );
          const chunks = [];
          for await (const c of res.Body) chunks.push(c);
          const buf = Buffer.concat(chunks);
          dbg("merge", jobId, "page downloaded", { pageIndex, key, bytes: buf.length });

          if (!buf?.length) {
            throw new Error(`Downloaded empty PDF buffer for pageIndex=${pageIndex} key=${key}`);
          }

          let pdf;
          try {
            pdf = await PDFDocument.load(buf);
          } catch (e) {
            dbg("merge", jobId, "PDF load failed", {
              pageIndex,
              key,
              message: e && e.message,
              name: e && e.name,
            });
            throw e;
          }

          const indices = pdf.getPageIndices();
          dbg("merge", jobId, "PDF loaded", { pageIndex, key, pages: indices.length });

          let copied;
          try {
            copied = await merged.copyPages(pdf, indices);
          } catch (e) {
            dbg("merge", jobId, "copyPages failed", {
              pageIndex,
              key,
              message: e && e.message,
              name: e && e.name,
            });
            throw e;
          }

          copied.forEach((pg) => merged.addPage(pg));
        }

        const finalPdf = Buffer.from(await merged.save());
        dbg("merge", jobId, "final pdf", { bytes: finalPdf.length });
        const { key, url } = await uploadToS3(
          finalPdf,
          "application/pdf",
          "generated/output/"
        );

        dbg("merge", jobId, "uploaded output", { key, url });

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

        dbg("merge", jobId, "merge failed", {
          bullmqJobId: job?.id,
          attemptsMade: job?.attemptsMade,
          data: job?.data,
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
