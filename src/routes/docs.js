import express from 'express';
import multer from 'multer';
import crypto from 'crypto';
import Document from '../models/Document.js';
import DocumentAccess from '../models/DocumentAccess.js';
import DocumentJobs from '../models/DocumentJobs.js';
import { uploadToS3, s3 } from '../services/s3.js';
import { GetObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { authMiddleware } from '../middleware/auth.js';
import { mergePdfQueue, outputPdfQueue } from '../../queues/outputPdfQueue.js';
import { redisEnabled } from '../redisAvailability.js';

const router = express.Router();
const upload = multer();

const lastJobHealAt = new Map();
const lastMergeHealAt = new Map();

async function selfHealJobIfStalled(job) {
  try {
    if (!redisEnabled) return;
    if (!job) return;
    const totalPages = Number(job.totalPages || 0);
    const completedPages = Number(job.completedPages || 0);
    if (!Number.isFinite(totalPages) || totalPages <= 0) return;

    const jobId = job._id?.toString?.();
    if (!jobId) return;

    const layoutPages = Array.isArray(job.layoutPages) ? job.layoutPages : [];
    const pageArtifacts = Array.isArray(job.pageArtifacts) ? job.pageArtifacts : [];
    const artifactPageIndexes = new Set(
      pageArtifacts.map((a) => a?.pageIndex).filter((v) => Number.isInteger(v))
    );

    const hasAllArtifacts = artifactPageIndexes.size >= totalPages;
    const pagesAreDone = completedPages >= totalPages && hasAllArtifacts;

    // 1) If all pages are rendered but merge/output is missing, retry/enqueue merge
    // This also covers the case where the job was marked failed even though progress is 100%.
    if (pagesAreDone) {
      if (!job.outputDocumentId) {
        const now = Date.now();
        const lastMergeAt = lastMergeHealAt.get(jobId) || 0;
        if (now - lastMergeAt < 60_000) return;
        lastMergeHealAt.set(jobId, now);

        const mergeJobId = `${jobId}-merge`;
        const existing = await mergePdfQueue.getJob(mergeJobId).catch(() => null);

        // Reset to processing so UI doesn't stay on "Failed" while we retry merge.
        await DocumentJobs.findByIdAndUpdate(jobId, {
          $set: { status: 'processing', stage: 'merging' },
        }).catch(() => null);

        if (existing) {
          const state = await existing.getState().catch(() => null);
          if (state === 'failed') {
            await existing.retry().catch(() => null);
          }
          return;
        }

        await mergePdfQueue.add('mergeJob', { jobId }, {
          jobId: mergeJobId,
          attempts: 1,
          removeOnComplete: true,
          removeOnFail: true,
        });
      }
      return;
    }

    const updatedAtMs = job.updatedAt ? new Date(job.updatedAt).getTime() : 0;
    if (!updatedAtMs || Number.isNaN(updatedAtMs)) return;

    const now = Date.now();
    if (now - updatedAtMs < 30_000) return;

    const lastAt = lastJobHealAt.get(jobId) || 0;
    if (now - lastAt < 60_000) return;
    lastJobHealAt.set(jobId, now);

    if (layoutPages.length < totalPages) return;

    const missing = [];
    for (let i = 0; i < totalPages; i += 1) {
      if (!artifactPageIndexes.has(i)) missing.push(i);
    }
    if (missing.length === 0) return;

    // If the job was previously marked failed, flip it back to processing while we retry missing pages.
    if (job.status === 'failed' || job.stage === 'failed') {
      await DocumentJobs.findByIdAndUpdate(jobId, {
        $set: { status: 'processing', stage: 'rendering' },
      }).catch(() => null);
    }

    for (const pageIndex of missing) {
      const renderJobId = `${jobId}-page-${pageIndex}`;
      const existing = await outputPdfQueue.getJob(renderJobId).catch(() => null);
      if (existing) {
        const state = await existing.getState().catch(() => null);
        if (state === 'failed') {
          await existing.retry().catch(() => null);
        }
        continue;
      }

      const pageLayout = layoutPages[pageIndex];
      const layoutMode =
        pageLayout && typeof pageLayout.layoutMode === 'string' ? pageLayout.layoutMode : 'raster';

      const payload = {
        jobId,
        documentJobId: jobId,
        email: typeof job.email === 'string' ? job.email.toLowerCase() : undefined,
        pageIndex,
        totalPages,
        pageLayout,
        layoutMode,
        assignedQuota: job.assignedQuota,
        s3TemplateKey: null,
        inputPdfKey: null,
      };

      await outputPdfQueue.add('renderPage', payload, {
        jobId: renderJobId,
        removeOnComplete: true,
        removeOnFail: false,
      });
    }
  } catch (err) {
    console.error('[docs/assigned] self-heal failed', err);
  }
}

// Download helper app (.exe) for printing
router.get('/print-agent', authMiddleware, async (req, res) => {
  try {
    const bucket = process.env.AWS_S3_BUCKET;
    if (!bucket) {
      return res.status(500).json({ message: 'S3 not configured' });
    }

    const installerKey = process.env.PRINT_AGENT_S3_KEY;
    if (!installerKey) {
      return res.status(500).json({ message: 'Print agent not configured (PRINT_AGENT_S3_KEY missing)' });
    }

    const rawFilename = process.env.PRINT_AGENT_FILENAME || 'SecurePrintHub-Setup.exe';
    const filename = rawFilename.toLowerCase().endsWith('.exe') ? rawFilename : `${rawFilename}.exe`;

    // Validate the object exists and looks like a real installer (not a 0-byte/HTML error upload)
    let head;
    try {
      head = await s3.send(
        new HeadObjectCommand({
          Bucket: bucket,
          Key: installerKey,
        })
      );
    } catch (err) {
      const statusCode = err?.$metadata?.httpStatusCode;
      const errName = err?.name;
      if (statusCode === 404 || errName === 'NotFound' || errName === 'NoSuchKey') {
        return res.status(404).json({
          message: 'Print agent installer not found. Please upload it to S3 and set PRINT_AGENT_S3_KEY correctly.',
        });
      }

      throw err;
    }

    const size = Number(head?.ContentLength ?? 0);
    // NSIS installers are typically many MB. If it is tiny, it's almost certainly broken.
    if (!Number.isFinite(size) || size < 1024 * 1024) {
      return res.status(500).json({
        message: 'Print agent installer is missing or corrupted. Please re-upload a valid installer build.',
      });
    }

    const command = new GetObjectCommand({
      Bucket: bucket,
      Key: installerKey,
      ResponseContentDisposition: `attachment; filename="${filename}"`,
      // Safer for downloads across browsers; Windows will still treat .exe correctly.
      ResponseContentType: 'application/octet-stream',
    });

    const signedUrl = await getSignedUrl(s3, command, { expiresIn: 60 * 5 });

    return res.json({ url: signedUrl, filename, size });
  } catch (err) {
    console.error('Print agent download error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Helper to generate opaque session tokens
const generateSessionToken = () => crypto.randomBytes(32).toString('hex');

// Upload document (PDF/SVG) for the logged-in user and create access record
router.post('/upload', authMiddleware, upload.single('file'), async (req, res) => {
  try {
    const { title, totalPrints } = req.body;
    const file = req.file;

    if (!file) {
      return res.status(400).json({ message: 'File is required' });
    }

    if (!title || !totalPrints) {
      return res.status(400).json({ message: 'Title and totalPrints are required' });
    }

    const parsedTotal = Number(totalPrints);
    if (Number.isNaN(parsedTotal) || parsedTotal <= 0) {
      return res.status(400).json({ message: 'totalPrints must be a positive number' });
    }

    const { key, url } = await uploadToS3(file.buffer, file.mimetype, 'securepdf/');

    const doc = await Document.create({
      title,
      fileKey: key,
      fileUrl: url,
      totalPrints: parsedTotal,
      createdBy: req.user._id,
    });

    const sessionToken = generateSessionToken();

    const access = await DocumentAccess.create({
      userId: req.user._id,
      documentId: doc._id,
      assignedQuota: parsedTotal,
      usedPrints: 0,
      sessionToken,
    });

    const loweredName = title.toLowerCase();
    const isSvg = file.mimetype === 'image/svg+xml' || loweredName.endsWith('.svg');
    const documentType = isSvg ? 'svg' : 'pdf';

    return res.status(201).json({
      sessionToken,
      documentTitle: doc.title,
      documentId: doc._id,
      remainingPrints: access.assignedQuota - access.usedPrints,
      maxPrints: access.assignedQuota,
      documentType,
    });
  } catch (err) {
    console.error('Docs upload error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Secure render: stream PDF/SVG bytes based on session token
router.post('/secure-render', authMiddleware, async (req, res) => {
  try {
    const { sessionToken } = req.body;

    if (!sessionToken) {
      return res.status(400).json({ message: 'sessionToken is required' });
    }

    const access = await DocumentAccess.findOne({ sessionToken }).populate('documentId');
    if (!access) {
      return res.status(404).json({ message: 'Access not found' });
    }

    if (access.userId.toString() !== req.user._id.toString()) {
      return res.status(403).json({ message: 'Not authorized for this document' });
    }

    const doc = access.documentId;
    if (!doc) {
      return res.status(404).json({ message: 'Document not found' });
    }

    const bucket = process.env.AWS_S3_BUCKET;
    if (!bucket) {
      return res.status(500).json({ message: 'S3 not configured' });
    }

    const command = new GetObjectCommand({
      Bucket: bucket,
      Key: doc.fileKey,
    });

    const s3Response = await s3.send(command);

    const chunks = [];
    for await (const chunk of s3Response.Body) {
      chunks.push(chunk);
    }
    const buffer = Buffer.concat(chunks);

    const loweredTitle = (doc.title || '').toLowerCase();
    const isSvg = loweredTitle.endsWith('.svg');

    res.setHeader('Content-Type', isSvg ? 'image/svg+xml' : 'application/pdf');
    return res.send(buffer);
  } catch (err) {
    console.error('Secure render error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Secure print: decrement quota and return presigned S3 URL for printing
router.post('/secure-print', authMiddleware, async (req, res) => {
  try {
    const { sessionToken } = req.body;

    if (!sessionToken) {
      return res.status(400).json({ message: 'sessionToken is required' });
    }

    const access = await DocumentAccess.findOne({ sessionToken }).populate('documentId');
    if (!access) {
      return res.status(404).json({ message: 'Access not found' });
    }

    const remaining = access.assignedQuota - access.usedPrints;
    if (remaining <= 0) {
      return res.status(400).json({ message: 'Print limit exceeded' });
    }

    access.usedPrints += 1;
    await access.save();

    const doc = access.documentId;
    if (!doc) {
      return res.status(404).json({ message: 'Document not found' });
    }

    const bucket = process.env.AWS_S3_BUCKET;
    if (!bucket) {
      return res.status(500).json({ message: 'S3 not configured' });
    }

    // Generate a short-lived presigned URL so browser securely fetches from S3 without AccessDenied
    const command = new GetObjectCommand({
      Bucket: bucket,
      Key: doc.fileKey,
    });

    const signedUrl = await getSignedUrl(s3, command, { expiresIn: 60 }); // 60 seconds

    return res.json({
      fileUrl: signedUrl,
      remainingPrints: access.assignedQuota - access.usedPrints,
      maxPrints: access.assignedQuota,
    });
  } catch (err) {
    console.error('Secure print error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// List documents assigned to the logged-in user, including background jobs
router.get('/assigned', authMiddleware, async (req, res) => {
  try {
    console.log('[docs/assigned] start', {
      userId: req.user?._id?.toString?.(),
    });

    const accesses = await DocumentAccess.find({ userId: req.user._id })
      .populate('documentId')
      .sort({ createdAt: -1 });

    const accessResults = accesses.map((access) => {
      const doc = access.documentId;
      const title = doc?.title || 'Untitled Document';
      const loweredTitle = title.toLowerCase();
      const isSvg = loweredTitle.endsWith('.svg');

      return {
        id: access._id,
        documentId: doc?._id,
        documentTitle: title,
        assignedQuota: access.assignedQuota,
        usedPrints: access.usedPrints,
        remainingPrints: access.assignedQuota - access.usedPrints,
        sessionToken: access.sessionToken,
        documentType: isSvg ? 'svg' : 'pdf',
        status: 'completed',
      };
    });

    const jobs = await DocumentJobs.find({ userId: req.user._id })
      .sort({ createdAt: -1 })
      .exec();

    console.log('[docs/assigned] found', {
      accessCount: accesses.length,
      jobCount: jobs.length,
    });

    const activeJobs = jobs.filter((job) => job.status !== 'completed');

    const jobResults = await Promise.all(
      activeJobs.map(async (job) => {
        await selfHealJobIfStalled(job);

        const result = {
          id: job._id,
          documentTitle: 'Generated Output',
          assignedQuota: job.assignedQuota,
          usedPrints: 0,
          documentType: 'pdf',
          status: job.status,
          stage: job.stage,
          totalPages: job.totalPages || 0,
          completedPages: job.completedPages || 0,
        };

        if (job.outputDocumentId) {
          result.documentId = job.outputDocumentId;

          const access = await DocumentAccess.findOne({
            userId: req.user._id,
            documentId: job.outputDocumentId,
          }).catch(() => null);

          if (access) {
            result.sessionToken = access.sessionToken;
            result.usedPrints = access.usedPrints;
            result.remainingPrints = access.assignedQuota - access.usedPrints;
          }
        }

        console.log('[docs/assigned] job', {
          jobId: job._id?.toString?.(),
          status: job.status,
          stage: job.stage,
          totalPages: job.totalPages,
          completedPages: job.completedPages,
          outputDocumentId: job.outputDocumentId?.toString?.(),
        });

        return result;
      })
    );

    const combined = [...jobResults, ...accessResults];

    console.log('[docs/assigned] response', {
      count: combined.length,
      jobResults: jobResults.length,
      accessResults: accessResults.length,
    });

    return res.json(combined);
  } catch (err) {
    console.error('List assigned docs error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

export default router;
