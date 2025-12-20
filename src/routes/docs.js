import express from 'express';
import multer from 'multer';
import crypto from 'crypto';
import Document from '../models/Document.js';
import DocumentAccess from '../models/DocumentAccess.js';
import DocumentJobs from '../models/DocumentJobs.js';
import PrintSession from '../models/PrintSession.js';
import PrintLog from '../models/PrintLog.js';
import OfflineToken from '../models/OfflineToken.js';
import { uploadToS3, s3 } from '../services/s3.js';
import { GetObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { Readable } from 'stream';
import { authMiddleware } from '../middleware/auth.js';
import { mergePdfQueue, outputPdfQueue } from '../../queues/outputPdfQueue.js';
import { redisEnabled } from '../redisAvailability.js';

const router = express.Router();
const upload = multer();

const lastJobHealAt = new Map();

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

      // Extract page size from layout or default to A4
      const pageWidthMm = pageLayout?.pageWidthMm || 210;
      const pageHeightMm = pageLayout?.pageHeightMm || 297;

      const payload = {
        jobId,
        documentJobId: jobId,
        email: typeof job.email === 'string' ? job.email.toLowerCase() : undefined,
        pageIndex,
        totalPages,
        pageLayout,
        layoutMode,
        pageWidthMm,
        pageHeightMm,
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
router.get('/print-agent', async (req, res) => {
  try {
    const bucket = process.env.AWS_S3_BUCKET;
    if (!bucket) {
      return res.status(500).json({ message: 'S3 not configured' });
    }

    const installerKey =
      process.env.PRINT_AGENT_S3_KEY || 'securepdf/print-agent/SecurePrintHub-Setup-1.0.0.exe';

    const rawFilename = process.env.PRINT_AGENT_FILENAME || 'SecurePrintHub-Setup-1.0.0.exe';
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

    const accept = String(req.headers.accept || '').toLowerCase();
    const wantsJson =
      (typeof req.query?.format === 'string' && req.query.format.toLowerCase() === 'json') ||
      accept.includes('application/json');

    if (wantsJson) {
      return res.json({ url: signedUrl, filename, size });
    }

    return res.redirect(signedUrl);
  } catch (err) {
    console.error('Print agent download error', err);
    const statusCode = err?.$metadata?.httpStatusCode;
    const errName = err?.name;
    const debug =
      process.env.NODE_ENV && process.env.NODE_ENV.toLowerCase() === 'production'
        ? undefined
        : {
            name: errName,
            statusCode,
            message: err?.message,
          };
    if (statusCode === 403 || errName === 'AccessDenied') {
      return res.status(500).json({
        message: 'S3 access denied. Check AWS credentials and bucket permissions.',
        ...(debug ? { debug } : {}),
      });
    }
    if (errName === 'CredentialsProviderError') {
      return res.status(500).json({
        message: 'AWS credentials not configured for backend server.',
        ...(debug ? { debug } : {}),
      });
    }
    return res.status(500).json({ message: 'Internal server error', ...(debug ? { debug } : {}) });
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

    if (!title) {
      return res.status(400).json({ message: 'Title is required' });
    }

    const parsedTotalRaw = totalPrints === undefined || totalPrints === null ? 0 : Number(totalPrints);
    if (!Number.isFinite(parsedTotalRaw) || parsedTotalRaw < 0) {
      return res.status(400).json({ message: 'totalPrints must be a non-negative number' });
    }
    const parsedTotal = parsedTotalRaw;

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

    const remaining = Number(access.assignedQuota || 0) - Number(access.usedPrints || 0);
    if (access.status === 'exhausted' || remaining <= 0) {
      return res.status(403).json({ message: 'Print limit reached' });
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

// Secure print: validate quota and mint a single-use print token
router.post('/secure-print', authMiddleware, async (req, res) => {
  try {
    const { sessionToken, printerName, printerType, portName, clientOS } = req.body;

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

    const remaining = access.assignedQuota - access.usedPrints;
    if (remaining <= 0) {
      return res.status(403).json({ message: 'Print limit exceeded' });
    }

    if (access.status === 'exhausted') {
      return res.status(403).json({ message: 'Print limit reached' });
    }

    const now = new Date();
    const inFlight = await PrintSession.findOne({
      userId: req.user._id,
      documentAccessId: access._id,
      usedAt: null,
      expiresAt: { $gt: now },
    })
      .select('token')
      .lean();
    if (inFlight) {
      return res.status(409).json({ message: 'Print already in progress' });
    }

    const doc = access.documentId;
    if (!doc) {
      return res.status(404).json({ message: 'Document not found' });
    }

    const ttlSeconds = 60;
    const expiresAt = new Date(Date.now() + ttlSeconds * 1000);
    const token = crypto.randomBytes(32).toString('hex');

    await PrintSession.create({
      token,
      userId: req.user._id,
      documentId: doc._id,
      documentAccessId: access._id,
      expiresAt,
      printerName: typeof printerName === 'string' ? printerName : null,
      printerType: typeof printerType === 'string' ? printerType : null,
      portName: typeof portName === 'string' ? portName : null,
      clientOS: typeof clientOS === 'string' ? clientOS : null,
    });

    return res.json({
      printToken: token,
      printUrlPath: `/api/docs/print-file/${token}`,
      expiresAt: expiresAt.toISOString(),
      remainingPrints: access.assignedQuota - access.usedPrints,
      maxPrints: access.assignedQuota,
    });
  } catch (err) {
    console.error('Secure print error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Print file stream: single-use token -> stream from S3 (no presigned URL leakage)
router.get('/print-file/:token', authMiddleware, async (req, res) => {
  try {
    const { token } = req.params;
    const now = new Date();

    // Try PrintSession first (online flow)
    let session = await PrintSession.findOne({ token, userId: req.user._id, expiresAt: { $gt: now } });

    let doc;
    if (session) {
      if (session.fetchedAt) {
        return res.status(404).json({ message: 'Print token not found or already used' });
      }

      const access = await DocumentAccess.findById(session.documentAccessId).catch(() => null);
      if (!access) {
        return res.status(404).json({ message: 'Access not found' });
      }

      const remaining = Number(access.assignedQuota || 0) - Number(access.usedPrints || 0);
      if (access.status === 'exhausted' || remaining <= 0) {
        return res.status(403).json({ message: 'Print limit reached' });
      }

      session = await PrintSession.findOneAndUpdate(
        { _id: session._id, fetchedAt: null },
        { $set: { fetchedAt: now }, $inc: { fetchCount: 1 } },
        { new: true }
      );
      if (!session) {
        return res.status(404).json({ message: 'Print token not found or already used' });
      }

      doc = await Document.findById(session.documentId);
    } else {
      // Try OfflineToken (offline caching flow)
      const offlineToken = await OfflineToken.findOne({ tokenId: token, userId: req.user._id, usedAt: null, expiresAt: { $gt: now } });
      if (!offlineToken) {
        return res.status(404).json({ message: 'Print token not found or already used' });
      }
      doc = await Document.findById(offlineToken.documentId);
    }

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
    const body = s3Response?.Body;
    if (!body) {
      return res.status(500).json({ message: 'Failed to read document bytes' });
    }

    res.setHeader('Cache-Control', 'no-store');
    res.setHeader('Content-Type', doc.mimeType || 'application/pdf');

    // Stream directly to client (handle both Node streams and async iterables)
    const stream = typeof body.pipe === 'function' ? body : Readable.from(body);
    stream.pipe(res);
  } catch (err) {
    console.error('Print file error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Print confirm: mark token used and write audit log
router.post('/print-confirm', authMiddleware, async (req, res) => {
  try {
    const { printToken, printerName, printerType, portName, clientOS } = req.body || {};
    if (!printToken || typeof printToken !== 'string') {
      return res.status(400).json({ message: 'printToken is required' });
    }

    const now = new Date();
    const session = await PrintSession.findOne({ token: printToken, userId: req.user._id });
    if (!session) {
      return res.status(404).json({ message: 'Print session not found' });
    }

    if (session.usedAt) {
      return res.status(409).json({ message: 'Print session already confirmed' });
    }

    if (session.expiresAt && now > new Date(session.expiresAt)) {
      return res.status(410).json({ message: 'Print session expired' });
    }

    session.usedAt = now;
    if (typeof printerName === 'string' && printerName.trim()) session.printerName = printerName.trim();
    if (typeof printerType === 'string' && printerType.trim()) session.printerType = printerType.trim();
    if (typeof portName === 'string' && portName.trim()) session.portName = portName.trim();
    if (typeof clientOS === 'string' && clientOS.trim()) session.clientOS = clientOS.trim();
    await session.save();

    let access = await DocumentAccess.findOneAndUpdate(
      {
        _id: session.documentAccessId,
        status: { $ne: 'exhausted' },
        $expr: { $lt: ['$usedPrints', '$assignedQuota'] },
      },
      { $inc: { usedPrints: 1 } },
      { new: true }
    );

    if (!access) {
      return res.status(403).json({ message: 'Print quota exceeded' });
    }

    if (access.usedPrints >= access.assignedQuota) {
      access = await DocumentAccess.findByIdAndUpdate(
        access._id,
        {
          $set: { status: 'exhausted', exhaustedAt: now },
          $unset: { sessionToken: 1 },
        },
        { new: true }
      );
      await PrintSession.deleteMany({ documentAccessId: session.documentAccessId, usedAt: null }).catch(() => null);
    }

    await PrintLog.create({
      userId: req.user._id,
      documentId: session.documentId,
      count: 1,
      meta: {
        printerName: session.printerName,
        printerType: session.printerType,
        portName: session.portName,
        clientOS: session.clientOS,
        printSessionToken: session.token,
        fetchedAt: session.fetchedAt,
      },
    });

    return res.json({
      success: true,
      usedPrints: access.usedPrints,
      assignedPrints: access.assignedQuota,
      remainingPrints: Math.max(access.assignedQuota - access.usedPrints, 0),
      status: access.status,
    });
  } catch (err) {
    console.error('Print confirm error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Offline token preparation: mint a single-use, machine-bound, time-bound offline token
router.post('/offline-token/prepare', authMiddleware, async (req, res) => {
  try {
    const { sessionToken, printerName, printerType, portName, clientOS, machineGuidHash, expiresInSeconds = 86400 } = req.body;

    if (!sessionToken || !machineGuidHash || !printerName) {
      return res.status(400).json({ message: 'sessionToken, machineGuidHash, and printerName are required' });
    }

    const access = await DocumentAccess.findOne({ sessionToken }).populate('documentId');
    if (!access) {
      return res.status(404).json({ message: 'Access not found' });
    }

    if (access.userId.toString() !== req.user._id.toString()) {
      return res.status(403).json({ message: 'Not authorized for this document' });
    }

    if (access.usedPrints >= access.assignedQuota) {
      return res.status(403).json({ message: 'Print quota exceeded' });
    }

    const doc = access.documentId;
    if (!doc) {
      return res.status(404).json({ message: 'Document not found' });
    }

    const tokenId = crypto.randomBytes(32).toString('hex');
    const expiresAt = new Date(Date.now() + expiresInSeconds * 1000);

    await OfflineToken.create({
      tokenId,
      userId: req.user._id,
      documentId: doc._id,
      documentAccessId: access._id,
      machineGuidHash,
      printerName,
      printerType: printerType || null,
      portName: portName || null,
      clientOS: clientOS || null,
      expiresAt,
    });

    // Decrement quota immediately (server-authoritative)
    await DocumentAccess.findByIdAndUpdate(access._id, { $inc: { usedPrints: 1 } });

    // Return a protected streaming URL for caching while online
    return res.json({
      offlineTokenId: tokenId,
      expiresAt: expiresAt.toISOString(),
      cacheUrl: `/api/docs/print-file/${tokenId}`, // Reuse streaming endpoint for caching
      remainingPrints: access.assignedQuota - (access.usedPrints + 1),
      maxPrints: access.assignedQuota,
    });
  } catch (err) {
    console.error('Offline token prepare error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Reconciliation: upload offline print history when back online
router.post('/offline-token/reconcile', authMiddleware, async (req, res) => {
  try {
    const { entries } = req.body || {};
    if (!Array.isArray(entries) || entries.length === 0) {
      return res.status(400).json({ message: 'entries array is required' });
    }

    const results = [];
    for (const entry of entries) {
      const { offlineTokenId, printedAt, printerName, printerType, portName, clientOS, machineGuidHash } = entry;

      const token = await OfflineToken.findOne({ tokenId: offlineTokenId, userId: req.user._id });
      if (!token) {
        results.push({ offlineTokenId, status: 'not_found' });
        continue;
      }

      if (token.usedAt || token.reconciledAt) {
        results.push({ offlineTokenId, status: 'already_used' });
        continue;
      }

      if (token.machineGuidHash !== machineGuidHash) {
        results.push({ offlineTokenId, status: 'machine_mismatch' });
        continue;
      }

      if (new Date(printedAt) < token.createdAt || new Date(printedAt) > token.expiresAt) {
        results.push({ offlineTokenId, status: 'time_invalid' });
        continue;
      }

      // Mark token used and reconciled
      await OfflineToken.findByIdAndUpdate(token._id, {
        usedAt: new Date(printedAt),
        reconciledAt: new Date(),
        printerName,
        printerType,
        portName,
        clientOS,
      });

      // Create audit log
      await PrintLog.create({
        userId: req.user._id,
        documentId: token.documentId,
        count: 1,
        meta: {
          printedAt: new Date(printedAt),
          printerName,
          printerType,
          portName,
          clientOS,
          offlineTokenId,
          reconciled: true,
        },
      });

      results.push({ offlineTokenId, status: 'reconciled' });
    }

    return res.json({ results });
  } catch (err) {
    console.error('Offline token reconcile error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// List documents assigned to the logged-in user, including background jobs
router.get('/assigned/summary', authMiddleware, async (req, res) => {
  try {
    const accesses = await DocumentAccess.find({ userId: req.user._id })
      .select('assignedQuota usedPrints')
      .lean();

    const documents = accesses.length;
    const totalPages = accesses.reduce((sum, a) => sum + (Number(a.assignedQuota) || 0), 0);
    const remainingPages = accesses.reduce((sum, a) => {
      const assigned = Number(a.assignedQuota) || 0;
      const used = Number(a.usedPrints) || 0;
      return sum + Math.max(assigned - used, 0);
    }, 0);

    return res.json({ documents, totalPages, remainingPages });
  } catch (err) {
    console.error('Assigned summary error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

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
      const assignedPrints = Number(access.assignedQuota) || 0;
      const usedPrints = Number(access.usedPrints) || 0;
      const remainingPrints = Math.max(assignedPrints - usedPrints, 0);

      return {
        id: access._id,
        documentId: doc?._id,
        documentTitle: title,
        assignedQuota: access.assignedQuota,
        usedPrints: access.usedPrints,
        remainingPrints,
        assignedPrints,
        sessionToken: access.sessionToken,
        documentType: isSvg ? 'svg' : 'pdf',
        status: access.status === 'exhausted' ? 'exhausted' : 'completed',
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
            result.remainingPrints = Math.max(access.assignedQuota - access.usedPrints, 0);
            result.assignedPrints = access.assignedQuota;
            if (access.status === 'exhausted' || access.usedPrints >= access.assignedQuota) {
              result.remainingPrints = 0;
              result.status = 'exhausted';
            }
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
