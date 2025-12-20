import express from 'express';
import { generateOutputPdfBuffer } from '../pdf/generateOutputPdf.js';
import { uploadToS3 } from '../services/s3.js';
import Document from '../models/Document.js';
import { authMiddleware } from '../middleware/auth.js';
import { s3 } from '../services/s3.js';
import { GetObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

const router = express.Router();

// GET /api/print-agent/download
// Returns a presigned URL (JSON) or redirects to it for downloading the desktop print agent.
router.get('/print-agent/download', async (req, res) => {
  try {
    const bucket = process.env.AWS_S3_BUCKET;
    if (!bucket) {
      return res.status(500).json({ message: 'S3 not configured' });
    }

    const installerKey =
      process.env.PRINT_AGENT_S3_KEY || 'securepdf/print-agent/SecurePrintHub-Setup-1.0.0.exe';

    const rawFilename = process.env.PRINT_AGENT_FILENAME || 'SecurePrintHub-Setup-1.0.0.exe';
    const filename = rawFilename.toLowerCase().endsWith('.exe') ? rawFilename : `${rawFilename}.exe`;

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
    if (!Number.isFinite(size) || size < 1024 * 1024) {
      return res.status(500).json({
        message: 'Print agent installer is missing or corrupted. Please re-upload a valid installer build.',
      });
    }

    const command = new GetObjectCommand({
      Bucket: bucket,
      Key: installerKey,
      ResponseContentDisposition: `attachment; filename="${filename}"`,
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

// POST /api/generate-output-pdf
router.post('/generate-output-pdf', authMiddleware, async (req, res) => {
  try {
    const { pages } = req.body || {};

    if (!Array.isArray(pages) || pages.length === 0) {
      return res.status(400).json({ message: 'pages array is required' });
    }

    // Extract page size from first page layout or default to A4
    const pageWidthMm = pages[0]?.pageWidthMm || 210;
    const pageHeightMm = pages[0]?.pageHeightMm || 297;
    const pageOptions = { pageWidthMm, pageHeightMm };

    // 1) Build PDF buffer via Puppeteer
    //    Sometimes on Windows Puppeteer can randomly close the Chrome target
    //    while evaluating, causing a TargetCloseError. We retry once in that case.
    let pdfBuffer;
    let hasRetried = false;
    while (true) {
      try {
        pdfBuffer = await generateOutputPdfBuffer(pages, pageOptions);
        break;
      } catch (err) {
        const isTargetClosed =
          err &&
          (err.name === 'TargetCloseError' ||
            err.cause?.name === 'ProtocolError' ||
            (typeof err.message === 'string' && err.message.includes('Target closed')));

        if (isTargetClosed && !hasRetried) {
          console.warn('generate-output-pdf TargetCloseError, retrying once...');
          hasRetried = true;
          continue;
        }

        throw err;
      }
    }

    // 2) Upload to S3
    const { key, url } = await uploadToS3(pdfBuffer, 'application/pdf', 'generated/output/');

    // 3) Create Document record
    const doc = await Document.create({
      title: 'Generated Output',
      fileKey: key,
      fileUrl: url,
      totalPrints: 0,
      createdBy: req.user._id,
      mimeType: 'application/pdf',
      documentType: 'generated-output',
    });

    return res.status(201).json({
      success: true,
      fileKey: key,
      fileUrl: url,
      documentId: doc._id,
    });
  } catch (err) {
    console.error('generate-output-pdf error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

router.post('/series/generate', async (req, res) => {
  try {
    const { templateBase64, templateType, startNumber, endNumber } = req.body || {};

    if (!templateBase64 || !templateType) {
      return res.status(400).json({ message: 'templateBase64 and templateType are required' });
    }

    const start = Number(startNumber);
    const end = Number(endNumber);

    if (!Number.isFinite(start) || !Number.isFinite(end)) {
      return res.status(400).json({ message: 'startNumber and endNumber must be numbers' });
    }

    if (start >= end) {
      return res
        .status(400)
        .json({ message: 'startNumber must be less than endNumber' });
    }

    if (end - start > 1000) {
      return res
        .status(400)
        .json({ message: 'Maximum 1000 numbers per batch' });
    }

    const mimeType =
      templateType === 'application/pdf' || templateType === 'pdf'
        ? 'application/pdf'
        : 'image/svg+xml';

    const dataUrl = `data:${mimeType};base64,${templateBase64}`;

    const pages = [];
    for (let n = start; n <= end; n++) {
      pages.push({
        items: [
          {
            type: 'image',
            src: dataUrl,
            x: 0,
            y: 0,
            width: 794,
            height: 1123,
          },
          {
            type: 'text',
            text: String(n),
            x: 80,
            y: 80,
            fontSize: 48,
          },
        ],
      });
    }

    const pdfBuffer = await generateOutputPdfBuffer(pages, { pageWidthMm: 210, pageHeightMm: 297 });

    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', 'attachment; filename="series.pdf"');
    return res.send(pdfBuffer);
  } catch (err) {
    console.error('series/generate error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

export default router;
