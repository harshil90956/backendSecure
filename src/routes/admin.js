import express from 'express';
import multer from 'multer';
import bcrypt from 'bcryptjs';
import crypto from 'crypto';
import User from '../models/User.js';
import Document from '../models/Document.js';
import DocumentAccess from '../models/DocumentAccess.js';
import DocumentJobs from '../models/DocumentJobs.js';
import { uploadToS3 } from '../services/s3.js';
import { outputPdfQueue } from '../../queues/outputPdfQueue.js';
import { redisEnabled } from '../redisAvailability.js';
import { authMiddleware, requireAdmin } from '../middleware/auth.js';
import Session from '../models/Session.js';
import BlockedIp from '../models/BlockedIp.js';

const router = express.Router();
const upload = multer();

// Upload a single base64 ticket image to S3 and return its key
router.post('/upload-ticket-image', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { base64 } = req.body || {};

    if (!base64 || typeof base64 !== 'string') {
      return res.status(400).json({ message: 'base64 field is required' });
    }

    const match = base64.match(/^data:(.+);base64,(.*)$/);
    if (!match) {
      return res.status(400).json({ message: 'Invalid base64 data URL' });
    }

    const contentType = match[1] || 'image/png';
    const base64Data = match[2];

    const buffer = Buffer.from(base64Data, 'base64');

    const { key } = await uploadToS3(buffer, contentType, 'generated/images/');

    return res.status(201).json({ success: true, key });
  } catch (err) {
    console.error('Upload ticket image error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Upload document (PDF/SVG) and create Document record
router.post('/documents', authMiddleware, requireAdmin, upload.single('file'), async (req, res) => {
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

    const { key, url } = await uploadToS3(file.buffer, file.mimetype);

    const doc = await Document.create({
      title,
      fileKey: key,
      fileUrl: url,
      totalPrints: parsedTotal,
      createdBy: req.user._id,
    });

    return res.status(201).json(doc);
  } catch (err) {
    console.error('Upload document error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Create background assignment job instead of synchronous PDF generation
router.post('/assign-job', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { email, assignedQuota, layoutPages } = req.body || {};

    console.log('[admin/assign-job] request', {
      adminUserId: req.user?._id?.toString?.(),
      email,
      assignedQuota,
      layoutPagesCount: Array.isArray(layoutPages) ? layoutPages.length : null,
    });

    if (!email || !assignedQuota || !layoutPages) {
      return res.status(400).json({ message: 'email, assignedQuota and layoutPages are required' });
    }

    if (!Array.isArray(layoutPages) || layoutPages.length === 0) {
      return res.status(400).json({ message: 'layoutPages must be a non-empty array' });
    }

    const pagesNum = Number(assignedQuota ?? layoutPages.length);
    if (Number.isNaN(pagesNum) || pagesNum <= 0) {
      return res.status(400).json({ message: 'assignedQuota must be a positive number' });
    }

    // Safety: ensure layoutPages only contain lightweight S3 references, no base64 blobs
    const sanitizedLayoutPages = layoutPages.map((page) => ({
      items: Array.isArray(page.items)
        ? page.items.map((item) => ({
            ...item,
            // Expecting src like "s3://<key>". We trust frontend to send only small strings.
            src: typeof item.src === 'string' ? item.src : '',
          }))
        : [],
    }));

    const user = await User.findOne({ email: email.toLowerCase() });
    if (!user) {
      return res.status(404).json({ message: 'User with this email not found' });
    }

    const totalPages = sanitizedLayoutPages.length;

    const jobDoc = await DocumentJobs.create({
      email: email.toLowerCase(),
      assignedQuota: pagesNum,
      // Optional lightweight meta; we do not store full layout in Mongo
      layoutPages: sanitizedLayoutPages,
      status: 'processing',
      stage: 'rendering',
      totalPages,
      completedPages: 0,
      outputDocumentId: null,
      userId: user._id,
      createdBy: req.user._id,
    });

    console.log('[admin/assign-job] job created', {
      jobId: jobDoc._id?.toString?.(),
      userId: user._id?.toString?.(),
      totalPages,
      assignedQuota: pagesNum,
    });

    const baseJobId = jobDoc._id.toString();

    if (!redisEnabled) {
      throw new Error('Redis is disabled but render jobs require Redis');
    }

    for (let pageIndex = 0; pageIndex < totalPages; pageIndex += 1) {
      try {
        const pageLayout = sanitizedLayoutPages[pageIndex];
        const layoutMode =
          pageLayout && typeof pageLayout.layoutMode === 'string' ? pageLayout.layoutMode : 'raster';

        const payload = {
          jobId: jobDoc._id.toString(),
          documentJobId: jobDoc._id.toString(),
          email: user.email.toLowerCase(),
          pageIndex,
          totalPages,
          pageLayout,
          layoutMode,
          assignedQuota: pagesNum,
          adminUserId: req.user._id,
          s3TemplateKey: null,
          inputPdfKey: null,
        };

        console.log('[admin/assign-job] enqueue payload', payload);

        await outputPdfQueue.add(
          'renderPage',
          payload,
          {
            jobId: `${jobDoc._id}-page-${pageIndex}`,
            removeOnComplete: true,
            removeOnFail: false,
          }
        );

        console.log(
          `📤 Enqueued render job page ${pageIndex + 1}/${totalPages} for job ${jobDoc._id}`
        );
      } catch (err) {
        console.error(
          `❌ Failed to enqueue page ${pageIndex + 1}/${totalPages} for job ${jobDoc._id}`,
          err
        );
        throw err;
      }
    }

    console.log('[admin/assign-job] enqueued render jobs', {
      jobId: baseJobId,
      pages: totalPages,
    });

    return res.status(201).json({
      success: true,
      message: 'Assignment job created',
      jobId: jobDoc._id.toString(),
    });
  } catch (err) {
    console.error('Create assign job error', err);
    const status = err && typeof err.statusCode === 'number' ? err.statusCode : 500;
    if (status === 503) {
      return res.status(503).json({
        success: false,
        message: 'Background queue is disabled',
      });
    }

    return res.status(500).json({ message: 'Internal server error' });
  }
});

router.get('/users/:userId/sessions', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { userId } = req.params;

    const sessions = await Session.find({ userId }).sort({ createdAt: -1 });

    return res.json({ sessions });
  } catch (err) {
    console.error('List user sessions error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

router.post('/sessions/:sessionId/logout', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { sessionId } = req.params;

    await Session.deleteOne({ _id: sessionId });

    return res.json({ success: true });
  } catch (err) {
    console.error('Logout session error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

router.post('/sessions/:sessionId/block-ip', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { sessionId } = req.params;
    const { reason } = req.body || {};

    const session = await Session.findById(sessionId);
    if (!session) {
      return res.status(404).json({ message: 'Session not found' });
    }

    const ip = session.ip;

    await BlockedIp.findOneAndUpdate(
      { ip },
      {
        ip,
        reason: reason || 'Blocked from admin panel',
        blockedBy: req.user._id,
        createdAt: new Date(),
      },
      { upsert: true, new: true }
    );

    await Session.deleteMany({ ip });

    return res.json({ success: true });
  } catch (err) {
    console.error('Block IP error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

router.post('/logout-all', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { userId } = req.body;

    if (!userId) {
      return res.status(400).json({ message: 'userId is required' });
    }

    await Session.deleteMany({ userId });

    return res.json({ success: true });
  } catch (err) {
    console.error('Logout all devices error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Get all documents created by admin
router.get('/documents', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const docs = await Document.find({ createdBy: req.user._id }).sort({ createdAt: -1 });
    return res.json(docs);
  } catch (err) {
    console.error('List documents error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Assign or update quota for a user on a document (by userId)
router.post('/documents/:id/assign', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const { userId, assignedQuota } = req.body;

    if (!userId || !assignedQuota) {
      return res.status(400).json({ message: 'userId and assignedQuota are required' });
    }

    const parsedQuota = Number(assignedQuota);
    if (Number.isNaN(parsedQuota) || parsedQuota <= 0) {
      return res.status(400).json({ message: 'assignedQuota must be a positive number' });
    }

    const access = await DocumentAccess.findOneAndUpdate(
      { userId, documentId: id },
      { userId, documentId: id, assignedQuota: parsedQuota },
      { upsert: true, new: true }
    );

    return res.json(access);
  } catch (err) {
    console.error('Assign quota error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Assign or update quota for a user on a document, using user email
router.post('/documents/:id/assign-by-email', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const { email, assignedQuota } = req.body;

    if (!email || !assignedQuota) {
      return res.status(400).json({ message: 'email and assignedQuota are required' });
    }

    const parsedQuota = Number(assignedQuota);
    if (Number.isNaN(parsedQuota) || parsedQuota <= 0) {
      return res.status(400).json({ message: 'assignedQuota must be a positive number' });
    }

    const user = await User.findOne({ email: email.toLowerCase() });
    if (!user) {
      return res.status(404).json({ message: 'User with this email not found' });
    }

    const access = await DocumentAccess.findOneAndUpdate(
      { userId: user._id, documentId: id },
      { userId: user._id, documentId: id, assignedQuota: parsedQuota },
      { upsert: true, new: true }
    );

    if (!access.sessionToken) {
      access.sessionToken = crypto.randomBytes(32).toString('hex');
      await access.save();
    }

    return res.json(access);
  } catch (err) {
    console.error('Assign quota by email error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Create a new user (admin only)
router.post('/users', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { email, password, role = 'user' } = req.body;

    if (!email || !password) {
      return res.status(400).json({ message: 'Email and password are required' });
    }

    if (!['admin', 'user'].includes(role)) {
      return res.status(400).json({ message: 'Invalid role. Must be either "admin" or "user"' });
    }

    const existingUser = await User.findOne({ email: email.toLowerCase() });
    if (existingUser) {
      return res.status(400).json({ message: 'Email already registered' });
    }

    const passwordHash = await bcrypt.hash(password, 10);

    const user = await User.create({
      email: email.toLowerCase(),
      passwordHash,
      role,
    });

    return res.status(201).json({
      user: { id: user._id, email: user.email, role: user.role },
    });
  } catch (err) {
    console.error('Create user error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Admin password change
router.put('/change-password', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { currentPassword, newPassword } = req.body;

    if (!currentPassword || !newPassword) {
      return res.status(400).json({ message: 'Current password and new password are required' });
    }

    if (newPassword.length < 6) {
      return res.status(400).json({ message: 'New password must be at least 6 characters long' });
    }

    // Get current admin user
    const admin = await User.findById(req.user._id);
    if (!admin) {
      return res.status(404).json({ message: 'Admin user not found' });
    }

    // Verify current password
    const isCurrentPasswordValid = await bcrypt.compare(currentPassword, admin.passwordHash);
    if (!isCurrentPasswordValid) {
      return res.status(400).json({ message: 'Current password is incorrect' });
    }

    // Hash new password
    const newPasswordHash = await bcrypt.hash(newPassword, 10);

    // Update password
    await User.findByIdAndUpdate(admin._id, { passwordHash: newPasswordHash });

    return res.json({ success: true, message: 'Password changed successfully' });
  } catch (err) {
    console.error('Admin change password error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Create a new admin (admin only)
router.post('/admins', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const { email, password } = req.body;

    if (!email || !password) {
      return res.status(400).json({ message: 'Email and password are required' });
    }

    if (password.length < 6) {
      return res.status(400).json({ message: 'Password must be at least 6 characters long' });
    }

    const existingUser = await User.findOne({ email: email.toLowerCase() });
    if (existingUser) {
      return res.status(400).json({ message: 'Email already registered' });
    }

    const passwordHash = await bcrypt.hash(password, 10);

    const admin = await User.create({
      email: email.toLowerCase(),
      passwordHash,
      role: 'admin',
    });

    return res.status(201).json({
      admin: { id: admin._id, email: admin.email, role: admin.role },
      message: 'Admin created successfully'
    });
  } catch (err) {
    console.error('Create admin error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Get all admins (admin only)
router.get('/admins', authMiddleware, requireAdmin, async (req, res) => {
  try {
    const admins = await User.find({ role: 'admin' })
      .select('email role createdAt')
      .sort({ createdAt: -1 });

    return res.json({ admins });
  } catch (err) {
    console.error('List admins error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

export default router;
