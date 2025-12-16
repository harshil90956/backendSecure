import jwt from 'jsonwebtoken';
import User from '../models/User.js';
import Session from '../models/Session.js';
import BlockedIp from '../models/BlockedIp.js';

const normalizeIp = (value) => {
  if (!value || typeof value !== 'string') return '';
  const first = value.split(',')[0].trim();
  if (first === '::1') return '127.0.0.1';
  if (first.startsWith('::ffff:')) return first.slice('::ffff:'.length);
  return first;
};

const getClientIp = (req) => {
  const forwarded = req.headers['x-forwarded-for'];
  const raw =
    (typeof forwarded === 'string' && forwarded.length > 0
      ? forwarded
      : req.ip || (req.connection && req.connection.remoteAddress) || '') || '';
  return normalizeIp(raw);
};

export const authMiddleware = async (req, res, next) => {
  try {
    const authHeader = req.headers.authorization || '';
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null;

    if (!token) {
      return res.status(401).json({ logout: true, message: 'Unauthorized' });
    }

    const jwtSecret = process.env.JWT_SECRET;
    if (!jwtSecret) {
      return res.status(500).json({ message: 'Auth not configured' });
    }

    const currentIp = getClientIp(req);

    const originalUrl = req.originalUrl || req.url || '';

    const isAdminRoute = originalUrl.startsWith('/api/admin');

    if (!isAdminRoute) {
      const blocked = await BlockedIp.findOne({ ip: currentIp });
      if (blocked) {
        return res
          .status(401)
          .json({ logout: true, message: 'Access from this IP is blocked' });
      }
    }

    let payload;
    try {
      payload = jwt.verify(token, jwtSecret);
    } catch (jwtErr) {
      if (jwtErr.name === 'TokenExpiredError') {
        return res.status(401).json({ 
          logout: true, 
          message: 'Session expired - please log in again',
          expiredAt: jwtErr.expiredAt 
        });
      }
      throw jwtErr; // Re-throw other JWT errors to be caught by outer handler
    }

    const session = await Session.findOne({ token });

    if (!session) {
      return res
        .status(401)
        .json({ logout: true, message: 'Session expired or invalid' });
    }

    if (!isAdminRoute && normalizeIp(session.ip) !== currentIp) {
      await Session.deleteOne({ _id: session._id });
      return res
        .status(401)
        .json({ logout: true, message: 'IP mismatch for this session' });
    }

    const user = await User.findById(payload.userId).select('-passwordHash');

    if (!user) {
      await Session.deleteOne({ _id: session._id });
      return res.status(401).json({ logout: true, message: 'User not found' });
    }

    req.user = user;
    req.session = session;
    next();
  } catch (err) {
    console.error('Auth error', err);
    if (err.name === 'TokenExpiredError') {
      return res.status(401).json({ 
        logout: true, 
        message: 'Session expired - please log in again',
        expiredAt: err.expiredAt 
      });
    }
    return res.status(401).json({ logout: true, message: 'Invalid token' });
  }
};

export const requireAdmin = (req, res, next) => {
  if (!req.user || req.user.role !== 'admin') {
    return res.status(403).json({ message: 'Admin access required' });
  }
  next();
};
