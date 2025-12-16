const mongoose = require('mongoose');
require('dotenv').config();

async function createTestDoc() {
  await mongoose.connect(process.env.MONGO_URI);
  const db = mongoose.connection.db;

  // Get or create a test user
  const users = db.collection('users');
  let user = await users.findOne({ email: 'test@example.com' });
  if (!user) {
    const result = await users.insertOne({ email: 'test@example.com', password: 'dummy', createdAt: new Date() });
    user = { _id: result.insertedId };
  }

  // Create a dummy document
  const docs = db.collection('documents');
  const docResult = await docs.insertOne({
    title: 'Test Document for Print',
    s3Key: 'dummy.pdf',
    uploadedBy: user._id,
    createdAt: new Date(),
  });
  const docId = docResult.insertedId;

  // Create DocumentAccess with sessionToken
  const crypto = require('crypto');
  const sessionToken = crypto.randomBytes(32).toString('hex');
  const accesses = db.collection('documentaccesses');
  await accesses.insertOne({
    userId: user._id,
    documentId: docId,
    assignedQuota: 3,
    usedPrints: 0,
    sessionToken,
    createdAt: new Date(),
  });

  console.log('Test document created!');
  console.log('Document ID:', docId.toString());
  console.log('Session Token:', sessionToken);
  await mongoose.disconnect();
}

createTestDoc().catch(console.error);
