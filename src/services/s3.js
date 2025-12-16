// src/services/s3.js
import dotenv from "dotenv";
dotenv.config();

import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

if (!process.env.AWS_REGION) {
  throw new Error("AWS_REGION missing");
}
if (!process.env.AWS_S3_BUCKET) {
  throw new Error("AWS_S3_BUCKET missing");
}

export const s3 = new S3Client({
  region: process.env.AWS_REGION,
});

export async function uploadToS3(buffer, contentType, prefix = "") {
  const key = `${prefix}${Date.now()}-${Math.random()
    .toString(36)
    .slice(2)}.pdf`;

  await s3.send(
    new PutObjectCommand({
      Bucket: process.env.AWS_S3_BUCKET,
      Key: key,
      Body: buffer,
      ContentType: contentType,
    })
  );

  return {
    key,
    url: `https://${process.env.AWS_S3_BUCKET}.s3.${process.env.AWS_REGION}.amazonaws.com/${key}`,
  };
}