import mongoose from "mongoose";

const documentSchema = new mongoose.Schema(
  {
    title: {
      type: String,
      required: true,
      trim: true,
    },

    fileKey: {
      type: String,
      required: true,
      index: true,
    },

    fileUrl: {
      type: String,
      required: true,
    },

    // 🔐 Print quota (system-managed)
    totalPrints: {
      type: Number,
      default: 0,
      min: 0,
    },

    createdBy: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "User",
      required: true,
      index: true,
    },

    mimeType: {
      type: String,
      default: "application/pdf",
    },

    documentType: {
      type: String,
      enum: ["source", "generated-output"],
      default: "source",
      index: true,
    },
  },
  {
    timestamps: true,
  }
);

const Document =
  mongoose.models.Document || mongoose.model("Document", documentSchema);

export default Document;
