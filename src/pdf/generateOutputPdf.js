import puppeteer from "puppeteer";

/**
 * generateOutputPdf
 * -----------------
 * RULES (very important):
 * 1. Physical units (mm) decide layout — NOT px
 * 2. Puppeteer viewport is irrelevant for print size
 * 3. CSS @page controls final PDF size
 * 4. preferCSSPageSize MUST be true
 *
 * If these rules hold → output can never be "small-small"
 */

export async function generateOutputPdf(htmlBody) {
  let browser;

  try {
    browser = await puppeteer.launch({
      headless: "new",
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
      ],
    });

    const page = await browser.newPage();

    /**
     * Viewport is ONLY for HTML rendering,
     * it does NOT affect PDF physical size.
     */
    await page.setViewport({
      width: 1200,
      height: 800,
      deviceScaleFactor: 1,
    });

    /**
     * HTML wrapper with STRICT print-safe CSS
     */
    const fullHtml = `
      <!DOCTYPE html>
      <html>
        <head>
          <meta charset="utf-8" />
          <style>
            /* ---- PRINT CONTRACT ---- */
            @page {
              size: A4;
              margin: 0;
            }

            html, body {
              margin: 0;
              padding: 0;
              width: 210mm;
              height: 297mm;
            }

            body {
              -webkit-print-color-adjust: exact;
              print-color-adjust: exact;
            }

            .page {
              width: 210mm;
              height: 297mm;
              position: relative;
              overflow: hidden;
              box-sizing: border-box;
            }
          </style>
        </head>
        <body>
          <div class="page">
            ${htmlBody}
          </div>
        </body>
      </html>
    `;

    await page.setContent(fullHtml, {
      waitUntil: ["domcontentloaded", "networkidle0"],
    });

    /**
     * PDF export
     * preferCSSPageSize = TRUE is NON-NEGOTIABLE
     */
    const pdfBuffer = await page.pdf({
      printBackground: true,
      preferCSSPageSize: true,
    });

    return pdfBuffer;
  } catch (err) {
    console.error("PDF generation failed:", err);
    throw err;
  } finally {
    if (browser) {
      await browser.close();
    }
  }
}
