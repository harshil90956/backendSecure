import puppeteer from 'puppeteer';

// A4 in px at 96 DPI (same as TicketOutputPreview.tsx uses)
const A4_WIDTH_PX = 794;
const A4_HEIGHT_PX = 1123;

const rawDeviceScaleFactor = Number(process.env.PDF_DEVICE_SCALE_FACTOR || 3);
const DEVICE_SCALE_FACTOR = Number.isFinite(rawDeviceScaleFactor)
  ? Math.max(1, Math.min(6, rawDeviceScaleFactor))
  : 3;

const debugEnabled =
  process.env.WORKER_DEBUG === '1' ||
  process.env.WORKER_DEBUG === 'true' ||
  process.env.WORKER_DEBUG === 'yes';

const debugLog = (...args) => {
  if (debugEnabled) console.log(...args);
};

let browserPromise = null;
let browserInstance = null;

async function resetBrowser() {
  try {
    if (browserInstance) {
      await browserInstance.close();
    }
  } catch {
    // ignore
  } finally {
    browserInstance = null;
    browserPromise = null;
  }
}

async function getBrowser() {
  if (browserInstance && typeof browserInstance.isConnected === 'function') {
    if (browserInstance.isConnected()) {
      return browserInstance;
    }
    await resetBrowser();
  }

  if (!browserPromise) {
    browserPromise = puppeteer
      .launch({
        headless: true,
        args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
      })
      .then((browser) => {
        browserInstance = browser;
        try {
          browser.on('disconnected', () => {
            browserInstance = null;
            browserPromise = null;
          });
        } catch {
          // ignore
        }
        return browser;
      })
      .catch(async (err) => {
        await resetBrowser();
        throw err;
      });
  }

  return browserPromise;
}

/**
 * pages: [ { items: [ { type, ... } ] } ]
 * Each item:
 *  - image: { type: 'image', src, x, y, width, height }
 *  - text:  { type: 'text', text, x, y, fontSize }
 */
export async function generateOutputPdfBuffer(pages) {
  let browser = await getBrowser();
  let page;
  let timeoutId;
  
  try {
    try {
      page = await browser.newPage();
    } catch (err) {
      const msg = err && typeof err.message === 'string' ? err.message : '';
      const name = err && typeof err.name === 'string' ? err.name : '';
      if (name === 'ConnectionClosedError' || msg.toLowerCase().includes('connection closed')) {
        await resetBrowser();
        browser = await getBrowser();
        page = await browser.newPage();
      } else {
        throw err;
      }
    }

    await page.setViewport({
      width: A4_WIDTH_PX,
      height: A4_HEIGHT_PX,
      deviceScaleFactor: DEVICE_SCALE_FACTOR,
    });
    // Disable navigation timeout so large/complex pages can finish rendering
    page.setDefaultNavigationTimeout(0);

    // Build HTML with one .page div per page, using absolute positioning
    const html = buildHtml(pages);
    debugLog('[generateOutputPdf] HTML built, length:', html.length);

    // Add a timeout to prevent hanging
    const renderPromise = page.setContent(html, { waitUntil: 'load', timeout: 0 });
    const timeoutPromise = new Promise((_, reject) => {
      timeoutId = setTimeout(() => reject(new Error('Page render timeout')), 30000);
    });
    
    await Promise.race([renderPromise, timeoutPromise]);
    clearTimeout(timeoutId);
    debugLog('[generateOutputPdf] Content set successfully');

    const pdfPromise = page.pdf({
      width: '210mm',
      height: '297mm',
      margin: { top: '0mm', right: '0mm', bottom: '0mm', left: '0mm' },
      printBackground: true,
      preferCSSPageSize: true,
    });

    // Add timeout for PDF generation
    const pdfTimeoutPromise = new Promise((_, reject) => {
      timeoutId = setTimeout(() => reject(new Error('PDF generation timeout')), 30000);
    });

    const pdfBuffer = await Promise.race([pdfPromise, pdfTimeoutPromise]);
    clearTimeout(timeoutId);
    debugLog('[generateOutputPdf] PDF generated, size:', pdfBuffer?.length);

    return pdfBuffer;
  } finally {
    try {
      if (page) await page.close();
    } catch (err) {
      if (err && err.code === 'EBUSY') {
        console.warn('Puppeteer page close EBUSY (ignoring):', err.path || err.message);
      } else {
        console.warn('Puppeteer page close error (ignored):', err);
      }
    }
  }
}

function escapeHtml(str = '') {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function buildHtml(pages = []) {
  const PX_TO_MM = 25.4 / 96;
  const PT_TO_MM = 25.4 / 72;

  const toMm = (value, unit) => {
    const n = Number(value);
    const safe = Number.isFinite(n) ? n : 0;
    return unit === 'mm' ? safe : safe * PX_TO_MM;
  };

  const formatMm = (value, unit) => `${toMm(value, unit)}mm`;

  const pageDivs = pages
    .map((page) => {
      const itemsHtml = (page.items || [])
        .map((item) => {
          if (item.type === 'image') {
            const src = item.src || '';
            const unit = item.unit === 'mm' || page.unit === 'mm' ? 'mm' : 'px';
            return `
              <img
                src="${src}"
                style="position:absolute; left:${formatMm(item.x, unit)}; top:${formatMm(item.y, unit)}; width:${formatMm(item.width, unit)}; height:${formatMm(item.height, unit)};"
              />
            `;
          }

          if (item.type === 'text') {
            const rawText = item.text || '';
            const unit = item.unit === 'mm' || page.unit === 'mm' ? 'mm' : 'px';
            const baseFontSize = item.fontSize || 12;
            const fontFamily = item.fontFamily || 'Arial, sans-serif';
            const color = item.color || '#000';

            if (Array.isArray(item.letterFontSizes) && item.letterFontSizes.length > 0) {
              const hasOffsets = Array.isArray(item.letterOffsets) && item.letterOffsets.length > 0;
              const spacingAfterArray =
                Array.isArray(item.letterSpacingAfterX) && item.letterSpacingAfterX.length > 0
                  ? item.letterSpacingAfterX
                  : Array.isArray(item.letterXOffsets) && item.letterXOffsets.length > 0
                    ? item.letterXOffsets
                    : null;

              const lettersHtml = Array.from(rawText)
                .map((ch, idx) => {
                  const safeChar = ch === ' ' ? '&nbsp;' : escapeHtml(ch);
                  const size = item.letterFontSizes[idx] || baseFontSize;
                  const offsetY = hasOffsets ? item.letterOffsets[idx] || 0 : 0;
                  const cumulativeSpacing = spacingAfterArray
                    ? spacingAfterArray
                        .slice(0, idx)
                        .reduce((sum, v) => sum + (Number(v) || 0), 0)
                    : 0;

                  const sizeMm = toMm(size, unit);
                  const offsetYmm = toMm(offsetY, unit);
                  // Legacy payloads used points for spacingAfterX, then converted pt->px (96/72).
                  // New payloads send mm directly. We always emit mm in CSS.
                  const cumulativeMm = unit === 'mm' ? cumulativeSpacing : cumulativeSpacing * PT_TO_MM;
                  return `<span style="font-size:${sizeMm}mm; font-family:${fontFamily}; white-space:pre; display:inline-block; transform: translate(${cumulativeMm}mm, ${offsetYmm}mm);">${safeChar}</span>`;
                })
                .join('');

              return `
              <div
                style="position:absolute; left:${formatMm(item.x, unit)}; top:${formatMm(item.y, unit)}; font-size:${toMm(baseFontSize, unit)}mm; font-family:${fontFamily}; color:${color}; white-space:pre;"
              >${lettersHtml}</div>
            `;
            }

            const text = escapeHtml(rawText);
            return `
              <div
                style="position:absolute; left:${formatMm(item.x, unit)}; top:${formatMm(item.y, unit)}; font-size:${toMm(baseFontSize, unit)}mm; font-family:${fontFamily}; color:${color}; white-space:pre;"
              >${text}</div>
            `;
          }

          return '';
        })
        .join('\n');

      return `
        <div class="page">
          ${itemsHtml}
        </div>
      `;
    })
    .join('\n');

  return `
    <html>
      <head>
        <style>
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
            background: white;
          }
          .page {
            position: relative;
            width: 210mm;
            height: 297mm;
            page-break-after: always;
            overflow: hidden;
          }
          .page:last-child {
            page-break-after: auto;
          }
          img {
            -webkit-print-color-adjust: exact;
            print-color-adjust: exact;
          }
        </style>
      </head>
      <body>
        ${pageDivs}
      </body>
    </html>
  `;
}
