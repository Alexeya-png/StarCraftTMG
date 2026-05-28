import fs from 'node:fs/promises';
import path from 'node:path';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);

const ROOT = process.cwd();
const SOURCE_DIR = process.env.TMG_ART_SOURCE_DIR || 'C:\\Users\\aradi\\Desktop\\photo';
const OUTPUT_DIR = path.join(ROOT, 'app', 'static', 'art', 'profile-banners');
const PLAYWRIGHT_CORE = path.join(ROOT, '.venv', 'Lib', 'site-packages', 'playwright', 'driver', 'package');
const CHROME_CANDIDATES = [
  'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
  'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',
];

const ACCENTS = {
  protoss: {
    primary: '#38f6d0',
    secondary: '#f6d56b',
    shadow: '#09071a',
  },
  terran: {
    primary: '#43b7ff',
    secondary: '#ff8f3d',
    shadow: '#071322',
  },
  zerg: {
    primary: '#b95dff',
    secondary: '#ff6048',
    shadow: '#170819',
  },
};

const VARIANTS = [
  {
    output: 'p1.jpg',
    source: 'PhotoCollage_1767020260214.jpg',
    faction: 'protoss',
    crop: { x: 0.02, y: 0.02, w: 0.56, h: 0.94 },
    anchorX: 0.46,
    anchorY: 0.5,
  },
  {
    output: 'p2.jpg',
    source: '7F78FC75-CEF6-42A0-A4E0-BDBEA72DC149.jpg',
    faction: 'protoss',
    anchorX: 0.5,
    anchorY: 0.52,
  },
  {
    output: 'p3.jpg',
    source: 'taldarim-zealots-v0-brx24kdzwqwg1.webp',
    faction: 'protoss',
    anchorX: 0.5,
    anchorY: 0.52,
  },
  {
    output: 't1.jpg',
    source: '20260427_192743.jpg',
    faction: 'terran',
    anchorX: 0.52,
    anchorY: 0.46,
  },
  {
    output: 't2.jpg',
    source: 'marauder-no-1-v0-z8slbxfpndxg1.webp',
    faction: 'terran',
    anchorX: 0.56,
    anchorY: 0.44,
  },
  {
    output: 't3.jpg',
    source: 'jacked-up-and-good-to-go-v0-3hff7f6gf1tg1.webp',
    faction: 'terran',
    anchorX: 0.38,
    anchorY: 0.48,
  },
  {
    output: 'z1.jpg',
    source: 'roach-corpser-v0-8ihi2bpno7wg1.webp',
    faction: 'zerg',
    anchorX: 0.52,
    anchorY: 0.52,
  },
  {
    output: 'z2.jpg',
    source: 'jg8wda8p3tzg1.png',
    faction: 'zerg',
    anchorX: 0.54,
    anchorY: 0.5,
  },
];

async function exists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function findChrome() {
  for (const candidate of CHROME_CANDIDATES) {
    if (await exists(candidate)) {
      return candidate;
    }
  }
  throw new Error('No Chrome or Edge executable was found for canvas rendering.');
}

function mimeForExtension(filePath) {
  switch (path.extname(filePath).toLowerCase()) {
    case '.jpg':
    case '.jpeg':
      return 'image/jpeg';
    case '.png':
      return 'image/png';
    case '.webp':
      return 'image/webp';
    default:
      return 'application/octet-stream';
  }
}

function decodeDataUrl(dataUrl) {
  const match = /^data:([^;]+);base64,(.+)$/.exec(dataUrl);
  if (!match) {
    throw new Error('Unexpected image data URL.');
  }
  return Buffer.from(match[2], 'base64');
}

async function renderVariant(page, variant) {
  const sourcePath = path.join(SOURCE_DIR, variant.source);
  if (!(await exists(sourcePath))) {
    throw new Error(`Missing source image: ${sourcePath}`);
  }

  const bytes = await fs.readFile(sourcePath);
  const imageDataUrl = `data:${mimeForExtension(sourcePath)};base64,${bytes.toString('base64')}`;
  const accent = ACCENTS[variant.faction];

  const renderedDataUrl = await page.evaluate(async ({ imageDataUrl, accent, variant }) => {
    function loadImage(src) {
      return new Promise((resolve, reject) => {
        const image = new Image();
        image.onload = () => resolve(image);
        image.onerror = () => reject(new Error(`Cannot decode ${variant.source}`));
        image.src = src;
      });
    }

    function hexToRgb(hex) {
      const clean = hex.replace('#', '');
      const value = Number.parseInt(clean.length === 3
        ? clean.split('').map((part) => part + part).join('')
        : clean, 16);
      return {
        r: (value >> 16) & 255,
        g: (value >> 8) & 255,
        b: value & 255,
      };
    }

    function rgba(hex, alpha) {
      const { r, g, b } = hexToRgb(hex);
      return `rgba(${r}, ${g}, ${b}, ${alpha})`;
    }

    function cropBox(image, crop) {
      if (!crop) {
        return { sx: 0, sy: 0, sw: image.width, sh: image.height };
      }
      return {
        sx: image.width * crop.x,
        sy: image.height * crop.y,
        sw: image.width * crop.w,
        sh: image.height * crop.h,
      };
    }

    function drawCover(ctx, image, width, height, crop, anchorX, anchorY, zoom = 1) {
      const { sx, sy, sw, sh } = cropBox(image, crop);
      const scale = Math.max(width / sw, height / sh) * zoom;
      const drawWidth = sw * scale;
      const drawHeight = sh * scale;
      const dx = (width - drawWidth) * anchorX;
      const dy = (height - drawHeight) * anchorY;
      ctx.drawImage(image, sx, sy, sw, sh, dx, dy, drawWidth, drawHeight);
    }

    function addNoise(ctx, width, height, alpha = 0.035) {
      const tileSize = 128;
      const tile = document.createElement('canvas');
      tile.width = tileSize;
      tile.height = tileSize;
      const tileCtx = tile.getContext('2d');
      const imageData = tileCtx.createImageData(tileSize, tileSize);
      for (let i = 0; i < imageData.data.length; i += 4) {
        const value = 100 + Math.random() * 92;
        imageData.data[i] = value;
        imageData.data[i + 1] = value;
        imageData.data[i + 2] = value;
        imageData.data[i + 3] = Math.floor(alpha * 255);
      }
      tileCtx.putImageData(imageData, 0, 0);
      ctx.fillStyle = ctx.createPattern(tile, 'repeat');
      ctx.fillRect(0, 0, width, height);
    }

    function addGrade(ctx, width, height) {
      const sideShade = ctx.createLinearGradient(0, 0, width, 0);
      sideShade.addColorStop(0, 'rgba(3, 6, 14, 0.76)');
      sideShade.addColorStop(0.24, 'rgba(3, 6, 14, 0.22)');
      sideShade.addColorStop(0.56, 'rgba(3, 6, 14, 0.04)');
      sideShade.addColorStop(0.82, 'rgba(3, 6, 14, 0.26)');
      sideShade.addColorStop(1, 'rgba(3, 6, 14, 0.82)');
      ctx.fillStyle = sideShade;
      ctx.fillRect(0, 0, width, height);

      const vertical = ctx.createLinearGradient(0, 0, 0, height);
      vertical.addColorStop(0, 'rgba(255,255,255,0.06)');
      vertical.addColorStop(0.34, 'rgba(255,255,255,0)');
      vertical.addColorStop(1, 'rgba(0,0,0,0.52)');
      ctx.fillStyle = vertical;
      ctx.fillRect(0, 0, width, height);

      const glow = ctx.createRadialGradient(width * 0.72, height * 0.42, 0, width * 0.72, height * 0.42, width * 0.58);
      glow.addColorStop(0, rgba(accent.primary, 0.22));
      glow.addColorStop(0.42, rgba(accent.secondary, 0.1));
      glow.addColorStop(1, 'rgba(0,0,0,0)');
      ctx.fillStyle = glow;
      ctx.fillRect(0, 0, width, height);
    }

    const image = await loadImage(imageDataUrl);
    const width = 2400;
    const height = 780;
    const canvas = document.createElement('canvas');
    canvas.width = width;
    canvas.height = height;
    const ctx = canvas.getContext('2d');

    ctx.fillStyle = accent.shadow;
    ctx.fillRect(0, 0, width, height);

    ctx.globalAlpha = 0.38;
    ctx.filter = 'blur(24px) brightness(0.52) saturate(1.42) contrast(1.12)';
    drawCover(ctx, image, width, height, variant.crop, variant.anchorX, variant.anchorY, 1.08);

    ctx.globalAlpha = 1;
    ctx.filter = 'brightness(1.04) saturate(1.18) contrast(1.1)';
    drawCover(ctx, image, width, height, variant.crop, variant.anchorX, variant.anchorY, 1);

    ctx.filter = 'none';
    addGrade(ctx, width, height);
    addNoise(ctx, width, height, 0.03);

    return canvas.toDataURL('image/jpeg', 0.93);
  }, { imageDataUrl, accent, variant });

  await fs.mkdir(OUTPUT_DIR, { recursive: true });
  await fs.writeFile(path.join(OUTPUT_DIR, variant.output), decodeDataUrl(renderedDataUrl));
}

async function main() {
  const { chromium } = require(PLAYWRIGHT_CORE);
  const chromePath = await findChrome();
  const browser = await chromium.launch({
    executablePath: chromePath,
    headless: true,
    args: ['--headless=new', '--disable-gpu'],
  });
  const page = await browser.newPage({ viewport: { width: 2400, height: 780 } });

  for (const variant of VARIANTS) {
    await renderVariant(page, variant);
    console.log(`generated ${variant.output} from ${variant.source}`);
  }

  await browser.close();
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
