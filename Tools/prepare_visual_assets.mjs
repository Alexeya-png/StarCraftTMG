import fs from 'node:fs/promises';
import path from 'node:path';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);

const ROOT = process.cwd();
const SOURCE_DIR = process.env.TMG_ART_SOURCE_DIR || 'C:\\Users\\aradi\\Desktop\\photo';
const ART_ROOT = path.join(ROOT, 'app', 'static', 'art');
const BANNER_DIR = path.join(ART_ROOT, 'profile-banners');
const BACKGROUND_DIR = path.join(ART_ROOT, 'site-backgrounds');
const CARD_DIR = path.join(ART_ROOT, 'cards');
const BADGE_DIR = path.join(ROOT, 'app', 'static', 'badges', 'league-sci-fi-v1');
const PLAYWRIGHT_CORE = path.join(ROOT, '.venv', 'Lib', 'site-packages', 'playwright', 'driver', 'package');
const CHROME_CANDIDATES = [
  'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
  'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',
];

const SOURCE_EXTENSIONS = new Set(['.jpg', '.jpeg', '.png', '.webp']);

const FACTION_BY_FILE = new Map([
  ['PhotoCollage_1767020260214.jpg', 'protoss'],
  ['PhotoCollage_1776730962871.jpg', 'zerg'],
  ['PhotoCollage_1777588962244.jpg', 'terran'],
  ['PXL_20260419_184929936.jpg', 'protoss'],
  ['20260427_192743.jpg', 'terran'],
  ['7F78FC75-CEF6-42A0-A4E0-BDBEA72DC149.jpg', 'protoss'],
  ['jg8wda8p3tzg1.png', 'zerg'],
  ['taldarim-zealots-v0-brx24kdzwqwg1.webp', 'protoss'],
  ['roach-corpser-v0-8ihi2bpno7wg1.webp', 'zerg'],
  ['jacked-up-and-good-to-go-v0-3hff7f6gf1tg1.webp', 'terran'],
  ['marauder-no-1-v0-z8slbxfpndxg1.webp', 'terran'],
]);

const FACTION_ACCENTS = {
  terran: {
    primary: '#43b7ff',
    secondary: '#ff8f3d',
    shadow: '#071322',
  },
  protoss: {
    primary: '#38f6d0',
    secondary: '#f6d56b',
    shadow: '#100b24',
  },
  zerg: {
    primary: '#b95dff',
    secondary: '#ff6048',
    shadow: '#170819',
  },
  neutral: {
    primary: '#8baeff',
    secondary: '#f5ba49',
    shadow: '#090d1a',
  },
};

function slugify(value) {
  return value
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80) || 'asset';
}

function inferFaction(filename) {
  const exact = FACTION_BY_FILE.get(filename);
  if (exact) {
    return exact;
  }

  const lower = filename.toLowerCase();
  if (/(terran|marauder|marine|jacked|tank|vulture|goliath)/.test(lower)) {
    return 'terran';
  }
  if (/(protoss|taldarim|zealot|templar|psi|pxl)/.test(lower)) {
    return 'protoss';
  }
  if (/(zerg|roach|hydra|corpser|nidus|brood)/.test(lower)) {
    return 'zerg';
  }
  return 'neutral';
}

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

function mimeForExtension(ext) {
  switch (ext.toLowerCase()) {
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

async function writeDataUrl(filePath, dataUrl) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, decodeDataUrl(dataUrl));
}

async function renderPhotoAssets(page, sourcePath, record) {
  const ext = path.extname(sourcePath);
  const mime = mimeForExtension(ext);
  const bytes = await fs.readFile(sourcePath);
  const imageDataUrl = `data:${mime};base64,${bytes.toString('base64')}`;
  const accent = FACTION_ACCENTS[record.faction] || FACTION_ACCENTS.neutral;

  const rendered = await page.evaluate(async ({ imageDataUrl, accent, label }) => {
    function loadImage(src) {
      return new Promise((resolve, reject) => {
        const image = new Image();
        image.onload = () => resolve(image);
        image.onerror = () => reject(new Error(`Cannot decode ${label}`));
        image.src = src;
      });
    }

    function hexToRgb(hex) {
      const clean = hex.replace('#', '');
      const value = Number.parseInt(clean.length === 3
        ? clean.split('').map((c) => c + c).join('')
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

    function drawCover(ctx, image, width, height, zoom = 1.0) {
      const scale = Math.max(width / image.width, height / image.height) * zoom;
      const drawWidth = image.width * scale;
      const drawHeight = image.height * scale;
      const dx = (width - drawWidth) / 2;
      const dy = (height - drawHeight) / 2;
      ctx.drawImage(image, dx, dy, drawWidth, drawHeight);
    }

    function drawContain(ctx, image, width, height, maxWidthFactor, maxHeightFactor) {
      const maxWidth = width * maxWidthFactor;
      const maxHeight = height * maxHeightFactor;
      const scale = Math.min(maxWidth / image.width, maxHeight / image.height);
      const drawWidth = image.width * scale;
      const drawHeight = image.height * scale;
      const dx = (width - drawWidth) / 2;
      const dy = (height - drawHeight) / 2;
      ctx.drawImage(image, dx, dy, drawWidth, drawHeight);
    }

    function addVignette(ctx, width, height, strength = 0.72) {
      const gradient = ctx.createRadialGradient(
        width * 0.52,
        height * 0.45,
        Math.min(width, height) * 0.12,
        width * 0.5,
        height * 0.5,
        Math.max(width, height) * 0.68,
      );
      gradient.addColorStop(0, 'rgba(0,0,0,0)');
      gradient.addColorStop(0.62, `rgba(0,0,0,${strength * 0.26})`);
      gradient.addColorStop(1, `rgba(0,0,0,${strength})`);
      ctx.fillStyle = gradient;
      ctx.fillRect(0, 0, width, height);
    }

    function addCinematicGrade(ctx, width, height) {
      const horizontal = ctx.createLinearGradient(0, 0, width, 0);
      horizontal.addColorStop(0, 'rgba(3, 6, 14, 0.76)');
      horizontal.addColorStop(0.34, 'rgba(3, 6, 14, 0.18)');
      horizontal.addColorStop(0.67, 'rgba(3, 6, 14, 0.16)');
      horizontal.addColorStop(1, 'rgba(3, 6, 14, 0.78)');
      ctx.fillStyle = horizontal;
      ctx.fillRect(0, 0, width, height);

      const top = ctx.createLinearGradient(0, 0, 0, height);
      top.addColorStop(0, 'rgba(255,255,255,0.08)');
      top.addColorStop(0.16, 'rgba(255,255,255,0.00)');
      top.addColorStop(1, 'rgba(0,0,0,0.62)');
      ctx.fillStyle = top;
      ctx.fillRect(0, 0, width, height);

      const glow = ctx.createRadialGradient(width * 0.72, height * 0.36, 0, width * 0.72, height * 0.36, width * 0.72);
      glow.addColorStop(0, rgba(accent.primary, 0.2));
      glow.addColorStop(0.38, rgba(accent.secondary, 0.08));
      glow.addColorStop(1, 'rgba(0,0,0,0)');
      ctx.fillStyle = glow;
      ctx.fillRect(0, 0, width, height);
    }

    function addBannerGrade(ctx, width, height) {
      const horizontal = ctx.createLinearGradient(0, 0, width, 0);
      horizontal.addColorStop(0, 'rgba(3, 6, 14, 0.72)');
      horizontal.addColorStop(0.26, 'rgba(3, 6, 14, 0.2)');
      horizontal.addColorStop(0.56, 'rgba(3, 6, 14, 0.02)');
      horizontal.addColorStop(0.82, 'rgba(3, 6, 14, 0.2)');
      horizontal.addColorStop(1, 'rgba(3, 6, 14, 0.7)');
      ctx.fillStyle = horizontal;
      ctx.fillRect(0, 0, width, height);

      const vertical = ctx.createLinearGradient(0, 0, 0, height);
      vertical.addColorStop(0, 'rgba(255,255,255,0.06)');
      vertical.addColorStop(0.32, 'rgba(255,255,255,0)');
      vertical.addColorStop(1, 'rgba(0,0,0,0.48)');
      ctx.fillStyle = vertical;
      ctx.fillRect(0, 0, width, height);

      const glow = ctx.createRadialGradient(width * 0.72, height * 0.42, 0, width * 0.72, height * 0.42, width * 0.58);
      glow.addColorStop(0, rgba(accent.primary, 0.2));
      glow.addColorStop(0.42, rgba(accent.secondary, 0.08));
      glow.addColorStop(1, 'rgba(0,0,0,0)');
      ctx.fillStyle = glow;
      ctx.fillRect(0, 0, width, height);
    }

    function addNoise(ctx, width, height, alpha = 0.045) {
      const tileSize = 128;
      const tile = document.createElement('canvas');
      tile.width = tileSize;
      tile.height = tileSize;
      const tileCtx = tile.getContext('2d');
      const imageData = tileCtx.createImageData(tileSize, tileSize);
      for (let i = 0; i < imageData.data.length; i += 4) {
        const value = 100 + Math.random() * 90;
        imageData.data[i] = value;
        imageData.data[i + 1] = value;
        imageData.data[i + 2] = value;
        imageData.data[i + 3] = Math.floor(alpha * 255);
      }
      tileCtx.putImageData(imageData, 0, 0);
      const pattern = ctx.createPattern(tile, 'repeat');
      ctx.fillStyle = pattern;
      ctx.fillRect(0, 0, width, height);
    }

    function drawFrame(ctx, width, height) {
      ctx.save();
      ctx.strokeStyle = rgba(accent.primary, 0.34);
      ctx.lineWidth = 3;
      ctx.strokeRect(1.5, 1.5, width - 3, height - 3);
      ctx.strokeStyle = 'rgba(255,255,255,0.12)';
      ctx.lineWidth = 1;
      ctx.strokeRect(12.5, 12.5, width - 25, height - 25);
      ctx.restore();
    }

    function makeProfileBanner(image) {
      const width = 1600;
      const height = 520;
      const canvas = document.createElement('canvas');
      canvas.width = width;
      canvas.height = height;
      const ctx = canvas.getContext('2d');

      ctx.fillStyle = accent.shadow;
      ctx.fillRect(0, 0, width, height);
      ctx.globalAlpha = 0.22;
      ctx.filter = 'blur(24px) brightness(0.52) saturate(1.38) contrast(1.12)';
      drawContain(ctx, image, width, height, 0.98, 0.98);
      ctx.filter = 'none';
      ctx.globalAlpha = image.width / image.height > 1.7 ? 0.84 : 0.92;
      ctx.filter = 'brightness(1.04) saturate(1.2) contrast(1.1)';
      drawContain(ctx, image, width, height, 0.98, 0.98);
      ctx.globalAlpha = 1;
      ctx.filter = 'none';
      addBannerGrade(ctx, width, height);
      addVignette(ctx, width, height, 0.48);
      addNoise(ctx, width, height, 0.035);
      drawFrame(ctx, width, height);
      return canvas.toDataURL('image/jpeg', 0.88);
    }

    function makeSiteBackground(image) {
      const width = 1920;
      const height = 1080;
      const canvas = document.createElement('canvas');
      canvas.width = width;
      canvas.height = height;
      const ctx = canvas.getContext('2d');

      ctx.fillStyle = '#070b14';
      ctx.fillRect(0, 0, width, height);
      ctx.globalAlpha = 0.26;
      ctx.filter = 'blur(16px) brightness(0.46) saturate(1.32) contrast(1.1)';
      drawContain(ctx, image, width, height, 0.98, 0.98);
      ctx.filter = 'none';
      ctx.globalAlpha = 0.58;
      ctx.filter = 'brightness(0.82) saturate(1.08)';
      drawContain(ctx, image, width, height, 0.98, 0.98);
      ctx.globalAlpha = 1;
      ctx.filter = 'none';
      addCinematicGrade(ctx, width, height);
      addVignette(ctx, width, height, 0.84);
      addNoise(ctx, width, height, 0.052);
      return canvas.toDataURL('image/jpeg', 0.84);
    }

    function makeCard(image) {
      const width = 900;
      const height = 900;
      const canvas = document.createElement('canvas');
      canvas.width = width;
      canvas.height = height;
      const ctx = canvas.getContext('2d');

      ctx.fillStyle = accent.shadow;
      ctx.fillRect(0, 0, width, height);
      ctx.globalAlpha = 0.24;
      ctx.filter = 'blur(18px) brightness(0.52) saturate(1.24) contrast(1.08)';
      drawContain(ctx, image, width, height, 0.98, 0.98);
      ctx.globalAlpha = 1;
      ctx.filter = 'brightness(0.86) saturate(1.2) contrast(1.12)';
      drawContain(ctx, image, width, height, 0.98, 0.98);
      ctx.filter = 'none';
      addCinematicGrade(ctx, width, height);
      addVignette(ctx, width, height, 0.72);
      addNoise(ctx, width, height, 0.035);
      drawFrame(ctx, width, height);
      return canvas.toDataURL('image/jpeg', 0.88);
    }

    const image = await loadImage(imageDataUrl);
    return {
      width: image.naturalWidth,
      height: image.naturalHeight,
      banner: makeProfileBanner(image),
      background: makeSiteBackground(image),
      card: makeCard(image),
    };
  }, { imageDataUrl, accent, label: record.source_filename });

  const bannerFile = `${record.slug}.jpg`;
  const backgroundFile = `${record.slug}.jpg`;
  const cardFile = `${record.slug}.jpg`;
  await writeDataUrl(path.join(BANNER_DIR, bannerFile), rendered.banner);
  await writeDataUrl(path.join(BACKGROUND_DIR, backgroundFile), rendered.background);
  await writeDataUrl(path.join(CARD_DIR, cardFile), rendered.card);

  return {
    ...record,
    source_width: rendered.width,
    source_height: rendered.height,
    assets: {
      profile_banner: `/static/art/profile-banners/${bannerFile}`,
      site_background: `/static/art/site-backgrounds/${backgroundFile}`,
      card: `/static/art/cards/${cardFile}`,
    },
  };
}

async function renderBadges(page) {
  const rendered = await page.evaluate(() => {
    const racePalette = {
      terran: {
        primary: '#46baff',
        secondary: '#ff9a42',
        core: '#d7e8ff',
        dark: '#07131f',
      },
      protoss: {
        primary: '#35f5d2',
        secondary: '#f6d76b',
        core: '#fff3b2',
        dark: '#100b28',
      },
      zerg: {
        primary: '#bb62ff',
        secondary: '#ff614c',
        core: '#ffd0f2',
        dark: '#190819',
      },
    };

    function hexToRgb(hex) {
      const clean = hex.replace('#', '');
      const value = Number.parseInt(clean, 16);
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

    function polygon(ctx, points) {
      ctx.beginPath();
      points.forEach(([x, y], index) => {
        if (index === 0) {
          ctx.moveTo(x, y);
        } else {
          ctx.lineTo(x, y);
        }
      });
      ctx.closePath();
    }

    function shieldPoints(cx, cy, r) {
      return [
        [cx, cy - r * 1.12],
        [cx + r * 0.9, cy - r * 0.58],
        [cx + r * 0.76, cy + r * 0.52],
        [cx, cy + r * 1.12],
        [cx - r * 0.76, cy + r * 0.52],
        [cx - r * 0.9, cy - r * 0.58],
      ];
    }

    function drawShield(ctx, palette, kind) {
      const outer = ctx.createRadialGradient(256, 226, 16, 256, 256, 222);
      outer.addColorStop(0, rgba(palette.primary, 0.26));
      outer.addColorStop(0.72, 'rgba(10,13,25,0.94)');
      outer.addColorStop(1, rgba(kind === 'champion' ? palette.secondary : palette.primary, 0.48));

      ctx.save();
      ctx.shadowColor = rgba(palette.primary, 0.74);
      ctx.shadowBlur = 34;
      polygon(ctx, shieldPoints(256, 260, 190));
      ctx.fillStyle = outer;
      ctx.fill();
      ctx.restore();

      polygon(ctx, shieldPoints(256, 260, 190));
      ctx.lineWidth = 12;
      ctx.strokeStyle = kind === 'champion' ? rgba(palette.secondary, 0.92) : rgba(palette.primary, 0.84);
      ctx.stroke();

      polygon(ctx, shieldPoints(256, 260, 154));
      const inner = ctx.createLinearGradient(116, 90, 390, 434);
      inner.addColorStop(0, 'rgba(255,255,255,0.12)');
      inner.addColorStop(0.42, rgba(palette.dark, 0.96));
      inner.addColorStop(1, 'rgba(0,0,0,0.92)');
      ctx.fillStyle = inner;
      ctx.fill();
      ctx.lineWidth = 3;
      ctx.strokeStyle = 'rgba(255,255,255,0.16)';
      ctx.stroke();
    }

    function drawChampionCrown(ctx, palette) {
      ctx.save();
      ctx.shadowColor = rgba(palette.secondary, 0.8);
      ctx.shadowBlur = 20;
      const points = [
        [170, 118],
        [204, 70],
        [238, 116],
        [256, 58],
        [276, 116],
        [310, 70],
        [344, 118],
        [326, 154],
        [186, 154],
      ];
      polygon(ctx, points);
      ctx.fillStyle = rgba(palette.secondary, 0.84);
      ctx.fill();
      ctx.lineWidth = 3;
      ctx.strokeStyle = 'rgba(255,255,255,0.55)';
      ctx.stroke();
      ctx.restore();
    }

    function drawContenderChevrons(ctx, palette) {
      ctx.save();
      ctx.lineJoin = 'round';
      ctx.lineCap = 'round';
      ctx.strokeStyle = rgba(palette.primary, 0.82);
      ctx.lineWidth = 9;
      ctx.shadowColor = rgba(palette.primary, 0.7);
      ctx.shadowBlur = 18;
      for (const y of [138, 164]) {
        ctx.beginPath();
        ctx.moveTo(186, y);
        ctx.lineTo(256, y - 30);
        ctx.lineTo(326, y);
        ctx.stroke();
      }
      ctx.restore();
    }

    function drawTerranGlyph(ctx, palette) {
      ctx.save();
      ctx.translate(256, 266);
      ctx.shadowColor = rgba(palette.primary, 0.72);
      ctx.shadowBlur = 18;
      ctx.fillStyle = rgba(palette.core, 0.92);
      ctx.strokeStyle = rgba(palette.primary, 0.94);
      ctx.lineWidth = 8;
      polygon(ctx, [[0, -84], [82, -28], [58, 76], [0, 104], [-58, 76], [-82, -28]]);
      ctx.stroke();
      ctx.fillStyle = 'rgba(7,15,25,0.94)';
      ctx.fill();
      ctx.beginPath();
      ctx.arc(0, 4, 42, 0, Math.PI * 2);
      ctx.strokeStyle = rgba(palette.secondary, 0.88);
      ctx.stroke();
      ctx.fillStyle = rgba(palette.primary, 0.8);
      ctx.fillRect(-96, -12, 46, 24);
      ctx.fillRect(50, -12, 46, 24);
      ctx.fillRect(-12, -102, 24, 44);
      ctx.fillStyle = rgba(palette.core, 0.9);
      ctx.beginPath();
      ctx.arc(0, 4, 17, 0, Math.PI * 2);
      ctx.fill();
      ctx.restore();
    }

    function drawProtossGlyph(ctx, palette) {
      ctx.save();
      ctx.translate(256, 270);
      ctx.shadowColor = rgba(palette.primary, 0.84);
      ctx.shadowBlur = 24;
      ctx.lineWidth = 7;
      ctx.strokeStyle = rgba(palette.primary, 0.96);
      ctx.fillStyle = 'rgba(10,14,34,0.94)';
      polygon(ctx, [[0, -118], [58, 48], [0, 104], [-58, 48]]);
      ctx.fill();
      ctx.stroke();
      ctx.fillStyle = rgba(palette.core, 0.88);
      polygon(ctx, [[0, -84], [28, 30], [0, 64], [-28, 30]]);
      ctx.fill();
      ctx.strokeStyle = rgba(palette.secondary, 0.82);
      ctx.beginPath();
      ctx.moveTo(-118, -26);
      ctx.quadraticCurveTo(-54, -80, 0, -118);
      ctx.quadraticCurveTo(54, -80, 118, -26);
      ctx.stroke();
      ctx.beginPath();
      ctx.moveTo(-104, 58);
      ctx.quadraticCurveTo(-42, 102, 0, 104);
      ctx.quadraticCurveTo(42, 102, 104, 58);
      ctx.stroke();
      ctx.restore();
    }

    function drawZergGlyph(ctx, palette) {
      ctx.save();
      ctx.translate(256, 272);
      ctx.shadowColor = rgba(palette.secondary, 0.78);
      ctx.shadowBlur = 24;
      ctx.strokeStyle = rgba(palette.secondary, 0.92);
      ctx.lineWidth = 12;
      ctx.lineCap = 'round';
      ctx.beginPath();
      ctx.moveTo(0, -104);
      ctx.bezierCurveTo(34, -44, 30, 44, 0, 112);
      ctx.bezierCurveTo(-30, 44, -34, -44, 0, -104);
      ctx.stroke();
      ctx.strokeStyle = rgba(palette.primary, 0.9);
      ctx.lineWidth = 10;
      ctx.beginPath();
      ctx.moveTo(-18, -36);
      ctx.bezierCurveTo(-106, -90, -126, 2, -66, 60);
      ctx.stroke();
      ctx.beginPath();
      ctx.moveTo(18, -36);
      ctx.bezierCurveTo(106, -90, 126, 2, 66, 60);
      ctx.stroke();
      ctx.fillStyle = rgba(palette.core, 0.88);
      for (const y of [-48, -6, 36]) {
        polygon(ctx, [[0, y - 18], [18, y + 6], [0, y + 24], [-18, y + 6]]);
        ctx.fill();
      }
      ctx.restore();
    }

    function drawBottomMark(ctx, palette, kind) {
      ctx.save();
      ctx.shadowColor = rgba(kind === 'champion' ? palette.secondary : palette.primary, 0.76);
      ctx.shadowBlur = 20;
      ctx.strokeStyle = rgba(kind === 'champion' ? palette.secondary : palette.primary, 0.9);
      ctx.lineWidth = 7;
      ctx.lineCap = 'round';
      ctx.beginPath();
      ctx.moveTo(178, 394);
      ctx.lineTo(256, 430);
      ctx.lineTo(334, 394);
      ctx.stroke();
      ctx.beginPath();
      ctx.moveTo(204, 374);
      ctx.lineTo(256, 398);
      ctx.lineTo(308, 374);
      ctx.stroke();
      ctx.restore();
    }

    function makeBadge(race, kind) {
      const palette = racePalette[race];
      const canvas = document.createElement('canvas');
      canvas.width = 512;
      canvas.height = 512;
      const ctx = canvas.getContext('2d');

      ctx.clearRect(0, 0, 512, 512);
      const halo = ctx.createRadialGradient(256, 246, 20, 256, 246, 248);
      halo.addColorStop(0, rgba(palette.primary, 0.3));
      halo.addColorStop(0.45, rgba(palette.secondary, 0.13));
      halo.addColorStop(1, 'rgba(0,0,0,0)');
      ctx.fillStyle = halo;
      ctx.fillRect(0, 0, 512, 512);

      drawShield(ctx, palette, kind);
      if (kind === 'champion') {
        drawChampionCrown(ctx, palette);
      } else {
        drawContenderChevrons(ctx, palette);
      }

      if (race === 'terran') {
        drawTerranGlyph(ctx, palette);
      } else if (race === 'protoss') {
        drawProtossGlyph(ctx, palette);
      } else {
        drawZergGlyph(ctx, palette);
      }
      drawBottomMark(ctx, palette, kind);

      ctx.globalCompositeOperation = 'screen';
      ctx.fillStyle = 'rgba(255,255,255,0.08)';
      ctx.beginPath();
      ctx.ellipse(218, 145, 118, 36, -0.44, 0, Math.PI * 2);
      ctx.fill();
      ctx.globalCompositeOperation = 'source-over';

      return canvas.toDataURL('image/png');
    }

    const outputs = {};
    for (const kind of ['contender', 'champion']) {
      for (const race of ['terran', 'protoss', 'zerg']) {
        outputs[`${kind}-${race}.png`] = makeBadge(race, kind);
      }
    }

    const sheet = document.createElement('canvas');
    sheet.width = 1536;
    sheet.height = 1024;
    const sheetCtx = sheet.getContext('2d');
    sheetCtx.fillStyle = '#070b14';
    sheetCtx.fillRect(0, 0, sheet.width, sheet.height);

    return outputs;
  });

  await fs.mkdir(BADGE_DIR, { recursive: true });
  const files = [];
  for (const [filename, dataUrl] of Object.entries(rendered)) {
    await writeDataUrl(path.join(BADGE_DIR, filename), dataUrl);
    files.push(`/static/badges/league-sci-fi-v1/${filename}`);
  }
  await fs.writeFile(
    path.join(BADGE_DIR, 'manifest.json'),
    JSON.stringify({
      generated_at: new Date().toISOString(),
      style: 'dark sci-fi faction shield, original abstract glyphs, transparent PNG',
      files,
    }, null, 2),
    'utf8',
  );
  await fs.writeFile(
    path.join(BADGE_DIR, 'README.md'),
    [
      '# League Sci-Fi V1 Badges',
      '',
      'Generated project-local league badge set.',
      '',
      '- Kinds: contender, champion',
      '- Races: terran, protoss, zerg',
      '- Format: transparent PNG, 512x512',
      '- Style: original dark sci-fi shields with abstract faction glyphs',
      '',
    ].join('\n'),
    'utf8',
  );
  return files;
}

async function main() {
  const { chromium } = require(PLAYWRIGHT_CORE);
  const chromePath = await findChrome();

  await fs.mkdir(BANNER_DIR, { recursive: true });
  await fs.mkdir(BACKGROUND_DIR, { recursive: true });
  await fs.mkdir(CARD_DIR, { recursive: true });

  const files = (await fs.readdir(SOURCE_DIR, { withFileTypes: true }))
    .filter((entry) => entry.isFile() && SOURCE_EXTENSIONS.has(path.extname(entry.name).toLowerCase()))
    .map((entry) => entry.name)
    .sort((a, b) => a.localeCompare(b, 'en'));

  const browser = await chromium.launch({
    executablePath: chromePath,
    headless: true,
    args: ['--headless=new', '--disable-gpu'],
  });
  const page = await browser.newPage({ viewport: { width: 1920, height: 1080 } });

  const records = [];
  const usedSlugs = new Map();
  for (const filename of files) {
    const basename = filename.slice(0, filename.length - path.extname(filename).length);
    const baseSlug = slugify(basename);
    const seen = usedSlugs.get(baseSlug) || 0;
    usedSlugs.set(baseSlug, seen + 1);
    const slug = seen ? `${baseSlug}-${seen + 1}` : baseSlug;
    const sourcePath = path.join(SOURCE_DIR, filename);
    const faction = inferFaction(filename);
    const record = {
      source_filename: filename,
      source_path: sourcePath,
      slug,
      faction,
      permission_status: 'pending',
      notes: 'Source art/photo supplied by project owner; public use should wait for creator permission.',
    };
    records.push(await renderPhotoAssets(page, sourcePath, record));
  }

  const badgeFiles = await renderBadges(page);
  await browser.close();

  const manifest = {
    generated_at: new Date().toISOString(),
    source_dir: SOURCE_DIR,
    style: {
      name: 'TMG dark sci-fi grade',
      profile_banner: '1600x520 JPEG with full-image contain fit, dark cinematic grade, accent glow, subtle frame',
      site_background: '1920x1080 JPEG with full-image contain fit, darkened grade for readable UI backgrounds',
      card: '900x900 JPEG with full-image contain fit for compact previews',
    },
    legal_note: 'Permission is marked pending for Reddit/community sources until creator approval is obtained.',
    records,
    league_badges: badgeFiles,
  };

  await fs.writeFile(path.join(ART_ROOT, 'manifest.json'), JSON.stringify(manifest, null, 2), 'utf8');
  await fs.writeFile(
    path.join(ART_ROOT, 'README.md'),
    [
      '# TMG Visual Assets',
      '',
      'Generated derivatives for the StarCraft TMG project.',
      '',
      '## Outputs',
      '',
      '- `profile-banners/`: 1600x520 long profile/header rectangles, full source image contained without cropping',
      '- `site-backgrounds/`: 1920x1080 dark UI backgrounds, full source image contained without cropping',
      '- `cards/`: 900x900 compact previews, full source image contained without cropping',
      '- `manifest.json`: source mapping and permission status',
      '',
      'The source images are marked as `permission_status: pending`. Do not treat these derivatives as cleared for public use until the original creators approve usage.',
      '',
    ].join('\n'),
    'utf8',
  );

  console.log(JSON.stringify({
    processed_images: records.length,
    art_root: ART_ROOT,
    badge_dir: BADGE_DIR,
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
