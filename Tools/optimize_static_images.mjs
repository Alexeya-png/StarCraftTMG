import { createRequire } from 'node:module';
import fs from 'node:fs/promises';
import path from 'node:path';

const root = process.cwd();
const appStatic = path.join(root, 'app', 'static');
const profileBanners = path.join(appStatic, 'art', 'profile-banners');
const siteBackgrounds = path.join(appStatic, 'art', 'site-backgrounds');
const cardsDir = path.join(appStatic, 'art', 'cards');
const badgesDir = path.join(appStatic, 'badges');
const edgePath = 'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe';
const playwrightPath = path.join(root, '.venv', 'Lib', 'site-packages', 'playwright', 'driver', 'package', 'index.js');
const require = createRequire(import.meta.url);
const playwright = require(playwrightPath);

function toPosix(value) {
  return value.split(path.sep).join('/');
}

function staticUrl(relPath) {
  return `/static/${toPosix(path.relative(appStatic, path.join(root, relPath)))}`;
}

function mimeFor(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === '.png') return 'image/png';
  if (ext === '.jpg' || ext === '.jpeg') return 'image/jpeg';
  if (ext === '.webp') return 'image/webp';
  throw new Error(`Unsupported image type: ${filePath}`);
}

async function fileExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function listFiles(dir, extensions) {
  const entries = await fs.readdir(dir, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...await listFiles(fullPath, extensions));
    } else if (extensions.has(path.extname(entry.name).toLowerCase())) {
      files.push(fullPath);
    }
  }
  return files;
}

async function encodeImage(page, srcPath, outPath, options) {
  const sourceBytes = await fs.readFile(srcPath);
  const sourceMime = mimeFor(srcPath);
  const dataUrl = `data:${sourceMime};base64,${sourceBytes.toString('base64')}`;
  const result = await page.evaluate(async ({ dataUrl, outputMime, quality, maxWidth, maxHeight }) => {
    const image = new Image();
    image.decoding = 'sync';
    image.src = dataUrl;
    await image.decode();

    const scale = Math.min(1, maxWidth / image.naturalWidth, maxHeight / image.naturalHeight);
    const width = Math.max(1, Math.round(image.naturalWidth * scale));
    const height = Math.max(1, Math.round(image.naturalHeight * scale));
    const canvas = document.createElement('canvas');
    canvas.width = width;
    canvas.height = height;
    const context = canvas.getContext('2d', { alpha: outputMime !== 'image/jpeg' });
    context.imageSmoothingEnabled = true;
    context.imageSmoothingQuality = 'high';
    context.drawImage(image, 0, 0, width, height);

    const blob = await new Promise((resolve, reject) => {
      canvas.toBlob((value) => value ? resolve(value) : reject(new Error('Canvas encode failed')), outputMime, quality);
    });
    const arrayBuffer = await blob.arrayBuffer();
    let binary = '';
    const bytes = new Uint8Array(arrayBuffer);
    for (let index = 0; index < bytes.length; index += 0x8000) {
      binary += String.fromCharCode(...bytes.subarray(index, index + 0x8000));
    }
    return {
      base64: btoa(binary),
      width,
      height,
      sourceWidth: image.naturalWidth,
      sourceHeight: image.naturalHeight,
    };
  }, {
    dataUrl,
    outputMime: options.outputMime,
    quality: options.quality,
    maxWidth: options.maxWidth,
    maxHeight: options.maxHeight,
  });

  const outputBytes = Buffer.from(result.base64, 'base64');
  const sameFile = path.resolve(srcPath).toLowerCase() === path.resolve(outPath).toLowerCase();
  const tempPath = sameFile ? `${outPath}.tmp` : outPath;
  await fs.mkdir(path.dirname(outPath), { recursive: true });

  const shouldWrite = !sameFile || outputBytes.length < sourceBytes.length * 0.98;
  if (!shouldWrite) {
    return {
      srcPath,
      outPath,
      skipped: true,
      sourceBytes: sourceBytes.length,
      outputBytes: sourceBytes.length,
      ...result,
    };
  }

  await fs.writeFile(tempPath, outputBytes);
  if (sameFile) {
    await fs.rename(tempPath, outPath);
  }

  return {
    srcPath,
    outPath,
    skipped: false,
    sourceBytes: sourceBytes.length,
    outputBytes: outputBytes.length,
    ...result,
  };
}

async function replaceInFile(filePath, replacements) {
  if (!await fileExists(filePath)) {
    return;
  }
  let text = await fs.readFile(filePath, 'utf8');
  const original = text;
  for (const [from, to] of replacements) {
    text = text.split(from).join(to);
  }
  if (text !== original) {
    await fs.writeFile(filePath, text, 'utf8');
  }
}

async function findTextReferences(needles) {
  const textExtensions = new Set(['.css', '.html', '.js', '.json', '.py', '.md', '.webmanifest']);
  const files = await listFiles(path.join(root, 'app'), textExtensions);
  const references = new Map(needles.map((needle) => [needle, []]));
  for (const file of files) {
    let text = '';
    try {
      text = await fs.readFile(file, 'utf8');
    } catch {
      continue;
    }
    for (const needle of needles) {
      if (text.includes(needle)) {
        references.get(needle).push(file);
      }
    }
  }
  return references;
}

function target(srcRel, outRel, options) {
  return {
    srcRel,
    outRel,
    srcPath: path.join(root, srcRel),
    outPath: path.join(root, outRel),
    ...options,
  };
}

async function main() {
  const conversions = [
    target('app/static/art/site-backgrounds/site-background.jpg', 'app/static/art/site-backgrounds/site-background.webp', {
      maxWidth: 1600,
      maxHeight: 900,
      quality: 0.72,
      outputMime: 'image/webp',
      removeOriginal: true,
    }),
    target('app/static/art/profile-banners/profile_back_terran.jpg', 'app/static/art/profile-banners/profile_back_terran.webp', {
      maxWidth: 1600,
      maxHeight: 700,
      quality: 0.74,
      outputMime: 'image/webp',
      removeOriginal: true,
    }),
    target('app/static/art/profile-banners/profile_back_protoss.jpg', 'app/static/art/profile-banners/profile_back_protoss.webp', {
      maxWidth: 1600,
      maxHeight: 700,
      quality: 0.74,
      outputMime: 'image/webp',
      removeOriginal: true,
    }),
    target('app/static/art/profile-banners/profile_back_zerg.jpg', 'app/static/art/profile-banners/profile_back_zerg.webp', {
      maxWidth: 1600,
      maxHeight: 900,
      quality: 0.74,
      outputMime: 'image/webp',
      removeOriginal: true,
    }),
    target('app/static/art/profile-banners/league_back_terran.webp', 'app/static/art/profile-banners/league_back_terran.webp', {
      maxWidth: 900,
      maxHeight: 900,
      quality: 0.68,
      outputMime: 'image/webp',
      removeOriginal: false,
    }),
    target('app/static/art/profile-banners/league_back_protoss.png', 'app/static/art/profile-banners/league_back_protoss.webp', {
      maxWidth: 900,
      maxHeight: 900,
      quality: 0.68,
      outputMime: 'image/webp',
      removeOriginal: true,
    }),
    target('app/static/art/profile-banners/league_back_zerg.png', 'app/static/art/profile-banners/league_back_zerg.webp', {
      maxWidth: 900,
      maxHeight: 900,
      quality: 0.68,
      outputMime: 'image/webp',
      removeOriginal: true,
    }),
  ];

  const cardFiles = await listFiles(cardsDir, new Set(['.jpg', '.jpeg', '.png']));
  for (const file of cardFiles) {
    const rel = toPosix(path.relative(root, file));
    const outRel = rel.replace(/\.(jpe?g|png)$/i, '.webp');
    conversions.push(target(rel, outRel, {
      maxWidth: 720,
      maxHeight: 720,
      quality: 0.68,
      outputMime: 'image/webp',
      removeOriginal: true,
    }));
  }

  const badgeFiles = await listFiles(badgesDir, new Set(['.webp']));
  for (const file of badgeFiles) {
    const rel = toPosix(path.relative(root, file));
    conversions.push(target(rel, rel, {
      maxWidth: 320,
      maxHeight: 320,
      quality: 0.78,
      outputMime: 'image/webp',
      removeOriginal: false,
    }));
  }

  for (const rel of ['app/static/favicon.webp']) {
    conversions.push(target(rel, rel, {
      maxWidth: 256,
      maxHeight: 256,
      quality: 0.78,
      outputMime: 'image/webp',
      removeOriginal: false,
    }));
  }

  for (const rel of ['app/static/app-icon-192.png', 'app/static/app-icon-512.png', 'app/static/favicon.png', 'app/static/Race/logo.png']) {
    conversions.push(target(rel, rel, {
      maxWidth: 512,
      maxHeight: 512,
      quality: 1,
      outputMime: 'image/png',
      removeOriginal: false,
    }));
  }

  const context = await playwright.chromium.launchPersistentContext(path.join(root, '.edge-profile-image-optimize'), {
    executablePath: edgePath,
    headless: true,
    viewport: { width: 1280, height: 720 },
    args: ['--no-first-run', '--disable-gpu', '--disable-features=msEdgeUserDataMigration'],
  });
  const page = context.pages()[0] || await context.newPage();

  const results = [];
  const replacements = new Map();
  try {
    for (const item of conversions) {
      if (!await fileExists(item.srcPath)) {
        continue;
      }
      const result = await encodeImage(page, item.srcPath, item.outPath, item);
      results.push({ ...result, removeOriginal: item.removeOriginal });
      if (path.resolve(item.srcPath).toLowerCase() !== path.resolve(item.outPath).toLowerCase()) {
        replacements.set(staticUrl(item.srcRel), staticUrl(item.outRel));
      }
    }
  } finally {
    await context.close();
  }

  const replacementPairs = Array.from(replacements.entries());
  for (const file of [
    path.join(appStatic, 'styles.css'),
    path.join(appStatic, 'art', 'player-backgrounds.json'),
    path.join(appStatic, 'art', 'manifest.json'),
  ]) {
    await replaceInFile(file, replacementPairs);
  }

  const references = await findTextReferences(Array.from(replacements.keys()));
  const removed = [];
  for (const result of results) {
    if (!result.removeOriginal) {
      continue;
    }
    const oldUrl = staticUrl(toPosix(path.relative(root, result.srcPath)));
    const refs = references.get(oldUrl) || [];
    if (refs.length === 0 && await fileExists(result.srcPath)) {
      await fs.unlink(result.srcPath);
      removed.push(result.srcPath);
    }
  }

  try {
    await fs.rm(path.join(root, '.edge-profile-image-optimize'), { recursive: true, force: true });
  } catch {
  }

  const sourceTotal = results.reduce((sum, result) => sum + result.sourceBytes, 0);
  const outputTotal = results.reduce((sum, result) => sum + result.outputBytes, 0);
  const lines = results
    .sort((a, b) => (b.sourceBytes - b.outputBytes) - (a.sourceBytes - a.outputBytes))
    .map((result) => {
      const rel = toPosix(path.relative(root, result.outPath));
      const saved = result.sourceBytes - result.outputBytes;
      const status = result.skipped ? 'kept' : 'optimized';
      return `${status.padEnd(9)} ${rel} ${result.sourceBytes} -> ${result.outputBytes} (${saved >= 0 ? '-' : '+'}${Math.abs(saved)} bytes)`;
    });

  console.log(`Processed ${results.length} images.`);
  console.log(`Total: ${sourceTotal} -> ${outputTotal} (${sourceTotal - outputTotal} bytes saved before removed originals).`);
  console.log(`Removed ${removed.length} superseded originals.`);
  console.log(lines.join('\n'));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
