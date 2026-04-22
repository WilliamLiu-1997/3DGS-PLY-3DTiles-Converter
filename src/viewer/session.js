const fs = require('fs');
const http = require('http');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');

const { ConversionError } = require('../parser');

const VIEWER_HTML_NAME = 'viewer.html';
const VIEWER_DIR_NAME = 'viewer';
const VIEWER_APP_NAME = 'app.js';
const VIEWER_CAMERA_CONTROLLER_NAME = 'cameraController.js';
const SAVE_ENDPOINT_PATH = '/__viewer/save-transform';
const SHUTDOWN_ENDPOINT_PATH = '/__viewer/shutdown';
const MAX_SAVE_BODY_BYTES = 64 * 1024;
const SHUTDOWN_DELAY_MS = 1000;
const IDENTITY_MATRIX4 = Object.freeze([
  1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0,
  1.0,
]);

const MIME_TYPES = {
  '.css': 'text/css; charset=utf-8',
  '.glb': 'model/gltf-binary',
  '.html': 'text/html; charset=utf-8',
  '.jpeg': 'image/jpeg',
  '.jpg': 'image/jpeg',
  '.js': 'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.subtree': 'application/octet-stream',
  '.svg': 'image/svg+xml; charset=utf-8',
  '.txt': 'text/plain; charset=utf-8',
};

const VIEWER_IMPORTS = {
  three: 'https://esm.sh/three@0.180.0',
  'three/addons/': 'https://esm.sh/three@0.180.0/addons/',
  'three/examples/jsm/': 'https://esm.sh/three@0.180.0/examples/jsm/',
  cesium: 'https://esm.sh/cesium@1.140.0',
  '@sparkjsdev/spark': 'https://esm.sh/@sparkjsdev/spark@2.0.0?external=three',
  '3d-tiles-renderer': 'https://esm.sh/3d-tiles-renderer@0.4.24?external=three',
  '3d-tiles-renderer/build/': 'https://esm.sh/3d-tiles-renderer@0.4.24/build/',
  '3d-tiles-renderer/plugins':
    'https://esm.sh/3d-tiles-renderer@0.4.24/plugins?external=three',
  '3d-tiles-renderer/three/plugins':
    'https://esm.sh/3d-tiles-renderer@0.4.24/three/plugins?external=three,3d-tiles-renderer',
  '3d-tiles-rendererjs-3dgs-plugin':
    'https://esm.sh/3d-tiles-rendererjs-3dgs-plugin@0.1.4?external=three,3d-tiles-renderer,@sparkjsdev/spark',
};

function cloneIdentityMatrix4() {
  return IDENTITY_MATRIX4.slice();
}

function normalizeMatrix4Array(value, name = 'transform') {
  if (!Array.isArray(value) || value.length !== 16) {
    throw new ConversionError(`${name} must be a 16-number matrix.`);
  }

  return value.map((entry, index) => {
    const number = Number(entry);
    if (!Number.isFinite(number)) {
      throw new ConversionError(`${name}[${index}] must be a finite number.`);
    }
    return number;
  });
}

function multiplyMatrix4(left, right) {
  const a = normalizeMatrix4Array(left, 'left');
  const b = normalizeMatrix4Array(right, 'right');
  const out = new Array(16);

  for (let column = 0; column < 4; column++) {
    for (let row = 0; row < 4; row++) {
      let sum = 0.0;
      for (let i = 0; i < 4; i++) {
        sum += a[i * 4 + row] * b[column * 4 + i];
      }
      out[column * 4 + row] = sum;
    }
  }

  return out;
}

function readJsonFile(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function writeJsonAtomic(filePath, value) {
  const tempPath = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tempPath, JSON.stringify(value), 'utf8');
  fs.renameSync(tempPath, filePath);
}

function createViewerAssetsDir() {
  const assetsDir = fs.mkdtempSync(
    path.join(os.tmpdir(), '3dgs-ply-3dtiles-viewer-'),
  );
  const viewerSubdir = path.join(assetsDir, VIEWER_DIR_NAME);
  fs.mkdirSync(viewerSubdir, { recursive: true });
  fs.writeFileSync(
    path.join(assetsDir, VIEWER_HTML_NAME),
    buildViewerHtml(),
    'utf8',
  );
  const sourceDir = __dirname;
  fs.copyFileSync(
    path.join(sourceDir, VIEWER_APP_NAME),
    path.join(viewerSubdir, VIEWER_APP_NAME),
  );
  fs.copyFileSync(
    path.join(sourceDir, VIEWER_CAMERA_CONTROLLER_NAME),
    path.join(viewerSubdir, VIEWER_CAMERA_CONTROLLER_NAME),
  );
  return assetsDir;
}

function removeViewerAssetsDir(assetsDir) {
  if (!assetsDir) {
    return;
  }
  fs.rmSync(assetsDir, { recursive: true, force: true });
}

function normalizePositiveFinite(value, name) {
  const number = Number(value);
  if (!Number.isFinite(number) || number <= 0) {
    throw new ConversionError(
      `${name} must be a finite number greater than 0.`,
    );
  }
  return number;
}

function scaleGeometricErrorValue(target, key, scale, label) {
  if (target[key] == null) {
    return;
  }

  const number = Number(target[key]);
  if (!Number.isFinite(number)) {
    throw new ConversionError(`${label} must be a finite number.`);
  }

  target[key] = number * scale;
}

function scaleTilesetGeometricErrors(tile, scale, pathLabel = 'tileset.root') {
  if (!tile || typeof tile !== 'object') {
    return;
  }

  scaleGeometricErrorValue(
    tile,
    'geometricError',
    scale,
    `${pathLabel}.geometricError`,
  );

  if (!Array.isArray(tile.children)) {
    return;
  }

  tile.children.forEach((child, index) => {
    scaleTilesetGeometricErrors(
      child,
      scale,
      `${pathLabel}.children[${index}]`,
    );
  });
}

function getLocalJsonReferencePath(baseDir, uri) {
  if (typeof uri !== 'string' || uri.length === 0) {
    return null;
  }

  if (/^[a-z][a-z\d+.-]*:/i.test(uri) || uri.startsWith('//')) {
    return null;
  }

  const normalized = uri.split('#', 1)[0].split('?', 1)[0];
  if (!/\.json$/i.test(normalized)) {
    return null;
  }

  return path.resolve(baseDir, normalized.replace(/\//g, path.sep));
}

function collectExternalTilesetPaths(tile, baseDir, results) {
  if (!tile || typeof tile !== 'object') {
    return;
  }

  const maybePaths = [];
  if (tile.content && typeof tile.content === 'object') {
    maybePaths.push(
      getLocalJsonReferencePath(baseDir, tile.content.uri || tile.content.url),
    );
  }

  if (Array.isArray(tile.contents)) {
    tile.contents.forEach((entry) => {
      if (entry && typeof entry === 'object') {
        maybePaths.push(
          getLocalJsonReferencePath(baseDir, entry.uri || entry.url),
        );
      }
    });
  }

  maybePaths.forEach((filePath) => {
    if (filePath) {
      results.add(filePath);
    }
  });

  if (!Array.isArray(tile.children)) {
    return;
  }

  tile.children.forEach((child) => {
    collectExternalTilesetPaths(child, baseDir, results);
  });
}

function updateTilesetJsonFile(
  tilesetPath,
  { geometricErrorScale, rootDir, rootTransform = null },
  visited = new Set(),
) {
  const resolvedPath = path.resolve(tilesetPath);
  if (visited.has(resolvedPath)) {
    return null;
  }
  visited.add(resolvedPath);

  if (
    resolvedPath !== rootDir &&
    !resolvedPath.startsWith(`${rootDir}${path.sep}`)
  ) {
    throw new ConversionError(
      `Nested tileset path escapes the viewer root: ${resolvedPath}`,
    );
  }

  if (!fs.existsSync(resolvedPath)) {
    throw new ConversionError(
      `Referenced nested tileset does not exist: ${resolvedPath}`,
    );
  }

  const tileset = readJsonFile(resolvedPath);
  if (!tileset || typeof tileset !== 'object' || !tileset.root) {
    throw new ConversionError(`${resolvedPath} must contain a root object.`);
  }

  if (rootTransform) {
    tileset.root.transform = rootTransform.slice();
  }

  scaleGeometricErrorValue(
    tileset,
    'geometricError',
    geometricErrorScale,
    `${resolvedPath}.geometricError`,
  );
  scaleTilesetGeometricErrors(
    tileset.root,
    geometricErrorScale,
    `${resolvedPath}.root`,
  );
  writeJsonAtomic(resolvedPath, tileset);

  const nestedTilesets = new Set();
  collectExternalTilesetPaths(
    tileset.root,
    path.dirname(resolvedPath),
    nestedTilesets,
  );
  nestedTilesets.forEach((childTilesetPath) => {
    updateTilesetJsonFile(
      childTilesetPath,
      {
        geometricErrorScale,
        rootDir,
      },
      visited,
    );
  });

  return tileset;
}

function saveViewerTransform(
  outputDir,
  editMatrix,
  { geometricErrorScale = 1 } = {},
) {
  const normalizedEdit = normalizeMatrix4Array(editMatrix, 'transform');
  const normalizedGeometricErrorScale = normalizePositiveFinite(
    geometricErrorScale,
    'geometricErrorScale',
  );
  const rootDir = path.resolve(outputDir);
  const tilesetPath = path.join(rootDir, 'tileset.json');

  if (!fs.existsSync(tilesetPath)) {
    throw new ConversionError(
      `Cannot save viewer transform because ${tilesetPath} does not exist.`,
    );
  }

  const tileset = readJsonFile(tilesetPath);
  if (!tileset || typeof tileset !== 'object' || !tileset.root) {
    throw new ConversionError('tileset.json must contain a root object.');
  }

  const currentRoot = Array.isArray(tileset.root.transform)
    ? normalizeMatrix4Array(tileset.root.transform, 'tileset.root.transform')
    : cloneIdentityMatrix4();
  const nextRoot = multiplyMatrix4(normalizedEdit, currentRoot);

  updateTilesetJsonFile(tilesetPath, {
    geometricErrorScale: normalizedGeometricErrorScale,
    rootDir,
    rootTransform: nextRoot,
  });

  const summaryPath = path.join(rootDir, 'build_summary.json');
  if (fs.existsSync(summaryPath)) {
    const summary = readJsonFile(summaryPath);
    const previousGeometricErrorScale =
      summary.viewer_geometric_error_scale == null
        ? 1
        : normalizePositiveFinite(
            summary.viewer_geometric_error_scale,
            'build_summary.viewer_geometric_error_scale',
          );
    summary.root_transform = nextRoot.slice();
    summary.root_transform_source = 'transform';
    summary.root_coordinate = null;
    summary.viewer_geometric_error_scale =
      previousGeometricErrorScale * normalizedGeometricErrorScale;
    writeJsonAtomic(summaryPath, summary);
  }

  return nextRoot;
}

function resolveStaticFilePath(tilesDir, viewerAssetsDir, pathname) {
  let decodedPathname;
  try {
    decodedPathname = decodeURIComponent(pathname || '/');
  } catch (err) {
    throw new ConversionError('Request path is not valid URL encoding.');
  }

  if (decodedPathname.includes('\0')) {
    throw new ConversionError('Request path contains invalid characters.');
  }

  const requested =
    decodedPathname === '/' || decodedPathname === ''
      ? VIEWER_HTML_NAME
      : decodedPathname.replace(/^[/\\]+/, '');

  const normalized = requested.replace(/\\/g, '/');
  const isViewerAsset =
    normalized === VIEWER_HTML_NAME ||
    normalized === VIEWER_DIR_NAME ||
    normalized.startsWith(`${VIEWER_DIR_NAME}/`);

  const resolvedRoot = path.resolve(isViewerAsset ? viewerAssetsDir : tilesDir);
  const resolvedPath = path.resolve(resolvedRoot, requested);

  if (
    resolvedPath !== resolvedRoot &&
    !resolvedPath.startsWith(`${resolvedRoot}${path.sep}`)
  ) {
    throw new ConversionError('Request path escapes the viewer root.');
  }

  return resolvedPath;
}

function sendJson(res, statusCode, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(statusCode, {
    'Content-Length': Buffer.byteLength(body),
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
  });
  res.end(body);
}

function sendText(res, statusCode, message, headers = {}) {
  const body = `${message}\n`;
  res.writeHead(statusCode, {
    'Content-Length': Buffer.byteLength(body),
    'Content-Type': 'text/plain; charset=utf-8',
    'Cache-Control': 'no-store',
    ...headers,
  });
  res.end(body);
}

function readRequestBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;

    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > MAX_SAVE_BODY_BYTES) {
        reject(new ConversionError('Request body is too large.'));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

function normalizeRequestTarget(rawTarget) {
  if (typeof rawTarget !== 'string' || rawTarget.length === 0) {
    return '/';
  }

  if (rawTarget.startsWith('//')) {
    return `/${rawTarget.replace(/^\/+/, '')}`;
  }

  return rawTarget;
}

async function handleSaveTransformRequest(rootDir, req, res) {
  let payload;
  try {
    const body = await readRequestBody(req);
    payload = body.length > 0 ? JSON.parse(body.toString('utf8')) : {};
  } catch (err) {
    sendJson(res, 400, {
      error:
        err instanceof Error && err.message
          ? err.message
          : 'Invalid JSON payload.',
    });
    return;
  }

  if (!payload || typeof payload !== 'object') {
    sendJson(res, 400, { error: 'Request payload must be a JSON object.' });
    return;
  }

  let normalizedEdit;
  try {
    normalizedEdit = normalizeMatrix4Array(payload.transform, 'transform');
  } catch (err) {
    sendJson(res, 400, {
      error:
        err instanceof Error && err.message
          ? err.message
          : 'transform must be a 16-number matrix.',
    });
    return;
  }

  let normalizedGeometricErrorScale;
  try {
    normalizedGeometricErrorScale = normalizePositiveFinite(
      payload.geometricErrorScale == null ? 1 : payload.geometricErrorScale,
      'geometricErrorScale',
    );
  } catch (err) {
    sendJson(res, 400, {
      error:
        err instanceof Error && err.message
          ? err.message
          : 'geometricErrorScale must be a finite number greater than 0.',
    });
    return;
  }

  let nextRoot;
  try {
    nextRoot = saveViewerTransform(rootDir, normalizedEdit, {
      geometricErrorScale: normalizedGeometricErrorScale,
    });
  } catch (err) {
    sendJson(res, 500, {
      error:
        err instanceof Error && err.message
          ? err.message
          : 'Failed to save transform.',
    });
    return;
  }

  sendJson(res, 200, {
    ok: true,
    transform: nextRoot,
    geometricErrorScale: normalizedGeometricErrorScale,
  });
}

async function handleViewerRequest(
  tilesDir,
  viewerAssetsDir,
  req,
  res,
  requestUrl = null,
) {
  const normalizedRequestUrl =
    requestUrl || new URL(normalizeRequestTarget(req.url), 'http://127.0.0.1');

  if (normalizedRequestUrl.pathname === SAVE_ENDPOINT_PATH) {
    if (req.method !== 'POST') {
      sendText(res, 405, 'Method Not Allowed', { Allow: 'POST' });
      return;
    }
    await handleSaveTransformRequest(tilesDir, req, res);
    return;
  }

  if (req.method !== 'GET' && req.method !== 'HEAD') {
    sendText(res, 405, 'Method Not Allowed', { Allow: 'GET, HEAD' });
    return;
  }

  let filePath;
  try {
    filePath = resolveStaticFilePath(
      tilesDir,
      viewerAssetsDir,
      normalizedRequestUrl.pathname,
    );
  } catch (err) {
    sendText(res, 403, err.message || 'Forbidden');
    return;
  }

  let stats;
  try {
    stats = fs.statSync(filePath);
  } catch (err) {
    sendText(res, 404, 'Not Found');
    return;
  }

  if (!stats.isFile()) {
    sendText(res, 404, 'Not Found');
    return;
  }

  const extension = path.extname(filePath).toLowerCase();
  const contentType = MIME_TYPES[extension] || 'application/octet-stream';
  res.writeHead(200, {
    'Content-Length': stats.size,
    'Content-Type': contentType,
    'Cache-Control': 'no-store',
  });

  if (req.method === 'HEAD') {
    res.end();
    return;
  }

  fs.createReadStream(filePath).pipe(res);
}

function escapeForSingleQuotedPowerShell(value) {
  return String(value).replace(/'/g, "''");
}

function openBrowser(url) {
  return new Promise((resolve, reject) => {
    let child;

    if (process.platform === 'win32') {
      child = spawn(
        'powershell',
        [
          '-NoProfile',
          '-Command',
          `Start-Process '${escapeForSingleQuotedPowerShell(url)}'`,
        ],
        {
          detached: true,
          stdio: 'ignore',
          windowsHide: true,
        },
      );
    } else if (process.platform === 'darwin') {
      child = spawn('open', [url], {
        detached: true,
        stdio: 'ignore',
      });
    } else {
      child = spawn('xdg-open', [url], {
        detached: true,
        stdio: 'ignore',
      });
    }

    child.once('error', reject);
    child.once('spawn', () => {
      child.unref();
      resolve();
    });
  });
}

function buildViewerHtml() {
  const importMap = JSON.stringify({ imports: VIEWER_IMPORTS }, null, 2);
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta
      name="viewport"
      content="width=device-width, initial-scale=1.0, viewport-fit=cover"
    />
    <title>3D Tiles Transform Viewer</title>
    <style>
      :root {
        color-scheme: light;
        font-family: "Segoe UI", "Helvetica Neue", Helvetica, Arial, sans-serif;
      }

      * {
        box-sizing: border-box;
      }

      body {
        margin: 0;
        overflow: hidden;
        background:
          radial-gradient(circle at top, rgba(255, 255, 255, 0.95), rgba(236, 240, 245, 0.9)),
          linear-gradient(180deg, #eef3f8 0%, #dfe7ef 100%);
        color: #16324f;
      }

      #app {
        position: fixed;
        inset: 0;
      }

      canvas {
        display: block;
      }

      .toolbar-dock {
        position: fixed;
        top: 16px;
        left: 16px;
        display: grid;
        align-items: start;
        gap: 12px;
        width: min(620px, calc(100vw - 32px));
        z-index: 10;
      }

      .toolbar-dock.expanded {
        gap: 0;
      }

      .toolbar {
        display: grid;
        gap: 12px;
        padding: 14px;
        border: 1px solid rgba(22, 50, 79, 0.12);
        border-radius: 18px;
        background: rgba(255, 255, 255, 0.9);
        box-shadow: 0 18px 44px rgba(33, 52, 73, 0.16);
        backdrop-filter: blur(14px);
        transition:
          opacity 160ms ease,
          transform 160ms ease;
      }

      .toolbar-dock.expanded .toolbar {
        border-top-left-radius: 0;
        border-top-right-radius: 0;
      }

      .toolbar.hidden {
        display: none;
      }

      .toolbar-toggle {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        min-height: 32px;
        padding: 7px 12px 8px;
        border: 1px solid rgba(22, 50, 79, 0.08);
        border-radius: 999px;
        font: inherit;
        font-size: 13px;
        font-weight: 600;
        letter-spacing: 0.02em;
        color: #506377;
        background: rgba(255, 255, 255, 0.94);
        box-shadow: 0 12px 28px rgba(33, 52, 73, 0.12);
        cursor: pointer;
        backdrop-filter: blur(14px);
        transition:
          background-color 120ms ease,
          color 120ms ease,
          box-shadow 120ms ease;
      }

      .toolbar-dock.expanded .toolbar-toggle {
        width: 100%;
        margin: 0;
        min-height: 32px;
        padding: 7px 10px 7px;
        border-radius: 18px 18px 0 0;
        border-bottom: 0;
        color: #16324f;
        background: rgba(255, 255, 255, 0.9);
        box-shadow: 0 18px 44px rgba(33, 52, 73, 0.16);
      }

      .toolbar-dock.collapsed .toolbar-toggle {
        justify-self: start;
      }

      .toolbar-toggle:hover {
        color: #16324f;
        background: rgba(239, 241, 243, 0.98);
        box-shadow: 0 10px 22px rgba(33, 52, 73, 0.1);
      }

      .toolbar-dock.expanded .toolbar-toggle:hover {
        color: #16324f;
        background: rgba(225, 226, 229, 0.98);
        box-shadow: 0 18px 40px rgba(33, 52, 73, 0.18);
      }

      .toolbar-toggle:focus-visible {
        outline: 2px solid rgba(13, 111, 131, 0.35);
        outline-offset: 2px;
      }

      .toolbar-section {
        display: grid;
        gap: 10px;
        padding: 12px;
        border: 1px solid rgba(22, 50, 79, 0.08);
        border-radius: 14px;
        background:
          linear-gradient(180deg, rgba(255, 255, 255, 0.78), rgba(243, 247, 251, 0.9));
      }

      .toolbar-section-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 10px;
      }

      .toolbar-section-title {
        margin: 0;
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: #5d738b;
      }

      .toolbar-value {
        margin: 0;
        font-size: 12px;
        font-weight: 700;
        color: #16324f;
      }

      .button-row {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
      }

      .toolbar button {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        border: 0;
        border-radius: 999px;
        padding: 9px 14px;
        font: inherit;
        font-size: 14px;
        font-weight: 600;
        color: #16324f;
        background: #dde7f2;
        cursor: pointer;
        transition:
          transform 120ms ease,
          background-color 120ms ease,
          color 120ms ease;
      }

      .toolbar button:hover {
        transform: translateY(-1px);
        background: #d0deeb;
      }

      .toolbar button.active {
        color: #fff;
        background: #0d6f83;
      }

      .toolbar button.save {
        color: #fff;
        background: #19765b;
      }

      .toolbar button:disabled {
        transform: none;
        opacity: 0.7;
        cursor: wait;
      }

      .range-field {
        display: grid;
        gap: 8px;
        min-width: 0;
      }

      .range-field span {
        font-size: 11px;
        font-weight: 600;
        letter-spacing: 0.04em;
        text-transform: uppercase;
        color: #5d738b;
      }

      .range-field input[type='range'] {
        width: 100%;
        margin: 0;
      }

      .coordinate-grid {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 10px;
      }

      .coordinate-grid label {
        display: grid;
        gap: 6px;
        min-width: 0;
        font-size: 11px;
        font-weight: 600;
        letter-spacing: 0.04em;
        text-transform: uppercase;
        color: #5d738b;
      }

      .coordinate-grid input {
        width: 100%;
        padding: 8px 10px;
        border: 1px solid rgba(22, 50, 79, 0.16);
        border-radius: 10px;
        font: inherit;
        font-size: 13px;
        font-weight: 600;
        color: #16324f;
        background: rgba(255, 255, 255, 0.92);
      }

      .coordinate-actions {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 8px;
      }

      .toolbar button.wide {
        width: 100%;
        justify-content: center;
      }

      .status {
        min-width: 0;
        font-size: 13px;
        line-height: 1.4;
        color: #38516c;
      }

      .status.error {
        color: #a33f2f;
      }

      .status-panel {
        display: grid;
        gap: 10px;
      }

      @media (max-width: 720px) {
        .toolbar-dock {
          top: auto;
          bottom: 16px;
          right: 16px;
          left: 16px;
          width: auto;
        }

        .coordinate-grid {
          grid-template-columns: 1fr;
        }

        .coordinate-actions {
          grid-template-columns: 1fr;
        }

        .button-row {
          flex-direction: column;
        }

        .button-row button,
        .coordinate-actions button {
          width: 100%;
        }
      }
    </style>
    <script type="importmap">${importMap}</script>
  </head>
  <body>
    <div id="app"></div>
    <div class="toolbar-dock expanded">
      <button
        id="toolbar-toggle"
        class="toolbar-toggle"
        type="button"
        aria-controls="toolbar"
        aria-expanded="true"
      >
        Hide Panel
      </button>
      <div id="toolbar" class="toolbar">
        <div class="toolbar-section">
          <div class="toolbar-section-header">
            <p class="toolbar-section-title">Transform</p>
          </div>
          <div class="button-row">
            <button id="translate" type="button">Translate</button>
            <button id="rotate" type="button">Rotate</button>
            <button id="move-to-tiles" type="button">Move To Tiles</button>
            <button id="set-position" type="button">Set Position</button>
            <button id="terrain" type="button">Terrain</button>
          </div>
        </div>
        <div class="toolbar-section">
          <div class="toolbar-section-header">
            <p class="toolbar-section-title">Coordinate</p>
          </div>
          <div class="coordinate-grid">
            <label>Lat <input id="latitude" type="number" step="any" value="0" /></label>
            <label>Lon <input id="longitude" type="number" step="any" value="0" /></label>
            <label>Height <input id="height" type="number" step="any" value="0" /></label>
          </div>
          <div class="coordinate-actions">
            <button id="move-camera-to-coordinate" class="wide" type="button">Move Camera</button>
            <button id="move-tiles-to-coordinate" class="wide" type="button">Move Tiles</button>
          </div>
        </div>
        <div class="toolbar-section">
          <div class="toolbar-section-header">
            <p class="toolbar-section-title">LOD</p>
            <p id="geometric-error-value" class="toolbar-value">x1.00</p>
          </div>
          <label class="range-field">
            <span>Geometric Error</span>
            <input
              id="geometric-error-scale"
              type="range"
              min="-4"
              max="4"
              step="0.1"
              value="0"
            />
          </label>
        </div>
        <div class="toolbar-section status-panel">
          <div class="button-row">
            <button id="reset" type="button">Reset</button>
            <button id="save" class="save" type="button">Save</button>
          </div>
          <div id="status" class="status">Loading tileset...</div>
        </div>
      </div>
    </div>
    <script type="module" src="./viewer/app.js"></script>
  </body>
</html>
`;
}

async function startViewerSession(
  outputDir,
  { openBrowser: shouldOpenBrowser = true, handleSignals = true } = {},
) {
  const rootDir = path.resolve(outputDir);
  const viewerAssetsDir = createViewerAssetsDir();
  let sessionOrigin = null;
  let closingPromise = null;
  let shutdownTimer = null;
  let cleanedUp = false;
  const signalHandlers = [];

  const removeSignalHandlers = () => {
    while (signalHandlers.length > 0) {
      const { event, handler } = signalHandlers.pop();
      process.off(event, handler);
    }
  };

  const cancelScheduledShutdown = () => {
    if (shutdownTimer) {
      clearTimeout(shutdownTimer);
      shutdownTimer = null;
    }
  };

  let closeResolve;
  const closed = new Promise((resolve) => {
    closeResolve = resolve;
  });

  const close = () => {
    if (closingPromise) {
      return closingPromise;
    }

    closingPromise = new Promise((resolve, reject) => {
      cancelScheduledShutdown();
      removeSignalHandlers();
      server.close((err) => {
        try {
          if (err) {
            reject(err);
            return;
          }
          if (!cleanedUp) {
            removeViewerAssetsDir(viewerAssetsDir);
            cleanedUp = true;
          }
          resolve();
        } catch (cleanupErr) {
          reject(cleanupErr);
        } finally {
          closeResolve();
        }
      });
    });

    return closingPromise;
  };

  const scheduleShutdown = () => {
    cancelScheduledShutdown();
    shutdownTimer = setTimeout(() => {
      shutdownTimer = null;
      close().catch((err) => {
        console.error(
          `[warn] failed to close viewer server cleanly: ${err.message || err}`,
        );
      });
    }, SHUTDOWN_DELAY_MS);
    if (typeof shutdownTimer.unref === 'function') {
      shutdownTimer.unref();
    }
  };

  const server = http.createServer((req, res) => {
    const requestUrl = new URL(
      normalizeRequestTarget(req.url),
      'http://127.0.0.1',
    );

    if (
      req.method === 'POST' &&
      requestUrl.pathname.startsWith('/__viewer/') &&
      req.headers.origin !== sessionOrigin
    ) {
      sendText(res, 403, 'Forbidden');
      return;
    }

    if (requestUrl.pathname === SHUTDOWN_ENDPOINT_PATH) {
      if (req.method !== 'POST') {
        sendText(res, 405, 'Method Not Allowed', { Allow: 'POST' });
        return;
      }
      sendJson(res, 200, { ok: true });
      scheduleShutdown();
      return;
    }

    cancelScheduledShutdown();
    handleViewerRequest(rootDir, viewerAssetsDir, req, res, requestUrl).catch(
      (err) => {
        sendJson(res, 500, {
          error:
            err instanceof Error && err.message
              ? err.message
              : 'Unexpected viewer server error.',
        });
      },
    );
  });

  if (handleSignals) {
    for (const event of ['SIGINT', 'SIGTERM']) {
      const handler = () => {
        close().catch((err) => {
          console.error(
            `[warn] failed to close viewer server cleanly: ${err.message || err}`,
          );
        });
      };
      signalHandlers.push({ event, handler });
      process.on(event, handler);
    }
  }

  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      server.off('error', reject);
      resolve();
    });
  });

  const address = server.address();
  if (!address || typeof address === 'string') {
    await close();
    throw new ConversionError('Viewer server failed to bind to a TCP port.');
  }

  sessionOrigin = `http://127.0.0.1:${address.port}`;
  const url = `${sessionOrigin}/${VIEWER_HTML_NAME}`;
  if (shouldOpenBrowser) {
    try {
      await openBrowser(url);
    } catch (err) {
      console.warn(
        `[warn] failed to open the browser automatically: ${err.message || err}`,
      );
    }
  }

  return {
    close,
    port: address.port,
    rootDir,
    server,
    url,
    viewerAssetsDir,
    waitUntilClosed() {
      return closed;
    },
  };
}

module.exports = {
  startViewerSession,
};
