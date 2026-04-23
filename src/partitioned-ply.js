const fs = require('fs');
const path = require('path');

const {
  ConversionError,
  GaussianCloud,
  Bounds,
  TileNode,
  ensure,
  shDegreeFromCoeffCount,
  _canonicalGaussianRowByteSize,
  _readPlyHeaderFromHandle,
  _buildGaussianPlyLayout,
  _forEachGaussianPlyPosition,
  _forEachGaussianPlyCanonicalRecord,
} = require('./parser');

const {
  SPZ_STREAM_VERSION,
  packCloudToSpz,
  serializeCloudForWorkerTask,
  transferListForCloud,
} = require('./codec');
const { GltfBuilder } = require('./gltf');
const {
  SOURCE_REPOSITORY,
  ConsoleProgressBar,
  SpzContentWorkerPool,
  computeBounds,
  computeThreeSigmaAabbDiagonalRadius,
  childBounds,
  chooseGridDims,
  normalizeSplatTargetCount,
  defaultVoxelTargetCount,
  constrainTargetSplatCount,
  percent95,
  planSimplifyCloudVoxel,
  samplingDivisorForDepth,
  geometricErrorScaleForDepth,
  rootGeometricErrorFromMinLevel,
  buildSubtreeArtifact,
  writeSubtreeFile,
} = require('./builder');

const DEFAULT_WORKER_SCRIPT = path.join(__dirname, 'convert-core.js');
const TEMP_WORKSPACE_NAME = '.tmp-ply-partitions';
const PIPELINE_STATE_FILE = 'pipeline-state.json';
const PIPELINE_STATE_VERSION = 2;
const LEAF_BUCKET_DIR = 'leaf';
const HANDOFF_BUCKET_DIR = 'handoff';
const LEAF_BUCKET_ENCODING = 'canonical32';
const HANDOFF_BUCKET_ENCODING = 'canonical32';
const PARTITION_FLUSH_BYTES = 16 * 1024 * 1024;
const SPZ_ASYNC_WRITE_THRESHOLD = 65536;
const BUILT_IN_SOURCE_TO_TILE_Z_UP = [
  [1.0, 0.0, 0.0],
  [0.0, 0.0, 1.0],
  [0.0, -1.0, 0.0],
];
const GLTF_TILESET_CONTENT_EXTENSION = '3DTILES_content_gltf';
const GAUSSIAN_SPLATTING_GLTF_EXTENSIONS = [
  'KHR_gaussian_splatting',
  'KHR_gaussian_splatting_compression_spz_2',
];

function makeTilesetAsset() {
  return {
    version: '1.1',
  };
}

function applyTilesetGltfContentExtensions(tileset) {
  tileset.extensions = {
    ...(tileset.extensions || {}),
    [GLTF_TILESET_CONTENT_EXTENSION]: {
      extensionsRequired: [...GAUSSIAN_SPLATTING_GLTF_EXTENSIONS],
      extensionsUsed: [...GAUSSIAN_SPLATTING_GLTF_EXTENSIONS],
    },
  };

  const extensionsUsed = Array.isArray(tileset.extensionsUsed)
    ? tileset.extensionsUsed.filter((name) => !!name)
    : [];
  if (!extensionsUsed.includes(GLTF_TILESET_CONTENT_EXTENSION)) {
    extensionsUsed.push(GLTF_TILESET_CONTENT_EXTENSION);
  }
  tileset.extensionsUsed = extensionsUsed;
  return tileset;
}

function applyRootTransform(root, transform) {
  if (!transform) {
    return root;
  }
  root.transform = transform.slice();
  return root;
}

function applyContentBoxTransform(box) {
  if (!Array.isArray(box) || box.length !== 12) {
    return box;
  }

  const out = box.slice();
  for (const base of [0, 3, 6, 9]) {
    const x = out[base];
    const y = out[base + 1];
    const z = out[base + 2];
    out[base] =
      BUILT_IN_SOURCE_TO_TILE_Z_UP[0][0] * x +
      BUILT_IN_SOURCE_TO_TILE_Z_UP[0][1] * y +
      BUILT_IN_SOURCE_TO_TILE_Z_UP[0][2] * z;
    out[base + 1] =
      BUILT_IN_SOURCE_TO_TILE_Z_UP[1][0] * x +
      BUILT_IN_SOURCE_TO_TILE_Z_UP[1][1] * y +
      BUILT_IN_SOURCE_TO_TILE_Z_UP[1][2] * z;
    out[base + 2] =
      BUILT_IN_SOURCE_TO_TILE_Z_UP[2][0] * x +
      BUILT_IN_SOURCE_TO_TILE_Z_UP[2][1] * y +
      BUILT_IN_SOURCE_TO_TILE_Z_UP[2][2] * z;
  }
  return out;
}

function makeNodeKey(level, x, y, z) {
  return `${level}/${x}/${y}/${z}`;
}

function pointOctant(bounds, x, y, z) {
  const c = bounds.center();
  return (
    (x >= c[0] ? 1 : 0) |
    (y >= c[1] ? 2 : 0) |
    (z >= c[2] ? 4 : 0)
  );
}

function targetSplatCountForDepth(
  depth,
  lodMaxDepth,
  samplingRatePerLevel,
  splatCount,
) {
  const divisor = samplingDivisorForDepth(
    depth,
    lodMaxDepth,
    samplingRatePerLevel,
  );
  return Math.max(1, Math.min(splatCount, Math.ceil(splatCount / divisor)));
}

function fallbackRootGeometricError(bounds, splatCount) {
  const ex = bounds.extents();
  const diag = Math.sqrt(ex[0] * ex[0] + ex[1] * ex[1] + ex[2] * ex[2]);
  if (splatCount <= 1) {
    return Math.max(diag * 1e-6, 1e-6);
  }
  return Math.max(diag * 0.125, diag * 1e-6, 1e-6);
}

function canonicalNodePath(baseDir, subdir, node) {
  return path.join(
    baseDir,
    subdir,
    String(node.level),
    String(node.x),
    String(node.y),
    `${node.z}.bin`,
  );
}

function contentRelPath(level, x, y, z) {
  return `tiles/${level}/${x}/${y}/${z}.glb`;
}

async function pathExists(targetPath) {
  if (!targetPath) {
    return false;
  }
  try {
    await fs.promises.access(targetPath);
    return true;
  } catch {
    return false;
  }
}

function serializeBoundsState(bounds) {
  if (!bounds) {
    return null;
  }
  return {
    minimum: bounds.minimum.slice(),
    maximum: bounds.maximum.slice(),
  };
}

function deserializeBoundsState(value) {
  if (!value) {
    return null;
  }
  return new Bounds(value.minimum.slice(), value.maximum.slice());
}

function buildNodeTreeFromCounts(counts, bounds, maxDepth, leafLimit, depth, x, y, z) {
  const key = makeNodeKey(depth, x, y, z);
  const count = counts.get(key) || 0;
  if (count <= 0) {
    return null;
  }

  const node = {
    key,
    level: depth,
    depth,
    x,
    y,
    z,
    count,
    bounds,
    leaf: depth >= maxDepth || count <= leafLimit,
    children: [],
    childrenByOct: new Array(8).fill(null),
    occupiedChildCount: 0,
    bucketPath: null,
    contentUri: null,
    handoffPath: null,
    handoffConsumed: false,
    ownError: null,
    buildState: 'pending',
  };

  if (!node.leaf) {
    for (let oct = 0; oct < 8; oct++) {
      const child = buildNodeTreeFromCounts(
        counts,
        childBounds(bounds, oct),
        maxDepth,
        leafLimit,
        depth + 1,
        (x << 1) | (oct & 1),
        (y << 1) | ((oct >> 1) & 1),
        (z << 1) | ((oct >> 2) & 1),
      );
      if (!child) {
        continue;
      }
      node.children.push(child);
      node.childrenByOct[oct] = child;
    }
    node.occupiedChildCount = node.children.length;
  }

  return node;
}

function serializeNodeMeta(node) {
  return {
    key: node.key,
    level: node.level,
    depth: node.depth,
    x: node.x,
    y: node.y,
    z: node.z,
    count: node.count,
    bounds: serializeBoundsState(node.bounds),
    leaf: node.leaf,
    occupiedChildCount: node.occupiedChildCount,
    bucketPath: node.bucketPath,
    contentUri: node.contentUri,
    handoffPath: node.handoffPath,
    handoffConsumed: !!node.handoffConsumed,
    ownError: node.ownError,
    buildState: node.buildState,
    children: node.children.map((child) => serializeNodeMeta(child)),
  };
}

function deserializeNodeMeta(data) {
  const node = {
    key: data.key,
    level: data.level,
    depth: data.depth,
    x: data.x,
    y: data.y,
    z: data.z,
    count: data.count,
    bounds: deserializeBoundsState(data.bounds),
    leaf: data.leaf,
    children: [],
    childrenByOct: new Array(8).fill(null),
    occupiedChildCount: data.occupiedChildCount || 0,
    bucketPath: data.bucketPath || null,
    contentUri: data.contentUri || null,
    handoffPath: data.handoffPath || null,
    handoffConsumed: !!data.handoffConsumed,
    ownError:
      Number.isFinite(data.ownError) || data.ownError === 0
        ? data.ownError
        : null,
    buildState: data.buildState || 'pending',
  };
  if (Array.isArray(data.children)) {
    for (const childData of data.children) {
      const child = deserializeNodeMeta(childData);
      node.children.push(child);
      const oct = ((child.x & 1) << 0) | ((child.y & 1) << 1) | ((child.z & 1) << 2);
      node.childrenByOct[oct] = child;
    }
  }
  return node;
}

function collectTreeStats(node) {
  const nodes = [];
  const leaves = [];
  let maxLevel = 0;
  const levels = [];
  const visit = (current) => {
    nodes.push(current);
    if (!levels[current.level]) {
      levels[current.level] = [];
    }
    levels[current.level].push(current);
    if (current.leaf) {
      leaves.push(current);
    }
    if (current.level > maxLevel) {
      maxLevel = current.level;
    }
    for (const child of current.children) {
      visit(child);
    }
  };
  visit(node);
  return { nodes, leaves, maxLevel, levels };
}

function resolveLeafNodeForPoint(root, x, y, z) {
  let node = root;
  while (!node.leaf) {
    const oct = pointOctant(node.bounds, x, y, z);
    const child = node.childrenByOct[oct];
    ensure(
      !!child,
      `Failed to resolve leaf bucket for point at node ${node.key}.`,
    );
    node = child;
  }
  return node;
}

function resetNodeArtifacts(node) {
  node.contentUri = null;
  node.handoffPath = null;
  node.handoffConsumed = false;
  node.ownError = null;
  node.buildState = 'pending';
  if (node.leaf) {
    node.bucketPath = null;
  }
  for (const child of node.children) {
    resetNodeArtifacts(child);
  }
}

function makePipelineFingerprint(inputPath, inputStat, args) {
  return {
    inputPath: path.resolve(inputPath),
    inputSize: inputStat.size,
    inputMtimeMs: inputStat.mtimeMs,
    inputConvention: args.inputConvention,
    linearScaleInput: args.linearScaleInput,
    maxDepth: args.maxDepth,
    leafLimit: args.leafLimit,
    samplingRatePerLevel: args.samplingRatePerLevel,
    sampleMode: args.sampleMode,
    plyBuildMode: args.plyBuildMode,
    tilingMode: args.tilingMode,
    subtreeLevels: args.subtreeLevels,
    minGeometricError: args.minGeometricError,
    colorSpace: args.colorSpace,
    spzSh1Bits: args.spzSh1Bits,
    spzShRestBits: args.spzShRestBits,
  };
}

function fingerprintsMatch(lhs, rhs) {
  return JSON.stringify(lhs) === JSON.stringify(rhs);
}

function makeEmptyPipelineState(fingerprint) {
  return {
    version: PIPELINE_STATE_VERSION,
    fingerprint,
    stage: 'init',
    rootBounds: null,
    layout: null,
    rootNode: null,
    updatedAt: null,
  };
}

async function readPipelineState(tempDir) {
  const statePath = path.join(tempDir, PIPELINE_STATE_FILE);
  if (!(await pathExists(statePath))) {
    return null;
  }
  const text = await fs.promises.readFile(statePath, 'utf8');
  return JSON.parse(text);
}

async function removeFileIfExists(filePath) {
  if (!filePath) {
    return;
  }
  try {
    await fs.promises.unlink(filePath);
  } catch (err) {
    if (!err || err.code !== 'ENOENT') {
      throw err;
    }
  }
}

function enqueuePipelineStateSave(ctx, stage = null) {
  if (stage) {
    ctx.pipelineState.stage = stage;
  }
  ctx.pipelineState.rootBounds = serializeBoundsState(ctx.rootBounds);
  ctx.pipelineState.rootNode = serializeNodeMeta(ctx.rootNode);
  ctx.pipelineState.layout = {
    degree: ctx.layout.degree,
    coeffCount: ctx.layout.coeffCount,
  };
  ctx.pipelineState.updatedAt = new Date().toISOString();

  const targetPath = path.join(ctx.tempDir, PIPELINE_STATE_FILE);
  ctx.savePromise = ctx.savePromise.then(() =>
    fs.promises.writeFile(
      targetPath,
      JSON.stringify(ctx.pipelineState),
      'utf8',
    ),
  );
  return ctx.savePromise;
}

function bucketRowByteSize(encoding, coeffCount) {
  if (
    encoding === LEAF_BUCKET_ENCODING ||
    encoding === HANDOFF_BUCKET_ENCODING
  ) {
    return _canonicalGaussianRowByteSize(coeffCount);
  }
  throw new ConversionError(`Unknown bucket encoding: ${encoding}`);
}

function makeBucketFileSpec(filePath, encoding) {
  return { filePath, encoding };
}

function leafBucketSpec(node) {
  return makeBucketFileSpec(node.bucketPath, LEAF_BUCKET_ENCODING);
}

function handoffBucketSpec(node) {
  return makeBucketFileSpec(node.handoffPath, HANDOFF_BUCKET_ENCODING);
}

function makeRowScratch(coeffCount) {
  return {
    position: new Float64Array(3),
    scaleLog: new Float64Array(3),
    quat: new Float64Array(4),
    opacity: 0.0,
    sh: new Float64Array(coeffCount * 3),
  };
}

function readBucketRowIntoScratch(encoding, view, base, coeffCount, scratch) {
  scratch.position[0] = view.getFloat32(base + 0, true);
  scratch.position[1] = view.getFloat32(base + 4, true);
  scratch.position[2] = view.getFloat32(base + 8, true);

  if (
    encoding === LEAF_BUCKET_ENCODING ||
    encoding === HANDOFF_BUCKET_ENCODING
  ) {
    scratch.scaleLog[0] = view.getFloat32(base + 12, true);
    scratch.scaleLog[1] = view.getFloat32(base + 16, true);
    scratch.scaleLog[2] = view.getFloat32(base + 20, true);
    scratch.quat[0] = view.getFloat32(base + 24, true);
    scratch.quat[1] = view.getFloat32(base + 28, true);
    scratch.quat[2] = view.getFloat32(base + 32, true);
    scratch.quat[3] = view.getFloat32(base + 36, true);
    scratch.opacity = view.getFloat32(base + 40, true);
    for (let i = 0; i < coeffCount * 3; i++) {
      scratch.sh[i] = view.getFloat32(base + (11 + i) * 4, true);
    }
    return;
  }

  throw new ConversionError(`Unknown bucket encoding: ${encoding}`);
}

function readBucketCoreRowIntoScratch(encoding, view, base, scratch) {
  scratch.position[0] = view.getFloat32(base + 0, true);
  scratch.position[1] = view.getFloat32(base + 4, true);
  scratch.position[2] = view.getFloat32(base + 8, true);

  if (
    encoding === LEAF_BUCKET_ENCODING ||
    encoding === HANDOFF_BUCKET_ENCODING
  ) {
    scratch.scaleLog[0] = view.getFloat32(base + 12, true);
    scratch.scaleLog[1] = view.getFloat32(base + 16, true);
    scratch.scaleLog[2] = view.getFloat32(base + 20, true);
    scratch.quat[0] = view.getFloat32(base + 24, true);
    scratch.quat[1] = view.getFloat32(base + 28, true);
    scratch.quat[2] = view.getFloat32(base + 32, true);
    scratch.quat[3] = view.getFloat32(base + 36, true);
    scratch.opacity = view.getFloat32(base + 40, true);
    return;
  }

  throw new ConversionError(`Unknown bucket encoding: ${encoding}`);
}

async function appendBufferedBatches(buffered, ensuredDirs) {
  if (buffered.size === 0) {
    return;
  }

  const writes = [];
  for (const [filePath, chunks] of buffered.entries()) {
    if (!chunks || chunks.length === 0) {
      continue;
    }
    const dir = path.dirname(filePath);
    if (!ensuredDirs.has(dir)) {
      await fs.promises.mkdir(dir, { recursive: true });
      ensuredDirs.add(dir);
    }
    const payload = chunks.length === 1 ? chunks[0] : Buffer.concat(chunks);
    writes.push(fs.promises.writeFile(filePath, payload, { flag: 'a' }));
  }
  await Promise.all(writes);
  buffered.clear();
}

async function partitionLeafBuckets(
  handle,
  filePath,
  header,
  layout,
  rootNode,
  tempDir,
) {
  const buffered = new Map();
  const ensuredDirs = new Set();
  let bufferedBytes = 0;

  const flush = async () => {
    await appendBufferedBatches(buffered, ensuredDirs);
    bufferedBytes = 0;
  };

  await _forEachGaussianPlyCanonicalRecord(
    handle,
    filePath,
    header,
    layout,
    async (_rowIndex, rowBuffer, rowView) => {
      const leaf = resolveLeafNodeForPoint(
        rootNode,
        rowView.getFloat32(0, true),
        rowView.getFloat32(4, true),
        rowView.getFloat32(8, true),
      );
      if (!leaf.bucketPath) {
        leaf.bucketPath = canonicalNodePath(tempDir, LEAF_BUCKET_DIR, leaf);
      }
      let chunks = buffered.get(leaf.bucketPath);
      if (!chunks) {
        chunks = [];
        buffered.set(leaf.bucketPath, chunks);
      }
      const rowCopy = Buffer.from(rowBuffer);
      chunks.push(rowCopy);
      bufferedBytes += rowCopy.length;
      if (bufferedBytes >= PARTITION_FLUSH_BYTES) {
        await flush();
      }
    },
  );

  await flush();
}

async function collectBucketEntries(fileSpecs, coeffCount) {
  const entries = [];
  let totalRows = 0;
  for (const fileSpec of fileSpecs) {
    if (!fileSpec || !fileSpec.filePath) {
      continue;
    }
    const rowByteSize = bucketRowByteSize(fileSpec.encoding, coeffCount);
    const stat = await fs.promises.stat(fileSpec.filePath);
    ensure(
      stat.size % rowByteSize === 0,
      `Bucket file has invalid byte length: ${fileSpec.filePath}`,
    );
    const rowCount = stat.size / rowByteSize;
    if (rowCount <= 0) {
      continue;
    }
    entries.push({ ...fileSpec, rowByteSize, rowCount });
    totalRows += rowCount;
  }
  return { entries, totalRows };
}

async function forEachBucketSpecRow(fileSpec, coeffCount, onRow) {
  if (!fileSpec || !fileSpec.filePath) {
    return;
  }
  const rowByteSize = bucketRowByteSize(fileSpec.encoding, coeffCount);
  const stat = await fs.promises.stat(fileSpec.filePath);
  ensure(
    stat.size % rowByteSize === 0,
    `Bucket file has invalid byte length: ${fileSpec.filePath}`,
  );
  if (stat.size === 0) {
    return;
  }

  const rowsPerChunk = Math.max(1, Math.floor((8 * 1024 * 1024) / rowByteSize));
  const chunkBytes = rowsPerChunk * rowByteSize;
  const chunk = Buffer.allocUnsafe(chunkBytes);
  const handle = await fs.promises.open(fileSpec.filePath, 'r');
  try {
    let fileOffset = 0;
    while (fileOffset < stat.size) {
      const expectedBytes = Math.min(chunkBytes, stat.size - fileOffset);
      const { bytesRead } = await handle.read(
        chunk,
        0,
        expectedBytes,
        fileOffset,
      );
      ensure(
        bytesRead === expectedBytes,
        `Bucket file ended early: ${fileSpec.filePath}`,
      );
      fileOffset += bytesRead;

      const view = new DataView(chunk.buffer, chunk.byteOffset, bytesRead);
      for (let offset = 0; offset < bytesRead; offset += rowByteSize) {
        const maybePromise = onRow(
          view,
          offset,
          fileSpec.encoding,
          fileSpec.filePath,
        );
        if (maybePromise && typeof maybePromise.then === 'function') {
          await maybePromise;
        }
      }
    }
  } finally {
    await handle.close();
  }
}

async function forEachBucketRow(fileSpecs, coeffCount, onRow) {
  for (const fileSpec of fileSpecs) {
    await forEachBucketSpecRow(fileSpec, coeffCount, onRow);
  }
}

async function forEachBucketEntryRow(entries, coeffCount, onRow) {
  for (const entry of entries) {
    await forEachBucketSpecRow(entry, coeffCount, onRow);
  }
}

async function loadBucketCloudFromSpecs(fileSpecs, coeffCount) {
  const { entries, totalRows } = await collectBucketEntries(fileSpecs, coeffCount);
  ensure(totalRows > 0, 'Cannot load an empty Gaussian cloud from bucket files.');

  const coeffStride = coeffCount * 3;
  const positions = new Float32Array(totalRows * 3);
  const scaleLog = new Float32Array(totalRows * 3);
  const quats = new Float32Array(totalRows * 4);
  const opacity = new Float32Array(totalRows);
  const shCoeffs = new Float32Array(totalRows * coeffStride);
  const scratch = makeRowScratch(coeffCount);
  let rowIndex = 0;

  for (const entry of entries) {
    await forEachBucketSpecRow(entry, coeffCount, (view, base, encoding) => {
      readBucketRowIntoScratch(encoding, view, base, coeffCount, scratch);
      const base3 = rowIndex * 3;
      const base4 = rowIndex * 4;
      const coeffBase = rowIndex * coeffStride;
      positions[base3 + 0] = scratch.position[0];
      positions[base3 + 1] = scratch.position[1];
      positions[base3 + 2] = scratch.position[2];
      scaleLog[base3 + 0] = scratch.scaleLog[0];
      scaleLog[base3 + 1] = scratch.scaleLog[1];
      scaleLog[base3 + 2] = scratch.scaleLog[2];
      quats[base4 + 0] = scratch.quat[0];
      quats[base4 + 1] = scratch.quat[1];
      quats[base4 + 2] = scratch.quat[2];
      quats[base4 + 3] = scratch.quat[3];
      opacity[rowIndex] = scratch.opacity;
      for (let c = 0; c < coeffStride; c++) {
        shCoeffs[coeffBase + c] = scratch.sh[c];
      }
      rowIndex += 1;
    });
  }

  const cloud = new GaussianCloud(
    positions,
    scaleLog,
    quats,
    opacity,
    shCoeffs,
    null,
  );
  cloud._shDegree = shDegreeFromCoeffCount(coeffCount);
  return cloud;
}

async function writeCanonicalCloudFile(filePath, cloud) {
  ensure(cloud.length > 0, 'Cannot write an empty handoff cloud.');
  await fs.promises.mkdir(path.dirname(filePath), { recursive: true });
  const coeffCount = cloud.shCoeffs.length / (cloud.length * 3);
  const coeffStride = coeffCount * 3;
  const rowByteSize = _canonicalGaussianRowByteSize(coeffCount);
  const rowsPerChunk = Math.max(1, Math.floor((8 * 1024 * 1024) / rowByteSize));
  const handle = await fs.promises.open(filePath, 'w');
  try {
    for (let rowBase = 0; rowBase < cloud.length; rowBase += rowsPerChunk) {
      const rowCount = Math.min(rowsPerChunk, cloud.length - rowBase);
      const chunk = Buffer.allocUnsafe(rowCount * rowByteSize);
      const view = new DataView(chunk.buffer, chunk.byteOffset, chunk.byteLength);
      for (let i = 0; i < rowCount; i++) {
        const rowIndex = rowBase + i;
        const base = i * rowByteSize;
        const base3 = rowIndex * 3;
        const base4 = rowIndex * 4;
        const coeffBase = rowIndex * coeffStride;
        view.setFloat32(base + 0, cloud.positions[base3 + 0], true);
        view.setFloat32(base + 4, cloud.positions[base3 + 1], true);
        view.setFloat32(base + 8, cloud.positions[base3 + 2], true);
        view.setFloat32(base + 12, cloud.scaleLog[base3 + 0], true);
        view.setFloat32(base + 16, cloud.scaleLog[base3 + 1], true);
        view.setFloat32(base + 20, cloud.scaleLog[base3 + 2], true);
        view.setFloat32(base + 24, cloud.quatsXYZW[base4 + 0], true);
        view.setFloat32(base + 28, cloud.quatsXYZW[base4 + 1], true);
        view.setFloat32(base + 32, cloud.quatsXYZW[base4 + 2], true);
        view.setFloat32(base + 36, cloud.quatsXYZW[base4 + 3], true);
        view.setFloat32(base + 40, cloud.opacity[rowIndex], true);
        for (let c = 0; c < coeffStride; c++) {
          view.setFloat32(
            base + (11 + c) * 4,
            cloud.shCoeffs[coeffBase + c],
            true,
          );
        }
      }
      await handle.write(chunk, 0, chunk.length, null);
    }
  } finally {
    await handle.close();
  }
}

function normalizeQuaternionInScratch(quat) {
  let x = quat[0];
  let y = quat[1];
  let z = quat[2];
  let w = quat[3];
  const len2 = x * x + y * y + z * z + w * w;
  if (len2 < 1e-20) {
    return [0.0, 0.0, 0.0, 1.0];
  }
  const inv = 1.0 / Math.sqrt(len2);
  return [x * inv, y * inv, z * inv, w * inv];
}

function covarianceComponentsFromScratch(scaleLogIn, quatIn, out) {
  const [x, y, z, w] = normalizeQuaternionInScratch(quatIn);
  const xx = x * x;
  const yy = y * y;
  const zz = z * z;
  const xy = x * y;
  const xz = x * z;
  const yz = y * z;
  const wx = w * x;
  const wy = w * y;
  const wz = w * z;

  const s0 = Math.exp(scaleLogIn[0]);
  const s1 = Math.exp(scaleLogIn[1]);
  const s2 = Math.exp(scaleLogIn[2]);
  const s2x = s0 * s0;
  const s2y = s1 * s1;
  const s2z = s2 * s2;

  const r00 = 1.0 - 2.0 * (yy + zz);
  const r10 = 2.0 * (xy - wz);
  const r20 = 2.0 * (xz + wy);
  const r01 = 2.0 * (xy + wz);
  const r11 = 1.0 - 2.0 * (xx + zz);
  const r21 = 2.0 * (yz - wx);
  const r02 = 2.0 * (xz - wy);
  const r12 = 2.0 * (yz + wx);
  const r22 = 1.0 - 2.0 * (xx + yy);

  out[0] = r00 * r00 * s2x + r10 * r10 * s2y + r20 * r20 * s2z;
  out[1] = r00 * r01 * s2x + r10 * r11 * s2y + r20 * r21 * s2z;
  out[2] = r00 * r02 * s2x + r10 * r12 * s2y + r20 * r22 * s2z;
  out[3] = r01 * r01 * s2x + r11 * r11 * s2y + r21 * r21 * s2z;
  out[4] = r01 * r02 * s2x + r11 * r12 * s2y + r21 * r22 * s2z;
  out[5] = r02 * r02 * s2x + r12 * r12 * s2y + r22 * r22 * s2z;
  return out;
}

function threeSigmaDiagonalRadiusFromScratch(scaleLogIn, quatIn) {
  const cov = new Float64Array(6);
  covarianceComponentsFromScratch(scaleLogIn, quatIn, cov);
  const ex = 3.0 * Math.sqrt(Math.max(cov[0], 1e-20));
  const ey = 3.0 * Math.sqrt(Math.max(cov[3], 1e-20));
  const ez = 3.0 * Math.sqrt(Math.max(cov[5], 1e-20));
  return Math.sqrt(ex * ex + ey * ey + ez * ez);
}

function quaternionFromRotationMatrix(
  r00,
  r01,
  r02,
  r10,
  r11,
  r12,
  r20,
  r21,
  r22,
) {
  const trace = r00 + r11 + r22;
  let x;
  let y;
  let z;
  let w;

  if (trace > 0.0) {
    const s = Math.sqrt(trace + 1.0) * 2.0;
    w = 0.25 * s;
    x = (r21 - r12) / s;
    y = (r02 - r20) / s;
    z = (r10 - r01) / s;
  } else if (r00 > r11 && r00 > r22) {
    const s = Math.sqrt(Math.max(1.0 + r00 - r11 - r22, 1e-20)) * 2.0;
    w = (r21 - r12) / s;
    x = 0.25 * s;
    y = (r01 + r10) / s;
    z = (r02 + r20) / s;
  } else if (r11 > r22) {
    const s = Math.sqrt(Math.max(1.0 + r11 - r00 - r22, 1e-20)) * 2.0;
    w = (r02 - r20) / s;
    x = (r01 + r10) / s;
    y = 0.25 * s;
    z = (r12 + r21) / s;
  } else {
    const s = Math.sqrt(Math.max(1.0 + r22 - r00 - r11, 1e-20)) * 2.0;
    w = (r10 - r01) / s;
    x = (r02 + r20) / s;
    y = (r12 + r21) / s;
    z = 0.25 * s;
  }

  const len2 = x * x + y * y + z * z + w * w;
  if (len2 < 1e-20) {
    return [0.0, 0.0, 0.0, 1.0];
  }
  const inv = 1.0 / Math.sqrt(len2);
  return [x * inv, y * inv, z * inv, w * inv];
}

function covarianceToScaleQuat(
  c00,
  c01,
  c02,
  c11,
  c12,
  c22,
  scaleLogOut,
  scaleOff,
  quatsOut,
  quatOff,
) {
  const a = [
    [c00, c01, c02],
    [c01, c11, c12],
    [c02, c12, c22],
  ];
  const v = [
    [1.0, 0.0, 0.0],
    [0.0, 1.0, 0.0],
    [0.0, 0.0, 1.0],
  ];

  for (let iter = 0; iter < 12; iter++) {
    let p = 0;
    let q = 1;
    let maxOff = Math.abs(a[0][1]);
    const abs02 = Math.abs(a[0][2]);
    if (abs02 > maxOff) {
      p = 0;
      q = 2;
      maxOff = abs02;
    }
    const abs12 = Math.abs(a[1][2]);
    if (abs12 > maxOff) {
      p = 1;
      q = 2;
      maxOff = abs12;
    }

    const scale =
      Math.abs(a[0][0]) + Math.abs(a[1][1]) + Math.abs(a[2][2]) + 1.0;
    if (maxOff <= 1e-10 * scale) {
      break;
    }

    const app = a[p][p];
    const aqq = a[q][q];
    const apq = a[p][q];
    if (Math.abs(apq) <= 1e-20) {
      continue;
    }

    const tau = (aqq - app) / (2.0 * apq);
    const signTau = tau >= 0.0 ? 1.0 : -1.0;
    const t = signTau / (Math.abs(tau) + Math.sqrt(1.0 + tau * tau));
    const c = 1.0 / Math.sqrt(1.0 + t * t);
    const s = t * c;

    for (let r = 0; r < 3; r++) {
      if (r === p || r === q) {
        continue;
      }
      const arp = a[r][p];
      const arq = a[r][q];
      const nextRp = c * arp - s * arq;
      const nextRq = s * arp + c * arq;
      a[r][p] = nextRp;
      a[p][r] = nextRp;
      a[r][q] = nextRq;
      a[q][r] = nextRq;
    }

    a[p][p] = c * c * app - 2.0 * s * c * apq + s * s * aqq;
    a[q][q] = s * s * app + 2.0 * s * c * apq + c * c * aqq;
    a[p][q] = 0.0;
    a[q][p] = 0.0;

    for (let r = 0; r < 3; r++) {
      const vrp = v[r][p];
      const vrq = v[r][q];
      v[r][p] = c * vrp - s * vrq;
      v[r][q] = s * vrp + c * vrq;
    }
  }

  const eigen = [
    { value: Math.max(a[0][0], 1e-20), col: 0 },
    { value: Math.max(a[1][1], 1e-20), col: 1 },
    { value: Math.max(a[2][2], 1e-20), col: 2 },
  ].sort((lhs, rhs) => rhs.value - lhs.value || lhs.col - rhs.col);

  const rot = new Float64Array(9);
  for (let dstCol = 0; dstCol < 3; dstCol++) {
    const srcCol = eigen[dstCol].col;
    rot[dstCol + 0] = v[0][srcCol];
    rot[dstCol + 3] = v[1][srcCol];
    rot[dstCol + 6] = v[2][srcCol];
  }

  const dot01 = rot[0] * rot[1] + rot[3] * rot[4] + rot[6] * rot[7];
  const dot02 = rot[0] * rot[2] + rot[3] * rot[5] + rot[6] * rot[8];
  const dot12 = rot[1] * rot[2] + rot[4] * rot[5] + rot[7] * rot[8];
  if (Math.abs(dot01) > 1e-10 || Math.abs(dot02) > 1e-10 || Math.abs(dot12) > 1e-10) {
    const c0x = rot[0];
    const c0y = rot[3];
    const c0z = rot[6];
    const len0 = Math.max(Math.sqrt(c0x * c0x + c0y * c0y + c0z * c0z), 1e-20);
    rot[0] /= len0;
    rot[3] /= len0;
    rot[6] /= len0;

    const proj1 = rot[0] * rot[1] + rot[3] * rot[4] + rot[6] * rot[7];
    rot[1] -= proj1 * rot[0];
    rot[4] -= proj1 * rot[3];
    rot[7] -= proj1 * rot[6];
    const len1 = Math.max(
      Math.sqrt(rot[1] * rot[1] + rot[4] * rot[4] + rot[7] * rot[7]),
      1e-20,
    );
    rot[1] /= len1;
    rot[4] /= len1;
    rot[7] /= len1;

    rot[2] = rot[3] * rot[7] - rot[6] * rot[4];
    rot[5] = rot[6] * rot[1] - rot[0] * rot[7];
    rot[8] = rot[0] * rot[4] - rot[3] * rot[1];
  }

  const det =
    rot[0] * (rot[4] * rot[8] - rot[5] * rot[7]) -
    rot[1] * (rot[3] * rot[8] - rot[5] * rot[6]) +
    rot[2] * (rot[3] * rot[7] - rot[4] * rot[6]);
  if (det < 0.0) {
    rot[2] *= -1.0;
    rot[5] *= -1.0;
    rot[8] *= -1.0;
  }

  scaleLogOut[scaleOff + 0] = Math.log(Math.sqrt(eigen[0].value));
  scaleLogOut[scaleOff + 1] = Math.log(Math.sqrt(eigen[1].value));
  scaleLogOut[scaleOff + 2] = Math.log(Math.sqrt(eigen[2].value));

  const quat = quaternionFromRotationMatrix(
    rot[0],
    rot[1],
    rot[2],
    rot[3],
    rot[4],
    rot[5],
    rot[6],
    rot[7],
    rot[8],
  );
  quatsOut[quatOff + 0] = quat[0];
  quatsOut[quatOff + 1] = quat[1];
  quatsOut[quatOff + 2] = quat[2];
  quatsOut[quatOff + 3] = quat[3];
}

function mergeAggregationWeight(opacity, radius, voxelDiag) {
  const alpha = Math.max(opacity, 1e-4);
  const radiusNorm = Math.max(radius / Math.max(voxelDiag, 1e-6), 0.35);
  return alpha * Math.sqrt(radiusNorm);
}

function makeVoxelGridParams(bounds, dims) {
  const ext = bounds.extents().map((v) => Math.max(v, 1e-6));
  const d0 = Math.max(1, dims[0]);
  const d1 = Math.max(1, dims[1]);
  const d2 = Math.max(1, dims[2]);
  const voxelSize0 = ext[0] / d0;
  const voxelSize1 = ext[1] / d1;
  const voxelSize2 = ext[2] / d2;
  return {
    mins: bounds.minimum,
    invExt0: 1.0 / ext[0],
    invExt1: 1.0 / ext[1],
    invExt2: 1.0 / ext[2],
    d0,
    d1,
    d2,
    d0m1: d0 - 1,
    d1m1: d1 - 1,
    d2m1: d2 - 1,
    voxelDiagSq:
      voxelSize0 * voxelSize0 +
      voxelSize1 * voxelSize1 +
      voxelSize2 * voxelSize2,
    voxelDiag: Math.max(
      Math.sqrt(
        voxelSize0 * voxelSize0 +
          voxelSize1 * voxelSize1 +
          voxelSize2 * voxelSize2,
      ),
      1e-6,
    ),
  };
}

function voxelFlatForGridParams(grid, x, y, z) {
  const uvw0 = Math.max(
    0.0,
    Math.min(0.999999, (x - grid.mins[0]) * grid.invExt0),
  );
  const uvw1 = Math.max(
    0.0,
    Math.min(0.999999, (y - grid.mins[1]) * grid.invExt1),
  );
  const uvw2 = Math.max(
    0.0,
    Math.min(0.999999, (z - grid.mins[2]) * grid.invExt2),
  );
  const iIdx = Math.min(grid.d0m1, Math.floor(uvw0 * grid.d0));
  const jIdx = Math.min(grid.d1m1, Math.floor(uvw1 * grid.d1));
  const kIdx = Math.min(grid.d2m1, Math.floor(uvw2 * grid.d2));
  return iIdx + grid.d0 * (jIdx + grid.d1 * kIdx);
}

function writeScratchRowToArrays(
  scratch,
  coeffStride,
  dstIndex,
  positions,
  scaleLog,
  quats,
  opacity,
  shCoeffs = null,
) {
  const base3 = dstIndex * 3;
  const base4 = dstIndex * 4;
  const coeffBase = dstIndex * coeffStride;
  positions[base3 + 0] = scratch.position[0];
  positions[base3 + 1] = scratch.position[1];
  positions[base3 + 2] = scratch.position[2];
  scaleLog[base3 + 0] = scratch.scaleLog[0];
  scaleLog[base3 + 1] = scratch.scaleLog[1];
  scaleLog[base3 + 2] = scratch.scaleLog[2];
  quats[base4 + 0] = scratch.quat[0];
  quats[base4 + 1] = scratch.quat[1];
  quats[base4 + 2] = scratch.quat[2];
  quats[base4 + 3] = scratch.quat[3];
  opacity[dstIndex] = scratch.opacity;
  if (shCoeffs) {
    for (let c = 0; c < coeffStride; c++) {
      shCoeffs[coeffBase + c] = scratch.sh[c];
    }
  }
}

function copySlotArrays(
  srcIndex,
  coeffStride,
  srcPositions,
  srcScaleLog,
  srcQuats,
  srcOpacity,
  srcShCoeffs,
  dstPositions,
  dstScaleLog,
  dstQuats,
  dstOpacity,
  dstShCoeffs,
) {
  const base3 = srcIndex * 3;
  const base4 = srcIndex * 4;
  const coeffBase = srcIndex * coeffStride;
  dstPositions[base3 + 0] = srcPositions[base3 + 0];
  dstPositions[base3 + 1] = srcPositions[base3 + 1];
  dstPositions[base3 + 2] = srcPositions[base3 + 2];
  dstScaleLog[base3 + 0] = srcScaleLog[base3 + 0];
  dstScaleLog[base3 + 1] = srcScaleLog[base3 + 1];
  dstScaleLog[base3 + 2] = srcScaleLog[base3 + 2];
  dstQuats[base4 + 0] = srcQuats[base4 + 0];
  dstQuats[base4 + 1] = srcQuats[base4 + 1];
  dstQuats[base4 + 2] = srcQuats[base4 + 2];
  dstQuats[base4 + 3] = srcQuats[base4 + 3];
  dstOpacity[srcIndex] = srcOpacity[srcIndex];
  for (let c = 0; c < coeffStride; c++) {
    dstShCoeffs[coeffBase + c] = srcShCoeffs[coeffBase + c];
  }
}

function makeRepresentativeStats(groupCount, radiusBias) {
  const fallbackRadius = new Float64Array(groupCount);
  fallbackRadius.fill(radiusBias >= 0.0 ? Infinity : -Infinity);
  const fallbackWeight = new Float64Array(groupCount);
  fallbackWeight.fill(-Infinity);
  const fallbackIndex = new Int32Array(groupCount);
  fallbackIndex.fill(-1);
  return {
    counts: new Uint32Array(groupCount),
    sumW: new Float64Array(groupCount),
    cx: new Float64Array(groupCount),
    cy: new Float64Array(groupCount),
    cz: new Float64Array(groupCount),
    fallbackIndex,
    fallbackWeight,
    fallbackRadius,
  };
}

function updateRepresentativeStats(
  stats,
  groupIndex,
  rowIndex,
  x,
  y,
  z,
  weight,
  radius,
  radiusBias,
) {
  stats.counts[groupIndex] += 1;
  stats.sumW[groupIndex] += weight;
  stats.cx[groupIndex] += x * weight;
  stats.cy[groupIndex] += y * weight;
  stats.cz[groupIndex] += z * weight;
  if (
    stats.fallbackIndex[groupIndex] < 0 ||
    weight > stats.fallbackWeight[groupIndex] + 1e-12 ||
    (Math.abs(weight - stats.fallbackWeight[groupIndex]) <= 1e-12 &&
      ((radiusBias >= 0.0 && radius < stats.fallbackRadius[groupIndex]) ||
        (radiusBias < 0.0 && radius > stats.fallbackRadius[groupIndex])))
  ) {
    stats.fallbackIndex[groupIndex] = rowIndex;
    stats.fallbackWeight[groupIndex] = weight;
    stats.fallbackRadius[groupIndex] = radius;
  }
}

function finalizeRepresentativeCentroids(stats) {
  const groupCount = stats.counts.length;
  const useFallback = new Uint8Array(groupCount);
  for (let i = 0; i < groupCount; i++) {
    if (
      stats.counts[i] <= 1 ||
      !Number.isFinite(stats.sumW[i]) ||
      stats.sumW[i] <= 0.0
    ) {
      useFallback[i] = 1;
      continue;
    }
    stats.cx[i] /= stats.sumW[i];
    stats.cy[i] /= stats.sumW[i];
    stats.cz[i] /= stats.sumW[i];
  }
  return useFallback;
}

async function scanBucketBoundsFromEntries(entries, coeffCount) {
  const minimum = [Infinity, Infinity, Infinity];
  const maximum = [-Infinity, -Infinity, -Infinity];
  let count = 0;
  await forEachBucketEntryRow(entries, coeffCount, (view, base) => {
    const x = view.getFloat32(base + 0, true);
    const y = view.getFloat32(base + 4, true);
    const z = view.getFloat32(base + 8, true);
    if (x < minimum[0]) minimum[0] = x;
    if (y < minimum[1]) minimum[1] = y;
    if (z < minimum[2]) minimum[2] = z;
    if (x > maximum[0]) maximum[0] = x;
    if (y > maximum[1]) maximum[1] = y;
    if (z > maximum[2]) maximum[2] = z;
    count += 1;
  });
  ensure(count > 0, 'Cannot simplify an empty bucket input.');
  return new Bounds(minimum, maximum);
}

async function chooseExactStreamingGrouping(entries, coeffCount, bounds, target, voxelTarget) {
  let dims = chooseGridDims(bounds, voxelTarget);
  let groupKeys = [];
  let grid = makeVoxelGridParams(bounds, dims);
  for (let iter = 0; iter < 24; iter++) {
    grid = makeVoxelGridParams(bounds, dims);
    const keySet = new Set();
    await forEachBucketEntryRow(entries, coeffCount, (view, base) => {
      const flat = voxelFlatForGridParams(
        grid,
        view.getFloat32(base + 0, true),
        view.getFloat32(base + 4, true),
        view.getFloat32(base + 8, true),
      );
      keySet.add(flat);
    });
    groupKeys = Array.from(keySet).sort((a, b) => a - b);
    if (
      groupKeys.length <= target ||
      (grid.d0 === 1 && grid.d1 === 1 && grid.d2 === 1)
    ) {
      break;
    }
    dims = [
      Math.max(1, Math.floor(dims[0] * 0.85)),
      Math.max(1, Math.floor(dims[1] * 0.85)),
      Math.max(1, Math.floor(dims[2] * 0.85)),
    ];
  }
  return {
    grid,
    groupKeys,
    groupIndexByFlat: new Map(groupKeys.map((flat, index) => [flat, index])),
  };
}

async function buildExactStreamingMetadata(entries, coeffCount, grouping, totalRows) {
  const groupCount = grouping.groupKeys.length;
  const positions = new Float32Array(totalRows * 3);
  const radii = new Float32Array(totalRows);
  const coarseWeights = new Float64Array(totalRows);
  const detailWeights = new Float64Array(totalRows);
  const groupIds = new Int32Array(totalRows);
  const coarseStats = makeRepresentativeStats(groupCount, -0.15);
  const detailStats = makeRepresentativeStats(groupCount, 0.15);
  const scratch = makeRowScratch(coeffCount);
  let rowIndex = 0;

  await forEachBucketEntryRow(entries, coeffCount, (view, base, encoding) => {
    readBucketCoreRowIntoScratch(encoding, view, base, scratch);
    const flat = voxelFlatForGridParams(
      grouping.grid,
      scratch.position[0],
      scratch.position[1],
      scratch.position[2],
    );
    const groupIndex = grouping.groupIndexByFlat.get(flat);
    ensure(
      groupIndex != null,
      `Failed to resolve simplify voxel group for row ${rowIndex}.`,
    );

    const base3 = rowIndex * 3;
    const radius = threeSigmaDiagonalRadiusFromScratch(
      scratch.scaleLog,
      scratch.quat,
    );
    const alpha = Math.max(scratch.opacity, 1e-4);
    const radiusNorm = Math.max(radius / grouping.grid.voxelDiag, 0.35);
    const detailWeight = alpha / Math.sqrt(radiusNorm);
    const coarseWeight = alpha * Math.sqrt(radiusNorm);

    positions[base3 + 0] = scratch.position[0];
    positions[base3 + 1] = scratch.position[1];
    positions[base3 + 2] = scratch.position[2];
    radii[rowIndex] = radius;
    coarseWeights[rowIndex] = coarseWeight;
    detailWeights[rowIndex] = detailWeight;
    groupIds[rowIndex] = groupIndex;

    updateRepresentativeStats(
      coarseStats,
      groupIndex,
      rowIndex,
      scratch.position[0],
      scratch.position[1],
      scratch.position[2],
      coarseWeight,
      radius,
      -0.15,
    );
    updateRepresentativeStats(
      detailStats,
      groupIndex,
      rowIndex,
      scratch.position[0],
      scratch.position[1],
      scratch.position[2],
      detailWeight,
      radius,
      0.15,
    );
    rowIndex += 1;
  });

  return {
    positions,
    radii,
    coarseWeights,
    detailWeights,
    groupIds,
    coarseStats,
    detailStats,
  };
}

function selectRepresentativeRows(
  positions,
  radii,
  weights,
  groupIds,
  stats,
  useFallback,
  voxelDiagSq,
  radiusBias,
) {
  const groupCount = stats.counts.length;
  const reps = new Int32Array(groupCount);
  reps.fill(-1);
  const bestCost = new Float64Array(groupCount);
  bestCost.fill(Infinity);
  const bestWeight = new Float64Array(groupCount);
  const bestRadius = new Float64Array(groupCount);

  for (let g = 0; g < groupCount; g++) {
    reps[g] = stats.fallbackIndex[g];
    bestWeight[g] = stats.fallbackWeight[g];
    bestRadius[g] = stats.fallbackRadius[g];
  }

  const invVoxelDiagSq = 1.0 / Math.max(voxelDiagSq, 1e-12);
  const rowCount = groupIds.length;
  for (let rowIndex = 0; rowIndex < rowCount; rowIndex++) {
    const groupIndex = groupIds[rowIndex];
    if (useFallback[groupIndex]) {
      continue;
    }
    const base3 = rowIndex * 3;
    const dx = positions[base3 + 0] - stats.cx[groupIndex];
    const dy = positions[base3 + 1] - stats.cy[groupIndex];
    const dz = positions[base3 + 2] - stats.cz[groupIndex];
    const radius = radii[rowIndex];
    const weight = weights[rowIndex];
    const cost =
      (dx * dx + dy * dy + dz * dz) * invVoxelDiagSq +
      radiusBias * radius * radius * invVoxelDiagSq;
    if (
      cost < bestCost[groupIndex] - 1e-12 ||
      (Math.abs(cost - bestCost[groupIndex]) <= 1e-12 &&
        (weight > bestWeight[groupIndex] + 1e-12 ||
          (Math.abs(weight - bestWeight[groupIndex]) <= 1e-12 &&
            ((radiusBias >= 0.0 && radius < bestRadius[groupIndex]) ||
              (radiusBias < 0.0 && radius > bestRadius[groupIndex])))))
    ) {
      reps[groupIndex] = rowIndex;
      bestCost[groupIndex] = cost;
      bestWeight[groupIndex] = weight;
      bestRadius[groupIndex] = radius;
    }
  }
  return reps;
}

function buildExcludedRepresentativeStats(
  positions,
  radii,
  coarseWeights,
  detailWeights,
  groupIds,
  coarseRep,
  detailRep,
  groupCount,
) {
  const extraCoarseStats = makeRepresentativeStats(groupCount, -0.15);
  const extraDetailStats = makeRepresentativeStats(groupCount, 0.15);
  const rowCount = groupIds.length;
  for (let rowIndex = 0; rowIndex < rowCount; rowIndex++) {
    const groupIndex = groupIds[rowIndex];
    if (rowIndex === coarseRep[groupIndex] || rowIndex === detailRep[groupIndex]) {
      continue;
    }
    const base3 = rowIndex * 3;
    const x = positions[base3 + 0];
    const y = positions[base3 + 1];
    const z = positions[base3 + 2];
    const radius = radii[rowIndex];
    updateRepresentativeStats(
      extraCoarseStats,
      groupIndex,
      rowIndex,
      x,
      y,
      z,
      coarseWeights[rowIndex],
      radius,
      -0.15,
    );
    updateRepresentativeStats(
      extraDetailStats,
      groupIndex,
      rowIndex,
      x,
      y,
      z,
      detailWeights[rowIndex],
      radius,
      0.15,
    );
  }
  return {
    extraCoarseStats,
    extraDetailStats,
    extraCoarseFallback: finalizeRepresentativeCentroids(extraCoarseStats),
    extraDetailFallback: finalizeRepresentativeCentroids(extraDetailStats),
  };
}

function buildExactStreamingSelection(meta, target, voxelDiag, voxelDiagSq) {
  const groupCount = meta.coarseStats.counts.length;
  const coarseUseFallback = finalizeRepresentativeCentroids(meta.coarseStats);
  const detailUseFallback = finalizeRepresentativeCentroids(meta.detailStats);
  const coarseRep = selectRepresentativeRows(
    meta.positions,
    meta.radii,
    meta.coarseWeights,
    meta.groupIds,
    meta.coarseStats,
    coarseUseFallback,
    voxelDiagSq,
    -0.15,
  );
  const detailRep = selectRepresentativeRows(
    meta.positions,
    meta.radii,
    meta.detailWeights,
    meta.groupIds,
    meta.detailStats,
    detailUseFallback,
    voxelDiagSq,
    0.15,
  );
  const excluded = buildExcludedRepresentativeStats(
    meta.positions,
    meta.radii,
    meta.coarseWeights,
    meta.detailWeights,
    meta.groupIds,
    coarseRep,
    detailRep,
    groupCount,
  );
  const extraCoarseRep = selectRepresentativeRows(
    meta.positions,
    meta.radii,
    meta.coarseWeights,
    meta.groupIds,
    excluded.extraCoarseStats,
    excluded.extraCoarseFallback,
    voxelDiagSq,
    -0.15,
  );
  const extraDetailRep = selectRepresentativeRows(
    meta.positions,
    meta.radii,
    meta.detailWeights,
    meta.groupIds,
    excluded.extraDetailStats,
    excluded.extraDetailFallback,
    voxelDiagSq,
    0.15,
  );

  const selected = [];
  const selectedSlotByOrig = new Int32Array(meta.groupIds.length);
  selectedSlotByOrig.fill(-1);
  const taken = new Uint8Array(meta.groupIds.length);
  const secondaryCandidates = [];
  const tertiaryDetailCandidates = [];
  const tertiaryCoarseCandidates = [];
  let coarseSelectedCount = 0;
  let detailSelectedCount = 0;

  for (let groupIndex = 0; groupIndex < groupCount; groupIndex++) {
    const coarse = coarseRep[groupIndex];
    ensure(coarse >= 0, `Missing coarse representative for group ${groupIndex}.`);
    const coarseSlot = selected.length;
    selected.push(coarse);
    selectedSlotByOrig[coarse] = coarseSlot;
    taken[coarse] = 1;
    coarseSelectedCount += 1;

    const detail = detailRep[groupIndex];
    if (detail >= 0 && detail !== coarse) {
      const coarseBase3 = coarse * 3;
      const detailBase3 = detail * 3;
      const dx = meta.positions[detailBase3 + 0] - meta.positions[coarseBase3 + 0];
      const dy = meta.positions[detailBase3 + 1] - meta.positions[coarseBase3 + 1];
      const dz = meta.positions[detailBase3 + 2] - meta.positions[coarseBase3 + 2];
      const sepNorm = Math.sqrt(dx * dx + dy * dy + dz * dz) / voxelDiag;
      const radiusRatio =
        Math.max(meta.radii[coarse], 1e-6) / Math.max(meta.radii[detail], 1e-6);
      secondaryCandidates.push({
        rep: detail,
        priority:
          meta.detailWeights[detail] *
          (1.0 + sepNorm) *
          (1.0 + Math.max(0.0, Math.log2(Math.max(radiusRatio, 1.0)))),
      });
    }

    const extraCoarse = extraCoarseRep[groupIndex];
    if (extraCoarse >= 0) {
      tertiaryCoarseCandidates.push({
        rep: extraCoarse,
        priority: meta.coarseWeights[extraCoarse],
      });
    }

    const extraDetail = extraDetailRep[groupIndex];
    if (extraDetail >= 0) {
      tertiaryDetailCandidates.push({
        rep: extraDetail,
        priority: meta.detailWeights[extraDetail],
      });
    }
  }

  if (selected.length < target && secondaryCandidates.length > 0) {
    secondaryCandidates.sort((a, b) => {
      if (b.priority !== a.priority) {
        return b.priority - a.priority;
      }
      return a.rep - b.rep;
    });
    for (
      let i = 0;
      i < secondaryCandidates.length && selected.length < target;
      i++
    ) {
      const rep = secondaryCandidates[i].rep;
      if (taken[rep]) {
        continue;
      }
      selectedSlotByOrig[rep] = selected.length;
      selected.push(rep);
      taken[rep] = 1;
      detailSelectedCount += 1;
    }
  }

  if (selected.length < target) {
    tertiaryDetailCandidates.sort((a, b) => {
      if (b.priority !== a.priority) {
        return b.priority - a.priority;
      }
      return a.rep - b.rep;
    });
    tertiaryCoarseCandidates.sort((a, b) => {
      if (b.priority !== a.priority) {
        return b.priority - a.priority;
      }
      return a.rep - b.rep;
    });

    let detailCursor = 0;
    let coarseCursor = 0;
    while (
      selected.length < target &&
      (detailCursor < tertiaryDetailCandidates.length ||
        coarseCursor < tertiaryCoarseCandidates.length)
    ) {
      const preferDetail = detailSelectedCount <= coarseSelectedCount;
      let picked = false;

      if (preferDetail) {
        while (detailCursor < tertiaryDetailCandidates.length) {
          const rep = tertiaryDetailCandidates[detailCursor++].rep;
          if (taken[rep]) {
            continue;
          }
          selectedSlotByOrig[rep] = selected.length;
          selected.push(rep);
          taken[rep] = 1;
          detailSelectedCount += 1;
          picked = true;
          break;
        }
      } else {
        while (coarseCursor < tertiaryCoarseCandidates.length) {
          const rep = tertiaryCoarseCandidates[coarseCursor++].rep;
          if (taken[rep]) {
            continue;
          }
          selectedSlotByOrig[rep] = selected.length;
          selected.push(rep);
          taken[rep] = 1;
          coarseSelectedCount += 1;
          picked = true;
          break;
        }
      }

      if (picked) {
        continue;
      }

      while (detailCursor < tertiaryDetailCandidates.length) {
        const rep = tertiaryDetailCandidates[detailCursor++].rep;
        if (taken[rep]) {
          continue;
        }
        selectedSlotByOrig[rep] = selected.length;
        selected.push(rep);
        taken[rep] = 1;
        detailSelectedCount += 1;
        picked = true;
        break;
      }
      if (picked || selected.length >= target) {
        continue;
      }

      while (coarseCursor < tertiaryCoarseCandidates.length) {
        const rep = tertiaryCoarseCandidates[coarseCursor++].rep;
        if (taken[rep]) {
          continue;
        }
        selectedSlotByOrig[rep] = selected.length;
        selected.push(rep);
        taken[rep] = 1;
        coarseSelectedCount += 1;
        picked = true;
        break;
      }
      if (!picked) {
        break;
      }
    }
  }

  if (selected.length < target) {
    const remain = [];
    for (let i = 0; i < taken.length; i++) {
      if (!taken[i]) {
        remain.push(i);
      }
    }
    const remainDetail = remain.slice().sort((a, b) => {
      const w = meta.detailWeights[b] - meta.detailWeights[a];
      if (w !== 0) return w;
      const r = meta.radii[a] - meta.radii[b];
      return r !== 0 ? r : b - a;
    });
    const remainCoarse = remain.slice().sort((a, b) => {
      const w = meta.coarseWeights[b] - meta.coarseWeights[a];
      if (w !== 0) return w;
      const r = meta.radii[b] - meta.radii[a];
      return r !== 0 ? r : b - a;
    });

    let detailCursor = 0;
    let coarseCursor = 0;
    while (
      selected.length < target &&
      (detailCursor < remainDetail.length || coarseCursor < remainCoarse.length)
    ) {
      const preferDetail = detailSelectedCount <= coarseSelectedCount;
      let picked = false;

      if (preferDetail) {
        while (detailCursor < remainDetail.length) {
          const rep = remainDetail[detailCursor++];
          if (taken[rep]) {
            continue;
          }
          selectedSlotByOrig[rep] = selected.length;
          selected.push(rep);
          taken[rep] = 1;
          detailSelectedCount += 1;
          picked = true;
          break;
        }
      } else {
        while (coarseCursor < remainCoarse.length) {
          const rep = remainCoarse[coarseCursor++];
          if (taken[rep]) {
            continue;
          }
          selectedSlotByOrig[rep] = selected.length;
          selected.push(rep);
          taken[rep] = 1;
          coarseSelectedCount += 1;
          picked = true;
          break;
        }
      }

      if (picked) {
        continue;
      }

      while (detailCursor < remainDetail.length) {
        const rep = remainDetail[detailCursor++];
        if (taken[rep]) {
          continue;
        }
        selectedSlotByOrig[rep] = selected.length;
        selected.push(rep);
        taken[rep] = 1;
        detailSelectedCount += 1;
        picked = true;
        break;
      }
      if (picked || selected.length >= target) {
        continue;
      }

      while (coarseCursor < remainCoarse.length) {
        const rep = remainCoarse[coarseCursor++];
        if (taken[rep]) {
          continue;
        }
        selectedSlotByOrig[rep] = selected.length;
        selected.push(rep);
        taken[rep] = 1;
        coarseSelectedCount += 1;
        picked = true;
        break;
      }
      if (!picked) {
        break;
      }
    }
  }

  return {
    selected,
    selectedSlotByOrig,
  };
}

function assignExactStreamingSelection(meta, selected) {
  const groupCount = meta.coarseStats.counts.length;
  const keptRadius = new Float32Array(selected.length);
  const groupSlots = Array.from({ length: groupCount }, () => []);
  for (let slot = 0; slot < selected.length; slot++) {
    const rep = selected[slot];
    keptRadius[slot] = meta.radii[rep];
    groupSlots[meta.groupIds[rep]].push(slot);
  }

  const assignment = new Int32Array(meta.groupIds.length);
  for (let rowIndex = 0; rowIndex < meta.groupIds.length; rowIndex++) {
    const slots = groupSlots[meta.groupIds[rowIndex]];
    ensure(slots.length > 0, `Missing selected representatives for row ${rowIndex}.`);
    if (slots.length === 1) {
      assignment[rowIndex] = slots[0];
      continue;
    }
    const base3 = rowIndex * 3;
    let bestSlot = slots[0];
    let bestScore = Infinity;
    for (let i = 0; i < slots.length; i++) {
      const slot = slots[i];
      const rep = selected[slot];
      const repBase3 = rep * 3;
      const dx = meta.positions[base3 + 0] - meta.positions[repBase3 + 0];
      const dy = meta.positions[base3 + 1] - meta.positions[repBase3 + 1];
      const dz = meta.positions[base3 + 2] - meta.positions[repBase3 + 2];
      const score = Math.sqrt(dx * dx + dy * dy + dz * dz) + keptRadius[slot];
      if (score < bestScore) {
        bestScore = score;
        bestSlot = slot;
      }
    }
    assignment[rowIndex] = bestSlot;
  }

  return {
    assignment,
    keptRadius,
  };
}

async function gatherSelectedBucketRowsToCloud(
  entries,
  coeffCount,
  outCount,
  selectedSlotByOrig,
) {
  const coeffStride = coeffCount * 3;
  const positions = new Float32Array(outCount * 3);
  const scaleLog = new Float32Array(outCount * 3);
  const quats = new Float32Array(outCount * 4);
  const opacity = new Float32Array(outCount);
  const shCoeffs = new Float32Array(outCount * coeffStride);
  const scratch = makeRowScratch(coeffCount);
  let rowIndex = 0;

  await forEachBucketEntryRow(entries, coeffCount, (view, base, encoding) => {
    const slot = selectedSlotByOrig[rowIndex];
    if (slot >= 0) {
      readBucketRowIntoScratch(encoding, view, base, coeffCount, scratch);
      writeScratchRowToArrays(
        scratch,
        coeffStride,
        slot,
        positions,
        scaleLog,
        quats,
        opacity,
        shCoeffs,
      );
    }
    rowIndex += 1;
  });

  const cloud = new GaussianCloud(
    positions,
    scaleLog,
    quats,
    opacity,
    shCoeffs,
    null,
  );
  cloud._shDegree = shDegreeFromCoeffCount(coeffCount);
  return cloud;
}

async function loadBucketSimplifyCoreFromEntries(entries, coeffCount) {
  let totalRows = 0;
  for (const entry of entries) {
    totalRows += entry.rowCount || 0;
  }
  ensure(totalRows > 0, 'Cannot load an empty simplify input from bucket files.');

  const positions = new Float32Array(totalRows * 3);
  const scaleLog = new Float32Array(totalRows * 3);
  const quatsXYZW = new Float32Array(totalRows * 4);
  const opacity = new Float32Array(totalRows);
  const scratch = makeRowScratch(coeffCount);
  let rowIndex = 0;

  await forEachBucketEntryRow(entries, coeffCount, (view, base, encoding) => {
    readBucketCoreRowIntoScratch(encoding, view, base, scratch);
    const base3 = rowIndex * 3;
    const base4 = rowIndex * 4;
    positions[base3 + 0] = scratch.position[0];
    positions[base3 + 1] = scratch.position[1];
    positions[base3 + 2] = scratch.position[2];
    scaleLog[base3 + 0] = scratch.scaleLog[0];
    scaleLog[base3 + 1] = scratch.scaleLog[1];
    scaleLog[base3 + 2] = scratch.scaleLog[2];
    quatsXYZW[base4 + 0] = scratch.quat[0];
    quatsXYZW[base4 + 1] = scratch.quat[1];
    quatsXYZW[base4 + 2] = scratch.quat[2];
    quatsXYZW[base4 + 3] = scratch.quat[3];
    opacity[rowIndex] = scratch.opacity;
    rowIndex += 1;
  });

  return {
    positions,
    scaleLog,
    quatsXYZW,
    opacity,
    length: totalRows,
  };
}

async function mergeSelectedBucketRowsToCloud(
  entries,
  coeffCount,
  selectedSlotByOrig,
  assignment,
  radii,
  selectedCount,
  voxelDiag,
) {
  const coeffStride = coeffCount * 3;
  const positions = new Float32Array(selectedCount * 3);
  const scaleLog = new Float32Array(selectedCount * 3);
  const quats = new Float32Array(selectedCount * 4);
  const opacity = new Float32Array(selectedCount);
  const shCoeffs = new Float32Array(selectedCount * coeffStride);
  const selectedPositions = new Float32Array(selectedCount * 3);
  const selectedScaleLog = new Float32Array(selectedCount * 3);
  const selectedQuats = new Float32Array(selectedCount * 4);
  const selectedOpacity = new Float32Array(selectedCount);
  const selectedShCoeffs = new Float32Array(selectedCount * coeffStride);
  const selectedSeen = new Uint8Array(selectedCount);
  const fallbackPositions = new Float32Array(selectedCount * 3);
  const fallbackScaleLog = new Float32Array(selectedCount * 3);
  const fallbackQuats = new Float32Array(selectedCount * 4);
  const fallbackOpacity = new Float32Array(selectedCount);
  const fallbackShCoeffs = new Float32Array(selectedCount * coeffStride);
  const weightSums = new Float64Array(selectedCount);
  const counts = new Uint32Array(selectedCount);
  const firstAssigned = new Int32Array(selectedCount);
  firstAssigned.fill(-1);
  const weightedPos = new Float64Array(selectedCount * 3);
  const weightedOpacity = new Float64Array(selectedCount);
  const weightedSh = new Float64Array(selectedCount * coeffStride);
  const covSums = new Float64Array(selectedCount * 6);
  const covScratch = new Float64Array(6);
  const scratch = makeRowScratch(coeffCount);
  let rowIndex = 0;

  await forEachBucketEntryRow(entries, coeffCount, (view, base, encoding) => {
    readBucketRowIntoScratch(encoding, view, base, coeffCount, scratch);
    const selectedSlot = selectedSlotByOrig[rowIndex];
    if (selectedSlot >= 0 && !selectedSeen[selectedSlot]) {
      writeScratchRowToArrays(
        scratch,
        coeffStride,
        selectedSlot,
        selectedPositions,
        selectedScaleLog,
        selectedQuats,
        selectedOpacity,
        selectedShCoeffs,
      );
      selectedSeen[selectedSlot] = 1;
    }
    const slot = assignment[rowIndex];
    const weight = mergeAggregationWeight(
      scratch.opacity,
      radii[rowIndex],
      voxelDiag,
    );
    const base3 = slot * 3;
    const coeffBase = slot * coeffStride;
    if (firstAssigned[slot] < 0) {
      firstAssigned[slot] = rowIndex;
      writeScratchRowToArrays(
        scratch,
        coeffStride,
        slot,
        fallbackPositions,
        fallbackScaleLog,
        fallbackQuats,
        fallbackOpacity,
        fallbackShCoeffs,
      );
    }
    weightSums[slot] += weight;
    counts[slot] += 1;
    weightedPos[base3 + 0] += scratch.position[0] * weight;
    weightedPos[base3 + 1] += scratch.position[1] * weight;
    weightedPos[base3 + 2] += scratch.position[2] * weight;
    weightedOpacity[slot] += scratch.opacity * weight;
    for (let c = 0; c < coeffStride; c++) {
      weightedSh[coeffBase + c] += scratch.sh[c] * weight;
    }
    rowIndex += 1;
  });

  for (let slot = 0; slot < selectedCount; slot++) {
    if (
      !Number.isFinite(weightSums[slot]) ||
      weightSums[slot] <= 1e-12 ||
      counts[slot] === 0
    ) {
      if (firstAssigned[slot] >= 0) {
        copySlotArrays(
          slot,
          coeffStride,
          fallbackPositions,
          fallbackScaleLog,
          fallbackQuats,
          fallbackOpacity,
          fallbackShCoeffs,
          positions,
          scaleLog,
          quats,
          opacity,
          shCoeffs,
        );
      } else {
        copySlotArrays(
          slot,
          coeffStride,
          selectedPositions,
          selectedScaleLog,
          selectedQuats,
          selectedOpacity,
          selectedShCoeffs,
          positions,
          scaleLog,
          quats,
          opacity,
          shCoeffs,
        );
      }
      continue;
    }

    const invWeight = 1.0 / weightSums[slot];
    const base3 = slot * 3;
    const coeffBase = slot * coeffStride;
    positions[base3 + 0] = weightedPos[base3 + 0] * invWeight;
    positions[base3 + 1] = weightedPos[base3 + 1] * invWeight;
    positions[base3 + 2] = weightedPos[base3 + 2] * invWeight;
    opacity[slot] = Math.max(
      0.0,
      Math.min(1.0, weightedOpacity[slot] * invWeight),
    );
    for (let c = 0; c < coeffStride; c++) {
      shCoeffs[coeffBase + c] = weightedSh[coeffBase + c] * invWeight;
    }
  }

  rowIndex = 0;
  await forEachBucketEntryRow(entries, coeffCount, (view, base, encoding) => {
    const slot = assignment[rowIndex];
    if (
      !Number.isFinite(weightSums[slot]) ||
      weightSums[slot] <= 1e-12 ||
      counts[slot] <= 1
    ) {
      rowIndex += 1;
      return;
    }
    readBucketCoreRowIntoScratch(encoding, view, base, scratch);
    const weight = mergeAggregationWeight(
      scratch.opacity,
      radii[rowIndex],
      voxelDiag,
    );
    covarianceComponentsFromScratch(scratch.scaleLog, scratch.quat, covScratch);
    const base3 = slot * 3;
    const covBase = slot * 6;
    const dx = scratch.position[0] - positions[base3 + 0];
    const dy = scratch.position[1] - positions[base3 + 1];
    const dz = scratch.position[2] - positions[base3 + 2];
    covSums[covBase + 0] += weight * (covScratch[0] + dx * dx);
    covSums[covBase + 1] += weight * (covScratch[1] + dx * dy);
    covSums[covBase + 2] += weight * (covScratch[2] + dx * dz);
    covSums[covBase + 3] += weight * (covScratch[3] + dy * dy);
    covSums[covBase + 4] += weight * (covScratch[4] + dy * dz);
    covSums[covBase + 5] += weight * (covScratch[5] + dz * dz);
    rowIndex += 1;
  });

  for (let slot = 0; slot < selectedCount; slot++) {
    if (
      !Number.isFinite(weightSums[slot]) ||
      weightSums[slot] <= 1e-12 ||
      counts[slot] <= 1
    ) {
      if (firstAssigned[slot] >= 0) {
        copySlotArrays(
          slot,
          coeffStride,
          fallbackPositions,
          fallbackScaleLog,
          fallbackQuats,
          fallbackOpacity,
          fallbackShCoeffs,
          positions,
          scaleLog,
          quats,
          opacity,
          shCoeffs,
        );
      } else {
        copySlotArrays(
          slot,
          coeffStride,
          selectedPositions,
          selectedScaleLog,
          selectedQuats,
          selectedOpacity,
          selectedShCoeffs,
          positions,
          scaleLog,
          quats,
          opacity,
          shCoeffs,
        );
      }
      continue;
    }

    const invWeight = 1.0 / weightSums[slot];
    const covBase = slot * 6;
    covarianceToScaleQuat(
      Math.max(covSums[covBase + 0] * invWeight, 1e-20),
      covSums[covBase + 1] * invWeight,
      covSums[covBase + 2] * invWeight,
      Math.max(covSums[covBase + 3] * invWeight, 1e-20),
      covSums[covBase + 4] * invWeight,
      Math.max(covSums[covBase + 5] * invWeight, 1e-20),
      scaleLog,
      slot * 3,
      quats,
      slot * 4,
    );
  }

  const cloud = new GaussianCloud(
    positions,
    scaleLog,
    quats,
    opacity,
    shCoeffs,
    null,
  );
  cloud._shDegree = shDegreeFromCoeffCount(coeffCount);
  return cloud;
}

function computeExactStreamingOwnError(
  inputPositions,
  inputRadii,
  assignment,
  outputCloud,
  outputRadius,
) {
  const err = new Float64Array(assignment.length);
  for (let rowIndex = 0; rowIndex < assignment.length; rowIndex++) {
    const slot = assignment[rowIndex];
    const srcBase3 = rowIndex * 3;
    const dstBase3 = slot * 3;
    const dx =
      inputPositions[srcBase3 + 0] - outputCloud.positions[dstBase3 + 0];
    const dy =
      inputPositions[srcBase3 + 1] - outputCloud.positions[dstBase3 + 1];
    const dz =
      inputPositions[srcBase3 + 2] - outputCloud.positions[dstBase3 + 2];
    err[rowIndex] =
      Math.sqrt(dx * dx + dy * dy + dz * dz) +
      inputRadii[rowIndex] +
      outputRadius[slot];
  }
  return percent95(err);
}

async function streamSimplifyBucketFilesExact(
  fileSpecs,
  coeffCount,
  targetCount,
  bounds,
  sampleMode,
) {
  const { entries, totalRows } = await collectBucketEntries(fileSpecs, coeffCount);
  ensure(totalRows > 0, 'Cannot simplify an empty bucket input.');
  const target = normalizeSplatTargetCount(targetCount, totalRows);
  if (totalRows <= target) {
    return {
      cloud: await loadBucketCloudFromSpecs(entries, coeffCount),
      ownError: 0.0,
    };
  }

  const lightCloud = await loadBucketSimplifyCoreFromEntries(entries, coeffCount);
  const activeBounds = bounds || computeBounds(lightCloud);
  const plan = planSimplifyCloudVoxel(
    lightCloud,
    target,
    activeBounds,
    defaultVoxelTargetCount(target, totalRows),
  );
  const selectedSlotByOrig = new Int32Array(totalRows);
  selectedSlotByOrig.fill(-1);
  for (let slot = 0; slot < plan.selected.length; slot++) {
    selectedSlotByOrig[plan.selected[slot]] = slot;
  }

  const outputCloud =
    sampleMode === 'merge'
      ? await mergeSelectedBucketRowsToCloud(
          entries,
          coeffCount,
          selectedSlotByOrig,
          plan.assignment,
          plan.origRadius,
          plan.selected.length,
          plan.voxelDiag,
        )
      : await gatherSelectedBucketRowsToCloud(
          entries,
          coeffCount,
          plan.selected.length,
          selectedSlotByOrig,
        );
  const outputRadius =
    sampleMode === 'merge'
      ? computeThreeSigmaAabbDiagonalRadius(
          outputCloud.scaleLog,
          outputCloud.quatsXYZW,
        )
      : plan.keptRadius;

  return {
    cloud: outputCloud,
    ownError: computeExactStreamingOwnError(
      lightCloud.positions,
      plan.origRadius,
      plan.assignment,
      outputCloud,
      outputRadius,
    ),
  };
}

function resolveNodeContentTarget(node, ctx, inputSplatCount) {
  const targetRaw = targetSplatCountForDepth(
    node.depth,
    ctx.lodMaxDepth,
    ctx.params.samplingRatePerLevel,
    node.count,
  );
  return constrainTargetSplatCount(
    targetRaw,
    inputSplatCount,
    node.occupiedChildCount,
  );
}

async function writeContentFile(
  params,
  cloud,
  level,
  x,
  y,
  z,
  { transferOwnership = false } = {},
) {
  const relPath = contentRelPath(level, x, y, z);
  const outPath = path.join(params.outputDir, relPath);
  await fs.promises.mkdir(path.dirname(outPath), { recursive: true });
  const translation = computeBounds(cloud).center();

  if (
    params.contentWorkerPool &&
    transferOwnership &&
    cloud.length >= SPZ_ASYNC_WRITE_THRESHOLD
  ) {
    await params.contentWorkerPool.submit(
      {
        kind: 'pack-spz',
        outPath,
        sh1Bits: params.spzSh1Bits,
        shRestBits: params.spzShRestBits,
        colorSpace: params.colorSpace,
        translation,
        cloud: serializeCloudForWorkerTask(cloud),
      },
      transferListForCloud(cloud),
    );
    return relPath;
  }

  const spzBytes = packCloudToSpz(
    cloud,
    params.spzSh1Bits,
    params.spzShRestBits,
    translation,
  );
  const builder = new GltfBuilder();
  builder.writeSpzStreamGlb(
    outPath,
    spzBytes,
    cloud,
    params.colorSpace,
    translation,
  );
  return relPath;
}

async function scanGlobalBounds(handle, filePath, header, layout) {
  const minimum = [Infinity, Infinity, Infinity];
  const maximum = [-Infinity, -Infinity, -Infinity];
  let count = 0;
  await _forEachGaussianPlyPosition(
    handle,
    filePath,
    header,
    layout,
    (_rowIndex, x, y, z) => {
      if (x < minimum[0]) minimum[0] = x;
      if (y < minimum[1]) minimum[1] = y;
      if (z < minimum[2]) minimum[2] = z;
      if (x > maximum[0]) maximum[0] = x;
      if (y > maximum[1]) maximum[1] = y;
      if (z > maximum[2]) maximum[2] = z;
      count += 1;
    },
  );
  ensure(count > 0, `PLY file ${filePath} does not contain any vertices.`);
  return new Bounds(minimum, maximum);
}

async function buildCountsTable(handle, filePath, header, layout, rootBounds, maxDepth) {
  const counts = new Map();
  const increment = (level, x, y, z) => {
    const key = makeNodeKey(level, x, y, z);
    counts.set(key, (counts.get(key) || 0) + 1);
  };

  await _forEachGaussianPlyPosition(
    handle,
    filePath,
    header,
    layout,
    (_rowIndex, x, y, z) => {
      let minX = rootBounds.minimum[0];
      let minY = rootBounds.minimum[1];
      let minZ = rootBounds.minimum[2];
      let maxX = rootBounds.maximum[0];
      let maxY = rootBounds.maximum[1];
      let maxZ = rootBounds.maximum[2];
      let ix = 0;
      let iy = 0;
      let iz = 0;

      increment(0, 0, 0, 0);
      for (let depth = 0; depth < maxDepth; depth++) {
        const cx = (minX + maxX) * 0.5;
        const cy = (minY + maxY) * 0.5;
        const cz = (minZ + maxZ) * 0.5;
        const bx = x >= cx ? 1 : 0;
        const by = y >= cy ? 1 : 0;
        const bz = z >= cz ? 1 : 0;
        ix = (ix << 1) | bx;
        iy = (iy << 1) | by;
        iz = (iz << 1) | bz;
        increment(depth + 1, ix, iy, iz);

        if (bx) minX = cx;
        else maxX = cx;
        if (by) minY = cy;
        else maxY = cy;
        if (bz) minZ = cz;
        else maxZ = cz;
      }
    },
  );
  return counts;
}

function flattenFileSpecLists(fileSpecLists) {
  const out = [];
  for (const list of fileSpecLists) {
    if (!list || list.length === 0) {
      continue;
    }
    for (let i = 0; i < list.length; i++) {
      out.push(list[i]);
    }
  }
  return out;
}

async function isPartitionedNodeComplete(node, ctx) {
  if (node.buildState !== 'completed' || !node.contentUri) {
    return false;
  }
  const contentPath = path.join(ctx.params.outputDir, node.contentUri);
  if (!(await pathExists(contentPath))) {
    return false;
  }
  if (node.depth > 0 && !node.handoffConsumed) {
    if (!node.handoffPath) {
      return false;
    }
    return pathExists(node.handoffPath);
  }
  return true;
}

async function processPartitionedLeafNode(node, ctx) {
  ensure(!!node.bucketPath, `Missing leaf bucket path for node ${node.key}.`);
  const leafCloud = await loadBucketCloudFromSpecs(
    [leafBucketSpec(node)],
    ctx.layout.coeffCount,
  );
  if (node.depth > 0) {
    node.handoffPath = canonicalNodePath(ctx.tempDir, HANDOFF_BUCKET_DIR, node);
    await writeCanonicalCloudFile(node.handoffPath, leafCloud);
    node.handoffConsumed = false;
  } else {
    node.handoffPath = null;
    node.handoffConsumed = true;
  }

  node.ownError = 0.0;
  node.contentUri = await writeContentFile(
    ctx.params,
    leafCloud,
    node.level,
    node.x,
    node.y,
    node.z,
    {
      transferOwnership: true,
    },
  );
  node.buildState = 'completed';
  await enqueuePipelineStateSave(ctx, 'partitioned');
}

async function processPartitionedInternalNode(node, ctx) {
  const inputSpecs = node.children.map((child) => {
    ensure(
      !!child.handoffPath,
      `Missing active child handoff for node ${node.key} <- ${child.key}.`,
    );
    return handoffBucketSpec(child);
  });
  const { entries, totalRows } = await collectBucketEntries(
    inputSpecs,
    ctx.layout.coeffCount,
  );
  const contentTarget = resolveNodeContentTarget(
    node,
    ctx,
    totalRows,
  );
  const { cloud: contentCloud, ownError } = await streamSimplifyBucketFilesExact(
    entries,
    ctx.layout.coeffCount,
    contentTarget,
    node.bounds,
    ctx.params.sampleMode,
  );
  if (node.depth > 0) {
    node.handoffPath = canonicalNodePath(ctx.tempDir, HANDOFF_BUCKET_DIR, node);
    await writeCanonicalCloudFile(node.handoffPath, contentCloud);
    node.handoffConsumed = false;
  } else {
    node.handoffPath = null;
    node.handoffConsumed = true;
  }

  node.ownError =
    Number.isFinite(ownError) && ownError > 0.0 ? ownError : 0.0;
  node.contentUri = await writeContentFile(
    ctx.params,
    contentCloud,
    node.level,
    node.x,
    node.y,
    node.z,
    {
      transferOwnership: true,
    },
  );
  node.buildState = 'completed';
  for (const child of node.children) {
    child.handoffConsumed = true;
  }
  await enqueuePipelineStateSave(ctx, 'partitioned');

  for (const child of node.children) {
    await removeFileIfExists(child.handoffPath);
  }
}

async function runWithConcurrency(items, limit, onItem) {
  if (!items || items.length === 0) {
    return;
  }
  const concurrency = Math.max(1, Math.min(items.length, Math.floor(limit || 1)));
  let cursor = 0;
  const workers = Array.from({ length: concurrency }, async () => {
    while (true) {
      const index = cursor++;
      if (index >= items.length) {
        return;
      }
      await onItem(items[index], index);
    }
  });
  await Promise.all(workers);
}

function tickBuildProgress(ctx, node, status) {
  if (!ctx || !ctx.buildProgress || !node) {
    return;
  }
  const kind = node.leaf ? 'leaf' : 'internal';
  const prefix = ctx.params && ctx.params.plyBuildMode
    ? `${ctx.params.plyBuildMode}`
    : 'build';
  const suffix = status ? ` ${status}` : '';
  ctx.buildProgress.tick(`${prefix} level=${node.level} ${kind}${suffix}`);
}

async function buildPartitionedBottomUp(rootNode, ctx) {
  const treeStats = collectTreeStats(rootNode);
  for (let level = treeStats.maxLevel; level >= 0; level--) {
    const levelNodes = treeStats.levels[level] || [];
    await runWithConcurrency(levelNodes, ctx.nodeConcurrency, async (node) => {
      if (await isPartitionedNodeComplete(node, ctx)) {
        tickBuildProgress(ctx, node, 'checkpoint');
        return;
      }
      if (node.leaf) {
        await processPartitionedLeafNode(node, ctx);
      } else {
        await processPartitionedInternalNode(node, ctx);
      }
      tickBuildProgress(ctx, node);
    });
  }
  await enqueuePipelineStateSave(ctx, 'built');
}

async function processNodeBottomUpEntire(node, ctx) {
  if (node.leaf) {
    ensure(!!node.bucketPath, `Missing leaf bucket path for node ${node.key}.`);
    const cloud = await loadBucketCloudFromSpecs(
      [leafBucketSpec(node)],
      ctx.layout.coeffCount,
    );
    node.contentUri = await writeContentFile(
      ctx.params,
      cloud,
      node.level,
      node.x,
      node.y,
      node.z,
      { transferOwnership: true },
    );
    node.ownError = 0.0;
    node.buildState = 'completed';
    tickBuildProgress(ctx, node);
    return [leafBucketSpec(node)];
  }

  const childLeafSpecs = [];
  for (const child of node.children) {
    childLeafSpecs.push(await processNodeBottomUpEntire(child, ctx));
  }

  const inputSpecs = flattenFileSpecLists(childLeafSpecs);
  const { totalRows } = await collectBucketEntries(
    inputSpecs,
    ctx.layout.coeffCount,
  );
  const targetCount = resolveNodeContentTarget(node, ctx, totalRows);
  const { cloud: lodCloud, ownError } = await streamSimplifyBucketFilesExact(
    inputSpecs,
    ctx.layout.coeffCount,
    targetCount,
    node.bounds,
    ctx.params.sampleMode,
  );
  node.ownError = ownError;
  node.contentUri = await writeContentFile(
    ctx.params,
    lodCloud,
    node.level,
    node.x,
    node.y,
    node.z,
    { transferOwnership: true },
  );
  node.buildState = 'completed';
  tickBuildProgress(ctx, node);
  return inputSpecs;
}

function resolveRootGeometricError(rootNode, rootBounds, params, lodMaxDepth) {
  if (params.minGeometricError != null && params.minGeometricError > 0.0) {
    return {
      value: rootGeometricErrorFromMinLevel(
        params.minGeometricError,
        lodMaxDepth,
        params.samplingRatePerLevel,
      ),
      source: 'configured_min_geometric_error',
    };
  }

  if (Number.isFinite(rootNode.ownError) && rootNode.ownError > 0.0) {
    const ex = rootBounds.extents();
    const diag = Math.sqrt(ex[0] * ex[0] + ex[1] * ex[1] + ex[2] * ex[2]);
    return {
      value: Math.max(rootNode.ownError, diag * 1e-6, 1e-6),
      source:
        params.plyBuildMode === 'entire'
          ? 'estimated_root_entire_simplify'
          : 'estimated_root_partitioned_simplify',
    };
  }

  return {
    value: fallbackRootGeometricError(rootBounds, rootNode.count),
    source:
      params.plyBuildMode === 'entire'
        ? 'estimated_root_entire_fallback'
        : 'estimated_root_partitioned_fallback',
  };
}

function buildTileNodeTree(
  node,
  rootGeometricError,
  lodMaxDepth,
  samplingRatePerLevel,
  nodesByKey,
) {
  const error =
    rootGeometricError *
    geometricErrorScaleForDepth(node.depth, lodMaxDepth, samplingRatePerLevel);
  const children = node.children.map((child) =>
    buildTileNodeTree(
      child,
      rootGeometricError,
      lodMaxDepth,
      samplingRatePerLevel,
      nodesByKey,
    ),
  );
  const tileNode = new TileNode(
    node.level,
    node.x,
    node.y,
    node.z,
    node.bounds,
    error,
    node.contentUri,
    children,
  );
  nodesByKey.set(tileNode.key(), tileNode);
  return tileNode;
}

function tileToJson(node) {
  const obj = {
    boundingVolume: {
      box: applyContentBoxTransform(node.bounds.toBoxArray()),
    },
    geometricError: node.error,
    refine: 'REPLACE',
    content: { uri: node.contentUri },
  };
  if (node.children.length > 0) {
    obj.children = node.children.map((child) => tileToJson(child));
  }
  return obj;
}

async function writeAllSubtrees(
  nodesByKey,
  subtreesDir,
  availableLevels,
  subtreeLevels,
) {
  const nodes = Array.from(nodesByKey.values())
    .filter((node) => node.level % subtreeLevels === 0)
    .sort((a, b) => {
      if (a.level !== b.level) return a.level - b.level;
      if (a.x !== b.x) return a.x - b.x;
      if (a.y !== b.y) return a.y - b.y;
      return a.z - b.z;
    });

  for (const node of nodes) {
    const subtreePath = path.join(
      subtreesDir,
      String(node.level),
      String(node.x),
      String(node.y),
      `${node.z}.subtree`,
    );
    const { subtree, blob } = buildSubtreeArtifact(
      node.level,
      node.x,
      node.y,
      node.z,
      availableLevels,
      subtreeLevels,
      (level, x, y, z) => nodesByKey.has(makeNodeKey(level, x, y, z)),
    );
    writeSubtreeFile(subtreePath, subtree, blob);
  }
}

function makeBuildSummary(
  args,
  header,
  layout,
  rootNode,
  rootGeometricError,
  rootGeometricErrorSource,
  nodeCount,
  maxLevel,
  availableLevels,
  subtreeLevels,
  implicitRootGeometricError,
  checkpointInfo,
) {
  const samplingDivisorsByDepth = {};
  const samplingRatesByDepth = {};
  const geometricErrorScaleByDepth = {};
  const geometricErrorByDepth = {};
  const effectiveMaxDepth = maxLevel;

  for (let depth = 0; depth <= effectiveMaxDepth; depth++) {
    const geometricScale = geometricErrorScaleForDepth(
      depth,
      effectiveMaxDepth,
      args.samplingRatePerLevel,
    );
    samplingDivisorsByDepth[String(depth)] = samplingDivisorForDepth(
      depth,
      effectiveMaxDepth,
      args.samplingRatePerLevel,
    );
    samplingRatesByDepth[String(depth)] =
      args.samplingRatePerLevel ** Math.max(0, effectiveMaxDepth - depth);
    geometricErrorScaleByDepth[String(depth)] = geometricScale;
    geometricErrorByDepth[String(depth)] = rootGeometricError * geometricScale;
  }

  return {
    input_splats: header.vertexCount,
    sh_degree: layout.degree,
    ply_build_mode: args.plyBuildMode,
    partitioned_handoff_encoding: HANDOFF_BUCKET_ENCODING,
    checkpoint_reused: !!checkpointInfo.reused,
    checkpoint_reused_stage: checkpointInfo.stage,
    max_depth: args.maxDepth,
    leaf_limit: args.leafLimit,
    color_space: args.colorSpace,
    build_concurrency: args.buildConcurrency,
    content_workers: args.contentWorkers,
    sampling_rate_per_level: args.samplingRatePerLevel,
    tiling_mode: args.tilingMode,
    subtree_levels: args.tilingMode === 'implicit' ? subtreeLevels : null,
    content_codec: 'spz_stream',
    spz_version: SPZ_STREAM_VERSION,
    spz_sh1_bits: args.spzSh1Bits,
    spz_sh_rest_bits: args.spzShRestBits,
    root_transform: args.transform ? args.transform.slice() : null,
    root_coordinate: args.coordinate ? args.coordinate.slice() : null,
    root_transform_source: args.coordinate
      ? 'coordinate'
      : args.transform
        ? 'transform'
        : null,
    sample_mode: args.sampleMode,
    configured_min_geometric_error:
      args.minGeometricError != null && args.minGeometricError > 0.0
        ? args.minGeometricError
        : null,
    node_count: nodeCount,
    available_levels: availableLevels,
    effective_max_depth: effectiveMaxDepth,
    root_geometric_error_source: rootGeometricErrorSource,
    implicit_root_geometric_error:
      args.tilingMode === 'implicit' ? implicitRootGeometricError : null,
    root_geometric_error: rootNode.error,
    min_geometric_error:
      rootGeometricError *
      geometricErrorScaleForDepth(
        effectiveMaxDepth,
        effectiveMaxDepth,
        args.samplingRatePerLevel,
      ),
    geometric_error_scale_by_depth: geometricErrorScaleByDepth,
    geometric_error_by_depth: geometricErrorByDepth,
    sampling_rates_by_depth: samplingRatesByDepth,
    sampling_divisors_by_depth: samplingDivisorsByDepth,
    source: SOURCE_REPOSITORY,
  };
}

async function ensureReusablePartitionState(rootNode) {
  const { leaves } = collectTreeStats(rootNode);
  for (const leaf of leaves) {
    if (!leaf.bucketPath || !(await pathExists(leaf.bucketPath))) {
      return false;
    }
  }
  return true;
}

async function convertPartitionedPlyTo3DTiles(inputPath, outputDir, args) {
  const outputDirAbs = path.resolve(outputDir);
  const tempDir = path.join(outputDirAbs, TEMP_WORKSPACE_NAME);
  const params = {
    outputDir: outputDirAbs,
    colorSpace: args.colorSpace,
    samplingRatePerLevel: args.samplingRatePerLevel,
    sampleMode: args.sampleMode,
    plyBuildMode: args.plyBuildMode,
    spzSh1Bits: args.spzSh1Bits,
    spzShRestBits: args.spzShRestBits,
    minGeometricError: args.minGeometricError,
    contentWorkerPool:
      args.contentWorkers > 0
        ? new SpzContentWorkerPool(args.contentWorkers, DEFAULT_WORKER_SCRIPT)
        : null,
  };

  if (fs.existsSync(outputDirAbs) && args.clean) {
    fs.rmSync(outputDirAbs, { recursive: true, force: true });
  }
  fs.mkdirSync(outputDirAbs, { recursive: true });
  if (fs.existsSync(path.join(outputDirAbs, 'viewer.html'))) {
    fs.unlinkSync(path.join(outputDirAbs, 'viewer.html'));
  }

  const inputStat = await fs.promises.stat(inputPath);
  const fingerprint = makePipelineFingerprint(inputPath, inputStat, args);
  let checkpointInfo = { reused: false, stage: null };
  let pipelineState = null;

  if (!args.clean) {
    pipelineState = await readPipelineState(tempDir);
    if (
      pipelineState &&
      pipelineState.version === PIPELINE_STATE_VERSION &&
      fingerprintsMatch(pipelineState.fingerprint, fingerprint)
    ) {
      checkpointInfo = {
        reused: true,
        stage: pipelineState.stage || 'init',
      };
      console.log(
        `[info] reusing partitioned checkpoint | stage=${checkpointInfo.stage}`,
      );
    } else {
      pipelineState = null;
    }
  }

  if (!pipelineState) {
    await fs.promises.rm(tempDir, { recursive: true, force: true });
    await fs.promises.mkdir(tempDir, { recursive: true });
    pipelineState = makeEmptyPipelineState(fingerprint);
  } else {
    await fs.promises.mkdir(tempDir, { recursive: true });
  }

  const handle = await fs.promises.open(inputPath, 'r');
  let success = false;
  try {
    console.log(`[info] scanning PLY header: ${inputPath}`);
    const header = await _readPlyHeaderFromHandle(handle, inputPath);
    const layout = _buildGaussianPlyLayout(
      header.vertexProps,
      inputPath,
      args.inputConvention,
      args.linearScaleInput,
    );

    let rootBounds = null;
    let rootNodeMeta = null;
    if (
      pipelineState.rootBounds &&
      pipelineState.rootNode &&
      pipelineState.layout &&
      pipelineState.layout.coeffCount === layout.coeffCount &&
      pipelineState.layout.degree === layout.degree
    ) {
      rootBounds = deserializeBoundsState(pipelineState.rootBounds);
      rootNodeMeta = deserializeNodeMeta(pipelineState.rootNode);
    }

    if (!rootBounds || !rootNodeMeta) {
      console.log(
        `[info] scan 1/3 | vertices=${header.vertexCount} | sh_degree=${layout.degree} | ply_build_mode=${args.plyBuildMode}`,
      );
      rootBounds = await scanGlobalBounds(handle, inputPath, header, layout);

      console.log('[info] scan 2/3 | building count tree');
      const counts = await buildCountsTable(
        handle,
        inputPath,
        header,
        layout,
        rootBounds,
        args.maxDepth,
      );

      rootNodeMeta = buildNodeTreeFromCounts(
        counts,
        rootBounds,
        args.maxDepth,
        args.leafLimit,
        0,
        0,
        0,
        0,
      );
      ensure(!!rootNodeMeta, 'Failed to build root node from PLY counts.');
      pipelineState.rootBounds = serializeBoundsState(rootBounds);
      pipelineState.rootNode = serializeNodeMeta(rootNodeMeta);
      pipelineState.layout = {
        degree: layout.degree,
        coeffCount: layout.coeffCount,
      };
      pipelineState.stage = 'scanned';
      pipelineState.updatedAt = new Date().toISOString();
      await fs.promises.writeFile(
        path.join(tempDir, PIPELINE_STATE_FILE),
        JSON.stringify(pipelineState),
        'utf8',
      );
    }

    const treeStats = collectTreeStats(rootNodeMeta);
    const lodMaxDepth = Math.max(0, Math.min(args.maxDepth, treeStats.maxLevel));

    const canReuseLeafBuckets =
      pipelineState.stage !== 'init' &&
      (await ensureReusablePartitionState(rootNodeMeta));
    if (!canReuseLeafBuckets) {
      resetNodeArtifacts(rootNodeMeta);
      await fs.promises.rm(path.join(tempDir, LEAF_BUCKET_DIR), {
        recursive: true,
        force: true,
      });
      await fs.promises.rm(path.join(tempDir, HANDOFF_BUCKET_DIR), {
        recursive: true,
        force: true,
      });
      console.log(
        `[info] scan 3/3 | partitioning ${treeStats.leaves.length} leaf buckets`,
      );
      await partitionLeafBuckets(
        handle,
        inputPath,
        header,
        layout,
        rootNodeMeta,
        tempDir,
      );
      pipelineState.rootNode = serializeNodeMeta(rootNodeMeta);
      pipelineState.stage = 'partitioned';
      pipelineState.updatedAt = new Date().toISOString();
      await fs.promises.writeFile(
        path.join(tempDir, PIPELINE_STATE_FILE),
        JSON.stringify(pipelineState),
        'utf8',
      );
    } else if (checkpointInfo.reused) {
      console.log('[info] scan 3/3 | reusing existing leaf buckets');
    }

    await handle.close();

    const stateCtx = {
      tempDir,
      pipelineState,
      rootBounds,
      rootNode: rootNodeMeta,
      layout,
      savePromise: Promise.resolve(),
    };

    const ctx = {
      layout,
      lodMaxDepth,
      params,
      tempDir,
      nodeConcurrency:
        args.plyBuildMode === 'partitioned'
          ? Math.max(1, args.buildConcurrency || 1)
          : 1,
      pipelineState,
      rootBounds,
      rootNode: rootNodeMeta,
      savePromise: Promise.resolve(),
      buildProgress: new ConsoleProgressBar('build', treeStats.nodes.length),
    };

    enqueuePipelineStateSave(stateCtx, pipelineState.stage);
    ctx.savePromise = stateCtx.savePromise;
    ctx.pipelineState = stateCtx.pipelineState;

    console.log('[info] building tiles bottom-up');
    if (args.plyBuildMode === 'partitioned') {
      await buildPartitionedBottomUp(rootNodeMeta, ctx);
    } else {
      await processNodeBottomUpEntire(rootNodeMeta, ctx);
    }
    ctx.buildProgress.done(
      `${args.plyBuildMode} nodes=${treeStats.nodes.length} levels=${treeStats.maxLevel + 1}`,
    );
    await ctx.savePromise;

    const { value: rootGeometricError, source: rootGeometricErrorSource } =
      resolveRootGeometricError(rootNodeMeta, rootBounds, params, lodMaxDepth);
    const nodesByKey = new Map();
    const rootTileNode = buildTileNodeTree(
      rootNodeMeta,
      rootGeometricError,
      lodMaxDepth,
      args.samplingRatePerLevel,
      nodesByKey,
    );

    let tileset;
    const availableLevels = treeStats.maxLevel + 1;
    const effectiveSubtreeLevels = Math.max(
      1,
      Math.min(args.subtreeLevels, Math.max(1, availableLevels)),
    );
    if (args.tilingMode === 'explicit') {
      tileset = applyTilesetGltfContentExtensions({
        asset: makeTilesetAsset(),
        geometricError: rootTileNode.error,
        root: applyRootTransform(tileToJson(rootTileNode), args.transform),
      });
    } else if (args.tilingMode === 'implicit') {
      const subtreesDir = path.join(outputDirAbs, 'subtrees');
      await writeAllSubtrees(
        nodesByKey,
        subtreesDir,
        availableLevels,
        effectiveSubtreeLevels,
      );
      tileset = applyTilesetGltfContentExtensions({
        asset: makeTilesetAsset(),
        geometricError: rootGeometricError,
        root: applyRootTransform(
          {
            boundingVolume: {
              box: applyContentBoxTransform(rootBounds.toBoxArray()),
            },
            refine: 'REPLACE',
            geometricError: rootGeometricError,
            content: { uri: 'tiles/{level}/{x}/{y}/{z}.glb' },
            implicitTiling: {
              subdivisionScheme: 'OCTREE',
              availableLevels,
              subtreeLevels: effectiveSubtreeLevels,
              subtrees: { uri: 'subtrees/{level}/{x}/{y}/{z}.subtree' },
            },
          },
          args.transform,
        ),
      });
    } else {
      throw new ConversionError(`Unknown tiling mode: ${args.tilingMode}`);
    }

    await fs.promises.writeFile(
      path.join(outputDirAbs, 'tileset.json'),
      JSON.stringify(tileset),
      'utf8',
    );
    await fs.promises.writeFile(
      path.join(outputDirAbs, 'build_summary.json'),
      JSON.stringify(
        makeBuildSummary(
          args,
          header,
          layout,
          rootTileNode,
          rootGeometricError,
          rootGeometricErrorSource,
          nodesByKey.size,
          treeStats.maxLevel,
          availableLevels,
          effectiveSubtreeLevels,
          args.tilingMode === 'implicit' ? rootGeometricError : null,
          checkpointInfo,
        ),
      ),
      'utf8',
    );

    console.log(
      `[info] nodes=${nodesByKey.size} | levels=${availableLevels} | splats=${header.vertexCount}`,
    );
    success = true;
    return {
      splatCount: header.vertexCount,
      shDegree: layout.degree,
      nodeCount: nodesByKey.size,
      levels: availableLevels,
    };
  } finally {
    try {
      await handle.close();
    } catch {
      // ignore close races
    }
    if (params.contentWorkerPool) {
      await params.contentWorkerPool.close();
    }
    if (success) {
      try {
        await fs.promises.rm(tempDir, { recursive: true, force: true });
      } catch {
        // Best-effort cleanup only.
      }
    } else {
      console.error(`[info] checkpoint preserved: ${tempDir}`);
    }
  }
}

module.exports = {
  convertPartitionedPlyTo3DTiles,
};
