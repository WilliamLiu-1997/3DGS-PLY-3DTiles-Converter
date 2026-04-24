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
  gzipSpzPacket,
  makeSpzPacketLayout,
  makeSpzShQuantBuckets,
  packQuaternionSmallestThreeInto,
  packCloudToSpz,
  quantizeSpzColor,
  quantizeSpzExtraSh,
  quantizeSpzOpacity,
  quantizeSpzPosition,
  quantizeSpzScale,
  serializeCloudForWorkerTask,
  transferListForCloud,
  validateSpzCoeffCount,
  validateSpzQuantBits,
  writeFixed24Into,
  writeSpzPacketHeader,
} = require('./codec');
const { GltfBuilder } = require('./gltf');
const {
  SOURCE_REPOSITORY,
  ConsoleProgressBar,
  SpzContentWorkerPool,
  computeBounds,
  computeThreeSigmaAabbDiagonalRadiusAt,
  computeThreeSigmaAabbDiagonalRadius,
  childBounds,
  normalizeSplatTargetCount,
  constrainTargetSplatCount,
  planSimplifyCloudVoxel,
  samplingDivisorForDepth,
  geometricErrorScaleForDepth,
  rootGeometricErrorFromMinLevel,
  writeThreeSigmaExtentComponents,
  buildSubtreeArtifact,
  writeSubtreeFile,
} = require('./builder');

const DEFAULT_WORKER_SCRIPT = path.join(__dirname, 'convert-core.js');
const TEMP_WORKSPACE_NAME = '.tmp-ply-partitions';
const PIPELINE_STATE_FILE = 'pipeline-state.json';
const PIPELINE_STATE_VERSION = 5;
const PIPELINE_STAGE_BUCKETED = 'bucketed';
const LEAF_BUCKET_DIR = 'leaf';
const HANDOFF_BUCKET_DIR = 'handoff';
const LEAF_BUCKET_ENCODING = 'canonical32';
const HANDOFF_BUCKET_ENCODING = 'canonical32';
const PARTITION_ARENA_BYTES = 4 * 1024 * 1024;
const POSITION_TMP_FILE = 'positions.tmp';
const POSITION_ROW_BYTE_SIZE = 12;
const POSITION_TMP_BUFFER_BYTES = 256 * 1024;
const WRITEV_BATCH_CHUNKS = 1024;
const SPZ_CLOUD_ASYNC_WRITE_THRESHOLD = 65536;
const SPZ_BUCKET_ASYNC_WRITE_THRESHOLD = 32768;
const MERGE_SH_COEFF_BLOCK = 12;
const PIPELINE_STATE_SAVE_INTERVAL_MS = 1000;
const PIPELINE_STATE_SAVE_NODE_INTERVAL = 64;
const SHALLOW_COUNT_MAX_LEVEL = 6;
const UINT32_MAX_COUNT = 0xffffffff;
const COUNT_KEY_MAX_PACKED_LEVEL = 16;
const COUNT_KEY_LEVEL_FACTOR = 281474976710656; // 2 ** 48
const COUNT_KEY_X_FACTOR = 4294967296; // 2 ** 32
const COUNT_KEY_Y_FACTOR = 65536; // 2 ** 16
const IS_LITTLE_ENDIAN = (() => {
  const probe = new Uint8Array(new Uint16Array([0x0102]).buffer);
  return probe[0] === 0x02;
})();
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

function makeCountKey(level, x, y, z) {
  if (level <= COUNT_KEY_MAX_PACKED_LEVEL) {
    return (
      level * COUNT_KEY_LEVEL_FACTOR +
      x * COUNT_KEY_X_FACTOR +
      y * COUNT_KEY_Y_FACTOR +
      z
    );
  }
  return makeNodeKey(level, x, y, z);
}

function packShallowCountIndex(level, x, y, z) {
  if (level === 0) {
    return 0;
  }
  return (x << (2 * level)) | (y << level) | z;
}

class CountsTable {
  constructor(maxDepth, totalRows) {
    this.shallowMaxLevel =
      totalRows <= UINT32_MAX_COUNT
        ? Math.min(maxDepth, SHALLOW_COUNT_MAX_LEVEL)
        : -1;
    this.shallow = [];
    for (let level = 0; level <= this.shallowMaxLevel; level++) {
      this.shallow[level] = new Uint32Array(1 << (3 * level));
    }
    this.deep = new Map();
  }

  increment(level, x, y, z) {
    if (level <= this.shallowMaxLevel) {
      this.shallow[level][packShallowCountIndex(level, x, y, z)] += 1;
      return;
    }
    const key = makeCountKey(level, x, y, z);
    this.deep.set(key, (this.deep.get(key) || 0) + 1);
  }

  getCount(level, x, y, z) {
    if (level <= this.shallowMaxLevel) {
      return this.shallow[level][packShallowCountIndex(level, x, y, z)] || 0;
    }
    return this.deep.get(makeCountKey(level, x, y, z)) || 0;
  }
}

function pointOctant(bounds, x, y, z) {
  const min = bounds.minimum;
  const max = bounds.maximum;
  return (
    (x >= (min[0] + max[0]) * 0.5 ? 1 : 0) |
    (y >= (min[1] + max[1]) * 0.5 ? 2 : 0) |
    (z >= (min[2] + max[2]) * 0.5 ? 4 : 0)
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

function cloneBounds(bounds) {
  if (!bounds) {
    return null;
  }
  return new Bounds(bounds.minimum.slice(), bounds.maximum.slice());
}

function buildNodeTreeFromCounts(
  counts,
  bounds,
  maxDepth,
  leafLimit,
  depth,
  x,
  y,
  z,
) {
  const key = makeNodeKey(depth, x, y, z);
  const count = counts.getCount(depth, x, y, z);
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
    bucketRowCount: null,
    handoffRowCount: null,
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
    bucketRowCount: node.bucketRowCount,
    handoffRowCount: node.handoffRowCount,
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
    bucketRowCount:
      Number.isFinite(data.bucketRowCount) && data.bucketRowCount >= 0
        ? data.bucketRowCount
        : null,
    handoffRowCount:
      Number.isFinite(data.handoffRowCount) && data.handoffRowCount >= 0
        ? data.handoffRowCount
        : null,
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
      const oct =
        ((child.x & 1) << 0) | ((child.y & 1) << 1) | ((child.z & 1) << 2);
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
  node.handoffRowCount = null;
  node.handoffConsumed = false;
  node.ownError = null;
  node.buildState = 'pending';
  if (node.leaf) {
    node.bucketPath = null;
    node.bucketRowCount = null;
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
  const state = JSON.parse(text);
  if (state && state.stage === 'partitioned') {
    state.stage = PIPELINE_STAGE_BUCKETED;
  }
  return state;
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

async function materializeLinkedHandoffFile(sourcePath, targetPath) {
  ensure(
    !!sourcePath,
    'Missing source bucket path for handoff materialization.',
  );
  ensure(!!targetPath, 'Missing handoff target path.');
  if (sourcePath === targetPath) {
    return;
  }

  await fs.promises.mkdir(path.dirname(targetPath), { recursive: true });
  await removeFileIfExists(targetPath);

  try {
    await fs.promises.link(sourcePath, targetPath);
  } catch (err) {
    if (
      err &&
      (err.code === 'EXDEV' ||
        err.code === 'EPERM' ||
        err.code === 'EACCES' ||
        err.code === 'EMLINK' ||
        err.code === 'ENOTSUP' ||
        err.code === 'EINVAL')
    ) {
      await fs.promises.copyFile(sourcePath, targetPath);
      return;
    }
    throw err;
  }
}

function enqueuePipelineStateSave(ctx, stage = null, { force = false } = {}) {
  if (stage) {
    ctx.pipelineState.stage = stage;
  }
  if (ctx.lastPipelineStateSaveAt == null) {
    ctx.lastPipelineStateSaveAt = 0;
  }
  if (ctx.nodesSincePipelineStateSave == null) {
    ctx.nodesSincePipelineStateSave = 0;
  }

  const now = Date.now();
  if (!force) {
    ctx.nodesSincePipelineStateSave += 1;
    const dueByTime =
      ctx.lastPipelineStateSaveAt <= 0 ||
      now - ctx.lastPipelineStateSaveAt >= PIPELINE_STATE_SAVE_INTERVAL_MS;
    const dueByCount =
      ctx.nodesSincePipelineStateSave >= PIPELINE_STATE_SAVE_NODE_INTERVAL;
    if (!dueByTime && !dueByCount) {
      return ctx.savePromise;
    }
  }

  ctx.lastPipelineStateSaveAt = now;
  ctx.nodesSincePipelineStateSave = 0;

  const targetPath = path.join(ctx.tempDir, PIPELINE_STATE_FILE);
  ctx.savePromise = ctx.savePromise.then(() => {
    ctx.pipelineState.rootBounds = serializeBoundsState(ctx.rootBounds);
    ctx.pipelineState.rootNode = serializeNodeMeta(ctx.rootNode);
    ctx.pipelineState.layout = {
      degree: ctx.layout.degree,
      coeffCount: ctx.layout.coeffCount,
    };
    ctx.pipelineState.updatedAt = new Date().toISOString();
    return fs.promises.writeFile(
      targetPath,
      JSON.stringify(ctx.pipelineState),
      'utf8',
    );
  });
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

function makeBucketFileSpec(filePath, encoding, rowCount) {
  return { filePath, encoding, rowCount };
}

function leafBucketSpec(node) {
  return makeBucketFileSpec(
    node.bucketPath,
    LEAF_BUCKET_ENCODING,
    node.bucketRowCount,
  );
}

function handoffBucketSpec(node) {
  return makeBucketFileSpec(
    node.handoffPath,
    HANDOFF_BUCKET_ENCODING,
    node.handoffRowCount,
  );
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

function readBucketRowIntoScratch(
  encoding,
  view,
  base,
  coeffCount,
  scratch,
  floatView = null,
  floatBase = 0,
) {
  if (
    encoding !== LEAF_BUCKET_ENCODING &&
    encoding !== HANDOFF_BUCKET_ENCODING
  ) {
    throw new ConversionError(`Unknown bucket encoding: ${encoding}`);
  }
  const shLen = coeffCount * 3;
  if (floatView) {
    const src = floatView;
    const off = floatBase;
    scratch.position[0] = src[off + 0];
    scratch.position[1] = src[off + 1];
    scratch.position[2] = src[off + 2];
    scratch.scaleLog[0] = src[off + 3];
    scratch.scaleLog[1] = src[off + 4];
    scratch.scaleLog[2] = src[off + 5];
    scratch.quat[0] = src[off + 6];
    scratch.quat[1] = src[off + 7];
    scratch.quat[2] = src[off + 8];
    scratch.quat[3] = src[off + 9];
    scratch.opacity = src[off + 10];
    if (shLen > 0) {
      scratch.sh.set(src.subarray(off + 11, off + 11 + shLen));
    }
    return;
  }
  scratch.position[0] = view.getFloat32(base + 0, true);
  scratch.position[1] = view.getFloat32(base + 4, true);
  scratch.position[2] = view.getFloat32(base + 8, true);
  scratch.scaleLog[0] = view.getFloat32(base + 12, true);
  scratch.scaleLog[1] = view.getFloat32(base + 16, true);
  scratch.scaleLog[2] = view.getFloat32(base + 20, true);
  scratch.quat[0] = view.getFloat32(base + 24, true);
  scratch.quat[1] = view.getFloat32(base + 28, true);
  scratch.quat[2] = view.getFloat32(base + 32, true);
  scratch.quat[3] = view.getFloat32(base + 36, true);
  scratch.opacity = view.getFloat32(base + 40, true);
  for (let i = 0; i < shLen; i++) {
    scratch.sh[i] = view.getFloat32(base + (11 + i) * 4, true);
  }
}

function readBucketCoreRowIntoScratch(
  encoding,
  view,
  base,
  scratch,
  floatView = null,
  floatBase = 0,
) {
  if (
    encoding !== LEAF_BUCKET_ENCODING &&
    encoding !== HANDOFF_BUCKET_ENCODING
  ) {
    throw new ConversionError(`Unknown bucket encoding: ${encoding}`);
  }
  if (floatView) {
    const src = floatView;
    const off = floatBase;
    scratch.position[0] = src[off + 0];
    scratch.position[1] = src[off + 1];
    scratch.position[2] = src[off + 2];
    scratch.scaleLog[0] = src[off + 3];
    scratch.scaleLog[1] = src[off + 4];
    scratch.scaleLog[2] = src[off + 5];
    scratch.quat[0] = src[off + 6];
    scratch.quat[1] = src[off + 7];
    scratch.quat[2] = src[off + 8];
    scratch.quat[3] = src[off + 9];
    scratch.opacity = src[off + 10];
    return;
  }
  scratch.position[0] = view.getFloat32(base + 0, true);
  scratch.position[1] = view.getFloat32(base + 4, true);
  scratch.position[2] = view.getFloat32(base + 8, true);
  scratch.scaleLog[0] = view.getFloat32(base + 12, true);
  scratch.scaleLog[1] = view.getFloat32(base + 16, true);
  scratch.scaleLog[2] = view.getFloat32(base + 20, true);
  scratch.quat[0] = view.getFloat32(base + 24, true);
  scratch.quat[1] = view.getFloat32(base + 28, true);
  scratch.quat[2] = view.getFloat32(base + 32, true);
  scratch.quat[3] = view.getFloat32(base + 36, true);
  scratch.opacity = view.getFloat32(base + 40, true);
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
    writes.push(appendChunksToFile(filePath, chunks));
  }
  await Promise.all(writes);
  buffered.clear();
}

async function appendChunksToFile(filePath, chunks) {
  if (chunks.length === 1) {
    await fs.promises.writeFile(filePath, chunks[0], { flag: 'a' });
    return;
  }

  const handle = await fs.promises.open(filePath, 'a');
  try {
    await writeChunksToHandle(handle, filePath, chunks);
  } finally {
    await handle.close();
  }
}

async function writeChunksToHandle(handle, filePath, chunks) {
  if (chunks.length === 0) {
    return;
  }
  if (chunks.length === 1) {
    const chunk = chunks[0];
    let written = 0;
    while (written < chunk.length) {
      const { bytesWritten } = await handle.write(
        chunk,
        written,
        chunk.length - written,
      );
      ensure(bytesWritten > 0, `Failed to append bucket file: ${filePath}`);
      written += bytesWritten;
    }
    return;
  }
  for (let start = 0; start < chunks.length; start += WRITEV_BATCH_CHUNKS) {
    const batch = chunks.slice(start, start + WRITEV_BATCH_CHUNKS);
    let offset = 0;
    while (offset < batch.length) {
      const { bytesWritten } = await handle.writev(batch.slice(offset));
      ensure(bytesWritten > 0, `Failed to append bucket file: ${filePath}`);
      let remaining = bytesWritten;
      while (offset < batch.length && remaining >= batch[offset].length) {
        remaining -= batch[offset].length;
        offset += 1;
      }
      if (remaining > 0 && offset < batch.length) {
        batch[offset] = batch[offset].subarray(remaining);
      }
    }
  }
}

async function partitionLeafBuckets(
  handle,
  filePath,
  header,
  layout,
  rootNode,
  tempDir,
) {
  const touchedLeaves = [];
  const ensuredDirs = new Set();
  const leafHandles = new Map();
  const rowByteSize = layout.canonicalByteSize;
  const arenaByteSize = Math.max(PARTITION_ARENA_BYTES, rowByteSize);
  const arenas = [0, 1].map(() => ({
    buffer: Buffer.allocUnsafe(arenaByteSize),
    offset: 0,
    activeLeaves: [],
    leafChunks: new Map(),
    flushPromise: null,
  }));
  let activeArenaIndex = 0;
  let flushError = null;

  const ensureLeafHandle = async (leaf) => {
    let fh = leafHandles.get(leaf);
    if (fh) {
      return fh;
    }
    const dir = path.dirname(leaf.bucketPath);
    if (!ensuredDirs.has(dir)) {
      await fs.promises.mkdir(dir, { recursive: true });
      ensuredDirs.add(dir);
    }
    fh = await fs.promises.open(leaf.bucketPath, 'a');
    leafHandles.set(leaf, fh);
    return fh;
  };

  const touchLeaf = (leaf) => {
    if (leaf._partitionTouched) {
      return;
    }
    leaf._partitionTouched = true;
    leaf._partitionWriteChain = Promise.resolve();
    touchedLeaves.push(leaf);
  };

  const throwIfFlushFailed = () => {
    if (flushError) {
      throw flushError;
    }
  };

  const resetArena = (arena) => {
    arena.offset = 0;
    arena.activeLeaves = [];
    arena.leafChunks = new Map();
  };

  const scheduleArenaFlush = (arena) => {
    if (arena.flushPromise) {
      return arena.flushPromise;
    }
    if (arena.leafChunks.size === 0) {
      arena.offset = 0;
      return Promise.resolve();
    }

    const writes = [];
    for (const leaf of arena.activeLeaves) {
      const chunks = arena.leafChunks.get(leaf);
      if (!chunks || chunks.length === 0) {
        continue;
      }
      const writePromise = leaf._partitionWriteChain.then(async () => {
        const fh = await ensureLeafHandle(leaf);
        await writeChunksToHandle(fh, leaf.bucketPath, chunks);
      });
      leaf._partitionWriteChain = writePromise;
      writes.push(writePromise);
    }

    arena.flushPromise = Promise.all(writes)
      .catch((err) => {
        if (!flushError) {
          flushError = err;
        }
      })
      .then(() => {
        resetArena(arena);
      })
      .finally(() => {
        arena.flushPromise = null;
      });
    return arena.flushPromise;
  };

  const switchArena = async () => {
    const currentArena = arenas[activeArenaIndex];
    scheduleArenaFlush(currentArena);
    activeArenaIndex = 1 - activeArenaIndex;
    const nextArena = arenas[activeArenaIndex];
    if (nextArena.flushPromise) {
      await nextArena.flushPromise;
    }
    throwIfFlushFailed();
  };

  const appendLeafRow = (leaf, rowBuffer) => {
    const arena = arenas[activeArenaIndex];
    if (arena.offset + rowByteSize > arena.buffer.length) {
      return switchArena().then(() => appendLeafRow(leaf, rowBuffer));
    }
    touchLeaf(leaf);

    let chunks = arena.leafChunks.get(leaf);
    if (!chunks) {
      chunks = [];
      arena.leafChunks.set(leaf, chunks);
      arena.activeLeaves.push(leaf);
    }
    rowBuffer.copy(arena.buffer, arena.offset, 0, rowByteSize);
    chunks.push(
      arena.buffer.subarray(arena.offset, arena.offset + rowByteSize),
    );
    arena.offset += rowByteSize;
    leaf.bucketRowCount = (leaf.bucketRowCount || 0) + 1;
    return null;
  };

  try {
    await _forEachGaussianPlyCanonicalRecord(
      handle,
      filePath,
      header,
      layout,
      (_rowIndex, rowBuffer, rowView, rowFloats) => {
        const x = rowFloats ? rowFloats[0] : rowView.getFloat32(0, true);
        const y = rowFloats ? rowFloats[1] : rowView.getFloat32(4, true);
        const z = rowFloats ? rowFloats[2] : rowView.getFloat32(8, true);
        const leaf = resolveLeafNodeForPoint(rootNode, x, y, z);
        if (!leaf.bucketPath) {
          leaf.bucketPath = canonicalNodePath(tempDir, LEAF_BUCKET_DIR, leaf);
        }
        return appendLeafRow(leaf, rowBuffer);
      },
    );

    await Promise.all(arenas.map((arena) => scheduleArenaFlush(arena)));
    await Promise.all(
      touchedLeaves.map((leaf) =>
        leaf._partitionWriteChain.catch((err) => {
          if (!flushError) {
            flushError = err;
          }
        }),
      ),
    );
    throwIfFlushFailed();
  } finally {
    await Promise.all(
      arenas
        .map((arena) => arena.flushPromise)
        .filter((promise) => !!promise),
    );
    await Promise.all(
      Array.from(leafHandles.values()).map((fh) => fh.close()),
    );
    leafHandles.clear();

    for (const leaf of touchedLeaves) {
      delete leaf._partitionTouched;
      delete leaf._partitionWriteChain;
    }
  }
  arenas.length = 0;
}

async function collectBucketEntries(fileSpecs, coeffCount) {
  const entries = [];
  let totalRows = 0;
  for (const fileSpec of fileSpecs) {
    if (!fileSpec || !fileSpec.filePath) {
      continue;
    }
    const rowByteSize = bucketRowByteSize(fileSpec.encoding, coeffCount);
    ensure(
      Number.isInteger(fileSpec.rowCount) && fileSpec.rowCount >= 0,
      `Bucket row count metadata is missing: ${fileSpec.filePath}`,
    );
    const rowCount = fileSpec.rowCount;
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
  ensure(
    Number.isInteger(fileSpec.rowCount) && fileSpec.rowCount >= 0,
    `Bucket row count metadata is missing: ${fileSpec.filePath}`,
  );
  const totalBytes = fileSpec.rowCount * rowByteSize;
  if (totalBytes === 0) {
    return;
  }

  const rowsPerChunk = Math.max(1, Math.floor((8 * 1024 * 1024) / rowByteSize));
  const chunkBytes = rowsPerChunk * rowByteSize;
  const chunk = Buffer.allocUnsafe(chunkBytes);
  const handle = await fs.promises.open(fileSpec.filePath, 'r');
  try {
    let fileOffset = 0;
    while (fileOffset < totalBytes) {
      const expectedBytes = Math.min(chunkBytes, totalBytes - fileOffset);
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
      const floatView =
        IS_LITTLE_ENDIAN && (chunk.byteOffset & 3) === 0
          ? new Float32Array(chunk.buffer, chunk.byteOffset, bytesRead >>> 2)
          : null;
      for (let offset = 0; offset < bytesRead; offset += rowByteSize) {
        const maybePromise = onRow(
          view,
          offset,
          fileSpec.encoding,
          fileSpec.filePath,
          floatView,
          floatView ? offset >>> 2 : 0,
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

async function forEachBucketEntryRow(entries, coeffCount, onRow) {
  for (const entry of entries) {
    await forEachBucketSpecRow(entry, coeffCount, onRow);
  }
}

function accumulateBoundsFromScratchRow(
  scratch,
  minimum,
  maximum,
  extentScratch,
) {
  writeThreeSigmaExtentComponents(
    scratch.scaleLog,
    0,
    scratch.quat,
    0,
    extentScratch,
    0,
  );

  const p0 = scratch.position[0];
  const p1 = scratch.position[1];
  const p2 = scratch.position[2];
  const ex = extentScratch[0];
  const ey = extentScratch[1];
  const ez = extentScratch[2];
  const min0 = p0 - ex;
  const min1 = p1 - ey;
  const min2 = p2 - ez;
  const max0 = p0 + ex;
  const max1 = p1 + ey;
  const max2 = p2 + ez;
  if (min0 < minimum[0]) minimum[0] = min0;
  if (min1 < minimum[1]) minimum[1] = min1;
  if (min2 < minimum[2]) minimum[2] = min2;
  if (max0 > maximum[0]) maximum[0] = max0;
  if (max1 > maximum[1]) maximum[1] = max1;
  if (max2 > maximum[2]) maximum[2] = max2;
}

async function computeBucketEntriesBounds(entries, coeffCount) {
  const minimum = [Infinity, Infinity, Infinity];
  const maximum = [-Infinity, -Infinity, -Infinity];
  const scratch = makeRowScratch(0);
  const extentScratch = new Float32Array(3);
  let rowCount = 0;

  await forEachBucketEntryRow(
    entries,
    coeffCount,
    (view, base, encoding, _filePath, floatView, floatBase) => {
      readBucketCoreRowIntoScratch(
        encoding,
        view,
        base,
        scratch,
        floatView,
        floatBase,
      );
      accumulateBoundsFromScratchRow(scratch, minimum, maximum, extentScratch);
      rowCount += 1;
    },
  );

  ensure(rowCount > 0, 'Cannot compute bounds for an empty bucket input.');
  return new Bounds(minimum, maximum);
}

async function packBucketEntriesToSpz(
  entries,
  coeffCount,
  shDegree,
  sh1Bits,
  shRestBits,
  translation = null,
) {
  validateSpzQuantBits(sh1Bits, shRestBits);

  const n = entries.reduce((sum, entry) => sum + entry.rowCount, 0);
  ensure(n > 0, 'Cannot pack an empty bucket input to SPZ.');
  validateSpzCoeffCount(shDegree, coeffCount);

  const extra = coeffCount - 1;
  const layout = makeSpzPacketLayout(n, coeffCount);
  const { packet } = layout;
  const tx = translation ? translation[0] : 0.0;
  const ty = translation ? translation[1] : 0.0;
  const tz = translation ? translation[2] : 0.0;
  const scratch = makeRowScratch(coeffCount);
  const shBuckets =
    extra > 0 ? makeSpzShQuantBuckets(coeffCount, sh1Bits, shRestBits) : null;

  let rowIndex = 0;
  await forEachBucketEntryRow(
    entries,
    coeffCount,
    (view, base, encoding, _filePath, floatView, floatBase) => {
      readBucketRowIntoScratch(
        encoding,
        view,
        base,
        coeffCount,
        scratch,
        floatView,
        floatBase,
      );

      const localX = translation
        ? Math.fround(scratch.position[0] - tx)
        : scratch.position[0];
      const posBase = layout.positionsOffset + rowIndex * 9;
      writeFixed24Into(packet, posBase + 0, quantizeSpzPosition(localX));

      const localY = translation
        ? Math.fround(scratch.position[1] - ty)
        : scratch.position[1];
      writeFixed24Into(packet, posBase + 3, quantizeSpzPosition(localY));

      const localZ = translation
        ? Math.fround(scratch.position[2] - tz)
        : scratch.position[2];
      writeFixed24Into(packet, posBase + 6, quantizeSpzPosition(localZ));

      packet[layout.opacityOffset + rowIndex] = quantizeSpzOpacity(
        scratch.opacity,
      );

      const colorBase = layout.colorOffset + rowIndex * 3;
      packet[colorBase + 0] = quantizeSpzColor(scratch.sh[0]);
      packet[colorBase + 1] = quantizeSpzColor(scratch.sh[1]);
      packet[colorBase + 2] = quantizeSpzColor(scratch.sh[2]);

      const scaleBase = layout.scaleOffset + rowIndex * 3;
      packet[scaleBase + 0] = quantizeSpzScale(scratch.scaleLog[0]);
      packet[scaleBase + 1] = quantizeSpzScale(scratch.scaleLog[1]);
      packet[scaleBase + 2] = quantizeSpzScale(scratch.scaleLog[2]);

      packQuaternionSmallestThreeInto(
        scratch.quat,
        0,
        packet,
        layout.quatOffset + rowIndex * 4,
      );

      if (extra > 0) {
        let shBase =
          layout.extraShOffset + rowIndex * layout.extraBytesPerPoint;
        for (let coeff = 1; coeff < coeffCount; coeff++) {
          const bucket = shBuckets.buckets[coeff];
          const halfBucket = shBuckets.halfBuckets[coeff];
          const invBucket = shBuckets.invBuckets[coeff];
          const coeffBase = coeff * 3;
          packet[shBase++] = quantizeSpzExtraSh(
            scratch.sh[coeffBase + 0],
            bucket,
            halfBucket,
            invBucket,
          );
          packet[shBase++] = quantizeSpzExtraSh(
            scratch.sh[coeffBase + 1],
            bucket,
            halfBucket,
            invBucket,
          );
          packet[shBase++] = quantizeSpzExtraSh(
            scratch.sh[coeffBase + 2],
            bucket,
            halfBucket,
            invBucket,
          );
        }
      }

      rowIndex += 1;
    },
  );
  ensure(
    rowIndex === n,
    `Bucket row count changed while packing SPZ: expected ${n}, read ${rowIndex}.`,
  );

  writeSpzPacketHeader(packet, n, shDegree);
  return gzipSpzPacket(packet);
}

async function writeBufferToHandle(handle, buffer, byteLength, targetPath) {
  let offset = 0;
  while (offset < byteLength) {
    const { bytesWritten } = await handle.write(
      buffer,
      offset,
      byteLength - offset,
      null,
    );
    ensure(bytesWritten > 0, `Failed to write bucket file: ${targetPath}`);
    offset += bytesWritten;
  }
}

async function appendBucketEntryToHandle(
  entry,
  coeffCount,
  handle,
  targetPath = null,
) {
  const rowByteSize =
    entry.rowByteSize || bucketRowByteSize(entry.encoding, coeffCount);
  const rowsPerChunk = Math.max(1, Math.floor((8 * 1024 * 1024) / rowByteSize));
  const chunkBytes = rowsPerChunk * rowByteSize;
  const chunk = Buffer.allocUnsafe(chunkBytes);
  const source = await fs.promises.open(entry.filePath, 'r');
  try {
    let fileOffset = 0;
    const totalBytes = entry.rowCount * rowByteSize;
    while (fileOffset < totalBytes) {
      const expectedBytes = Math.min(chunkBytes, totalBytes - fileOffset);
      const { bytesRead } = await source.read(
        chunk,
        0,
        expectedBytes,
        fileOffset,
      );
      ensure(
        bytesRead === expectedBytes,
        `Bucket file ended early: ${entry.filePath}`,
      );
      fileOffset += bytesRead;

      await writeBufferToHandle(handle, chunk, bytesRead, targetPath);
    }
  } finally {
    await source.close();
  }
}

async function materializeCanonicalEntriesFile(
  entries,
  targetPath,
  coeffCount,
) {
  ensure(
    entries.length > 0,
    'Cannot materialize an empty canonical bucket set.',
  );
  if (entries.length === 1) {
    await materializeLinkedHandoffFile(entries[0].filePath, targetPath);
    return;
  }

  await fs.promises.mkdir(path.dirname(targetPath), { recursive: true });
  await removeFileIfExists(targetPath);
  const handle = await fs.promises.open(targetPath, 'w');
  try {
    for (const entry of entries) {
      await appendBucketEntryToHandle(entry, coeffCount, handle, targetPath);
    }
  } finally {
    await handle.close();
  }
}

async function loadBucketCloudFromEntries(
  entries,
  coeffCount,
  totalRows = null,
) {
  const resolvedTotalRows =
    totalRows == null
      ? entries.reduce((sum, entry) => sum + (entry.rowCount || 0), 0)
      : totalRows;
  ensure(
    resolvedTotalRows > 0,
    'Cannot load an empty Gaussian cloud from bucket files.',
  );

  const coeffStride = coeffCount * 3;
  const positions = new Float32Array(resolvedTotalRows * 3);
  const scaleLog = new Float32Array(resolvedTotalRows * 3);
  const quats = new Float32Array(resolvedTotalRows * 4);
  const opacity = new Float32Array(resolvedTotalRows);
  const shCoeffs = new Float32Array(resolvedTotalRows * coeffStride);
  const scratch = makeRowScratch(coeffCount);
  let rowIndex = 0;

  await forEachBucketEntryRow(
    entries,
    coeffCount,
    (view, base, encoding, _filePath, floatView, floatBase) => {
      readBucketRowIntoScratch(
        encoding,
        view,
        base,
        coeffCount,
        scratch,
        floatView,
        floatBase,
      );
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
    },
  );

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
      const chunkByteOffset = chunk.byteOffset;
      const canUseFastPath = IS_LITTLE_ENDIAN && (chunkByteOffset & 3) === 0;
      if (canUseFastPath) {
        const floatView = new Float32Array(
          chunk.buffer,
          chunkByteOffset,
          (rowCount * rowByteSize) >>> 2,
        );
        const floatsPerRow = 11 + coeffStride;
        for (let i = 0; i < rowCount; i++) {
          const rowIndex = rowBase + i;
          const base3 = rowIndex * 3;
          const base4 = rowIndex * 4;
          const coeffBase = rowIndex * coeffStride;
          const rowOff = i * floatsPerRow;
          floatView[rowOff + 0] = cloud.positions[base3 + 0];
          floatView[rowOff + 1] = cloud.positions[base3 + 1];
          floatView[rowOff + 2] = cloud.positions[base3 + 2];
          floatView[rowOff + 3] = cloud.scaleLog[base3 + 0];
          floatView[rowOff + 4] = cloud.scaleLog[base3 + 1];
          floatView[rowOff + 5] = cloud.scaleLog[base3 + 2];
          floatView[rowOff + 6] = cloud.quatsXYZW[base4 + 0];
          floatView[rowOff + 7] = cloud.quatsXYZW[base4 + 1];
          floatView[rowOff + 8] = cloud.quatsXYZW[base4 + 2];
          floatView[rowOff + 9] = cloud.quatsXYZW[base4 + 3];
          floatView[rowOff + 10] = cloud.opacity[rowIndex];
          if (coeffStride > 0) {
            floatView.set(
              cloud.shCoeffs.subarray(coeffBase, coeffBase + coeffStride),
              rowOff + 11,
            );
          }
        }
      } else {
        const view = new DataView(
          chunk.buffer,
          chunkByteOffset,
          chunk.byteLength,
        );
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
  if (
    Math.abs(dot01) > 1e-10 ||
    Math.abs(dot02) > 1e-10 ||
    Math.abs(dot12) > 1e-10
  ) {
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

function scratchThreeSigmaRadiusFloat32(scratch) {
  return Math.fround(
    computeThreeSigmaAabbDiagonalRadiusAt(scratch.scaleLog, 0, scratch.quat, 0),
  );
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

async function gatherSelectedBucketRowsToCloud(
  entries,
  coeffCount,
  outCount,
  selectedRows,
) {
  const coeffStride = coeffCount * 3;
  const positions = new Float32Array(outCount * 3);
  const scaleLog = new Float32Array(outCount * 3);
  const quats = new Float32Array(outCount * 4);
  const opacity = new Float32Array(outCount);
  const shCoeffs = new Float32Array(outCount * coeffStride);
  await materializeBucketRowsToSlots(
    entries,
    coeffCount,
    selectedRows,
    positions,
    scaleLog,
    quats,
    opacity,
    shCoeffs,
  );

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

async function materializeBucketRowsToSlots(
  entries,
  coeffCount,
  rowIndicesBySlot,
  positions,
  scaleLog,
  quats,
  opacity,
  shCoeffs,
) {
  const wantedRows = new Map();
  for (let slot = 0; slot < rowIndicesBySlot.length; slot++) {
    const rowIndex = rowIndicesBySlot[slot];
    if (rowIndex < 0) {
      continue;
    }
    let slots = wantedRows.get(rowIndex);
    if (!slots) {
      slots = [];
      wantedRows.set(rowIndex, slots);
    }
    slots.push(slot);
  }
  if (wantedRows.size === 0) {
    return;
  }

  const coeffStride = coeffCount * 3;
  const scratch = makeRowScratch(coeffCount);
  let rowIndex = 0;

  await forEachBucketEntryRow(
    entries,
    coeffCount,
    (view, base, encoding, _filePath, floatView, floatBase) => {
      const slots = wantedRows.get(rowIndex);
      if (slots) {
        readBucketRowIntoScratch(
          encoding,
          view,
          base,
          coeffCount,
          scratch,
          floatView,
          floatBase,
        );
        for (const slot of slots) {
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
      }
      rowIndex += 1;
    },
  );
}

async function loadBucketSimplifyCoreFromEntries(
  entries,
  coeffCount,
  { keepScaleQuat = false } = {},
) {
  let totalRows = 0;
  for (const entry of entries) {
    totalRows += entry.rowCount || 0;
  }
  ensure(
    totalRows > 0,
    'Cannot load an empty simplify input from bucket files.',
  );

  const positions = new Float32Array(totalRows * 3);
  const scaleLog = keepScaleQuat ? new Float32Array(totalRows * 3) : null;
  const quatsXYZW = keepScaleQuat ? new Float32Array(totalRows * 4) : null;
  const opacity = new Float32Array(totalRows);
  const origRadius = new Float32Array(totalRows);
  const scratch = makeRowScratch(coeffCount);
  let rowIndex = 0;

  await forEachBucketEntryRow(
    entries,
    coeffCount,
    (view, base, encoding, _filePath, floatView, floatBase) => {
      readBucketCoreRowIntoScratch(
        encoding,
        view,
        base,
        scratch,
        floatView,
        floatBase,
      );
      const base3 = rowIndex * 3;
      const base4 = rowIndex * 4;
      positions[base3 + 0] = scratch.position[0];
      positions[base3 + 1] = scratch.position[1];
      positions[base3 + 2] = scratch.position[2];
      if (keepScaleQuat) {
        scaleLog[base3 + 0] = scratch.scaleLog[0];
        scaleLog[base3 + 1] = scratch.scaleLog[1];
        scaleLog[base3 + 2] = scratch.scaleLog[2];
        quatsXYZW[base4 + 0] = scratch.quat[0];
        quatsXYZW[base4 + 1] = scratch.quat[1];
        quatsXYZW[base4 + 2] = scratch.quat[2];
        quatsXYZW[base4 + 3] = scratch.quat[3];
      }
      opacity[rowIndex] = scratch.opacity;
      origRadius[rowIndex] = computeThreeSigmaAabbDiagonalRadiusAt(
        scratch.scaleLog,
        0,
        scratch.quat,
        0,
      );
      rowIndex += 1;
    },
  );

  return {
    positions,
    scaleLog,
    quatsXYZW,
    opacity,
    origRadius,
    length: totalRows,
  };
}

async function mergeSelectedBucketRowsToCloud(
  entries,
  coeffCount,
  selectedRows,
  assignment,
  selectedCount,
  voxelDiag,
) {
  const coeffStride = coeffCount * 3;
  let positions = null;
  let scaleLog = null;
  let quats = null;
  let opacity = null;
  let shCoeffs = null;
  const weightSums = new Float64Array(selectedCount);
  const counts = new Uint32Array(selectedCount);
  const firstAssigned = new Int32Array(selectedCount);
  firstAssigned.fill(-1);
  const fallbackRowIndex = new Int32Array(selectedCount);
  fallbackRowIndex.fill(-1);
  let weightedPos = new Float64Array(selectedCount * 3);
  let weightedOpacity = new Float64Array(selectedCount);
  const covScratch = new Float64Array(6);
  const scratch = makeRowScratch(coeffCount);
  let rowIndex = 0;

  await forEachBucketEntryRow(
    entries,
    coeffCount,
    (view, base, encoding, _filePath, floatView, floatBase) => {
      readBucketCoreRowIntoScratch(
        encoding,
        view,
        base,
        scratch,
        floatView,
        floatBase,
      );
      const slot = assignment[rowIndex];
      const radius = scratchThreeSigmaRadiusFloat32(scratch);
      const weight = mergeAggregationWeight(scratch.opacity, radius, voxelDiag);
      const base3 = slot * 3;
      if (firstAssigned[slot] < 0) {
        firstAssigned[slot] = rowIndex;
      }
      weightSums[slot] += weight;
      counts[slot] += 1;
      weightedPos[base3 + 0] += scratch.position[0] * weight;
      weightedPos[base3 + 1] += scratch.position[1] * weight;
      weightedPos[base3 + 2] += scratch.position[2] * weight;
      weightedOpacity[slot] += scratch.opacity * weight;
      rowIndex += 1;
    },
  );

  positions = new Float32Array(selectedCount * 3);
  opacity = new Float32Array(selectedCount);
  shCoeffs = new Float32Array(selectedCount * coeffStride);

  for (let slot = 0; slot < selectedCount; slot++) {
    if (
      !Number.isFinite(weightSums[slot]) ||
      weightSums[slot] <= 1e-12 ||
      counts[slot] === 0
    ) {
      fallbackRowIndex[slot] =
        firstAssigned[slot] >= 0 ? firstAssigned[slot] : selectedRows[slot];
      continue;
    }

    const invWeight = 1.0 / weightSums[slot];
    const base3 = slot * 3;
    positions[base3 + 0] = weightedPos[base3 + 0] * invWeight;
    positions[base3 + 1] = weightedPos[base3 + 1] * invWeight;
    positions[base3 + 2] = weightedPos[base3 + 2] * invWeight;
    opacity[slot] = Math.max(
      0.0,
      Math.min(1.0, weightedOpacity[slot] * invWeight),
    );
  }

  weightedPos = null;
  weightedOpacity = null;

  for (
    let coeffStart = 0;
    coeffStart < coeffStride;
    coeffStart += MERGE_SH_COEFF_BLOCK
  ) {
    const blockWidth = Math.min(MERGE_SH_COEFF_BLOCK, coeffStride - coeffStart);
    let weightedShBlock = new Float64Array(selectedCount * blockWidth);
    rowIndex = 0;
    await forEachBucketEntryRow(
      entries,
      coeffCount,
      (view, base, encoding, _filePath, floatView, floatBase) => {
        const slot = assignment[rowIndex];
        if (
          !Number.isFinite(weightSums[slot]) ||
          weightSums[slot] <= 1e-12 ||
          counts[slot] === 0
        ) {
          rowIndex += 1;
          return;
        }
        readBucketRowIntoScratch(
          encoding,
          view,
          base,
          coeffCount,
          scratch,
          floatView,
          floatBase,
        );
        const radius = scratchThreeSigmaRadiusFloat32(scratch);
        const weight = mergeAggregationWeight(
          scratch.opacity,
          radius,
          voxelDiag,
        );
        const blockBase = slot * blockWidth;
        for (let c = 0; c < blockWidth; c++) {
          weightedShBlock[blockBase + c] += scratch.sh[coeffStart + c] * weight;
        }
        rowIndex += 1;
      },
    );

    for (let slot = 0; slot < selectedCount; slot++) {
      if (
        !Number.isFinite(weightSums[slot]) ||
        weightSums[slot] <= 1e-12 ||
        counts[slot] === 0
      ) {
        continue;
      }
      const invWeight = 1.0 / weightSums[slot];
      const blockBase = slot * blockWidth;
      const coeffBase = slot * coeffStride + coeffStart;
      for (let c = 0; c < blockWidth; c++) {
        shCoeffs[coeffBase + c] = weightedShBlock[blockBase + c] * invWeight;
      }
    }
    weightedShBlock = null;
  }

  let covSums = new Float64Array(selectedCount * 6);
  rowIndex = 0;
  await forEachBucketEntryRow(
    entries,
    coeffCount,
    (view, base, encoding, _filePath, floatView, floatBase) => {
      const slot = assignment[rowIndex];
      if (
        !Number.isFinite(weightSums[slot]) ||
        weightSums[slot] <= 1e-12 ||
        counts[slot] <= 1
      ) {
        rowIndex += 1;
        return;
      }
      readBucketCoreRowIntoScratch(
        encoding,
        view,
        base,
        scratch,
        floatView,
        floatBase,
      );
      const radius = scratchThreeSigmaRadiusFloat32(scratch);
      const weight = mergeAggregationWeight(scratch.opacity, radius, voxelDiag);
      covarianceComponentsFromScratch(
        scratch.scaleLog,
        scratch.quat,
        covScratch,
      );
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
    },
  );

  for (let slot = 0; slot < selectedCount; slot++) {
    if (
      !Number.isFinite(weightSums[slot]) ||
      weightSums[slot] <= 1e-12 ||
      counts[slot] <= 1
    ) {
      fallbackRowIndex[slot] =
        firstAssigned[slot] >= 0 ? firstAssigned[slot] : selectedRows[slot];
      continue;
    }

    const invWeight = 1.0 / weightSums[slot];
    const covBase = slot * 6;
    if (!scaleLog) {
      scaleLog = new Float32Array(selectedCount * 3);
      quats = new Float32Array(selectedCount * 4);
    }
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

  if (!scaleLog) {
    scaleLog = new Float32Array(selectedCount * 3);
    quats = new Float32Array(selectedCount * 4);
  }
  covSums = null;

  await materializeBucketRowsToSlots(
    entries,
    coeffCount,
    fallbackRowIndex,
    positions,
    scaleLog,
    quats,
    opacity,
    shCoeffs,
  );

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

class FixedMinHeap {
  constructor(capacity) {
    this.values = new Float64Array(capacity);
    this.length = 0;
    this.capacity = capacity;
  }

  pushCandidate(value) {
    if (this.capacity <= 0) {
      return;
    }
    if (this.length < this.capacity) {
      const index = this.length++;
      this.values[index] = value;
      this._siftUp(index);
      return;
    }
    if (value <= this.values[0]) {
      return;
    }
    this.values[0] = value;
    this._siftDown(0);
  }

  _siftUp(index) {
    const values = this.values;
    const value = values[index];
    while (index > 0) {
      const parent = (index - 1) >> 1;
      const parentValue = values[parent];
      if (parentValue <= value) {
        break;
      }
      values[index] = parentValue;
      index = parent;
    }
    values[index] = value;
  }

  _siftDown(index) {
    const values = this.values;
    const length = this.length;
    const value = values[index];
    while (true) {
      const left = index * 2 + 1;
      if (left >= length) {
        break;
      }
      const right = left + 1;
      let child = left;
      let childValue = values[left];
      if (right < length && values[right] < childValue) {
        child = right;
        childValue = values[right];
      }
      if (childValue >= value) {
        break;
      }
      values[index] = childValue;
      index = child;
    }
    values[index] = value;
  }

  sortedValues() {
    const out = this.values.subarray(0, this.length);
    out.sort((a, b) => a - b);
    return out;
  }
}

async function computeExactStreamingOwnErrorFromEntries(
  entries,
  coeffCount,
  assignment,
  outputCloud,
  outputRadius,
) {
  const totalRows = assignment.length;
  const pos95 = 0.95 * (totalRows - 1);
  const lo = Math.floor(pos95);
  const hi = Math.min(totalRows - 1, lo + 1);
  const frac = pos95 - lo;
  const tailStart = lo;
  const tail = new FixedMinHeap(totalRows - tailStart);
  const scratch = makeRowScratch(coeffCount);
  let rowIndex = 0;

  await forEachBucketEntryRow(
    entries,
    coeffCount,
    (view, base, encoding, _filePath, floatView, floatBase) => {
      readBucketCoreRowIntoScratch(
        encoding,
        view,
        base,
        scratch,
        floatView,
        floatBase,
      );
      const slot = assignment[rowIndex];
      const dstBase3 = slot * 3;
      const dx = scratch.position[0] - outputCloud.positions[dstBase3 + 0];
      const dy = scratch.position[1] - outputCloud.positions[dstBase3 + 1];
      const dz = scratch.position[2] - outputCloud.positions[dstBase3 + 2];
      const radius = scratchThreeSigmaRadiusFloat32(scratch);
      const error =
        Math.sqrt(dx * dx + dy * dy + dz * dz) + radius + outputRadius[slot];
      tail.pushCandidate(error);
      rowIndex += 1;
    },
  );

  const sortedTail = tail.sortedValues();
  const loValue = sortedTail[0];
  if (frac === 0) {
    return loValue;
  }
  return loValue * (1 - frac) + sortedTail[hi - tailStart] * frac;
}

async function planExactStreamingSimplify(
  entries,
  coeffCount,
  target,
  bounds,
  totalRows,
  { sampleMode = 'merge' } = {},
) {
  const lightCloud = await loadBucketSimplifyCoreFromEntries(
    entries,
    coeffCount,
    { keepScaleQuat: bounds == null },
  );
  const activeBounds = bounds || computeBounds(lightCloud);
  return planSimplifyCloudVoxel(
    lightCloud,
    target,
    activeBounds,
    normalizeSplatTargetCount(target, totalRows),
    {
      returnOrigRadius: false,
      returnKeptRadius: sampleMode !== 'merge',
    },
  );
}

async function streamSimplifyBucketEntriesExact(
  entries,
  coeffCount,
  targetCount,
  bounds,
  sampleMode,
) {
  const totalRows = entries.reduce((sum, entry) => sum + entry.rowCount, 0);
  ensure(totalRows > 0, 'Cannot simplify an empty bucket input.');
  const target = normalizeSplatTargetCount(targetCount, totalRows);
  if (totalRows <= target) {
    return {
      cloud: await loadBucketCloudFromEntries(entries, coeffCount, totalRows),
      ownError: 0.0,
    };
  }

  const plan = await planExactStreamingSimplify(
    entries,
    coeffCount,
    target,
    bounds,
    totalRows,
    { sampleMode },
  );
  const selectedRows = plan.selected;
  const selectedCount = selectedRows.length;
  plan.selected = null;

  const outputCloud =
    sampleMode === 'merge'
      ? await mergeSelectedBucketRowsToCloud(
          entries,
          coeffCount,
          selectedRows,
          plan.assignment,
          selectedCount,
          plan.voxelDiag,
        )
      : await gatherSelectedBucketRowsToCloud(
          entries,
          coeffCount,
          selectedCount,
          selectedRows,
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
    ownError: await computeExactStreamingOwnErrorFromEntries(
      entries,
      coeffCount,
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
    cloud.length >= SPZ_CLOUD_ASYNC_WRITE_THRESHOLD
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

function bucketEntriesForWorkerTask(entries) {
  return entries.map((entry) => ({
    filePath: entry.filePath,
    encoding: entry.encoding,
    rowCount: entry.rowCount,
  }));
}

async function writeBucketGlbTaskOutput(task) {
  ensure(task && task.outPath, 'Missing bucket GLB task output path.');
  ensure(task.pointCount > 0, 'Cannot write empty bucket content.');
  const translation =
    task.translation ||
    (await computeBucketEntriesBounds(task.entries, task.coeffCount)).center();
  const spzBytes = await packBucketEntriesToSpz(
    task.entries,
    task.coeffCount,
    task.shDegree,
    task.sh1Bits,
    task.shRestBits,
    translation,
  );
  const builder = new GltfBuilder();
  builder.writeSpzStreamGlb(
    task.outPath,
    spzBytes,
    { length: task.pointCount, shDegree: task.shDegree },
    task.colorSpace,
    translation,
  );
  return true;
}

async function writeBucketContentFile(
  params,
  entries,
  coeffCount,
  pointCount,
  shDegree,
  level,
  x,
  y,
  z,
) {
  ensure(pointCount > 0, 'Cannot write empty bucket content.');
  const relPath = contentRelPath(level, x, y, z);
  const outPath = path.join(params.outputDir, relPath);
  await fs.promises.mkdir(path.dirname(outPath), { recursive: true });

  const task = {
    kind: 'pack-bucket-spz',
    outPath,
    entries: bucketEntriesForWorkerTask(entries),
    coeffCount,
    pointCount,
    shDegree,
    sh1Bits: params.spzSh1Bits,
    shRestBits: params.spzShRestBits,
    colorSpace: params.colorSpace,
  };

  if (
    params.contentWorkerPool &&
    pointCount >= SPZ_BUCKET_ASYNC_WRITE_THRESHOLD
  ) {
    await params.contentWorkerPool.submit(task);
    return relPath;
  }

  await writeBucketGlbTaskOutput(task);
  return relPath;
}

function updatePositionBounds(minimum, maximum, x, y, z) {
  if (x < minimum[0]) minimum[0] = x;
  if (y < minimum[1]) minimum[1] = y;
  if (z < minimum[2]) minimum[2] = z;
  if (x > maximum[0]) maximum[0] = x;
  if (y > maximum[1]) maximum[1] = y;
  if (z > maximum[2]) maximum[2] = z;
}

async function scanGlobalBoundsAndWritePositions(
  handle,
  filePath,
  header,
  layout,
  positionsPath,
) {
  const minimum = [Infinity, Infinity, Infinity];
  const maximum = [-Infinity, -Infinity, -Infinity];
  const rowsPerBuffer = Math.max(
    1,
    Math.floor(POSITION_TMP_BUFFER_BYTES / POSITION_ROW_BYTE_SIZE),
  );
  const buffer = Buffer.allocUnsafe(rowsPerBuffer * POSITION_ROW_BYTE_SIZE);
  const floatView =
    IS_LITTLE_ENDIAN && (buffer.byteOffset & 3) === 0
      ? new Float32Array(buffer.buffer, buffer.byteOffset, rowsPerBuffer * 3)
      : null;
  let bufferedRows = 0;
  let count = 0;

  await fs.promises.mkdir(path.dirname(positionsPath), { recursive: true });
  await removeFileIfExists(positionsPath);
  const positionFd = fs.openSync(positionsPath, 'w');
  const flush = () => {
    if (bufferedRows === 0) {
      return;
    }
    const byteLength = bufferedRows * POSITION_ROW_BYTE_SIZE;
    let written = 0;
    while (written < byteLength) {
      const bytesWritten = fs.writeSync(
        positionFd,
        buffer,
        written,
        byteLength - written,
      );
      ensure(
        bytesWritten > 0,
        `Failed to write staged positions: ${positionsPath}`,
      );
      written += bytesWritten;
    }
    bufferedRows = 0;
  };

  try {
    await _forEachGaussianPlyPosition(
      handle,
      filePath,
      header,
      layout,
      (_rowIndex, x, y, z) => {
        const fx = Math.fround(x);
        const fy = Math.fround(y);
        const fz = Math.fround(z);
        updatePositionBounds(minimum, maximum, fx, fy, fz);

        if (floatView) {
          const base = bufferedRows * 3;
          floatView[base + 0] = fx;
          floatView[base + 1] = fy;
          floatView[base + 2] = fz;
        } else {
          const base = bufferedRows * POSITION_ROW_BYTE_SIZE;
          buffer.writeFloatLE(fx, base + 0);
          buffer.writeFloatLE(fy, base + 4);
          buffer.writeFloatLE(fz, base + 8);
        }

        bufferedRows += 1;
        count += 1;
        if (bufferedRows === rowsPerBuffer) {
          flush();
        }
      },
    );
    flush();
  } finally {
    fs.closeSync(positionFd);
  }

  ensure(count > 0, `PLY file ${filePath} does not contain any vertices.`);
  ensure(
    count === header.vertexCount,
    `PLY position row count mismatch. Expected ${header.vertexCount}, got ${count}.`,
  );
  return new Bounds(minimum, maximum);
}

function addPositionToCounts(counts, rootBounds, maxDepth, x, y, z) {
  let minX = rootBounds.minimum[0];
  let minY = rootBounds.minimum[1];
  let minZ = rootBounds.minimum[2];
  let maxX = rootBounds.maximum[0];
  let maxY = rootBounds.maximum[1];
  let maxZ = rootBounds.maximum[2];
  let ix = 0;
  let iy = 0;
  let iz = 0;

  counts.increment(0, 0, 0, 0);
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
    counts.increment(depth + 1, ix, iy, iz);

    if (bx) minX = cx;
    else maxX = cx;
    if (by) minY = cy;
    else maxY = cy;
    if (bz) minZ = cz;
    else maxZ = cz;
  }
}

async function readExactFromHandle(handle, buffer, length, position, message) {
  let offset = 0;
  while (offset < length) {
    const { bytesRead } = await handle.read(
      buffer,
      offset,
      length - offset,
      position + offset,
    );
    if (bytesRead === 0) {
      throw new ConversionError(message);
    }
    offset += bytesRead;
  }
}

async function buildCountsTableFromPositionFile(
  positionsPath,
  vertexCount,
  rootBounds,
  maxDepth,
) {
  const counts = new CountsTable(maxDepth, vertexCount);
  const rowsPerChunk = Math.max(
    1,
    Math.floor((8 * 1024 * 1024) / POSITION_ROW_BYTE_SIZE),
  );
  const chunk = Buffer.allocUnsafe(rowsPerChunk * POSITION_ROW_BYTE_SIZE);
  const floatView =
    IS_LITTLE_ENDIAN && (chunk.byteOffset & 3) === 0
      ? new Float32Array(chunk.buffer, chunk.byteOffset, rowsPerChunk * 3)
      : null;
  const handle = await fs.promises.open(positionsPath, 'r');
  try {
    let fileOffset = 0;
    for (let rowBase = 0; rowBase < vertexCount; rowBase += rowsPerChunk) {
      const rowCount = Math.min(rowsPerChunk, vertexCount - rowBase);
      const byteCount = rowCount * POSITION_ROW_BYTE_SIZE;
      await readExactFromHandle(
        handle,
        chunk,
        byteCount,
        fileOffset,
        `Staged position file ended early: ${positionsPath}`,
      );
      fileOffset += byteCount;

      for (let i = 0; i < rowCount; i++) {
        let x;
        let y;
        let z;
        if (floatView) {
          const base = i * 3;
          x = floatView[base + 0];
          y = floatView[base + 1];
          z = floatView[base + 2];
        } else {
          const base = i * POSITION_ROW_BYTE_SIZE;
          x = chunk.readFloatLE(base + 0);
          y = chunk.readFloatLE(base + 4);
          z = chunk.readFloatLE(base + 8);
        }
        addPositionToCounts(counts, rootBounds, maxDepth, x, y, z);
      }
    }
  } finally {
    await handle.close();
  }

  return counts;
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
  const { entries, totalRows } = await collectBucketEntries(
    [leafBucketSpec(node)],
    ctx.layout.coeffCount,
  );
  node.bucketRowCount = totalRows;
  if (node.depth > 0) {
    node.handoffPath = canonicalNodePath(ctx.tempDir, HANDOFF_BUCKET_DIR, node);
    await materializeLinkedHandoffFile(node.bucketPath, node.handoffPath);
    node.handoffRowCount = totalRows;
    node.handoffConsumed = false;
  } else {
    node.handoffPath = null;
    node.handoffRowCount = null;
    node.handoffConsumed = true;
  }

  node.ownError = 0.0;
  node.contentUri = await writeBucketContentFile(
    ctx.params,
    entries,
    ctx.layout.coeffCount,
    totalRows,
    ctx.layout.degree,
    node.level,
    node.x,
    node.y,
    node.z,
  );
  node.buildState = 'completed';
  enqueuePipelineStateSave(ctx, PIPELINE_STAGE_BUCKETED);
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
  const contentTarget = resolveNodeContentTarget(node, ctx, totalRows);
  if (totalRows <= contentTarget) {
    if (node.depth > 0) {
      node.handoffPath = canonicalNodePath(
        ctx.tempDir,
        HANDOFF_BUCKET_DIR,
        node,
      );
      await materializeCanonicalEntriesFile(
        entries,
        node.handoffPath,
        ctx.layout.coeffCount,
      );
      node.handoffRowCount = totalRows;
      node.handoffConsumed = false;
    } else {
      node.handoffPath = null;
      node.handoffRowCount = null;
      node.handoffConsumed = true;
    }

    node.ownError = 0.0;
    node.contentUri = await writeBucketContentFile(
      ctx.params,
      entries,
      ctx.layout.coeffCount,
      totalRows,
      ctx.layout.degree,
      node.level,
      node.x,
      node.y,
      node.z,
    );
  } else {
    const { cloud: contentCloud, ownError } =
      await streamSimplifyBucketEntriesExact(
        entries,
        ctx.layout.coeffCount,
        contentTarget,
        node.bounds,
        ctx.params.sampleMode,
      );
    if (node.depth > 0) {
      node.handoffPath = canonicalNodePath(
        ctx.tempDir,
        HANDOFF_BUCKET_DIR,
        node,
      );
      await writeCanonicalCloudFile(node.handoffPath, contentCloud);
      node.handoffRowCount = contentCloud.length;
      node.handoffConsumed = false;
    } else {
      node.handoffPath = null;
      node.handoffRowCount = null;
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
  }
  node.buildState = 'completed';
  for (const child of node.children) {
    child.handoffConsumed = true;
    if (child.handoffPath) {
      ctx.pendingHandoffCleanup.add(child.handoffPath);
    }
    child.handoffRowCount = null;
  }
  enqueuePipelineStateSave(ctx, PIPELINE_STAGE_BUCKETED);
}

async function runWithConcurrency(items, limit, onItem) {
  if (!items || items.length === 0) {
    return;
  }
  const concurrency = Math.max(
    1,
    Math.min(items.length, Math.floor(limit || 1)),
  );
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

async function processBuildNodes(nodes, concurrency, ctx) {
  await runWithConcurrency(nodes, concurrency, async (node) => {
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

function tickBuildProgress(ctx, node, status) {
  if (!ctx || !ctx.buildProgress || !node) {
    return;
  }
  const kind = node.leaf ? 'leaf' : 'internal';
  const suffix = status ? ` ${status}` : '';
  ctx.buildProgress.tick(`level=${node.level} ${kind}${suffix}`);
}

async function buildPartitionedBottomUp(rootNode, ctx) {
  const treeStats = collectTreeStats(rootNode);
  const cleanupTasks = [];
  for (let level = treeStats.maxLevel; level >= 0; level--) {
    const levelNodes = treeStats.levels[level] || [];
    ctx.pendingHandoffCleanup = new Set();
    const leafNodes = [];
    const internalNodes = [];
    for (const node of levelNodes) {
      if (node.leaf) {
        leafNodes.push(node);
      } else {
        internalNodes.push(node);
      }
    }
    await processBuildNodes(leafNodes, ctx.leafNodeConcurrency, ctx);
    await processBuildNodes(internalNodes, ctx.nodeConcurrency, ctx);
    if (levelNodes.length > 0) {
      await enqueuePipelineStateSave(ctx, PIPELINE_STAGE_BUCKETED, {
        force: true,
      });
      const handoffPaths = Array.from(ctx.pendingHandoffCleanup);
      if (handoffPaths.length > 0) {
        cleanupTasks.push(
          runWithConcurrency(
            handoffPaths,
            ctx.nodeConcurrency,
            async (filePath) => {
              await removeFileIfExists(filePath);
            },
          ).then(
            () => null,
            (err) => err,
          ),
        );
      }
    }
  }
  const cleanupErrors = (await Promise.all(cleanupTasks)).filter(Boolean);
  if (cleanupErrors.length > 0) {
    throw cleanupErrors[0];
  }
  await enqueuePipelineStateSave(ctx, 'built', { force: true });
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
      source: 'estimated_root_simplify',
    };
  }

  return {
    value: fallbackRootGeometricError(rootBounds, rootNode.count),
    source: 'estimated_root_fallback',
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
    handoff_encoding: HANDOFF_BUCKET_ENCODING,
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
    if (
      !leaf.bucketPath ||
      !Number.isInteger(leaf.bucketRowCount) ||
      leaf.bucketRowCount <= 0 ||
      !(await pathExists(leaf.bucketPath))
    ) {
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
      console.log(`[info] reusing checkpoint | stage=${checkpointInfo.stage}`);
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
      const positionsPath = path.join(tempDir, POSITION_TMP_FILE);
      let counts = null;
      try {
        console.log(
          `[info] scan 1/4 | vertices=${header.vertexCount} | sh_degree=${layout.degree} | staging positions`,
        );
        rootBounds = await scanGlobalBoundsAndWritePositions(
          handle,
          inputPath,
          header,
          layout,
          positionsPath,
        );

        console.log('[info] scan 2/4 | building count tree from positions');
        counts = await buildCountsTableFromPositionFile(
          positionsPath,
          header.vertexCount,
          rootBounds,
          args.maxDepth,
        );
      } finally {
        await removeFileIfExists(positionsPath);
      }

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
    const lodMaxDepth = Math.max(
      0,
      Math.min(args.maxDepth, treeStats.maxLevel),
    );

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
        `[info] scan 3/4 | partitioning ${treeStats.leaves.length} leaf buckets`,
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
      pipelineState.stage = PIPELINE_STAGE_BUCKETED;
      pipelineState.updatedAt = new Date().toISOString();
      console.log(
        `[info] scan 4/4 | writing ${treeStats.leaves.length} leaf buckets`,
      );
      await fs.promises.writeFile(
        path.join(tempDir, PIPELINE_STATE_FILE),
        JSON.stringify(pipelineState),
        'utf8',
      );
    } else if (checkpointInfo.reused) {
      console.log('[info] scan 4/4 | reusing existing leaf buckets');
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
      nodeConcurrency: Math.max(1, args.buildConcurrency || 1),
      leafNodeConcurrency: Math.max(1, args.buildConcurrency || 1),
      pipelineState,
      rootBounds,
      rootNode: rootNodeMeta,
      savePromise: Promise.resolve(),
      lastPipelineStateSaveAt: Date.now(),
      nodesSincePipelineStateSave: 0,
      pendingHandoffCleanup: new Set(),
      buildProgress: new ConsoleProgressBar('build', treeStats.nodes.length),
    };

    enqueuePipelineStateSave(stateCtx, pipelineState.stage);
    ctx.savePromise = stateCtx.savePromise;
    ctx.pipelineState = stateCtx.pipelineState;

    console.log('[info] building tiles bottom-up');
    await buildPartitionedBottomUp(rootNodeMeta, ctx);
    ctx.buildProgress.done(
      `nodes=${treeStats.nodes.length} levels=${treeStats.maxLevel + 1}`,
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
    } catch {}
    if (params.contentWorkerPool) {
      await params.contentWorkerPool.close();
    }
    if (success) {
      try {
        await fs.promises.rm(tempDir, { recursive: true, force: true });
      } catch {}
    } else {
      console.error('[info] checkpoint preserved in output temp workspace');
    }
  }
}

module.exports = {
  convertPartitionedPlyTo3DTiles,
  _writeBucketGlbTaskOutput: writeBucketGlbTaskOutput,
};
