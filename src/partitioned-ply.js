const fs = require('fs');
const os = require('os');
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
  _forEachGaussianPlyCanonicalRecord,
} = require('./parser');
const {
  DEFAULT_SOURCE_COORDINATE_SYSTEM,
  detectSourceCoordinateSystemFromPlyHeader,
  sourceCoordinateSystemInfo,
} = require('./coordinates');

const {
  SPZ_STREAM_VERSION,
  SPZ_FIXED24_LIMIT,
  SPZ_FRACTIONAL_BITS,
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
  normalizeSplatTargetCount,
  constrainTargetSplatCount,
  planSimplifyCloudVoxel,
  samplingDivisorForDepth,
  geometricErrorScaleForDepth,
  rootGeometricErrorFromMinLevel,
  writeThreeSigmaExtentComponents,
} = require('./builder');

const DEFAULT_WORKER_SCRIPT = path.join(__dirname, 'convert-core.js');
const TEMP_WORKSPACE_NAME = '.tmp-ply-partitions';
const PIPELINE_STATE_FILE = 'pipeline-state.json';
const PIPELINE_STATE_VERSION = 14;
const PIPELINE_STAGE_BUCKETED = 'bucketed';
const LEAF_BUCKET_DIR = 'leaf';
const HANDOFF_BUCKET_DIR = 'handoff';
const LEAF_BUCKET_ENCODING = 'canonical32';
const HANDOFF_BUCKET_ENCODING = 'canonical32';
const BUILD_MIN_TASK_MEMORY_BYTES = 32 * 1024 * 1024;
const PARTITION_ARENA_COUNT = 2;
const PARTITION_LEAF_HANDLE_LIMIT = 256;
const SCAN_PROGRESS_ROW_INTERVAL = 8192;
const PARTITION_PROGRESS_ROW_INTERVAL = 8192;
const TILING_TREE_PROGRESS_ROW_INTERVAL = 65536;
const POSITION_TMP_FILE = 'positions.tmp';
const POSITION_ROW_BYTE_SIZE = 16;
const POSITION_INDEX_ROW_BYTE_SIZE = Uint32Array.BYTES_PER_ELEMENT;
const POSITION_TMP_BUFFER_BYTES = 256 * 1024;
const WRITEV_BATCH_CHUNKS = 1024;
const SPZ_CLOUD_ASYNC_WRITE_THRESHOLD = 4096;
const SPZ_BUCKET_ASYNC_WRITE_THRESHOLD = 4096;
const MERGE_SH_COEFF_BLOCK = 12;
const PIPELINE_STATE_SAVE_INTERVAL_MS = 5000;
const PIPELINE_STATE_SAVE_NODE_INTERVAL = 512;
const TILING_STRATEGY_KD_TREE = 'kd_tree';
const ROUTE_MODE_KD = 'kd';
const ROUTE_MODE_AXIS_SEGMENTS = 'axis_segments';
const LONG_TILE_SPLIT_MODE = 'virtual_equal_length_axis_segments';
const KD_TREE_SPLIT_DIRECTION = 'root_pca_basis_axis';
const KD_TREE_SPLIT_DIRECTION_AABB = 'aabb_axis';
const KD_TREE_COVARIANCE_WEIGHTING = 'unweighted_splat_centers';
const KD_TREE_SPLIT_PLANE = 'visual_weighted_projection_histogram_median';
const KD_TREE_SPLIT_BALANCE = 'root_pca_axis_visual_weighted';
const KD_TREE_SPLIT_BALANCE_AABB = 'aabb_axis_visual_weighted';
const TILE_BOUNDING_VOLUME_MODE_ROOT_PCA_OBB = 'root_pca_obb';
const TILE_BOUNDING_VOLUME_MODE_AABB = 'aabb';
const SPLIT_WEIGHT_FORMULA = 'max(opacity,1e-4)*radius3sigma^2';
const ADAPTIVE_SPLIT_HISTOGRAM_BINS = 64;
const ADAPTIVE_DENSE_CHILD_BUCKET_LIMIT = 4096;
const ADAPTIVE_MAX_LONG_WIDTH_RATIO = 2.0;
const ADAPTIVE_SPLIT_EPSILON = 1e-9;
const IS_LITTLE_ENDIAN = (() => {
  const probe = new Uint8Array(new Uint16Array([0x0102]).buffer);
  return probe[0] === 0x02;
})();
const GLTF_TILESET_CONTENT_EXTENSION = '3DTILES_content_gltf';
const GAUSSIAN_SPLATTING_GLTF_EXTENSIONS = [
  'KHR_gaussian_splatting',
  'KHR_gaussian_splatting_compression_spz_2',
];
const BYTES_PER_GB = 1024 * 1024 * 1024;
const BYTES_PER_MB = 1024 * 1024;
const DEFAULT_MEMORY_BUDGET_BYTES = 2 * BYTES_PER_GB;
const RUNTIME_RESERVE_FRACTION = 0.25;
const RUNTIME_RESERVE_MAX_BYTES = 512 * BYTES_PER_MB;
const DERIVED_CONCURRENCY_BYTES = 128 * 1024 * 1024;
const PARTITION_WRITE_CONCURRENCY_BYTES = 16 * 1024 * 1024;
const MAX_DERIVED_CONCURRENCY = 64;
const MAX_PARTITION_WRITE_CONCURRENCY = 256;
const MIN_STREAM_CHUNK_BYTES = 1 * BYTES_PER_MB;
const MAX_STREAM_CHUNK_BYTES = 64 * BYTES_PER_MB;
const MAX_PARTITION_ARENA_BYTES = 512 * BYTES_PER_MB;
const MAX_SIMPLIFY_SCRATCH_BYTES = 512 * BYTES_PER_MB;
const MAX_BUCKET_ENTRY_CACHE_BYTES = 512 * BYTES_PER_MB;
const MAX_DERIVED_WORKERS = 8;

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

function memoryBudgetBytesFromArgs(args) {
  const budgetGb =
    args && Number.isFinite(args.memoryBudget)
      ? args.memoryBudget
      : DEFAULT_MEMORY_BUDGET_BYTES / BYTES_PER_GB;
  return Math.max(1, Math.floor(budgetGb * BYTES_PER_GB));
}

function clampInteger(value, min, max) {
  return Math.max(min, Math.min(max, Math.floor(value)));
}

function availableWorkerLimit() {
  const detected =
    typeof os.availableParallelism === 'function'
      ? os.availableParallelism()
      : os.cpus().length;
  return Math.max(1, Math.min(MAX_DERIVED_WORKERS, detected || 1));
}

function deriveWorkerCount(memoryBudgetBytes) {
  return Math.max(
    1,
    Math.min(
      MAX_DERIVED_CONCURRENCY,
      availableWorkerLimit(),
      Math.floor(memoryBudgetBytes / DERIVED_CONCURRENCY_BYTES),
    ),
  );
}

function deriveBuildConcurrency(memoryBudgetBytes) {
  return deriveWorkerCount(memoryBudgetBytes);
}

function deriveContentWorkerCount(memoryBudgetBytes) {
  return deriveWorkerCount(memoryBudgetBytes);
}

function derivePartitionWriteConcurrency(memoryBudgetBytes) {
  return Math.max(
    1,
    Math.min(
      MAX_PARTITION_WRITE_CONCURRENCY,
      Math.floor(memoryBudgetBytes / PARTITION_WRITE_CONCURRENCY_BYTES),
    ),
  );
}

function makeMemoryBudgetPlan(args, { header = null, layout = null } = {}) {
  const budgetBytes = memoryBudgetBytesFromArgs(args);
  const runtimeReserveBytes = Math.min(
    Math.floor(budgetBytes * RUNTIME_RESERVE_FRACTION),
    RUNTIME_RESERVE_MAX_BYTES,
  );
  const usableBudgetBytes = Math.max(1, budgetBytes - runtimeReserveBytes);
  const buildConcurrency = deriveBuildConcurrency(usableBudgetBytes);
  const contentWorkers = deriveContentWorkerCount(usableBudgetBytes);
  const partitionWriteConcurrency = Math.min(
    derivePartitionWriteConcurrency(usableBudgetBytes),
    Math.max(1, contentWorkers * 8),
  );
  const streamChunkMax = Math.max(
    1,
    Math.min(MAX_STREAM_CHUNK_BYTES, Math.floor(usableBudgetBytes / 4)),
  );
  const streamChunkBytes = clampInteger(
    usableBudgetBytes / 64,
    Math.min(MIN_STREAM_CHUNK_BYTES, streamChunkMax),
    streamChunkMax,
  );
  const bucketChunkBytes = streamChunkBytes;
  const rowByteSize = layout ? layout.canonicalByteSize : null;
  const partitionArenaBytes =
    rowByteSize == null
      ? null
      : Math.max(
          rowByteSize,
          Math.min(
            MAX_PARTITION_ARENA_BYTES,
            Math.floor(usableBudgetBytes / (PARTITION_ARENA_COUNT * 2)),
          ),
        );
  const positionBytes = header
    ? header.vertexCount * POSITION_ROW_BYTE_SIZE
    : null;
  const positionIndexBytes = header
    ? header.vertexCount * POSITION_INDEX_ROW_BYTE_SIZE
    : null;
  const positionTilingBytes =
    positionBytes == null || positionIndexBytes == null
      ? null
      : positionBytes + positionIndexBytes;
  const positionScanBytes =
    positionBytes == null ? null : positionBytes + streamChunkBytes;
  const positionMemoryPeakBytes =
    positionTilingBytes == null || positionScanBytes == null
      ? null
      : Math.max(positionTilingBytes, positionScanBytes);
  const positionFloatCount = header ? header.vertexCount * 4 : null;
  const positionTmpBufferMax = Math.max(
    POSITION_ROW_BYTE_SIZE,
    Math.min(8 * BYTES_PER_MB, Math.floor(usableBudgetBytes / 16)),
  );
  const positionTmpBufferBytes = clampInteger(
    usableBudgetBytes / 1024,
    Math.min(POSITION_TMP_BUFFER_BYTES, positionTmpBufferMax),
    positionTmpBufferMax,
  );
  const simplifyScratchMax = Math.max(
    1,
    Math.min(
      MAX_SIMPLIFY_SCRATCH_BYTES,
      Math.floor(usableBudgetBytes / Math.max(1, buildConcurrency)),
    ),
  );
  const simplifyScratchBytes = clampInteger(
    usableBudgetBytes / Math.max(1, buildConcurrency * 8),
    Math.min(1 * BYTES_PER_MB, simplifyScratchMax),
    simplifyScratchMax,
  );
  const bucketEntryCacheMax = Math.max(
    0,
    Math.min(
      MAX_BUCKET_ENTRY_CACHE_BYTES,
      Math.floor(usableBudgetBytes / Math.max(1, buildConcurrency * 2)),
    ),
  );
  const bucketEntryCacheBytes = clampInteger(
    usableBudgetBytes / Math.max(1, buildConcurrency * 4),
    0,
    bucketEntryCacheMax,
  );

  return {
    budgetBytes,
    runtimeReserveBytes,
    usableBudgetBytes,
    buildConcurrency,
    contentWorkers,
    partitionWriteConcurrency,
    scanChunkBytes: streamChunkBytes,
    bucketChunkBytes,
    positionBytes,
    positionIndexBytes,
    positionTilingBytes,
    positionScanBytes,
    positionMemoryPeakBytes,
    inMemoryPositions:
      positionMemoryPeakBytes == null
        ? null
        : positionMemoryPeakBytes <= usableBudgetBytes &&
          positionFloatCount <= 0x7fffffff,
    positionTmpBufferBytes,
    partitionArenaBytes,
    simplifyScratchBytes,
    bucketEntryCacheBytes,
  };
}

function serializeMemoryBudgetPlan(plan) {
  if (!plan) {
    return null;
  }
  return {
    budget_bytes: plan.budgetBytes,
    runtime_reserve_bytes: plan.runtimeReserveBytes,
    usable_budget_bytes: plan.usableBudgetBytes,
    scan_chunk_bytes: plan.scanChunkBytes,
    bucket_chunk_bytes: plan.bucketChunkBytes,
    partition_arena_bytes: plan.partitionArenaBytes,
    in_memory_positions: plan.inMemoryPositions,
    position_bytes: plan.positionBytes,
    position_index_bytes: plan.positionIndexBytes,
    position_tiling_bytes: plan.positionTilingBytes,
    position_scan_bytes: plan.positionScanBytes,
    position_memory_peak_bytes: plan.positionMemoryPeakBytes,
    position_tmp_buffer_bytes: plan.positionTmpBufferBytes,
    simplify_scratch_bytes: plan.simplifyScratchBytes,
    bucket_entry_cache_bytes: plan.bucketEntryCacheBytes,
    build_concurrency: plan.buildConcurrency,
    build_worker_limit: availableWorkerLimit(),
    content_workers: plan.contentWorkers,
    content_worker_limit: availableWorkerLimit(),
    partition_write_concurrency: plan.partitionWriteConcurrency,
  };
}

function currentPeakRssBytes() {
  if (typeof process.resourceUsage === 'function') {
    const usage = process.resourceUsage();
    if (usage && Number.isFinite(usage.maxRSS) && usage.maxRSS > 0) {
      return usage.maxRSS * 1024;
    }
  }
  return process.memoryUsage().rss;
}

function elapsedMsSince(startMs) {
  return Math.max(0, Date.now() - startMs);
}

function useOrientedBoundingBoxes(args) {
  return !args || args.orientedBoundingBoxes !== false;
}

function tileBoundingVolumeModeForArgs(args) {
  return useOrientedBoundingBoxes(args)
    ? TILE_BOUNDING_VOLUME_MODE_ROOT_PCA_OBB
    : TILE_BOUNDING_VOLUME_MODE_AABB;
}

function kdTreeSplitDirectionForArgs(args) {
  return useOrientedBoundingBoxes(args)
    ? KD_TREE_SPLIT_DIRECTION
    : KD_TREE_SPLIT_DIRECTION_AABB;
}

function kdTreeSplitBalanceForArgs(args) {
  return useOrientedBoundingBoxes(args)
    ? KD_TREE_SPLIT_BALANCE
    : KD_TREE_SPLIT_BALANCE_AABB;
}

function splitBasisLabelForArgs(args) {
  return useOrientedBoundingBoxes(args) ? 'root-basis' : 'AABB-axis';
}

function applyRootTransform(root, transform) {
  if (!transform) {
    return root;
  }
  root.transform = transform.slice();
  return root;
}

function applyContentBoxTransform(box, sourceCoordinateSystem = null) {
  if (!Array.isArray(box) || box.length !== 12) {
    return box;
  }

  const sourceToTileZUp = sourceCoordinateSystemInfo(
    sourceCoordinateSystem,
  ).sourceToTileZUp;
  const out = box.slice();
  for (const base of [0, 3, 6, 9]) {
    const x = out[base];
    const y = out[base + 1];
    const z = out[base + 2];
    out[base] =
      sourceToTileZUp[0][0] * x +
      sourceToTileZUp[0][1] * y +
      sourceToTileZUp[0][2] * z;
    out[base + 1] =
      sourceToTileZUp[1][0] * x +
      sourceToTileZUp[1][1] * y +
      sourceToTileZUp[1][2] * z;
    out[base + 2] =
      sourceToTileZUp[2][0] * x +
      sourceToTileZUp[2][1] * y +
      sourceToTileZUp[2][2] * z;
  }
  return out;
}

function makeNodeKey(level, x, y, z) {
  return `${level}/${x}/${y}/${z}`;
}

function defaultSplitPointForBounds(bounds) {
  const min = bounds.minimum;
  const max = bounds.maximum;
  return [
    (min[0] + max[0]) * 0.5,
    (min[1] + max[1]) * 0.5,
    (min[2] + max[2]) * 0.5,
  ];
}

function pointOctant(bounds, x, y, z, splitPoint = null, splitAxes = null) {
  const split = splitPoint || defaultSplitPointForBounds(bounds);
  const axes = splitAxes || [true, true, true];
  return (
    (axes[0] && x >= split[0] ? 1 : 0) |
    (axes[1] && y >= split[1] ? 2 : 0) |
    (axes[2] && z >= split[2] ? 4 : 0)
  );
}

function pointOctantForNode(node, x, y, z) {
  return pointOctant(node.bounds, x, y, z, node.splitPoint, node.splitAxes);
}

function dotDirection(direction, x, y, z) {
  return direction[0] * x + direction[1] * y + direction[2] * z;
}

function pointPlaneSlot(splitDirection, splitOffset, x, y, z) {
  return dotDirection(splitDirection, x, y, z) >= splitOffset ? 1 : 0;
}

function pointKdSlotForNode(node, x, y, z) {
  if (node.splitDirection && Number.isFinite(node.splitOffset)) {
    return pointPlaneSlot(node.splitDirection, node.splitOffset, x, y, z);
  }
  return pointOctantForNode(node, x, y, z);
}

function coordinateForAxis(axis, x, y, z) {
  return axis === 1 ? y : axis === 2 ? z : x;
}

function coordinateForDirectionOrAxis(direction, axis, x, y, z) {
  return direction
    ? dotDirection(direction, x, y, z)
    : coordinateForAxis(axis, x, y, z);
}

function pointSegmentForNode(node, x, y, z) {
  const axis = Number.isInteger(node.segmentAxis) ? node.segmentAxis : 0;
  const segmentCount =
    Number.isInteger(node.segmentCount) && node.segmentCount > 0
      ? node.segmentCount
      : 1;
  const min = Number.isFinite(node.segmentMin)
    ? node.segmentMin
    : node.bounds.minimum[axis];
  const max = Number.isFinite(node.segmentMax)
    ? node.segmentMax
    : node.bounds.maximum[axis];
  const extent = max - min;
  if (!Number.isFinite(extent) || extent <= 0.0 || segmentCount <= 1) {
    return 0;
  }
  const coordinate = coordinateForDirectionOrAxis(
    node.segmentDirection,
    axis,
    x,
    y,
    z,
  );
  const slot = Math.floor(
    ((coordinate - min) / extent) * segmentCount,
  );
  return Math.max(0, Math.min(segmentCount - 1, slot));
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

function normalizeBoxArray(box) {
  if (!Array.isArray(box) || box.length !== 12) {
    return null;
  }
  const out = box.map((value) => Number(value));
  return out.every((value) => Number.isFinite(value)) ? out : null;
}

function normalizeOrientedBox(box) {
  const normalized = normalizeBoxArray(box);
  return normalized && normalized.length === 12 ? normalized : null;
}

function normalizeSplitPoint(splitPoint) {
  return Array.isArray(splitPoint) && splitPoint.length === 3
    ? splitPoint.map((value) => Number(value))
    : null;
}

function normalizeSplitAxes(splitAxes) {
  return Array.isArray(splitAxes) && splitAxes.length === 3
    ? splitAxes.map((value) => !!value)
    : null;
}

function normalizeVector3(values) {
  if (!Array.isArray(values) && !ArrayBuffer.isView(values)) {
    return null;
  }
  if (values.length !== 3) {
    return null;
  }
  const out = Array.from(values, Number);
  const length = Math.sqrt(
    out[0] * out[0] + out[1] * out[1] + out[2] * out[2],
  );
  if (!Number.isFinite(length) || length <= ADAPTIVE_SPLIT_EPSILON) {
    return null;
  }
  out[0] /= length;
  out[1] /= length;
  out[2] /= length;
  return out;
}

function normalizeSplitDirection(splitDirection) {
  return normalizeVector3(splitDirection);
}

function normalizeSplitOffset(splitOffset) {
  return Number.isFinite(splitOffset) ? Number(splitOffset) : null;
}

function usesSegmentRouting(node) {
  return (
    !!node && (node.routeMode === ROUTE_MODE_AXIS_SEGMENTS || !!node.virtual)
  );
}

function ensureChildrenBySegment(node) {
  if (!node.childrenBySegment) {
    node.childrenBySegment = new Map();
  }
  return node.childrenBySegment;
}

function makePartitionTreeNode({
  level,
  depth = level,
  x,
  y,
  z,
  bounds,
  orientedBox = null,
  count,
  leaf = true,
  splitPoint = null,
  splitAxes = null,
  splitDirection = null,
  splitOffset = null,
  childSlot = null,
  virtual = false,
  routeMode = ROUTE_MODE_KD,
  segmentAxis = null,
  segmentDirection = null,
  segmentCount = null,
  segmentMin = null,
  segmentMax = null,
  contentTargetOverride = null,
}) {
  return {
    key: makeNodeKey(level, x, y, z),
    level,
    depth,
    x,
    y,
    z,
    childSlot,
    count,
    bounds,
    orientedBox: normalizeOrientedBox(orientedBox),
    leaf,
    virtual: !!virtual,
    routeMode,
    splitPoint: normalizeSplitPoint(splitPoint),
    splitAxes: normalizeSplitAxes(splitAxes),
    splitDirection: normalizeSplitDirection(splitDirection),
    splitOffset: normalizeSplitOffset(splitOffset),
    segmentAxis,
    segmentDirection: normalizeSplitDirection(segmentDirection),
    segmentCount,
    segmentMin,
    segmentMax,
    contentTargetOverride,
    tilingStrategy: TILING_STRATEGY_KD_TREE,
    children: [],
    childrenByOct: new Array(8).fill(null),
    childrenBySegment: usesSegmentRouting({ routeMode, virtual })
      ? new Map()
      : null,
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
}

function serializeNodeMeta(node) {
  const meta = {
    key: node.key,
    level: node.level,
    x: node.x,
    y: node.y,
    z: node.z,
    count: node.count,
    bounds: serializeBoundsState(node.bounds),
  };
  if (node.orientedBox) {
    meta.orientedBox = node.orientedBox.slice();
  }
  if (node.depth !== node.level) {
    meta.depth = node.depth;
  }
  if (Number.isInteger(node.childSlot)) {
    meta.childSlot = node.childSlot;
  }
  if (node.leaf === false) {
    meta.leaf = false;
  }
  if (node.virtual) {
    meta.virtual = true;
  }
  const routeMode = node.routeMode || ROUTE_MODE_KD;
  if (routeMode !== ROUTE_MODE_KD) {
    meta.routeMode = routeMode;
  }
  if (node.splitPoint) {
    meta.splitPoint = node.splitPoint.slice();
  }
  if (node.splitAxes) {
    meta.splitAxes = node.splitAxes.slice();
  }
  if (node.splitDirection) {
    meta.splitDirection = node.splitDirection.slice();
  }
  if (Number.isFinite(node.splitOffset)) {
    meta.splitOffset = node.splitOffset;
  }
  if (Number.isInteger(node.segmentAxis)) {
    meta.segmentAxis = node.segmentAxis;
  }
  if (node.segmentDirection) {
    meta.segmentDirection = node.segmentDirection.slice();
  }
  if (Number.isInteger(node.segmentCount) && node.segmentCount > 0) {
    meta.segmentCount = node.segmentCount;
  }
  if (Number.isFinite(node.segmentMin)) {
    meta.segmentMin = node.segmentMin;
  }
  if (Number.isFinite(node.segmentMax)) {
    meta.segmentMax = node.segmentMax;
  }
  if (
    Number.isFinite(node.contentTargetOverride) &&
    node.contentTargetOverride > 0
  ) {
    meta.contentTargetOverride = node.contentTargetOverride;
  }
  const tilingStrategy = node.tilingStrategy || TILING_STRATEGY_KD_TREE;
  if (tilingStrategy !== TILING_STRATEGY_KD_TREE) {
    meta.tilingStrategy = tilingStrategy;
  }
  if (
    Number.isInteger(node.occupiedChildCount) &&
    node.occupiedChildCount > 0
  ) {
    meta.occupiedChildCount = node.occupiedChildCount;
  }
  if (node.bucketPath) {
    meta.bucketPath = node.bucketPath;
  }
  if (node.contentUri) {
    meta.contentUri = node.contentUri;
  }
  if (node.handoffPath) {
    meta.handoffPath = node.handoffPath;
  }
  if (Number.isFinite(node.bucketRowCount) && node.bucketRowCount >= 0) {
    meta.bucketRowCount = node.bucketRowCount;
  }
  if (Number.isFinite(node.handoffRowCount) && node.handoffRowCount >= 0) {
    meta.handoffRowCount = node.handoffRowCount;
  }
  if (node.handoffConsumed) {
    meta.handoffConsumed = true;
  }
  if (Number.isFinite(node.ownError) || node.ownError === 0) {
    meta.ownError = node.ownError;
  }
  if (node.buildState && node.buildState !== 'pending') {
    meta.buildState = node.buildState;
  }
  if (node.children.length > 0) {
    meta.children = node.children.map((child) => serializeNodeMeta(child));
  }
  return meta;
}

function deserializeNodeMeta(data) {
  const node = {
    key: data.key,
    level: data.level,
    depth: Number.isInteger(data.depth) ? data.depth : data.level,
    x: data.x,
    y: data.y,
    z: data.z,
    childSlot: Number.isInteger(data.childSlot) ? data.childSlot : null,
    count: data.count,
    bounds: deserializeBoundsState(data.bounds),
    orientedBox: normalizeOrientedBox(data.orientedBox),
    leaf: data.leaf !== false,
    virtual: !!data.virtual,
    routeMode: data.routeMode ?? ROUTE_MODE_KD,
    splitPoint: normalizeSplitPoint(data.splitPoint),
    splitAxes: normalizeSplitAxes(data.splitAxes),
    splitDirection: normalizeSplitDirection(data.splitDirection),
    splitOffset: normalizeSplitOffset(data.splitOffset),
    segmentAxis: Number.isInteger(data.segmentAxis) ? data.segmentAxis : null,
    segmentDirection: normalizeSplitDirection(data.segmentDirection),
    segmentCount:
      Number.isInteger(data.segmentCount) && data.segmentCount > 0
        ? data.segmentCount
        : null,
    segmentMin: Number.isFinite(data.segmentMin) ? data.segmentMin : null,
    segmentMax: Number.isFinite(data.segmentMax) ? data.segmentMax : null,
    contentTargetOverride:
      Number.isFinite(data.contentTargetOverride) &&
      data.contentTargetOverride > 0
        ? data.contentTargetOverride
        : null,
    tilingStrategy: data.tilingStrategy ?? TILING_STRATEGY_KD_TREE,
    children: [],
    childrenByOct: new Array(8).fill(null),
    childrenBySegment: null,
    occupiedChildCount: Number.isInteger(data.occupiedChildCount)
      ? data.occupiedChildCount
      : 0,
    bucketPath: data.bucketPath ?? null,
    contentUri: data.contentUri ?? null,
    handoffPath: data.handoffPath ?? null,
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
    buildState: data.buildState ?? 'pending',
  };
  if (usesSegmentRouting(node)) {
    node.childrenBySegment = new Map();
  }
  if (Array.isArray(data.children)) {
    for (const childData of data.children) {
      const child = deserializeNodeMeta(childData);
      node.children.push(child);
      const slot =
        Number.isInteger(child.childSlot) && child.childSlot >= 0
          ? child.childSlot
          : ((child.x & 1) << 0) | ((child.y & 1) << 1) | ((child.z & 1) << 2);
      if (usesSegmentRouting(node)) {
        ensureChildrenBySegment(node).set(slot, child);
      } else {
        node.childrenByOct[slot] = child;
      }
    }
  }
  if (!Number.isInteger(data.occupiedChildCount) && node.children.length > 0) {
    node.occupiedChildCount = node.children.length;
  }
  return node;
}

function collectTreeStats(node) {
  const nodes = [];
  const leaves = [];
  const virtualNodes = [];
  let maxLevel = 0;
  let maxDepth = 0;
  const levels = [];
  const visit = (current) => {
    nodes.push(current);
    if (current.virtual) {
      virtualNodes.push(current);
    }
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
    if (current.depth > maxDepth) {
      maxDepth = current.depth;
    }
    for (const child of current.children) {
      visit(child);
    }
  };
  visit(node);
  return { nodes, leaves, virtualNodes, maxLevel, maxDepth, levels };
}

function resolveLeafNodeForPoint(root, x, y, z) {
  let node = root;
  while (!node.leaf) {
    let slot;
    let child;
    if (usesSegmentRouting(node)) {
      slot = pointSegmentForNode(node, x, y, z);
      child = ensureChildrenBySegment(node).get(slot);
    } else {
      slot = pointKdSlotForNode(node, x, y, z);
      child = node.childrenByOct[slot];
    }
    ensure(
      !!child,
      `Failed to resolve leaf bucket for point at node ${node.key} slot=${slot}.`,
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

function makePipelineFingerprint(
  inputPath,
  inputStat,
  args,
  sourceCoordinateSystem,
) {
  return {
    inputPath: path.resolve(inputPath),
    inputSize: inputStat.size,
    inputMtimeMs: inputStat.mtimeMs,
    inputConvention: args.inputConvention,
    linearScaleInput: args.linearScaleInput,
    sourceCoordinateSystem:
      sourceCoordinateSystem || DEFAULT_SOURCE_COORDINATE_SYSTEM,
    maxDepth: args.maxDepth,
    leafLimit: args.leafLimit,
    tilingStrategy: TILING_STRATEGY_KD_TREE,
    kdTreeSplitDirection: kdTreeSplitDirectionForArgs(args),
    kdTreeCovarianceWeighting: KD_TREE_COVARIANCE_WEIGHTING,
    kdTreeSplitPlane: KD_TREE_SPLIT_PLANE,
    kdTreeSplitBalance: kdTreeSplitBalanceForArgs(args),
    orientedBoundingBoxes: useOrientedBoundingBoxes(args),
    tileBoundingVolumeMode: tileBoundingVolumeModeForArgs(args),
    splitWeightFormula: SPLIT_WEIGHT_FORMULA,
    kdTreeSplitHistogramBins: ADAPTIVE_SPLIT_HISTOGRAM_BINS,
    kdTreeMaxLongWidthRatio: ADAPTIVE_MAX_LONG_WIDTH_RATIO,
    kdTreeLongTileSplitMode: LONG_TILE_SPLIT_MODE,
    samplingRatePerLevel: args.samplingRatePerLevel,
    sampleMode: args.sampleMode,
    minGeometricError: args.minGeometricError,
    colorSpace: args.colorSpace,
    spzSh1Bits: args.spzSh1Bits,
    spzShRestBits: args.spzShRestBits,
    spzCompressionLevel: args.spzCompressionLevel,
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
  return { kind: 'file', filePath, encoding, rowCount };
}

function makeBucketAggregateSpec(sources) {
  const resolvedSources = sources.filter(Boolean);
  return {
    kind: 'aggregate',
    sources: resolvedSources,
    rowCount: resolvedSources.reduce(
      (sum, source) =>
        sum + (Number.isInteger(source.rowCount) ? source.rowCount : 0),
      0,
    ),
  };
}

function leafBucketSpec(node) {
  return makeBucketFileSpec(
    node.bucketPath,
    LEAF_BUCKET_ENCODING,
    node.bucketRowCount,
  );
}

function fileHandoffBucketSpec(node) {
  return makeBucketFileSpec(
    node.handoffPath,
    HANDOFF_BUCKET_ENCODING,
    node.handoffRowCount,
  );
}

function isActiveHandoffSource(node) {
  return (
    !!node &&
    !node.handoffConsumed &&
    Number.isInteger(node.handoffRowCount) &&
    node.handoffRowCount >= 0
  );
}

function collectActiveHandoffSourceSpecs(node, out) {
  if (!node) {
    return;
  }
  if (!node.virtual) {
    ensure(
      isActiveHandoffSource(node) && !!node.handoffPath,
      `Missing active handoff for node ${node.key}.`,
    );
    out.push(fileHandoffBucketSpec(node));
    return;
  }
  if (node.handoffPath) {
    ensure(
      isActiveHandoffSource(node),
      `Missing active materialized handoff for virtual node ${node.key}.`,
    );
    out.push(fileHandoffBucketSpec(node));
    return;
  }
  for (const child of node.children) {
    collectActiveHandoffSourceSpecs(child, out);
  }
}

function handoffBucketSpec(node) {
  if (!node.virtual) {
    ensure(
      isActiveHandoffSource(node) && !!node.handoffPath,
      `Missing active handoff for node ${node.key}.`,
    );
    return fileHandoffBucketSpec(node);
  }
  if (node.handoffPath) {
    ensure(
      isActiveHandoffSource(node),
      `Missing active materialized handoff for virtual node ${node.key}.`,
    );
    return fileHandoffBucketSpec(node);
  }
  const sources = [];
  collectActiveHandoffSourceSpecs(node, sources);
  ensure(
    sources.length > 0,
    `Missing active handoff sources for virtual node ${node.key}.`,
  );
  return makeBucketAggregateSpec(sources);
}

function flattenBucketSpec(fileSpec, out) {
  if (!fileSpec) {
    return;
  }
  if (fileSpec.kind === 'aggregate') {
    for (const source of fileSpec.sources || []) {
      flattenBucketSpec(source, out);
    }
    return;
  }
  out.push(fileSpec);
}

function flattenBucketSpecs(fileSpecs) {
  const flattened = [];
  for (const fileSpec of fileSpecs) {
    flattenBucketSpec(fileSpec, flattened);
  }
  return flattened;
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

async function writeArenaRunsToHandle(handle, filePath, sourceBuffer, entry) {
  if (!entry || entry.byteLength <= 0 || entry.runs.length === 0) {
    return;
  }

  if (entry.runs.length === 2) {
    const start = entry.runs[0];
    const byteLength = entry.runs[1];
    await writeBufferToHandle(
      handle,
      sourceBuffer.subarray(start, start + byteLength),
      byteLength,
      filePath,
    );
    return;
  }

  const compacted = Buffer.allocUnsafe(entry.byteLength);
  let writeOffset = 0;
  for (let i = 0; i < entry.runs.length; i += 2) {
    const start = entry.runs[i];
    const byteLength = entry.runs[i + 1];
    sourceBuffer.copy(compacted, writeOffset, start, start + byteLength);
    writeOffset += byteLength;
  }
  await writeBufferToHandle(handle, compacted, compacted.length, filePath);
}

async function partitionLeafBuckets(
  handle,
  filePath,
  header,
  layout,
  rootNode,
  tempDir,
  options = {},
) {
  const touchedLeaves = [];
  const ensuredDirs = new Set();
  const leafHandles = new Map();
  const progress = options.progress || null;
  const rowByteSize = layout.canonicalByteSize;
  const memoryPlan = options.memoryPlan || null;
  const memoryBudgetBytes = memoryPlan
    ? memoryPlan.usableBudgetBytes
    : Number.isFinite(options.memoryBudgetBytes) &&
        options.memoryBudgetBytes > 0
      ? Math.floor(options.memoryBudgetBytes)
      : DEFAULT_MEMORY_BUDGET_BYTES;
  const writeConcurrency =
    Number.isInteger(options.writeConcurrency) && options.writeConcurrency > 0
      ? options.writeConcurrency
      : memoryPlan
        ? memoryPlan.partitionWriteConcurrency
        : derivePartitionWriteConcurrency(memoryBudgetBytes);
  // Budget two read arenas plus two worst-case compaction buffers during flush.
  const arenaByteSize =
    memoryPlan && memoryPlan.partitionArenaBytes != null
      ? memoryPlan.partitionArenaBytes
      : Math.max(
          rowByteSize,
          Math.min(
            MAX_PARTITION_ARENA_BYTES,
            Math.floor(memoryBudgetBytes / (PARTITION_ARENA_COUNT * 2)),
          ),
        );
  const arenas = Array.from({ length: PARTITION_ARENA_COUNT }, () => ({
    buffer: Buffer.allocUnsafe(arenaByteSize),
    offset: 0,
    activeLeaves: [],
    leafRuns: new Map(),
    flushPromise: null,
  }));
  let activeArenaIndex = 0;
  let flushError = null;
  let processedRows = 0;

  const updateProgress = (force = false) => {
    if (!progress) {
      return;
    }
    if (!force && processedRows % PARTITION_PROGRESS_ROW_INTERVAL !== 0) {
      return;
    }
    progress.update(processedRows);
  };

  const tickProgress = () => {
    processedRows += 1;
    updateProgress();
  };

  const closeLeafHandleEntry = async (leaf, entry) => {
    leafHandles.delete(leaf);
    await entry.fh.close();
  };

  const evictLeafHandleIfNeeded = async () => {
    while (leafHandles.size >= PARTITION_LEAF_HANDLE_LIMIT) {
      let evicted = false;
      for (const [leaf, entry] of leafHandles) {
        if (entry.active > 0) {
          continue;
        }
        await closeLeafHandleEntry(leaf, entry);
        evicted = true;
        break;
      }
      if (evicted) {
        return;
      }
      await new Promise((resolve) => setImmediate(resolve));
    }
  };

  const acquireLeafHandle = async (leaf) => {
    let entry = leafHandles.get(leaf);
    if (entry) {
      leafHandles.delete(leaf);
      leafHandles.set(leaf, entry);
      entry.active += 1;
      return entry;
    }
    const dir = path.dirname(leaf.bucketPath);
    if (!ensuredDirs.has(dir)) {
      await fs.promises.mkdir(dir, { recursive: true });
      ensuredDirs.add(dir);
    }
    await evictLeafHandleIfNeeded();
    entry = {
      fh: await fs.promises.open(leaf.bucketPath, 'a'),
      active: 1,
    };
    leafHandles.set(leaf, entry);
    return entry;
  };

  const releaseLeafHandle = (entry) => {
    entry.active -= 1;
    ensure(entry.active >= 0, 'Partition leaf handle released too many times.');
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
    arena.leafRuns = new Map();
  };

  const scheduleArenaFlush = (arena) => {
    if (arena.flushPromise) {
      return arena.flushPromise;
    }
    if (arena.leafRuns.size === 0) {
      arena.offset = 0;
      return Promise.resolve();
    }

    const writeTasks = [];
    for (const leaf of arena.activeLeaves) {
      const runEntry = arena.leafRuns.get(leaf);
      if (!runEntry || runEntry.byteLength <= 0) {
        continue;
      }
      let startWrite;
      const startPromise = new Promise((resolve) => {
        startWrite = resolve;
      });
      const writePromise = leaf._partitionWriteChain
        .then(() => startPromise)
        .then(async () => {
          const entry = await acquireLeafHandle(leaf);
          try {
            await writeArenaRunsToHandle(
              entry.fh,
              leaf.bucketPath,
              arena.buffer,
              runEntry,
            );
          } finally {
            releaseLeafHandle(entry);
          }
        });
      leaf._partitionWriteChain = writePromise;
      writePromise.catch(() => {});
      writeTasks.push({ startWrite, writePromise });
    }

    arena.flushPromise = runWithConcurrency(
      writeTasks,
      writeConcurrency,
      async (task) => {
        task.startWrite();
        try {
          await task.writePromise;
        } catch (err) {
          if (!flushError) {
            flushError = err;
          }
        }
      },
    )
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
    activeArenaIndex = (activeArenaIndex + 1) % arenas.length;
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

    let runEntry = arena.leafRuns.get(leaf);
    if (!runEntry) {
      runEntry = {
        runs: [],
        byteLength: 0,
      };
      arena.leafRuns.set(leaf, runEntry);
      arena.activeLeaves.push(leaf);
    }
    const lastRunIndex = runEntry.runs.length - 2;
    if (
      lastRunIndex >= 0 &&
      runEntry.runs[lastRunIndex] + runEntry.runs[lastRunIndex + 1] ===
        arena.offset
    ) {
      runEntry.runs[lastRunIndex + 1] += rowByteSize;
    } else {
      runEntry.runs.push(arena.offset, rowByteSize);
    }
    runEntry.byteLength += rowByteSize;

    rowBuffer.copy(arena.buffer, arena.offset, 0, rowByteSize);
    arena.offset += rowByteSize;
    leaf.bucketRowCount = (leaf.bucketRowCount || 0) + 1;
    return null;
  };

  try {
    updateProgress(true);
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
        const appendResult = appendLeafRow(leaf, rowBuffer);
        if (appendResult && typeof appendResult.then === 'function') {
          return appendResult.then(tickProgress);
        }
        tickProgress();
        return null;
      },
      { chunkBytes: memoryPlan ? memoryPlan.scanChunkBytes : undefined },
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
    updateProgress(true);
  } finally {
    await Promise.all(
      arenas.map((arena) => arena.flushPromise).filter((promise) => !!promise),
    );
    await Promise.all(
      Array.from(leafHandles.values()).map((entry) => entry.fh.close()),
    );
    leafHandles.clear();

    for (const leaf of touchedLeaves) {
      delete leaf._partitionTouched;
      delete leaf._partitionWriteChain;
    }
  }
  arenas.length = 0;
  return {
    rowCount: processedRows,
    leafCount: touchedLeaves.length,
  };
}

async function collectBucketEntries(fileSpecs, coeffCount) {
  const entries = [];
  let totalRows = 0;
  for (const fileSpec of flattenBucketSpecs(fileSpecs)) {
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

async function forEachBucketSpecRow(fileSpec, coeffCount, onRow, options = {}) {
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

  if (Buffer.isBuffer(fileSpec.buffer)) {
    ensure(
      fileSpec.buffer.length >= totalBytes,
      `Cached bucket buffer is smaller than expected: ${fileSpec.filePath}`,
    );
    const view = new DataView(
      fileSpec.buffer.buffer,
      fileSpec.buffer.byteOffset,
      totalBytes,
    );
    const floatView =
      IS_LITTLE_ENDIAN && (fileSpec.buffer.byteOffset & 3) === 0
        ? new Float32Array(
            fileSpec.buffer.buffer,
            fileSpec.buffer.byteOffset,
            totalBytes >>> 2,
          )
        : null;
    for (let offset = 0; offset < totalBytes; offset += rowByteSize) {
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
    return;
  }

  const targetChunkBytes =
    Number.isFinite(options.chunkBytes) && options.chunkBytes > 0
      ? Math.floor(options.chunkBytes)
      : 8 * 1024 * 1024;
  const rowsPerChunk = Math.max(1, Math.floor(targetChunkBytes / rowByteSize));
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

async function forEachBucketEntryRow(entries, coeffCount, onRow, options = {}) {
  for (const entry of entries) {
    await forEachBucketSpecRow(entry, coeffCount, onRow, options);
  }
}

async function cacheBucketEntriesIfAffordable(
  entries,
  coeffCount,
  cacheBudgetBytes,
) {
  if (!Number.isFinite(cacheBudgetBytes) || cacheBudgetBytes <= 0) {
    return entries;
  }

  let totalBytes = 0;
  for (const entry of entries) {
    if (Buffer.isBuffer(entry.buffer)) {
      return entries;
    }
    const rowByteSize =
      entry.rowByteSize || bucketRowByteSize(entry.encoding, coeffCount);
    totalBytes += entry.rowCount * rowByteSize;
    if (totalBytes > cacheBudgetBytes) {
      return entries;
    }
  }

  const cached = [];
  for (const entry of entries) {
    const rowByteSize =
      entry.rowByteSize || bucketRowByteSize(entry.encoding, coeffCount);
    const expectedBytes = entry.rowCount * rowByteSize;
    const buffer = await fs.promises.readFile(entry.filePath);
    ensure(
      buffer.length >= expectedBytes,
      `Bucket file ended early while caching: ${entry.filePath}`,
    );
    cached.push({
      ...entry,
      rowByteSize,
      buffer:
        buffer.length === expectedBytes
          ? buffer
          : buffer.subarray(0, expectedBytes),
    });
  }
  return cached;
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

async function computeBucketEntriesBounds(entries, coeffCount, options = {}) {
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
    { chunkBytes: options.bucketChunkBytes },
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
  options = {},
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
      if (
        floatView &&
        (encoding === LEAF_BUCKET_ENCODING ||
          encoding === HANDOFF_BUCKET_ENCODING)
      ) {
        const src = floatView;
        const off = floatBase;
        const localX = translation
          ? Math.fround(src[off + 0] - tx)
          : src[off + 0];
        const posBase = layout.positionsOffset + rowIndex * 9;
        writeFixed24Into(packet, posBase + 0, quantizeSpzPosition(localX));

        const localY = translation
          ? Math.fround(src[off + 1] - ty)
          : src[off + 1];
        writeFixed24Into(packet, posBase + 3, quantizeSpzPosition(localY));

        const localZ = translation
          ? Math.fround(src[off + 2] - tz)
          : src[off + 2];
        writeFixed24Into(packet, posBase + 6, quantizeSpzPosition(localZ));

        packet[layout.opacityOffset + rowIndex] = quantizeSpzOpacity(
          src[off + 10],
        );

        const colorBase = layout.colorOffset + rowIndex * 3;
        packet[colorBase + 0] = quantizeSpzColor(src[off + 11]);
        packet[colorBase + 1] = quantizeSpzColor(src[off + 12]);
        packet[colorBase + 2] = quantizeSpzColor(src[off + 13]);

        const scaleBase = layout.scaleOffset + rowIndex * 3;
        packet[scaleBase + 0] = quantizeSpzScale(src[off + 3]);
        packet[scaleBase + 1] = quantizeSpzScale(src[off + 4]);
        packet[scaleBase + 2] = quantizeSpzScale(src[off + 5]);

        scratch.quat[0] = src[off + 6];
        scratch.quat[1] = src[off + 7];
        scratch.quat[2] = src[off + 8];
        scratch.quat[3] = src[off + 9];
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
            const coeffBase = off + 11 + coeff * 3;
            packet[shBase++] = quantizeSpzExtraSh(
              src[coeffBase + 0],
              bucket,
              halfBucket,
              invBucket,
            );
            packet[shBase++] = quantizeSpzExtraSh(
              src[coeffBase + 1],
              bucket,
              halfBucket,
              invBucket,
            );
            packet[shBase++] = quantizeSpzExtraSh(
              src[coeffBase + 2],
              bucket,
              halfBucket,
              invBucket,
            );
          }
        }

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
    { chunkBytes: options.bucketChunkBytes },
  );
  ensure(
    rowIndex === n,
    `Bucket row count changed while packing SPZ: expected ${n}, read ${rowIndex}.`,
  );

  writeSpzPacketHeader(packet, n, shDegree);
  return gzipSpzPacket(packet, packet.length, options);
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
  options = {},
) {
  const rowByteSize =
    entry.rowByteSize || bucketRowByteSize(entry.encoding, coeffCount);
  const targetChunkBytes =
    Number.isFinite(options.bucketChunkBytes) && options.bucketChunkBytes > 0
      ? Math.floor(options.bucketChunkBytes)
      : 8 * 1024 * 1024;
  const rowsPerChunk = Math.max(1, Math.floor(targetChunkBytes / rowByteSize));
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
  options = {},
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
      await appendBucketEntryToHandle(
        entry,
        coeffCount,
        handle,
        targetPath,
        options,
      );
    }
  } finally {
    await handle.close();
  }
}

async function loadBucketCloudFromEntries(
  entries,
  coeffCount,
  totalRows = null,
  options = {},
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
    { chunkBytes: options.bucketChunkBytes },
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

async function writeCanonicalCloudFile(filePath, cloud, options = {}) {
  ensure(cloud.length > 0, 'Cannot write an empty handoff cloud.');
  await fs.promises.mkdir(path.dirname(filePath), { recursive: true });
  const coeffCount = cloud.shCoeffs.length / (cloud.length * 3);
  const coeffStride = coeffCount * 3;
  const rowByteSize = _canonicalGaussianRowByteSize(coeffCount);
  const targetChunkBytes =
    Number.isFinite(options.bucketChunkBytes) && options.bucketChunkBytes > 0
      ? Math.floor(options.bucketChunkBytes)
      : 8 * 1024 * 1024;
  const rowsPerChunk = Math.max(1, Math.floor(targetChunkBytes / rowByteSize));
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

function rowRadiusOrScratch(inputRadius, rowIndex, scratch) {
  return inputRadius && rowIndex < inputRadius.length
    ? inputRadius[rowIndex]
    : scratchThreeSigmaRadiusFloat32(scratch);
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
  options = {},
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
    options,
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
  options = {},
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
    { chunkBytes: options.bucketChunkBytes },
  );
}

async function loadBucketSimplifyCoreFromEntries(
  entries,
  coeffCount,
  { keepScaleQuat = false, bucketChunkBytes = null } = {},
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
    { chunkBytes: bucketChunkBytes },
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

function resolveMergeShCoeffBlock(coeffStride, selectedCount, scratchBytes) {
  if (coeffStride <= 0) {
    return 0;
  }
  const fixedBytes = selectedCount * 6 * Float64Array.BYTES_PER_ELEMENT;
  const available =
    Number.isFinite(scratchBytes) && scratchBytes > fixedBytes
      ? scratchBytes - fixedBytes
      : MERGE_SH_COEFF_BLOCK * selectedCount * Float64Array.BYTES_PER_ELEMENT;
  const byBudget = Math.floor(
    available / Math.max(1, selectedCount * Float64Array.BYTES_PER_ELEMENT),
  );
  return Math.max(1, Math.min(coeffStride, byBudget || 1));
}

async function mergeSelectedBucketRowsToCloud(
  entries,
  coeffCount,
  selectedRows,
  assignment,
  selectedCount,
  voxelDiag,
  options = {},
) {
  const coeffStride = coeffCount * 3;
  const bucketChunkBytes = options.bucketChunkBytes;
  const inputRadius = options.inputRadius || null;
  const mergeShCoeffBlock = resolveMergeShCoeffBlock(
    coeffStride,
    selectedCount,
    options.simplifyScratchBytes,
  );
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
      const radius = rowRadiusOrScratch(inputRadius, rowIndex, scratch);
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
    { chunkBytes: bucketChunkBytes },
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

  let covSums = new Float64Array(selectedCount * 6);
  const outputRadius = new Float32Array(selectedCount);
  for (
    let coeffStart = 0;
    coeffStart < coeffStride;
    coeffStart += mergeShCoeffBlock
  ) {
    const blockWidth = Math.min(mergeShCoeffBlock, coeffStride - coeffStart);
    const accumulateCovariance = coeffStart === 0;
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
        const radius = rowRadiusOrScratch(inputRadius, rowIndex, scratch);
        const weight = mergeAggregationWeight(
          scratch.opacity,
          radius,
          voxelDiag,
        );
        const blockBase = slot * blockWidth;
        for (let c = 0; c < blockWidth; c++) {
          weightedShBlock[blockBase + c] += scratch.sh[coeffStart + c] * weight;
        }
        if (accumulateCovariance && counts[slot] > 1) {
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
        }
        rowIndex += 1;
      },
      { chunkBytes: bucketChunkBytes },
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

  let hasFallbackRows = false;
  for (let slot = 0; slot < selectedCount; slot++) {
    if (
      !Number.isFinite(weightSums[slot]) ||
      weightSums[slot] <= 1e-12 ||
      counts[slot] <= 1
    ) {
      fallbackRowIndex[slot] =
        firstAssigned[slot] >= 0 ? firstAssigned[slot] : selectedRows[slot];
      hasFallbackRows = true;
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
    outputRadius[slot] = Math.fround(
      computeThreeSigmaAabbDiagonalRadiusAt(
        scaleLog,
        slot * 3,
        quats,
        slot * 4,
      ),
    );
  }

  if (!scaleLog) {
    scaleLog = new Float32Array(selectedCount * 3);
    quats = new Float32Array(selectedCount * 4);
  }
  covSums = null;

  if (hasFallbackRows) {
    await materializeBucketRowsToSlots(
      entries,
      coeffCount,
      fallbackRowIndex,
      positions,
      scaleLog,
      quats,
      opacity,
      shCoeffs,
      { bucketChunkBytes },
    );
    for (let slot = 0; slot < selectedCount; slot++) {
      if (fallbackRowIndex[slot] < 0) {
        continue;
      }
      outputRadius[slot] = Math.fround(
        computeThreeSigmaAabbDiagonalRadiusAt(
          scaleLog,
          slot * 3,
          quats,
          slot * 4,
        ),
      );
    }
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
  return { cloud, outputRadius };
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

function quickselectFloat64(values, kth) {
  let left = 0;
  let right = values.length - 1;
  while (left < right) {
    const mid = (left + right) >> 1;
    const pivot = values[mid];
    let i = left;
    let j = right;

    while (i <= j) {
      while (values[i] < pivot) i++;
      while (values[j] > pivot) j--;
      if (i <= j) {
        const tmp = values[i];
        values[i] = values[j];
        values[j] = tmp;
        i++;
        j--;
      }
    }

    if (kth <= j) {
      right = j;
    } else if (kth >= i) {
      left = i;
    } else {
      return values[kth];
    }
  }
  return values[kth];
}

async function computeExactStreamingOwnErrorFromEntries(
  entries,
  coeffCount,
  assignment,
  outputCloud,
  outputRadius,
  options = {},
) {
  const totalRows = assignment.length;
  const pos95 = 0.95 * (totalRows - 1);
  const lo = Math.floor(pos95);
  const hi = Math.min(totalRows - 1, lo + 1);
  const frac = pos95 - lo;
  const errorBufferBytes = totalRows * Float64Array.BYTES_PER_ELEMENT;
  const inputRadius = options.inputRadius || null;
  if (
    Number.isFinite(options.errorBufferBytes) &&
    errorBufferBytes <= options.errorBufferBytes
  ) {
    const errors = new Float64Array(totalRows);
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
        const radius = rowRadiusOrScratch(inputRadius, rowIndex, scratch);
        errors[rowIndex] =
          Math.sqrt(dx * dx + dy * dy + dz * dz) + radius + outputRadius[slot];
        rowIndex += 1;
      },
      { chunkBytes: options.bucketChunkBytes },
    );

    const loValue = quickselectFloat64(errors, lo);
    if (frac === 0) {
      return loValue;
    }
    const hiValue = quickselectFloat64(errors, hi);
    return loValue * (1 - frac) + hiValue * frac;
  }

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
      const radius = rowRadiusOrScratch(inputRadius, rowIndex, scratch);
      const error =
        Math.sqrt(dx * dx + dy * dy + dz * dz) + radius + outputRadius[slot];
      tail.pushCandidate(error);
      rowIndex += 1;
    },
    { chunkBytes: options.bucketChunkBytes },
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
  {
    sampleMode = 'merge',
    bucketChunkBytes = null,
    retainInputRadius = false,
  } = {},
) {
  const lightCloud = await loadBucketSimplifyCoreFromEntries(
    entries,
    coeffCount,
    { keepScaleQuat: bounds == null, bucketChunkBytes },
  );
  const activeBounds = bounds || computeBounds(lightCloud);
  return planSimplifyCloudVoxel(
    lightCloud,
    target,
    activeBounds,
    normalizeSplatTargetCount(target, totalRows),
    {
      returnOrigRadius: retainInputRadius,
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
  options = {},
) {
  const activeEntries = await cacheBucketEntriesIfAffordable(
    entries,
    coeffCount,
    options.bucketEntryCacheBytes,
  );
  const totalRows = activeEntries.reduce(
    (sum, entry) => sum + entry.rowCount,
    0,
  );
  ensure(totalRows > 0, 'Cannot simplify an empty bucket input.');
  const target = normalizeSplatTargetCount(targetCount, totalRows);
  const inputRadiusBytes = totalRows * Float32Array.BYTES_PER_ELEMENT;
  const retainInputRadius =
    options.reuseInputRadius !== false &&
    (!Number.isFinite(options.simplifyScratchBytes) ||
      inputRadiusBytes <= Math.max(0, options.simplifyScratchBytes));
  if (totalRows <= target) {
    return {
      cloud: await loadBucketCloudFromEntries(
        activeEntries,
        coeffCount,
        totalRows,
        { bucketChunkBytes: options.bucketChunkBytes },
      ),
      ownError: 0.0,
    };
  }

  const plan = await planExactStreamingSimplify(
    activeEntries,
    coeffCount,
    target,
    bounds,
    totalRows,
    {
      sampleMode,
      bucketChunkBytes: options.bucketChunkBytes,
      retainInputRadius,
    },
  );
  const selectedRows = plan.selected;
  const selectedCount = selectedRows.length;
  const inputRadius = plan.origRadius || null;
  plan.selected = null;

  let outputCloud = null;
  let outputRadius = null;
  if (sampleMode === 'merge') {
    const merged = await mergeSelectedBucketRowsToCloud(
      activeEntries,
      coeffCount,
      selectedRows,
      plan.assignment,
      selectedCount,
      plan.voxelDiag,
      { ...options, inputRadius },
    );
    outputCloud = merged.cloud;
    outputRadius = merged.outputRadius;
  } else {
    outputCloud = await gatherSelectedBucketRowsToCloud(
      activeEntries,
      coeffCount,
      selectedCount,
      selectedRows,
      options,
    );
    outputRadius = plan.keptRadius;
  }

  return {
    cloud: outputCloud,
    ownError: await computeExactStreamingOwnErrorFromEntries(
      activeEntries,
      coeffCount,
      plan.assignment,
      outputCloud,
      outputRadius,
      {
        bucketChunkBytes: options.bucketChunkBytes,
        errorBufferBytes: options.errorBufferBytes,
        inputRadius,
      },
    ),
  };
}

function resolveNodeContentTarget(node, ctx, inputSplatCount) {
  if (
    Number.isFinite(node.contentTargetOverride) &&
    node.contentTargetOverride > 0
  ) {
    return normalizeSplatTargetCount(
      node.contentTargetOverride,
      inputSplatCount,
    );
  }
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

function resolveVirtualNodeContentTarget(
  node,
  lodMaxDepth,
  samplingRatePerLevel,
) {
  const targetRaw =
    Number.isFinite(node.contentTargetOverride) &&
    node.contentTargetOverride > 0
      ? node.contentTargetOverride
      : targetSplatCountForDepth(
          node.depth,
          lodMaxDepth,
          samplingRatePerLevel,
          node.count,
        );
  return constrainTargetSplatCount(targetRaw, node.count, node.children.length);
}

function minimumContentTargetForVirtualBudget(node) {
  if (
    Number.isInteger(node._virtualBudgetMinimum) &&
    node._virtualBudgetMinimum >= 0
  ) {
    return node._virtualBudgetMinimum;
  }
  return computeVirtualBudgetMinimums(node);
}

function computeVirtualBudgetMinimums(node) {
  let minimum;
  if (!node.virtual) {
    minimum = node.count > 0 ? 1 : 0;
  } else {
    minimum = node.children.reduce(
      (sum, child) => sum + computeVirtualBudgetMinimums(child),
      0,
    );
  }
  node._virtualBudgetMinimum = minimum;
  return minimum;
}

function childSlotSortKey(child) {
  return Number.isInteger(child.childSlot) ? child.childSlot : 0;
}

function compareContentAllocationPriority(a, b) {
  if (b.remainder !== a.remainder) return b.remainder - a.remainder;
  if (b.child.count !== a.child.count) return b.child.count - a.child.count;
  return childSlotSortKey(a.child) - childSlotSortKey(b.child);
}

function distributeRemainingByPriority(allocations, remaining) {
  const active = allocations
    .filter((entry) => entry.capacity > 0)
    .sort(compareContentAllocationPriority);
  let activeCount = active.length;
  while (remaining > 0 && activeCount > 0) {
    if (remaining < activeCount) {
      for (let i = 0; i < remaining; i++) {
        active[i].allocation += 1;
        active[i].capacity -= 1;
      }
      return 0;
    }

    let minCapacity = Infinity;
    for (let i = 0; i < activeCount; i++) {
      if (active[i].capacity < minCapacity) {
        minCapacity = active[i].capacity;
      }
    }
    const rounds = Math.min(minCapacity, Math.floor(remaining / activeCount));
    if (rounds <= 0) {
      break;
    }
    for (let i = 0; i < activeCount; i++) {
      active[i].allocation += rounds;
      active[i].capacity -= rounds;
    }
    remaining -= rounds * activeCount;

    let writeIndex = 0;
    for (let i = 0; i < activeCount; i++) {
      if (active[i].capacity > 0) {
        active[writeIndex++] = active[i];
      }
    }
    activeCount = writeIndex;
  }
  return remaining;
}

function allocateContentTargetsByChildCount(node, totalTarget) {
  const children = node.children.filter((child) => child.count > 0);
  if (children.length === 0) {
    return;
  }

  const minTotal = children.reduce(
    (sum, child) => sum + minimumContentTargetForVirtualBudget(child),
    0,
  );
  const total = Math.max(
    minTotal,
    Math.min(node.count, Math.floor(totalTarget)),
  );
  const allocations = children.map((child) => {
    const minimum = minimumContentTargetForVirtualBudget(child);
    return {
      child,
      allocation: minimum,
      capacity: Math.max(0, child.count - minimum),
      remainder: 0.0,
    };
  });
  let remaining =
    total - allocations.reduce((sum, entry) => sum + entry.allocation, 0);
  if (remaining > 0) {
    const denominator = Math.max(1, node.count);
    for (const entry of allocations) {
      const exact = (remaining * entry.child.count) / denominator;
      const extra = Math.min(entry.capacity, Math.floor(exact));
      entry.allocation += extra;
      entry.capacity -= extra;
      entry.remainder = exact - extra;
    }
    remaining =
      total - allocations.reduce((sum, entry) => sum + entry.allocation, 0);
  }

  remaining = distributeRemainingByPriority(allocations, remaining);

  for (const entry of allocations) {
    entry.child.contentTargetOverride = entry.allocation;
  }
}

function assignVirtualSegmentContentTargetsRecursive(
  node,
  lodMaxDepth,
  samplingRatePerLevel,
) {
  if (node.virtual) {
    const target = resolveVirtualNodeContentTarget(
      node,
      lodMaxDepth,
      samplingRatePerLevel,
    );
    allocateContentTargetsByChildCount(node, target);
  }
  for (const child of node.children) {
    assignVirtualSegmentContentTargetsRecursive(
      child,
      lodMaxDepth,
      samplingRatePerLevel,
    );
  }
}

function assignVirtualSegmentContentTargets(
  node,
  lodMaxDepth,
  samplingRatePerLevel,
) {
  computeVirtualBudgetMinimums(node);
  assignVirtualSegmentContentTargetsRecursive(
    node,
    lodMaxDepth,
    samplingRatePerLevel,
  );
}

function spzBytesPerBucketRow(coeffCount) {
  return 9 + 1 + 3 + 3 + 4 + (coeffCount > 1 ? (coeffCount - 1) * 3 : 0);
}

function safeNodeBoundsTranslation(node) {
  if (!node || !node.bounds) {
    return null;
  }
  const ext = node.bounds.extents();
  const maxLocal = SPZ_FIXED24_LIMIT / (1 << SPZ_FRACTIONAL_BITS);
  if (ext.some((value) => value * 0.5 > maxLocal)) {
    return null;
  }
  return node.bounds.center();
}

async function writeContentFile(
  params,
  cloud,
  level,
  x,
  y,
  z,
  { transferOwnership = false, translation = null } = {},
) {
  const relPath = contentRelPath(level, x, y, z);
  const outPath = path.join(params.outputDir, relPath);
  await fs.promises.mkdir(path.dirname(outPath), { recursive: true });
  const resolvedTranslation = translation || computeBounds(cloud).center();

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
        compressionLevel: params.spzCompressionLevel,
        colorSpace: params.colorSpace,
        sourceCoordinateSystem: params.sourceCoordinateSystem,
        translation: resolvedTranslation,
        cloud: serializeCloudForWorkerTask(cloud),
      },
      transferListForCloud(cloud),
    );
    return relPath;
  }

  writeCloudGlbOutput(
    outPath,
    cloud,
    params.colorSpace,
    params.spzSh1Bits,
    params.spzShRestBits,
    params.spzCompressionLevel,
    resolvedTranslation,
    params.sourceCoordinateSystem,
  );
  return relPath;
}

function writeCloudGlbOutput(
  outPath,
  cloud,
  colorSpace,
  sh1Bits,
  shRestBits,
  compressionLevel,
  translation = null,
  sourceCoordinateSystem = null,
) {
  const resolvedTranslation = translation || computeBounds(cloud).center();
  const spzBytes = packCloudToSpz(
    cloud,
    sh1Bits,
    shRestBits,
    resolvedTranslation,
    { compressionLevel },
  );
  const builder = new GltfBuilder();
  builder.writeSpzStreamGlb(
    outPath,
    spzBytes,
    cloud,
    colorSpace,
    resolvedTranslation,
    sourceCoordinateSystem,
  );
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
    (
      await computeBucketEntriesBounds(task.entries, task.coeffCount, {
        bucketChunkBytes: task.bucketChunkBytes,
      })
    ).center();
  const spzBytes = await packBucketEntriesToSpz(
    task.entries,
    task.coeffCount,
    task.shDegree,
    task.sh1Bits,
    task.shRestBits,
    translation,
    {
      bucketChunkBytes: task.bucketChunkBytes,
      compressionLevel: task.compressionLevel,
    },
  );
  const builder = new GltfBuilder();
  builder.writeSpzStreamGlb(
    task.outPath,
    spzBytes,
    { length: task.pointCount, shDegree: task.shDegree },
    task.colorSpace,
    translation,
    task.sourceCoordinateSystem,
  );
  return true;
}

async function writeSimplifiedBucketGlbTaskOutput(task) {
  ensure(
    task && task.outPath,
    'Missing simplified bucket GLB task output path.',
  );
  ensure(task.pointCount > 0, 'Cannot simplify an empty bucket content task.');
  const bounds = deserializeBoundsState(task.bounds);
  const { cloud, ownError } = await streamSimplifyBucketEntriesExact(
    task.entries,
    task.coeffCount,
    task.targetCount,
    bounds,
    task.sampleMode,
    {
      bucketChunkBytes: task.bucketChunkBytes,
      simplifyScratchBytes: task.simplifyScratchBytes,
      bucketEntryCacheBytes: task.bucketEntryCacheBytes,
      errorBufferBytes: task.errorBufferBytes,
    },
  );

  const handoffPromise = task.handoffPath
    ? writeCanonicalCloudFile(task.handoffPath, cloud, {
        bucketChunkBytes: task.bucketChunkBytes,
      })
    : Promise.resolve();
  const contentPromise = (async () => {
    await fs.promises.mkdir(path.dirname(task.outPath), { recursive: true });
    writeCloudGlbOutput(
      task.outPath,
      cloud,
      task.colorSpace,
      task.sh1Bits,
      task.shRestBits,
      task.compressionLevel,
      task.translation,
      task.sourceCoordinateSystem,
    );
  })();
  await Promise.all([handoffPromise, contentPromise]);

  return {
    contentUri: task.relPath,
    handoffRowCount: task.handoffPath ? cloud.length : null,
    ownError: Number.isFinite(ownError) && ownError > 0.0 ? ownError : 0.0,
  };
}

async function writeSimplifiedBucketContentFile(
  params,
  entries,
  coeffCount,
  pointCount,
  targetCount,
  bounds,
  level,
  x,
  y,
  z,
  handoffPath,
  options = {},
) {
  ensure(pointCount > 0, 'Cannot write empty simplified bucket content.');
  const relPath = contentRelPath(level, x, y, z);
  const outPath = path.join(params.outputDir, relPath);
  const task = {
    kind: 'simplify-bucket-spz',
    outPath,
    relPath,
    handoffPath,
    entries: bucketEntriesForWorkerTask(entries),
    coeffCount,
    pointCount,
    targetCount,
    bounds: serializeBoundsState(bounds),
    sampleMode: params.sampleMode,
    sh1Bits: params.spzSh1Bits,
    shRestBits: params.spzShRestBits,
    compressionLevel: params.spzCompressionLevel,
    colorSpace: params.colorSpace,
    sourceCoordinateSystem: params.sourceCoordinateSystem,
    translation: options.translation || null,
    bucketChunkBytes: options.bucketChunkBytes,
    simplifyScratchBytes: options.simplifyScratchBytes,
    bucketEntryCacheBytes: options.bucketEntryCacheBytes,
    errorBufferBytes: options.errorBufferBytes,
  };

  if (
    params.contentWorkerPool &&
    pointCount >= SPZ_BUCKET_ASYNC_WRITE_THRESHOLD
  ) {
    return params.contentWorkerPool.submit(task);
  }

  return writeSimplifiedBucketGlbTaskOutput(task);
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
  translation = null,
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
    compressionLevel: params.spzCompressionLevel,
    colorSpace: params.colorSpace,
    sourceCoordinateSystem: params.sourceCoordinateSystem,
    translation,
    bucketChunkBytes: params.bucketChunkBytes,
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

function normalizeSplitWeight(weight) {
  return Number.isFinite(weight) && weight > 0.0 ? weight : 1.0;
}

function splitWeightFromScratch(scratch, extentScratch) {
  writeThreeSigmaExtentComponents(
    scratch.scaleLog,
    0,
    scratch.quat,
    0,
    extentScratch,
    0,
  );
  const radiusSquared =
    extentScratch[0] * extentScratch[0] +
    extentScratch[1] * extentScratch[1] +
    extentScratch[2] * extentScratch[2];
  const opacity = Math.max(
    Number.isFinite(scratch.opacity) ? scratch.opacity : 0.0,
    1e-4,
  );
  return normalizeSplitWeight(
    opacity *
      (Number.isFinite(radiusSquared) && radiusSquared > 0.0
        ? radiusSquared
        : 1.0),
  );
}

async function scanGlobalBoundsAndWritePositions(
  handle,
  filePath,
  header,
  layout,
  positionsPath,
  options = {},
) {
  const minimum = [Infinity, Infinity, Infinity];
  const maximum = [-Infinity, -Infinity, -Infinity];
  const bufferBytes =
    Number.isFinite(options.positionTmpBufferBytes) &&
    options.positionTmpBufferBytes > 0
      ? Math.floor(options.positionTmpBufferBytes)
      : POSITION_TMP_BUFFER_BYTES;
  const rowsPerBuffer = Math.max(
    1,
    Math.floor(bufferBytes / POSITION_ROW_BYTE_SIZE),
  );
  const buffer = Buffer.allocUnsafe(rowsPerBuffer * POSITION_ROW_BYTE_SIZE);
  const floatView =
    IS_LITTLE_ENDIAN && (buffer.byteOffset & 3) === 0
      ? new Float32Array(buffer.buffer, buffer.byteOffset, rowsPerBuffer * 4)
      : null;
  let bufferedRows = 0;
  let count = 0;
  const scratch = makeRowScratch(0);
  const extentScratch = new Float32Array(3);
  const progress = options.progress || null;
  const updateProgress = (force = false) => {
    if (!progress) {
      return;
    }
    if (!force && count % SCAN_PROGRESS_ROW_INTERVAL !== 0) {
      return;
    }
    progress.update(count);
  };

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
    await _forEachGaussianPlyCanonicalRecord(
      handle,
      filePath,
      header,
      layout,
      (_rowIndex, _rowBuffer, rowView, rowFloats) => {
        readBucketCoreRowIntoScratch(
          LEAF_BUCKET_ENCODING,
          rowView,
          0,
          scratch,
          rowFloats,
          0,
        );
        const x = scratch.position[0];
        const y = scratch.position[1];
        const z = scratch.position[2];
        const weight = splitWeightFromScratch(scratch, extentScratch);
        const fx = Math.fround(x);
        const fy = Math.fround(y);
        const fz = Math.fround(z);
        const fw = Math.fround(weight);
        updatePositionBounds(minimum, maximum, fx, fy, fz);

        if (floatView) {
          const base = bufferedRows * 4;
          floatView[base + 0] = fx;
          floatView[base + 1] = fy;
          floatView[base + 2] = fz;
          floatView[base + 3] = fw;
        } else {
          const base = bufferedRows * POSITION_ROW_BYTE_SIZE;
          buffer.writeFloatLE(fx, base + 0);
          buffer.writeFloatLE(fy, base + 4);
          buffer.writeFloatLE(fz, base + 8);
          buffer.writeFloatLE(fw, base + 12);
        }

        bufferedRows += 1;
        count += 1;
        updateProgress();
        if (bufferedRows === rowsPerBuffer) {
          flush();
        }
      },
      { chunkBytes: options.chunkBytes },
    );
    flush();
    updateProgress(true);
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

async function scanGlobalBoundsAndStagePositionsInMemory(
  handle,
  filePath,
  header,
  layout,
  options = {},
) {
  const minimum = [Infinity, Infinity, Infinity];
  const maximum = [-Infinity, -Infinity, -Infinity];
  const positions = new Float32Array(header.vertexCount * 3);
  const weights = new Float32Array(header.vertexCount);
  let count = 0;
  const scratch = makeRowScratch(0);
  const extentScratch = new Float32Array(3);
  const progress = options.progress || null;
  const updateProgress = (force = false) => {
    if (!progress) {
      return;
    }
    if (!force && count % SCAN_PROGRESS_ROW_INTERVAL !== 0) {
      return;
    }
    progress.update(count);
  };

  await _forEachGaussianPlyCanonicalRecord(
    handle,
    filePath,
    header,
    layout,
    (rowIndex, _rowBuffer, rowView, rowFloats) => {
      readBucketCoreRowIntoScratch(
        LEAF_BUCKET_ENCODING,
        rowView,
        0,
        scratch,
        rowFloats,
        0,
      );
      const x = scratch.position[0];
      const y = scratch.position[1];
      const z = scratch.position[2];
      const weight = splitWeightFromScratch(scratch, extentScratch);
      const fx = Math.fround(x);
      const fy = Math.fround(y);
      const fz = Math.fround(z);
      updatePositionBounds(minimum, maximum, fx, fy, fz);

      const base = rowIndex * 3;
      positions[base + 0] = fx;
      positions[base + 1] = fy;
      positions[base + 2] = fz;
      weights[rowIndex] = Math.fround(weight);
      count += 1;
      updateProgress();
    },
    { chunkBytes: options.chunkBytes },
  );
  updateProgress(true);

  ensure(count > 0, `PLY file ${filePath} does not contain any vertices.`);
  ensure(
    count === header.vertexCount,
    `PLY position row count mismatch. Expected ${header.vertexCount}, got ${count}.`,
  );
  return {
    bounds: new Bounds(minimum, maximum),
    positions,
    weights,
  };
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

function collectAdaptiveSplitCandidates(node, maxDepth, leafLimit, out) {
  if (node.leaf) {
    const canSplitByCount =
      !node.splitExhausted && node.depth < maxDepth && node.count > leafLimit;
    const canSplitByAspect =
      node.level > 0 &&
      !node.aspectSplitExhausted &&
      boundsLongWidthAspect(node.bounds) > ADAPTIVE_MAX_LONG_WIDTH_RATIO;
    if (node.count > 1 && (canSplitByAspect || canSplitByCount)) {
      out.push(node);
    }
    return;
  }
  for (const child of node.children) {
    collectAdaptiveSplitCandidates(child, maxDepth, leafLimit, out);
  }
}

function makeAdaptiveSplitStats(node, basisAxes = null) {
  const normalizedBasisAxes = basisAxes ? normalizeBasisAxes(basisAxes) : null;
  return {
    node,
    count: 0,
    totalWeight: 0.0,
    mean: [0.0, 0.0, 0.0],
    covarianceM2: [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    minimum: [Infinity, Infinity, Infinity],
    maximum: [-Infinity, -Infinity, -Infinity],
    splitPoint: null,
    splitAxes: null,
    splitDirection: null,
    splitOffset: null,
    projectionMin: null,
    projectionMax: null,
    projectionHistogram: null,
    projectionTotalWeight: 0.0,
    basisAxes: normalizedBasisAxes,
    basisProjectionMinimum: normalizedBasisAxes
      ? [Infinity, Infinity, Infinity]
      : null,
    basisProjectionMaximum: normalizedBasisAxes
      ? [-Infinity, -Infinity, -Infinity]
      : null,
    action: null,
    childStats: null,
  };
}

function boundsFromMinMax(minimum, maximum, fallbackBounds) {
  if (
    minimum.every((value) => Number.isFinite(value)) &&
    maximum.every((value) => Number.isFinite(value))
  ) {
    return new Bounds(minimum.slice(), maximum.slice());
  }
  return cloneBounds(fallbackBounds);
}

function longWidthAspectFromExtents(extents) {
  const ranked = extents
    .map((extent, axis) => ({
      axis,
      extent: Number.isFinite(extent) ? Math.max(0.0, extent) : 0.0,
    }))
    .sort((a, b) => b.extent - a.extent);
  const longest = ranked[0];
  const width = ranked[1];
  const epsilon = Math.max(
    ADAPTIVE_SPLIT_EPSILON,
    longest.extent * ADAPTIVE_SPLIT_EPSILON,
  );
  return {
    aspect:
      longest.extent > epsilon
        ? longest.extent / Math.max(width.extent, epsilon)
        : 1.0,
    longestAxis: longest.axis,
    widthAxis: width.axis,
    longestExtent: longest.extent,
    widthExtent: width.extent,
  };
}

function boundsLongWidthAspect(bounds) {
  return longWidthAspectFromExtents(bounds.extents()).aspect;
}

function aspectSegmentCount(aspect) {
  if (!Number.isFinite(aspect) || aspect <= ADAPTIVE_MAX_LONG_WIDTH_RATIO) {
    return 1;
  }
  return Math.max(2, Math.ceil(aspect / ADAPTIVE_MAX_LONG_WIDTH_RATIO));
}

function updatePositionMomentStats(stats, x, y, z) {
  const minimum = stats.minimum;
  const maximum = stats.maximum;
  const nextCount = stats.count + 1;
  const mean = stats.mean;
  const dx = x - mean[0];
  const dy = y - mean[1];
  const dz = z - mean[2];
  const invCount = 1.0 / nextCount;

  mean[0] += dx * invCount;
  mean[1] += dy * invCount;
  mean[2] += dz * invCount;

  const dx2 = x - mean[0];
  const dy2 = y - mean[1];
  const dz2 = z - mean[2];
  const m2 = stats.covarianceM2;
  m2[0] += dx * dx2;
  m2[1] += dx * dy2;
  m2[2] += dx * dz2;
  m2[3] += dy * dy2;
  m2[4] += dy * dz2;
  m2[5] += dz * dz2;

  stats.count = nextCount;

  if (x < minimum[0]) minimum[0] = x;
  if (x > maximum[0]) maximum[0] = x;

  if (y < minimum[1]) minimum[1] = y;
  if (y > maximum[1]) maximum[1] = y;

  if (z < minimum[2]) minimum[2] = z;
  if (z > maximum[2]) maximum[2] = z;
}

function updateAdaptiveSplitStats(stats, x, y, z, weight = 1.0) {
  updatePositionMomentStats(stats, x, y, z);
  stats.totalWeight += normalizeSplitWeight(weight);
  if (stats.basisAxes) {
    for (let axis = 0; axis < 3; axis++) {
      const projection = dotDirection(stats.basisAxes[axis], x, y, z);
      if (projection < stats.basisProjectionMinimum[axis]) {
        stats.basisProjectionMinimum[axis] = projection;
      }
      if (projection > stats.basisProjectionMaximum[axis]) {
        stats.basisProjectionMaximum[axis] = projection;
      }
    }
  }
}

function eigenDecompositionSymmetric3x3(matrix) {
  const a = matrix.slice();
  const vectors = [
    1.0, 0.0, 0.0,
    0.0, 1.0, 0.0,
    0.0, 0.0, 1.0,
  ];
  const index = (row, col) => row * 3 + col;

  for (let sweep = 0; sweep < 16; sweep++) {
    let p = 0;
    let q = 1;
    let largest = Math.abs(a[index(0, 1)]);
    const xz = Math.abs(a[index(0, 2)]);
    if (xz > largest) {
      p = 0;
      q = 2;
      largest = xz;
    }
    const yz = Math.abs(a[index(1, 2)]);
    if (yz > largest) {
      p = 1;
      q = 2;
      largest = yz;
    }
    if (largest <= ADAPTIVE_SPLIT_EPSILON) {
      break;
    }

    const app = a[index(p, p)];
    const aqq = a[index(q, q)];
    const apq = a[index(p, q)];
    if (!Number.isFinite(apq) || Math.abs(apq) <= ADAPTIVE_SPLIT_EPSILON) {
      break;
    }

    const tau = (aqq - app) / (2.0 * apq);
    const t =
      tau >= 0.0
        ? 1.0 / (tau + Math.sqrt(1.0 + tau * tau))
        : -1.0 / (-tau + Math.sqrt(1.0 + tau * tau));
    const c = 1.0 / Math.sqrt(1.0 + t * t);
    const s = t * c;

    for (let k = 0; k < 3; k++) {
      if (k === p || k === q) {
        continue;
      }
      const akp = a[index(k, p)];
      const akq = a[index(k, q)];
      const newKp = c * akp - s * akq;
      const newKq = s * akp + c * akq;
      a[index(k, p)] = newKp;
      a[index(p, k)] = newKp;
      a[index(k, q)] = newKq;
      a[index(q, k)] = newKq;
    }

    a[index(p, p)] = app - t * apq;
    a[index(q, q)] = aqq + t * apq;
    a[index(p, q)] = 0.0;
    a[index(q, p)] = 0.0;

    for (let k = 0; k < 3; k++) {
      const vkp = vectors[index(k, p)];
      const vkq = vectors[index(k, q)];
      vectors[index(k, p)] = c * vkp - s * vkq;
      vectors[index(k, q)] = s * vkp + c * vkq;
    }
  }

  const entries = [0, 1, 2]
    .map((axis) => ({
      value: a[index(axis, axis)],
      vector: [
        vectors[index(0, axis)],
        vectors[index(1, axis)],
        vectors[index(2, axis)],
      ],
    }))
    .sort((lhs, rhs) => rhs.value - lhs.value);

  return {
    values: entries.map((entry) => entry.value),
    vectors: entries.map((entry) => entry.vector),
  };
}

function projectionRangeForBounds(bounds, direction) {
  const min = bounds.minimum;
  const max = bounds.maximum;
  let projectionMin = Infinity;
  let projectionMax = -Infinity;
  for (let ix = 0; ix < 2; ix++) {
    const x = ix === 0 ? min[0] : max[0];
    for (let iy = 0; iy < 2; iy++) {
      const y = iy === 0 ? min[1] : max[1];
      for (let iz = 0; iz < 2; iz++) {
        const z = iz === 0 ? min[2] : max[2];
        const projection = dotDirection(direction, x, y, z);
        if (projection < projectionMin) projectionMin = projection;
        if (projection > projectionMax) projectionMax = projection;
      }
    }
  }
  return { minimum: projectionMin, maximum: projectionMax };
}

function defaultBasisAxes() {
  return [
    [1.0, 0.0, 0.0],
    [0.0, 1.0, 0.0],
    [0.0, 0.0, 1.0],
  ];
}

function normalizeBasisAxes(axes) {
  if (!Array.isArray(axes) || axes.length !== 3) {
    return defaultBasisAxes();
  }
  const normalized = axes.map((axis) => normalizeVector3(axis));
  return normalized.every((axis) => !!axis) ? normalized : defaultBasisAxes();
}

function projectedAspectInfoForBasis(bounds, basisAxes) {
  const axes = normalizeBasisAxes(basisAxes);
  const ranked = axes
    .map((direction, axis) => {
      const range = projectionRangeForBounds(bounds, direction);
      const extent = range.maximum - range.minimum;
      return {
        axis,
        direction,
        range,
        extent: Number.isFinite(extent) ? Math.max(0.0, extent) : 0.0,
      };
    })
    .sort((a, b) => b.extent - a.extent);
  const longest = ranked[0];
  const width = ranked[1];
  const epsilon = Math.max(
    ADAPTIVE_SPLIT_EPSILON,
    longest.extent * ADAPTIVE_SPLIT_EPSILON,
  );
  return {
    aspect:
      longest.extent > epsilon
        ? longest.extent / Math.max(width.extent, epsilon)
        : 1.0,
    axis: longest.axis,
    direction: longest.direction,
    range: longest.range,
    longestExtent: longest.extent,
    widthExtent: width.extent,
  };
}

function projectedAspectInfoFromStats(stats, bounds, basisAxes) {
  if (
    stats &&
    stats.basisAxes &&
    stats.basisProjectionMinimum &&
    stats.basisProjectionMaximum
  ) {
    const ranked = stats.basisAxes
      .map((direction, axis) => {
        const minimum = stats.basisProjectionMinimum[axis];
        const maximum = stats.basisProjectionMaximum[axis];
        const extent = maximum - minimum;
        return {
          axis,
          direction,
          range: { minimum, maximum },
          extent: Number.isFinite(extent) ? Math.max(0.0, extent) : 0.0,
        };
      })
      .sort((a, b) => b.extent - a.extent);
    const longest = ranked[0];
    const width = ranked[1];
    const epsilon = Math.max(
      ADAPTIVE_SPLIT_EPSILON,
      longest.extent * ADAPTIVE_SPLIT_EPSILON,
    );
    return {
      aspect:
        longest.extent > epsilon
          ? longest.extent / Math.max(width.extent, epsilon)
          : 1.0,
      axis: longest.axis,
      direction: longest.direction,
      range: longest.range,
      longestExtent: longest.extent,
      widthExtent: width.extent,
    };
  }
  return projectedAspectInfoForBasis(bounds, basisAxes);
}

function cross3(lhs, rhs) {
  return [
    lhs[1] * rhs[2] - lhs[2] * rhs[1],
    lhs[2] * rhs[0] - lhs[0] * rhs[2],
    lhs[0] * rhs[1] - lhs[1] * rhs[0],
  ];
}

function dot3(lhs, rhs) {
  return lhs[0] * rhs[0] + lhs[1] * rhs[1] + lhs[2] * rhs[2];
}

function subtractProjection(vector, axis) {
  const scale = dot3(vector, axis);
  return [
    vector[0] - axis[0] * scale,
    vector[1] - axis[1] * scale,
    vector[2] - axis[2] * scale,
  ];
}

function fallbackAxisLeastAlignedWith(axis) {
  const absX = Math.abs(axis[0]);
  const absY = Math.abs(axis[1]);
  const absZ = Math.abs(axis[2]);
  if (absX <= absY && absX <= absZ) {
    return [1.0, 0.0, 0.0];
  }
  if (absY <= absX && absY <= absZ) {
    return [0.0, 1.0, 0.0];
  }
  return [0.0, 0.0, 1.0];
}

function stableOrientedAxis(axis) {
  const normalized = normalizeVector3(axis);
  if (!normalized) {
    return null;
  }
  let strongestAxis = 0;
  let strongestAbs = Math.abs(normalized[0]);
  for (let i = 1; i < 3; i++) {
    const absValue = Math.abs(normalized[i]);
    if (absValue > strongestAbs) {
      strongestAxis = i;
      strongestAbs = absValue;
    }
  }
  if (normalized[strongestAxis] < 0.0) {
    normalized[0] = -normalized[0];
    normalized[1] = -normalized[1];
    normalized[2] = -normalized[2];
  }
  return normalized;
}

function orthonormalBasisFromMomentStats(stats) {
  const m2 = stats.covarianceM2 || [];
  const maxAbs = Math.max(
    Math.abs(m2[0] || 0.0),
    Math.abs(m2[1] || 0.0),
    Math.abs(m2[2] || 0.0),
    Math.abs(m2[3] || 0.0),
    Math.abs(m2[4] || 0.0),
    Math.abs(m2[5] || 0.0),
  );
  if (!Number.isFinite(maxAbs) || maxAbs <= ADAPTIVE_SPLIT_EPSILON) {
    return [
      [1.0, 0.0, 0.0],
      [0.0, 1.0, 0.0],
      [0.0, 0.0, 1.0],
    ];
  }

  const decomposition = eigenDecompositionSymmetric3x3([
    m2[0], m2[1], m2[2],
    m2[1], m2[3], m2[4],
    m2[2], m2[4], m2[5],
  ]);
  const axis0 =
    stableOrientedAxis(decomposition.vectors[0]) || [1.0, 0.0, 0.0];
  let axis1 = stableOrientedAxis(
    subtractProjection(decomposition.vectors[1], axis0),
  );
  if (!axis1) {
    axis1 = stableOrientedAxis(
      subtractProjection(fallbackAxisLeastAlignedWith(axis0), axis0),
    );
  }
  if (!axis1) {
    axis1 = [0.0, 1.0, 0.0];
  }
  let axis2 = normalizeVector3(cross3(axis0, axis1));
  if (!axis2) {
    axis2 = stableOrientedAxis(
      subtractProjection(fallbackAxisLeastAlignedWith(axis1), axis1),
    );
  }
  axis2 = axis2 || [0.0, 0.0, 1.0];
  return [axis0, axis1, axis2];
}

function makeOrientedBoxStats(node) {
  return {
    node,
    count: 0,
    mean: [0.0, 0.0, 0.0],
    covarianceM2: [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    minimum: [Infinity, Infinity, Infinity],
    maximum: [-Infinity, -Infinity, -Infinity],
    axes: null,
    projectionMinimum: [Infinity, Infinity, Infinity],
    projectionMaximum: [-Infinity, -Infinity, -Infinity],
  };
}

function cloneBasisAxes(axes) {
  return axes.map((axis) => axis.slice());
}

function prepareOrientedBoxStats(stats, basisAxes = null) {
  stats.axes = basisAxes
    ? cloneBasisAxes(basisAxes)
    : orthonormalBasisFromMomentStats(stats);
  stats.projectionMinimum = [Infinity, Infinity, Infinity];
  stats.projectionMaximum = [-Infinity, -Infinity, -Infinity];
}

function updateOrientedBoxProjectionStats(stats, x, y, z) {
  const axes = stats.axes;
  if (!axes) {
    return;
  }
  for (let axis = 0; axis < 3; axis++) {
    const projection = dotDirection(axes[axis], x, y, z);
    if (projection < stats.projectionMinimum[axis]) {
      stats.projectionMinimum[axis] = projection;
    }
    if (projection > stats.projectionMaximum[axis]) {
      stats.projectionMaximum[axis] = projection;
    }
  }
}

function orientedBoxFromProjectionStats(stats, fallbackBounds) {
  if (!stats || !stats.axes || stats.count <= 0) {
    return fallbackBounds ? fallbackBounds.toBoxArray() : null;
  }
  const center = [0.0, 0.0, 0.0];
  const halfAxes = [];
  for (let axis = 0; axis < 3; axis++) {
    const min = stats.projectionMinimum[axis];
    const max = stats.projectionMaximum[axis];
    if (!Number.isFinite(min) || !Number.isFinite(max)) {
      return fallbackBounds ? fallbackBounds.toBoxArray() : null;
    }
    const midpoint = (min + max) * 0.5;
    const halfExtent = Math.max((max - min) * 0.5, 1e-6);
    const direction = stats.axes[axis];
    center[0] += direction[0] * midpoint;
    center[1] += direction[1] * midpoint;
    center[2] += direction[2] * midpoint;
    halfAxes.push([
      direction[0] * halfExtent,
      direction[1] * halfExtent,
      direction[2] * halfExtent,
    ]);
  }
  return [
    center[0],
    center[1],
    center[2],
    halfAxes[0][0],
    halfAxes[0][1],
    halfAxes[0][2],
    halfAxes[1][0],
    halfAxes[1][1],
    halfAxes[1][2],
    halfAxes[2][0],
    halfAxes[2][1],
    halfAxes[2][2],
  ];
}

function makeKdSplitAction(stats, tightBounds, basisAxes = null) {
  const basisInfo = projectedAspectInfoFromStats(stats, tightBounds, basisAxes);
  const splitDirection = basisInfo.direction;
  if (!splitDirection || !basisInfo.range) {
    return null;
  }
  const projectionRange = basisInfo.range;
  const projectionExtent = projectionRange.maximum - projectionRange.minimum;
  const epsilon = Math.max(
    ADAPTIVE_SPLIT_EPSILON,
    Math.abs(projectionExtent) * ADAPTIVE_SPLIT_EPSILON,
  );
  if (
    !Number.isFinite(projectionExtent) ||
    projectionExtent <= epsilon ||
    !Number.isFinite(projectionRange.minimum) ||
    !Number.isFinite(projectionRange.maximum)
  ) {
    return null;
  }
  return {
    kind: ROUTE_MODE_KD,
    basisAxis: basisInfo.axis,
    splitDirection,
    splitOffset: null,
    projectionMin: projectionRange.minimum,
    projectionMax: projectionRange.maximum,
  };
}

function resetAdaptiveProjectionStats(stats) {
  const action = stats.action;
  ensure(
    action && action.kind === ROUTE_MODE_KD,
    `Missing k-d projection action for node ${stats.node.key}.`,
  );
  stats.projectionMin = action.projectionMin;
  stats.projectionMax = action.projectionMax;
  stats.projectionHistogram = new Float64Array(ADAPTIVE_SPLIT_HISTOGRAM_BINS);
  stats.projectionTotalWeight = 0.0;
}

function updateAdaptiveProjectionStats(stats, x, y, z, weight = 1.0) {
  const action = stats.action;
  const direction = action ? action.splitDirection : null;
  if (!direction || !stats.projectionHistogram) {
    return;
  }
  const min = stats.projectionMin;
  const max = stats.projectionMax;
  const extent = max - min;
  if (!Number.isFinite(extent) || extent <= 0.0) {
    return;
  }
  const projection = dotDirection(direction, x, y, z);
  const splitWeight = normalizeSplitWeight(weight);
  let bin = Math.floor(
    ((projection - min) / extent) * ADAPTIVE_SPLIT_HISTOGRAM_BINS,
  );
  bin = Math.max(0, Math.min(ADAPTIVE_SPLIT_HISTOGRAM_BINS - 1, bin));
  stats.projectionHistogram[bin] += splitWeight;
  stats.projectionTotalWeight += splitWeight;
}

function chooseProjectionSplitOffset(stats) {
  const action = stats.action;
  const histogram = stats.projectionHistogram;
  if (!action || !histogram) {
    return null;
  }
  const min = stats.projectionMin;
  const max = stats.projectionMax;
  const extent = max - min;
  const epsilon = Math.max(
    ADAPTIVE_SPLIT_EPSILON,
    Math.abs(extent) * ADAPTIVE_SPLIT_EPSILON,
  );
  if (
    !Number.isFinite(extent) ||
    extent <= epsilon ||
    !Number.isFinite(stats.projectionTotalWeight) ||
    stats.projectionTotalWeight <= 0.0
  ) {
    return null;
  }

  let lower = 0.0;
  let bestBoundary = -1;
  let bestBalance = Infinity;
  for (let boundary = 1; boundary < ADAPTIVE_SPLIT_HISTOGRAM_BINS; boundary++) {
    lower += histogram[boundary - 1];
    if (lower <= 0.0 || lower >= stats.projectionTotalWeight) {
      continue;
    }
    const balance = Math.abs(lower - stats.projectionTotalWeight * 0.5);
    if (balance < bestBalance) {
      bestBalance = balance;
      bestBoundary = boundary;
    }
  }

  if (bestBoundary > 0) {
    const split =
      min + (extent * bestBoundary) / ADAPTIVE_SPLIT_HISTOGRAM_BINS;
    if (split > min + epsilon && split < max - epsilon) {
      return split;
    }
  }

  const meanProjection = dotDirection(
    action.splitDirection,
    stats.mean[0],
    stats.mean[1],
    stats.mean[2],
  );
  if (meanProjection > min + epsilon && meanProjection < max - epsilon) {
    return meanProjection;
  }
  return null;
}

function finalizeKdSplitAction(stats) {
  const action = stats.action;
  if (!action || action.kind !== ROUTE_MODE_KD) {
    return true;
  }
  const splitOffset = chooseProjectionSplitOffset(stats);
  if (!Number.isFinite(splitOffset)) {
    return false;
  }
  action.splitOffset = splitOffset;
  stats.splitDirection = action.splitDirection.slice();
  stats.splitOffset = splitOffset;
  return true;
}

function makeVirtualSegmentSplitAction(
  node,
  tightBounds,
  basisAxes = null,
  projectedAspectInfo = null,
) {
  if (node.level <= 0 || node.aspectSplitExhausted) {
    return null;
  }

  const aspectInfo = projectedAspectInfo || (basisAxes
    ? projectedAspectInfoForBasis(tightBounds, basisAxes)
    : longWidthAspectFromExtents(tightBounds.extents()));
  if (aspectInfo.aspect <= ADAPTIVE_MAX_LONG_WIDTH_RATIO) {
    return null;
  }

  const axis = basisAxes ? aspectInfo.axis : aspectInfo.longestAxis;
  const segmentDirection = basisAxes ? aspectInfo.direction : null;
  const segmentMin = basisAxes
    ? aspectInfo.range.minimum
    : tightBounds.minimum[axis];
  const segmentMax = basisAxes
    ? aspectInfo.range.maximum
    : tightBounds.maximum[axis];
  const segmentExtent = segmentMax - segmentMin;
  const epsilon = Math.max(
    ADAPTIVE_SPLIT_EPSILON,
    Math.abs(segmentExtent) * ADAPTIVE_SPLIT_EPSILON,
  );
  if (
    !Number.isFinite(segmentExtent) ||
    segmentExtent <= epsilon ||
    aspectInfo.widthExtent <= epsilon
  ) {
    return null;
  }

  const segmentCount = aspectSegmentCount(aspectInfo.aspect);
  if (!Number.isInteger(segmentCount) || segmentCount <= 1) {
    return null;
  }

  return {
    kind: ROUTE_MODE_AXIS_SEGMENTS,
    axis,
    segmentDirection,
    segmentCount,
    segmentMin,
    segmentMax,
  };
}

function chooseAdaptiveSplitAction(
  stats,
  tightBounds,
  maxDepth,
  leafLimit,
  basisAxes = null,
) {
  const node = stats.node;
  const projectedAspectInfo = basisAxes
    ? projectedAspectInfoFromStats(stats, tightBounds, basisAxes)
    : null;
  const aspectCandidate =
    node.level > 0 &&
    !node.aspectSplitExhausted &&
    (projectedAspectInfo
      ? projectedAspectInfo.aspect
      : boundsLongWidthAspect(tightBounds)) >
      ADAPTIVE_MAX_LONG_WIDTH_RATIO;
  if (aspectCandidate) {
    const action = makeVirtualSegmentSplitAction(
      node,
      tightBounds,
      basisAxes,
      projectedAspectInfo,
    );
    if (action) {
      return action;
    }
  }

  if (!node.splitExhausted && node.depth < maxDepth && node.count > leafLimit) {
    const action = makeKdSplitAction(stats, tightBounds, basisAxes);
    if (action) {
      return action;
    }
    return {
      kind: null,
      aspectExhausted: aspectCandidate,
      splitExhausted: true,
    };
  }

  return {
    kind: null,
    aspectExhausted: aspectCandidate,
    splitExhausted: false,
  };
}

function clearNodeSplitRouting(node) {
  node.splitPoint = null;
  node.splitAxes = null;
  node.splitDirection = null;
  node.splitOffset = null;
  node.segmentAxis = null;
  node.segmentDirection = null;
  node.segmentCount = null;
  node.segmentMin = null;
  node.segmentMax = null;
  node.routeMode = ROUTE_MODE_KD;
}

function applyAdaptiveSplitActionToNode(node, action) {
  if (action.kind === ROUTE_MODE_KD) {
    node.splitPoint = null;
    node.splitAxes = null;
    node.splitDirection = action.splitDirection.slice();
    node.splitOffset = action.splitOffset;
    node.routeMode = ROUTE_MODE_KD;
    node.segmentAxis = null;
    node.segmentDirection = null;
    node.segmentCount = null;
    node.segmentMin = null;
    node.segmentMax = null;
    return;
  }

  node.splitPoint = null;
  node.splitAxes = null;
  node.splitDirection = null;
  node.splitOffset = null;
  node.routeMode = ROUTE_MODE_AXIS_SEGMENTS;
  node.segmentAxis = action.axis;
  node.segmentDirection = action.segmentDirection
    ? action.segmentDirection.slice()
    : null;
  node.segmentCount = action.segmentCount;
  node.segmentMin = action.segmentMin;
  node.segmentMax = action.segmentMax;
}

function pointSegmentForSplitAction(action, x, y, z) {
  const extent = action.segmentMax - action.segmentMin;
  if (!Number.isFinite(extent) || extent <= 0.0) {
    return 0;
  }
  const coordinate = coordinateForDirectionOrAxis(
    action.segmentDirection,
    action.axis,
    x,
    y,
    z,
  );
  const slot = Math.floor(
    ((coordinate - action.segmentMin) / extent) * action.segmentCount,
  );
  return Math.max(0, Math.min(action.segmentCount - 1, slot));
}

function pointKdSlotForAction(action, bounds, x, y, z) {
  if (action.splitDirection && Number.isFinite(action.splitOffset)) {
    return pointPlaneSlot(action.splitDirection, action.splitOffset, x, y, z);
  }
  return pointOctant(
    bounds,
    x,
    y,
    z,
    action.splitPoint,
    action.splitAxes,
  );
}

function childSlotForAdaptiveAction(stats, x, y, z) {
  const action = stats.action;
  if (action.kind === ROUTE_MODE_AXIS_SEGMENTS) {
    return pointSegmentForSplitAction(action, x, y, z);
  }
  return pointKdSlotForAction(action, stats.node.bounds, x, y, z);
}

function kdSplitAxisForAction(action) {
  if (!action || action.kind !== ROUTE_MODE_KD) {
    return null;
  }
  const axes = action.splitAxes || [];
  for (let axis = 0; axis < 3; axis++) {
    if (axes[axis]) {
      return axis;
    }
  }
  return null;
}

function updateAdaptiveChildStats(stats, slot, x, y, z) {
  if (!stats.childStats) {
    stats.childStats = new Map();
  }
  let entry = stats.childStats.get(slot);
  if (!entry) {
    entry = {
      slot,
      count: 0,
      minimum: [Infinity, Infinity, Infinity],
      maximum: [-Infinity, -Infinity, -Infinity],
    };
    stats.childStats.set(slot, entry);
  }
  entry.count += 1;
  const minimum = entry.minimum;
  const maximum = entry.maximum;
  if (x < minimum[0]) minimum[0] = x;
  if (y < minimum[1]) minimum[1] = y;
  if (z < minimum[2]) minimum[2] = z;
  if (x > maximum[0]) maximum[0] = x;
  if (y > maximum[1]) maximum[1] = y;
  if (z > maximum[2]) maximum[2] = z;
}

function sortedAdaptiveChildStats(stats) {
  if (!stats.childStats) {
    return [];
  }
  return Array.from(stats.childStats.values()).sort((a, b) => a.slot - b.slot);
}

function makeAdaptiveChildEntry(slot, start = null, end = null) {
  return {
    slot,
    count: 0,
    minimum: [Infinity, Infinity, Infinity],
    maximum: [-Infinity, -Infinity, -Infinity],
    start,
    end,
  };
}

function updateAdaptiveChildEntry(entry, x, y, z) {
  entry.count += 1;
  if (x < entry.minimum[0]) entry.minimum[0] = x;
  if (y < entry.minimum[1]) entry.minimum[1] = y;
  if (z < entry.minimum[2]) entry.minimum[2] = z;
  if (x > entry.maximum[0]) entry.maximum[0] = x;
  if (y > entry.maximum[1]) entry.maximum[1] = y;
  if (z > entry.maximum[2]) entry.maximum[2] = z;
}

function makeAdaptiveRootNode(rootBounds, vertexCount) {
  return makePartitionTreeNode({
    level: 0,
    depth: 0,
    x: 0,
    y: 0,
    z: 0,
    bounds: cloneBounds(rootBounds),
    count: vertexCount,
    leaf: true,
  });
}

function makePositionIndexArray(vertexCount) {
  const indices = new Uint32Array(vertexCount);
  for (let rowIndex = 0; rowIndex < vertexCount; rowIndex++) {
    indices[rowIndex] = rowIndex;
  }
  return indices;
}

function setNodePositionIndexRange(node, start, end) {
  node._positionIndexStart = start;
  node._positionIndexEnd = end;
}

function clearNodePositionIndexRanges(node) {
  delete node._positionIndexStart;
  delete node._positionIndexEnd;
  for (const child of node.children) {
    clearNodePositionIndexRanges(child);
  }
}

function makeTilingProgressState(progress, vertexCount, maxDepth) {
  if (!progress) {
    return null;
  }
  const rows = Math.max(1, Math.floor(vertexCount || 1));
  const depthRounds = Math.max(1, Math.floor(maxDepth || 1));
  const total = rows * depthRounds * 2;
  const state = {
    progress,
    total,
    estimatedTotal: total,
    current: 0,
    message: 'building k-d tree',
    estimateExpanded: false,
    estimateExpansionLogged: false,
    virtualSegmentActions: 0,
    virtualSegmentCount: 0,
  };
  if (typeof progress.reset === 'function') {
    progress.reset(total, formatTilingProgressMessage(state));
  } else {
    progress.current = 0;
    progress.setTotal(total);
    progress.update(0, formatTilingProgressMessage(state));
  }
  return state;
}

function formatProgressInteger(value) {
  return Math.max(0, Math.floor(value || 0)).toLocaleString('en-US');
}

function formatTilingProgressMessage(state, message = null) {
  const base = message || (state ? state.message : '') || 'building k-d tree';
  if (!state || !state.estimateExpanded) {
    return base;
  }
  const overEstimateRows = Math.max(0, state.current - state.estimatedTotal);
  if (state.virtualSegmentActions > 0) {
    return (
      `${base} | extra virtual long-tile work ` +
      `splits=${state.virtualSegmentActions} ` +
      `segments=${state.virtualSegmentCount} ` +
      `over=${formatProgressInteger(overEstimateRows)} rows`
    );
  }
  return `${base} | work estimate expanded`;
}

function noteTilingVirtualSegmentAction(progressState, action) {
  if (
    !progressState ||
    !action ||
    action.kind !== ROUTE_MODE_AXIS_SEGMENTS
  ) {
    return;
  }
  progressState.virtualSegmentActions += 1;
  progressState.virtualSegmentCount += Math.max(
    0,
    Math.floor(action.segmentCount || 0),
  );
}

function maybeExpandTilingProgressTotal(state) {
  if (!state || state.current < state.total) {
    return;
  }
  const increment = Math.max(
    TILING_TREE_PROGRESS_ROW_INTERVAL,
    Math.floor(state.total * 0.1),
  );
  state.estimateExpanded = true;
  state.total = state.current + increment;
  state.progress.setTotal(state.total);
  if (
    !state.estimateExpansionLogged &&
    typeof state.progress.logDetail === 'function'
  ) {
    state.progress.logDetail('work estimate expanded');
    state.estimateExpansionLogged = true;
  }
}

function startTilingProgressPhase(progressState, total, message) {
  if (!progressState) {
    return null;
  }
  if (message) {
    progressState.message = message;
    progressState.progress.update(
      progressState.current,
      formatTilingProgressMessage(progressState, message),
    );
  }
  return {
    overall: progressState,
    total: Math.max(0, Math.floor(total || 0)),
    current: 0,
    nextUpdate: TILING_TREE_PROGRESS_ROW_INTERVAL,
  };
}

function advanceTilingProgress(state, rows, { force = false } = {}) {
  if (!state || rows < 0) {
    return;
  }
  if (rows > 0) {
    state.current += rows;
    state.overall.current += rows;
  }
  if (!force && state.current < state.nextUpdate) {
    return;
  }
  maybeExpandTilingProgressTotal(state.overall);
  const visibleCurrent =
    state.overall.total > 0
      ? Math.min(state.overall.current, state.overall.total)
      : state.overall.current;
  state.overall.progress.update(
    visibleCurrent,
    formatTilingProgressMessage(state.overall),
  );
  state.nextUpdate =
    state.current + Math.max(1, TILING_TREE_PROGRESS_ROW_INTERVAL);
}

function childSlotForAdaptiveActionAtRow(stats, positions, rowIndex) {
  const base = rowIndex * 3;
  return childSlotForAdaptiveAction(
    stats,
    positions[base + 0],
    positions[base + 1],
    positions[base + 2],
  );
}

function updateAdaptiveSplitStatsForPositionIndexRange(
  stats,
  positions,
  weights,
  indices,
  start,
  end,
  progressState = null,
) {
  let pendingProgressRows = 0;
  for (let offset = start; offset < end; offset++) {
    const rowIndex = indices[offset];
    const base = rowIndex * 3;
    updateAdaptiveSplitStats(
      stats,
      positions[base + 0],
      positions[base + 1],
      positions[base + 2],
      weights ? weights[rowIndex] : 1.0,
    );
    pendingProgressRows += 1;
    if (pendingProgressRows >= TILING_TREE_PROGRESS_ROW_INTERVAL) {
      advanceTilingProgress(progressState, pendingProgressRows);
      pendingProgressRows = 0;
    }
  }
  if (pendingProgressRows > 0) {
    advanceTilingProgress(progressState, pendingProgressRows);
  }
}

function updateAdaptiveProjectionStatsForPositionIndexRange(
  stats,
  positions,
  weights,
  indices,
  start,
  end,
  progressState = null,
) {
  let pendingProgressRows = 0;
  for (let offset = start; offset < end; offset++) {
    const rowIndex = indices[offset];
    const base = rowIndex * 3;
    updateAdaptiveProjectionStats(
      stats,
      positions[base + 0],
      positions[base + 1],
      positions[base + 2],
      weights ? weights[rowIndex] : 1.0,
    );
    pendingProgressRows += 1;
    if (pendingProgressRows >= TILING_TREE_PROGRESS_ROW_INTERVAL) {
      advanceTilingProgress(progressState, pendingProgressRows);
      pendingProgressRows = 0;
    }
  }
  if (pendingProgressRows > 0) {
    advanceTilingProgress(progressState, pendingProgressRows);
  }
}

function makeAdaptiveChildBucketStats(action) {
  const bucketCount =
    action.kind === ROUTE_MODE_AXIS_SEGMENTS ? action.segmentCount : 8;
  if (
    action.kind === ROUTE_MODE_AXIS_SEGMENTS &&
    bucketCount > ADAPTIVE_DENSE_CHILD_BUCKET_LIMIT
  ) {
    return {
      sparse: true,
      actionBucketCount: bucketCount,
      bucketCount: 0,
      entriesBySlot: new Map(),
      orderedEntries: null,
      starts: null,
      ends: null,
      slotToBucketIndex: null,
    };
  }
  const minimumX = new Float64Array(bucketCount);
  const minimumY = new Float64Array(bucketCount);
  const minimumZ = new Float64Array(bucketCount);
  const maximumX = new Float64Array(bucketCount);
  const maximumY = new Float64Array(bucketCount);
  const maximumZ = new Float64Array(bucketCount);
  minimumX.fill(Infinity);
  minimumY.fill(Infinity);
  minimumZ.fill(Infinity);
  maximumX.fill(-Infinity);
  maximumY.fill(-Infinity);
  maximumZ.fill(-Infinity);
  return {
    sparse: false,
    actionBucketCount: bucketCount,
    bucketCount,
    counts: new Uint32Array(bucketCount),
    minimumX,
    minimumY,
    minimumZ,
    maximumX,
    maximumY,
    maximumZ,
    starts: null,
    ends: null,
  };
}

function updateAdaptiveChildBucketStats(bucketStats, slot, x, y, z) {
  if (bucketStats.sparse) {
    let entry = bucketStats.entriesBySlot.get(slot);
    if (!entry) {
      entry = makeAdaptiveChildEntry(slot);
      bucketStats.entriesBySlot.set(slot, entry);
    }
    updateAdaptiveChildEntry(entry, x, y, z);
    return;
  }
  bucketStats.counts[slot] += 1;
  if (x < bucketStats.minimumX[slot]) bucketStats.minimumX[slot] = x;
  if (y < bucketStats.minimumY[slot]) bucketStats.minimumY[slot] = y;
  if (z < bucketStats.minimumZ[slot]) bucketStats.minimumZ[slot] = z;
  if (x > bucketStats.maximumX[slot]) bucketStats.maximumX[slot] = x;
  if (y > bucketStats.maximumY[slot]) bucketStats.maximumY[slot] = y;
  if (z > bucketStats.maximumZ[slot]) bucketStats.maximumZ[slot] = z;
}

function collectAdaptiveChildBucketStatsForPositionRange(
  stats,
  positions,
  indices,
  start,
  end,
  progressState = null,
) {
  const bucketStats = makeAdaptiveChildBucketStats(stats.action);
  let pendingProgressRows = 0;
  for (let offset = start; offset < end; offset++) {
    const rowIndex = indices[offset];
    const base = rowIndex * 3;
    const x = positions[base + 0];
    const y = positions[base + 1];
    const z = positions[base + 2];
    const slot = childSlotForAdaptiveAction(stats, x, y, z);
    updateAdaptiveChildBucketStats(bucketStats, slot, x, y, z);
    pendingProgressRows += 1;
    if (pendingProgressRows >= TILING_TREE_PROGRESS_ROW_INTERVAL) {
      advanceTilingProgress(progressState, pendingProgressRows);
      pendingProgressRows = 0;
    }
  }
  if (pendingProgressRows > 0) {
    advanceTilingProgress(progressState, pendingProgressRows);
  }
  return bucketStats;
}

function assignAdaptiveChildBucketRanges(bucketStats, start, end) {
  if (bucketStats.sparse) {
    const orderedEntries = Array.from(bucketStats.entriesBySlot.values()).sort(
      (a, b) => a.slot - b.slot,
    );
    const starts = new Uint32Array(orderedEntries.length);
    const ends = new Uint32Array(orderedEntries.length);
    const slotToBucketIndex = new Map();
    let cursor = start;
    for (let index = 0; index < orderedEntries.length; index++) {
      const entry = orderedEntries[index];
      starts[index] = cursor;
      entry.start = cursor;
      cursor += entry.count;
      entry.end = cursor;
      ends[index] = cursor;
      slotToBucketIndex.set(entry.slot, index);
    }
    ensure(
      cursor === end,
      `Adaptive child bucket range mismatch. Expected ${end}, got ${cursor}.`,
    );
    bucketStats.bucketCount = orderedEntries.length;
    bucketStats.orderedEntries = orderedEntries;
    bucketStats.starts = starts;
    bucketStats.ends = ends;
    bucketStats.slotToBucketIndex = slotToBucketIndex;
    return;
  }

  const starts = new Uint32Array(bucketStats.bucketCount);
  const ends = new Uint32Array(bucketStats.bucketCount);
  let cursor = start;
  for (let slot = 0; slot < bucketStats.bucketCount; slot++) {
    starts[slot] = cursor;
    cursor += bucketStats.counts[slot];
    ends[slot] = cursor;
  }
  ensure(
    cursor === end,
    `Adaptive child bucket range mismatch. Expected ${end}, got ${cursor}.`,
  );
  bucketStats.starts = starts;
  bucketStats.ends = ends;
}

function adaptiveChildBucketEntries(bucketStats) {
  if (bucketStats.sparse) {
    return bucketStats.orderedEntries || [];
  }

  const entries = [];
  for (let slot = 0; slot < bucketStats.bucketCount; slot++) {
    const count = bucketStats.counts[slot];
    if (count <= 0) {
      continue;
    }
    entries.push({
      slot,
      count,
      start: bucketStats.starts ? bucketStats.starts[slot] : null,
      end: bucketStats.ends ? bucketStats.ends[slot] : null,
      minimum: [
        bucketStats.minimumX[slot],
        bucketStats.minimumY[slot],
        bucketStats.minimumZ[slot],
      ],
      maximum: [
        bucketStats.maximumX[slot],
        bucketStats.maximumY[slot],
        bucketStats.maximumZ[slot],
      ],
    });
  }
  return entries;
}

function partitionKdPositionIndexRange(
  stats,
  positions,
  indices,
  start,
  end,
  progressState = null,
) {
  const action = stats.action;
  const usesPlane =
    action &&
    action.splitDirection &&
    Number.isFinite(action.splitOffset);
  const axis = usesPlane ? null : kdSplitAxisForAction(action);
  ensure(
    usesPlane || (Number.isInteger(axis) && axis >= 0 && axis < 3),
    `Missing k-d split plane for node ${stats.node.key}.`,
  );
  const split = usesPlane ? action.splitOffset : action.splitPoint[axis];
  ensure(
    Number.isFinite(split),
    `Missing k-d split offset for node ${stats.node.key}.`,
  );

  const lower = makeAdaptiveChildEntry(0, start, null);
  const upperSlot = usesPlane ? 1 : 1 << axis;
  const upper = makeAdaptiveChildEntry(upperSlot, null, end);
  let left = start;
  let right = end - 1;
  let pendingProgressRows = 0;

  while (left <= right) {
    pendingProgressRows += 1;
    if (pendingProgressRows >= TILING_TREE_PROGRESS_ROW_INTERVAL) {
      advanceTilingProgress(progressState, pendingProgressRows);
      pendingProgressRows = 0;
    }

    const rowIndex = indices[left];
    const base = rowIndex * 3;
    const x = positions[base + 0];
    const y = positions[base + 1];
    const z = positions[base + 2];
    const coordinate = usesPlane
      ? dotDirection(action.splitDirection, x, y, z)
      : coordinateForAxis(axis, x, y, z);

    if (coordinate >= split) {
      updateAdaptiveChildEntry(upper, x, y, z);
      const swap = indices[right];
      indices[right] = rowIndex;
      indices[left] = swap;
      right -= 1;
    } else {
      updateAdaptiveChildEntry(lower, x, y, z);
      left += 1;
    }
  }

  lower.end = left;
  upper.start = left;
  ensure(
    lower.count + upper.count === end - start,
    `K-d child partition row mismatch for node ${stats.node.key}.`,
  );
  ensure(
    lower.end - lower.start === lower.count &&
      upper.end - upper.start === upper.count,
    `K-d child partition range mismatch for node ${stats.node.key}.`,
  );

  if (pendingProgressRows > 0) {
    advanceTilingProgress(progressState, pendingProgressRows);
  }

  const occupied = [];
  if (lower.count > 0) {
    occupied.push(lower);
  }
  if (upper.count > 0) {
    occupied.push(upper);
  }
  return occupied;
}

function partitionPositionIndexRangeByAdaptiveSlot(
  stats,
  bucketStats,
  positions,
  indices,
  progressState = null,
) {
  const starts = bucketStats.starts;
  const ends = bucketStats.ends;
  const next = new Uint32Array(starts);
  let pendingProgressRows = 0;
  for (let bucket = 0; bucket < bucketStats.bucketCount; bucket++) {
    let offset = next[bucket];
    const bucketEnd = ends[bucket];
    while (offset < bucketEnd) {
      pendingProgressRows += 1;
      if (pendingProgressRows >= TILING_TREE_PROGRESS_ROW_INTERVAL) {
        advanceTilingProgress(progressState, pendingProgressRows);
        pendingProgressRows = 0;
      }
      const rowIndex = indices[offset];
      const slot = childSlotForAdaptiveActionAtRow(stats, positions, rowIndex);
      let targetBucket = slot;
      if (bucketStats.sparse) {
        targetBucket = bucketStats.slotToBucketIndex.get(slot);
        ensure(
          Number.isInteger(targetBucket),
          `Adaptive child slot ${slot} is not occupied while partitioning node ${stats.node.key}.`,
        );
      }
      if (targetBucket === bucket) {
        offset += 1;
        next[bucket] = offset;
        continue;
      }
      ensure(
        targetBucket >= 0 && targetBucket < bucketStats.bucketCount,
        `Adaptive child slot ${slot} is outside bucket count ${bucketStats.actionBucketCount}.`,
      );
      const target = next[targetBucket];
      ensure(
        target < ends[targetBucket],
        `Adaptive child bucket ${slot} overflow while partitioning node ${stats.node.key}.`,
      );
      next[targetBucket] = target + 1;
      indices[offset] = indices[target];
      indices[target] = rowIndex;
    }
  }
  if (pendingProgressRows > 0) {
    advanceTilingProgress(progressState, pendingProgressRows);
  }
}

async function forEachStagedPosition(source, callback) {
  if (source.positions) {
    const positions = source.positions;
    const weights = source.weights || null;
    for (let rowIndex = 0; rowIndex < source.vertexCount; rowIndex++) {
      const base = rowIndex * 3;
      callback(
        positions[base + 0],
        positions[base + 1],
        positions[base + 2],
        weights ? weights[rowIndex] : 1.0,
      );
    }
    return;
  }

  const targetChunkBytes =
    Number.isFinite(source.chunkBytes) && source.chunkBytes > 0
      ? Math.floor(source.chunkBytes)
      : 8 * 1024 * 1024;
  const rowsPerChunk = Math.max(
    1,
    Math.floor(targetChunkBytes / POSITION_ROW_BYTE_SIZE),
  );
  const chunk = Buffer.allocUnsafe(rowsPerChunk * POSITION_ROW_BYTE_SIZE);
  const floatView =
    IS_LITTLE_ENDIAN && (chunk.byteOffset & 3) === 0
      ? new Float32Array(chunk.buffer, chunk.byteOffset, rowsPerChunk * 4)
      : null;
  const handle = await fs.promises.open(source.positionsPath, 'r');
  try {
    let fileOffset = 0;
    for (
      let rowBase = 0;
      rowBase < source.vertexCount;
      rowBase += rowsPerChunk
    ) {
      const rowCount = Math.min(rowsPerChunk, source.vertexCount - rowBase);
      const byteCount = rowCount * POSITION_ROW_BYTE_SIZE;
      await readExactFromHandle(
        handle,
        chunk,
        byteCount,
        fileOffset,
        `Staged position file ended early: ${source.positionsPath}`,
      );
      fileOffset += byteCount;

      for (let i = 0; i < rowCount; i++) {
        let x;
        let y;
        let z;
        let weight;
        if (floatView) {
          const base = i * 4;
          x = floatView[base + 0];
          y = floatView[base + 1];
          z = floatView[base + 2];
          weight = floatView[base + 3];
        } else {
          const base = i * POSITION_ROW_BYTE_SIZE;
          x = chunk.readFloatLE(base + 0);
          y = chunk.readFloatLE(base + 4);
          z = chunk.readFloatLE(base + 8);
          weight = chunk.readFloatLE(base + 12);
        }
        callback(x, y, z, weight);
      }
    }
  } finally {
    await handle.close();
  }
}

function forEachNodeOnPointPath(root, x, y, z, callback) {
  let node = root;
  while (node) {
    callback(node);
    if (node.leaf) {
      return;
    }

    let slot;
    let child;
    if (usesSegmentRouting(node)) {
      slot = pointSegmentForNode(node, x, y, z);
      child = ensureChildrenBySegment(node).get(slot);
    } else {
      slot = pointKdSlotForNode(node, x, y, z);
      child = node.childrenByOct[slot];
    }
    ensure(
      !!child,
      `Failed to resolve OBB stats path for point at node ${node.key} slot=${slot}.`,
    );
    node = child;
  }
}

async function computeRootBasisAxesFromPositions(source) {
  const stats = makeOrientedBoxStats(null);
  await forEachStagedPosition(source, (x, y, z) => {
    updatePositionMomentStats(stats, x, y, z);
  });
  return orthonormalBasisFromMomentStats(stats);
}

async function computeNodeOrientedBoxesFromPositions(
  root,
  source,
  rootBasisAxes = null,
) {
  const statsByNode = new Map();
  await forEachStagedPosition(source, (x, y, z) => {
    forEachNodeOnPointPath(root, x, y, z, (node) => {
      let stats = statsByNode.get(node);
      if (!stats) {
        stats = makeOrientedBoxStats(node);
        statsByNode.set(node, stats);
      }
      updatePositionMomentStats(stats, x, y, z);
    });
  });

  const rootStats = statsByNode.get(root);
  const activeRootBasisAxes =
    rootBasisAxes || (rootStats ? orthonormalBasisFromMomentStats(rootStats) : null);

  for (const stats of statsByNode.values()) {
    const node = stats.node;
    node.count = stats.count;
    node.bounds = boundsFromMinMax(stats.minimum, stats.maximum, node.bounds);
    prepareOrientedBoxStats(stats, activeRootBasisAxes);
  }

  await forEachStagedPosition(source, (x, y, z) => {
    forEachNodeOnPointPath(root, x, y, z, (node) => {
      const stats = statsByNode.get(node);
      if (stats) {
        updateOrientedBoxProjectionStats(stats, x, y, z);
      }
    });
  });

  for (const stats of statsByNode.values()) {
    const node = stats.node;
    node.orientedBox = normalizeOrientedBox(
      orientedBoxFromProjectionStats(stats, node.bounds),
    );
  }
}

async function buildAdaptiveNodeTreeFromPositions(
  source,
  rootBounds,
  maxDepth,
  leafLimit,
  options = {},
) {
  const orientedBoundingBoxes = options.orientedBoundingBoxes !== false;
  const rootBasisAxes = orientedBoundingBoxes
    ? await computeRootBasisAxesFromPositions(source)
    : null;
  const buildOptions = {
    ...options,
    splitBasisAxes: rootBasisAxes,
  };
  const root = source.positions
    ? await buildAdaptiveNodeTreeFromMemoryPositions(
      source,
      rootBounds,
      maxDepth,
      leafLimit,
      buildOptions,
    )
    : await buildAdaptiveNodeTreeFromStreamingPositions(
        source,
        rootBounds,
        maxDepth,
        leafLimit,
        buildOptions,
      );
  if (orientedBoundingBoxes) {
    await computeNodeOrientedBoxesFromPositions(root, source, rootBasisAxes);
  }
  return root;
}

async function buildAdaptiveNodeTreeFromMemoryPositions(
  source,
  rootBounds,
  maxDepth,
  leafLimit,
  options = {},
) {
  let nextPhysicalCoordinate = 1;
  const allocateChildCoordinates = (parent) => ({
    level: parent.level + 1,
    x: nextPhysicalCoordinate++,
    y: 0,
    z: 0,
  });

  const root = makeAdaptiveRootNode(rootBounds, source.vertexCount);

  if (maxDepth <= 0 || source.vertexCount <= 1) {
    return root;
  }

  const positions = source.positions;
  const weights = source.weights || null;
  ensure(
    positions && positions.length >= source.vertexCount * 3,
    'In-memory staged positions are missing or truncated.',
  );
  const indices = makePositionIndexArray(source.vertexCount);
  const progress = makeTilingProgressState(
    options.progress || null,
    source.vertexCount,
    maxDepth,
  );
  const splitBasisAxes = options.splitBasisAxes || null;
  setNodePositionIndexRange(root, 0, source.vertexCount);

  try {
    while (true) {
      const candidates = [];
      collectAdaptiveSplitCandidates(root, maxDepth, leafLimit, candidates);
      if (candidates.length === 0) {
        break;
      }

      const candidateRanges = [];
      let candidateRowTotal = 0;
      for (const node of candidates) {
        const start = node._positionIndexStart;
        const end = node._positionIndexEnd;
        ensure(
          Number.isInteger(start) && Number.isInteger(end) && end >= start,
          `Missing in-memory position index range for node ${node.key}.`,
        );
        candidateRanges.push({ node, start, end });
        candidateRowTotal += end - start;
      }
      const splitProgress = startTilingProgressPhase(
        progress,
        candidateRowTotal,
        `building k-d tree | split candidates=${candidates.length}`,
      );
      const statsByNode = new Map();
      for (const range of candidateRanges) {
        const node = range.node;
        const stats = makeAdaptiveSplitStats(node, splitBasisAxes);
        updateAdaptiveSplitStatsForPositionIndexRange(
          stats,
          positions,
          weights,
          indices,
          range.start,
          range.end,
          splitProgress,
        );
        statsByNode.set(node, stats);
      }
      advanceTilingProgress(splitProgress, 0, { force: true });

      const actionStats = [];
      const kdActionStats = [];
      for (const stats of statsByNode.values()) {
        stats.node.count = stats.count;
        const tightBounds = boundsFromMinMax(
          stats.minimum,
          stats.maximum,
          stats.node.bounds,
        );
        const action = chooseAdaptiveSplitAction(
          stats,
          tightBounds,
          maxDepth,
          leafLimit,
          splitBasisAxes,
        );
        if (!action.kind) {
          stats.node.bounds = tightBounds;
          if (action.aspectExhausted) {
            stats.node.aspectSplitExhausted = true;
          }
          if (action.splitExhausted) {
            stats.node.splitExhausted = true;
          }
          continue;
        }
        stats.node.bounds = tightBounds;
        stats.action = action;
        actionStats.push(stats);
        if (action.kind === ROUTE_MODE_KD) {
          resetAdaptiveProjectionStats(stats);
          kdActionStats.push(stats);
        }
      }

      if (kdActionStats.length > 0) {
        let projectionRowTotal = 0;
        for (const stats of kdActionStats) {
          projectionRowTotal +=
            stats.node._positionIndexEnd - stats.node._positionIndexStart;
        }
        const projectionProgress = startTilingProgressPhase(
          progress,
          projectionRowTotal,
          `building k-d tree | root-basis splits=${kdActionStats.length}`,
        );
        for (const stats of kdActionStats) {
          updateAdaptiveProjectionStatsForPositionIndexRange(
            stats,
            positions,
            weights,
            indices,
            stats.node._positionIndexStart,
            stats.node._positionIndexEnd,
            projectionProgress,
          );
        }
        advanceTilingProgress(projectionProgress, 0, { force: true });
      }

      const splittableStats = [];
      for (const stats of actionStats) {
        if (
          stats.action.kind === ROUTE_MODE_KD &&
          !finalizeKdSplitAction(stats)
        ) {
          stats.node.splitExhausted = true;
          continue;
        }
        applyAdaptiveSplitActionToNode(stats.node, stats.action);
        splittableStats.push(stats);
      }

      if (splittableStats.length === 0) {
        break;
      }

      let bucketProgressRowTotal = 0;
      for (const stats of splittableStats) {
        const rowCount =
          stats.node._positionIndexEnd - stats.node._positionIndexStart;
        noteTilingVirtualSegmentAction(progress, stats.action);
        bucketProgressRowTotal +=
          rowCount * (stats.action.kind === ROUTE_MODE_KD ? 1 : 2);
      }
      const bucketProgress = startTilingProgressPhase(
        progress,
        bucketProgressRowTotal,
        `building k-d tree | bucket splits=${splittableStats.length}`,
      );
      let splitNodeCount = 0;
      for (const stats of splittableStats) {
        const node = stats.node;
        const start = node._positionIndexStart;
        const end = node._positionIndexEnd;
        let occupied;
        if (stats.action.kind === ROUTE_MODE_KD) {
          occupied = partitionKdPositionIndexRange(
            stats,
            positions,
            indices,
            start,
            end,
            bucketProgress,
          );
        } else {
          const bucketStats = collectAdaptiveChildBucketStatsForPositionRange(
            stats,
            positions,
            indices,
            start,
            end,
            bucketProgress,
          );
          assignAdaptiveChildBucketRanges(bucketStats, start, end);
          occupied = adaptiveChildBucketEntries(bucketStats);

          if (occupied.length > 1) {
            partitionPositionIndexRangeByAdaptiveSlot(
              stats,
              bucketStats,
              positions,
              indices,
              bucketProgress,
            );
          } else {
            advanceTilingProgress(bucketProgress, end - start);
          }
        }

        if (occupied.length <= 1) {
          clearNodeSplitRouting(node);
          node.leaf = true;
          if (stats.action.kind === ROUTE_MODE_AXIS_SEGMENTS) {
            node.aspectSplitExhausted = true;
          } else {
            node.splitExhausted = true;
          }
          continue;
        }

        node.leaf = false;
        node.virtual = stats.action.kind === ROUTE_MODE_AXIS_SEGMENTS;
        node.children = [];
        node.childrenByOct = new Array(8).fill(null);
        node.childrenBySegment = node.virtual ? new Map() : null;
        node.occupiedChildCount = occupied.length;
        for (const entry of occupied) {
          const slot = entry.slot;
          const coords = allocateChildCoordinates(node);
          const child = makePartitionTreeNode({
            level: coords.level,
            depth:
              stats.action.kind === ROUTE_MODE_AXIS_SEGMENTS
                ? node.depth
                : node.depth + 1,
            x: coords.x,
            y: coords.y,
            z: coords.z,
            childSlot: slot,
            bounds: boundsFromMinMax(entry.minimum, entry.maximum, node.bounds),
            count: entry.count,
            leaf: true,
          });
          setNodePositionIndexRange(child, entry.start, entry.end);
          node.children.push(child);
          if (node.virtual) {
            ensureChildrenBySegment(node).set(slot, child);
          } else {
            node.childrenByOct[slot] = child;
          }
        }
        splitNodeCount += 1;
      }

      advanceTilingProgress(bucketProgress, 0, { force: true });
      if (splitNodeCount === 0) {
        break;
      }
    }
  } finally {
    clearNodePositionIndexRanges(root);
  }

  return root;
}

async function buildAdaptiveNodeTreeFromStreamingPositions(
  source,
  rootBounds,
  maxDepth,
  leafLimit,
  options = {},
) {
  let nextPhysicalCoordinate = 1;
  const allocateChildCoordinates = (parent) => ({
    level: parent.level + 1,
    x: nextPhysicalCoordinate++,
    y: 0,
    z: 0,
  });

  const root = makeAdaptiveRootNode(rootBounds, source.vertexCount);
  const splitBasisAxes = options.splitBasisAxes || null;

  if (maxDepth <= 0 || source.vertexCount <= 1) {
    return root;
  }

  while (true) {
    const candidates = [];
    collectAdaptiveSplitCandidates(root, maxDepth, leafLimit, candidates);
    if (candidates.length === 0) {
      break;
    }

    const statsByNode = new Map();
    for (const node of candidates) {
      statsByNode.set(node, makeAdaptiveSplitStats(node, splitBasisAxes));
    }

    await forEachStagedPosition(source, (x, y, z, weight) => {
      const leaf = resolveLeafNodeForPoint(root, x, y, z);
      const stats = statsByNode.get(leaf);
      if (stats) {
        updateAdaptiveSplitStats(stats, x, y, z, weight);
      }
    });

    const actionStats = [];
    const kdActionStats = [];
    for (const stats of statsByNode.values()) {
      stats.node.count = stats.count;
      const tightBounds = boundsFromMinMax(
        stats.minimum,
        stats.maximum,
        stats.node.bounds,
      );
      const action = chooseAdaptiveSplitAction(
        stats,
        tightBounds,
        maxDepth,
        leafLimit,
        splitBasisAxes,
      );
      if (!action.kind) {
        stats.node.bounds = tightBounds;
        if (action.aspectExhausted) {
          stats.node.aspectSplitExhausted = true;
        }
        if (action.splitExhausted) {
          stats.node.splitExhausted = true;
        }
        continue;
      }
      stats.node.bounds = tightBounds;
      stats.action = action;
      if (action.kind === ROUTE_MODE_KD) {
        resetAdaptiveProjectionStats(stats);
        kdActionStats.push(stats);
      }
      actionStats.push(stats);
    }

    if (kdActionStats.length > 0) {
      const kdStatsByNode = new Map();
      for (const stats of kdActionStats) {
        kdStatsByNode.set(stats.node, stats);
      }
      await forEachStagedPosition(source, (x, y, z, weight) => {
        const leaf = resolveLeafNodeForPoint(root, x, y, z);
        const stats = kdStatsByNode.get(leaf);
        if (stats) {
          updateAdaptiveProjectionStats(stats, x, y, z, weight);
        }
      });
    }

    const splittableStats = [];
    for (const stats of actionStats) {
      if (
        stats.action.kind === ROUTE_MODE_KD &&
        !finalizeKdSplitAction(stats)
      ) {
        stats.node.splitExhausted = true;
        continue;
      }
      applyAdaptiveSplitActionToNode(stats.node, stats.action);
      stats.childStats = new Map();
      splittableStats.push(stats);
    }

    if (splittableStats.length === 0) {
      break;
    }

    const splittableByNode = new Map();
    for (const stats of splittableStats) {
      splittableByNode.set(stats.node, stats);
    }

    await forEachStagedPosition(source, (x, y, z) => {
      const leaf = resolveLeafNodeForPoint(root, x, y, z);
      const stats = splittableByNode.get(leaf);
      if (!stats) {
        return;
      }
      const slot = childSlotForAdaptiveAction(stats, x, y, z);
      updateAdaptiveChildStats(stats, slot, x, y, z);
    });

    let splitNodeCount = 0;
    for (const stats of splittableStats) {
      const node = stats.node;
      const occupied = sortedAdaptiveChildStats(stats);

      if (occupied.length <= 1) {
        clearNodeSplitRouting(node);
        node.leaf = true;
        if (stats.action.kind === ROUTE_MODE_AXIS_SEGMENTS) {
          node.aspectSplitExhausted = true;
        } else {
          node.splitExhausted = true;
        }
        continue;
      }

      node.leaf = false;
      node.virtual = stats.action.kind === ROUTE_MODE_AXIS_SEGMENTS;
      node.children = [];
      node.childrenByOct = new Array(8).fill(null);
      node.childrenBySegment = node.virtual ? new Map() : null;
      node.occupiedChildCount = occupied.length;
      for (const entry of occupied) {
        const slot = entry.slot;
        const coords = allocateChildCoordinates(node);
        const child = makePartitionTreeNode({
          level: coords.level,
          depth:
            stats.action.kind === ROUTE_MODE_AXIS_SEGMENTS
              ? node.depth
              : node.depth + 1,
          x: coords.x,
          y: coords.y,
          z: coords.z,
          childSlot: slot,
          bounds: boundsFromMinMax(entry.minimum, entry.maximum, node.bounds),
          count: entry.count,
          leaf: true,
        });
        node.children.push(child);
        if (node.virtual) {
          ensureChildrenBySegment(node).set(slot, child);
        } else {
          node.childrenByOct[slot] = child;
        }
      }
      splitNodeCount += 1;
    }

    if (splitNodeCount === 0) {
      break;
    }
  }

  return root;
}

async function isPartitionedNodeComplete(node, ctx) {
  if (node.virtual) {
    if (node.buildState !== 'completed') {
      return false;
    }
    if (node.depth > 0 && !node.handoffConsumed) {
      return hasExistingActiveHandoffSources(node);
    }
    return true;
  }
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

async function hasExistingActiveHandoffSources(node) {
  let spec;
  try {
    spec = handoffBucketSpec(node);
  } catch {
    return false;
  }
  const sources = flattenBucketSpecs([spec]).filter(
    (source) => source && source.filePath,
  );
  if (sources.length === 0) {
    return false;
  }
  for (const source of sources) {
    if (!(await pathExists(source.filePath))) {
      return false;
    }
  }
  return true;
}

function consumeHandoffSources(node, ctx) {
  if (!node || node.handoffConsumed) {
    return;
  }
  if (node.handoffPath) {
    ctx.pendingHandoffCleanup.add(node.handoffPath);
    node.handoffPath = null;
    node.handoffRowCount = null;
    node.handoffConsumed = true;
    return;
  }
  if (node.virtual) {
    for (const child of node.children) {
      consumeHandoffSources(child, ctx);
    }
  }
  node.handoffPath = null;
  node.handoffRowCount = null;
  node.handoffConsumed = true;
}

async function processPartitionedLeafNode(node, ctx) {
  ensure(!!node.bucketPath, `Missing leaf bucket path for node ${node.key}.`);
  const { entries, totalRows } = await collectBucketEntries(
    [leafBucketSpec(node)],
    ctx.layout.coeffCount,
  );
  node.bucketRowCount = totalRows;
  let handoffPromise = Promise.resolve();
  if (node.depth > 0) {
    node.handoffPath = canonicalNodePath(ctx.tempDir, HANDOFF_BUCKET_DIR, node);
    handoffPromise = materializeLinkedHandoffFile(
      node.bucketPath,
      node.handoffPath,
    );
    node.handoffRowCount = totalRows;
    node.handoffConsumed = false;
  } else {
    node.handoffPath = null;
    node.handoffRowCount = null;
    node.handoffConsumed = true;
  }

  node.ownError = 0.0;
  const contentPromise = writeBucketContentFile(
    ctx.params,
    entries,
    ctx.layout.coeffCount,
    totalRows,
    ctx.layout.degree,
    node.level,
    node.x,
    node.y,
    node.z,
    safeNodeBoundsTranslation(node),
  ).then((contentUri) => {
    node.contentUri = contentUri;
  });
  await Promise.all([handoffPromise, contentPromise]);
  node.buildState = 'completed';
  enqueuePipelineStateSave(ctx, PIPELINE_STAGE_BUCKETED);
}

async function processPartitionedVirtualInternalNode(node, ctx) {
  const inputSpecs = node.children.map((child) => handoffBucketSpec(child));
  const { entries, totalRows } = await collectBucketEntries(
    inputSpecs,
    ctx.layout.coeffCount,
  );

  ensure(
    entries.length > 0 && totalRows > 0,
    `Virtual node ${node.key} has no active handoff sources.`,
  );
  node.handoffPath = null;
  node.handoffRowCount = totalRows;
  node.handoffConsumed = node.depth <= 0;

  node.ownError = 0.0;
  node.contentUri = null;
  node.buildState = 'completed';
  enqueuePipelineStateSave(ctx, PIPELINE_STAGE_BUCKETED);
}

async function processPartitionedInternalNode(node, ctx) {
  if (node.virtual) {
    await processPartitionedVirtualInternalNode(node, ctx);
    return;
  }

  const inputSpecs = node.children.map((child) => handoffBucketSpec(child));
  const { entries, totalRows } = await collectBucketEntries(
    inputSpecs,
    ctx.layout.coeffCount,
  );
  const contentTarget = resolveNodeContentTarget(node, ctx, totalRows);
  if (totalRows <= contentTarget) {
    let handoffPromise = Promise.resolve();
    if (node.depth > 0) {
      node.handoffPath = canonicalNodePath(
        ctx.tempDir,
        HANDOFF_BUCKET_DIR,
        node,
      );
      handoffPromise = materializeCanonicalEntriesFile(
        entries,
        node.handoffPath,
        ctx.layout.coeffCount,
        { bucketChunkBytes: ctx.bucketChunkBytes },
      );
      node.handoffRowCount = totalRows;
      node.handoffConsumed = false;
    } else {
      node.handoffPath = null;
      node.handoffRowCount = null;
      node.handoffConsumed = true;
    }

    node.ownError = 0.0;
    const contentPromise = writeBucketContentFile(
      ctx.params,
      entries,
      ctx.layout.coeffCount,
      totalRows,
      ctx.layout.degree,
      node.level,
      node.x,
      node.y,
      node.z,
      safeNodeBoundsTranslation(node),
    ).then((contentUri) => {
      node.contentUri = contentUri;
    });
    await Promise.all([handoffPromise, contentPromise]);
  } else {
    const handoffPath =
      node.depth > 0
        ? canonicalNodePath(ctx.tempDir, HANDOFF_BUCKET_DIR, node)
        : null;
    const result = await writeSimplifiedBucketContentFile(
      ctx.params,
      entries,
      ctx.layout.coeffCount,
      totalRows,
      contentTarget,
      node.bounds,
      node.level,
      node.x,
      node.y,
      node.z,
      handoffPath,
      {
        bucketChunkBytes: ctx.bucketChunkBytes,
        simplifyScratchBytes: ctx.simplifyScratchBytes,
        bucketEntryCacheBytes: ctx.bucketEntryCacheBytes,
        errorBufferBytes: ctx.simplifyScratchBytes,
        translation: safeNodeBoundsTranslation(node),
      },
    );
    if (node.depth > 0) {
      node.handoffPath = handoffPath;
      node.handoffRowCount = result.handoffRowCount;
      node.handoffConsumed = false;
    } else {
      node.handoffPath = null;
      node.handoffRowCount = null;
      node.handoffConsumed = true;
    }

    node.ownError = result.ownError;
    node.contentUri = result.contentUri;
  }
  node.buildState = 'completed';
  for (const child of node.children) {
    consumeHandoffSources(child, ctx);
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

async function runWithConcurrencyBudget(
  items,
  limit,
  budgetBytes,
  estimateBytes,
  onItem,
) {
  if (!items || items.length === 0) {
    return;
  }

  const concurrency = Math.max(
    1,
    Math.min(items.length, Math.floor(limit || 1)),
  );
  const budget = Math.max(1, Math.floor(budgetBytes || 1));
  const pending = items.map((item, index) => ({
    item,
    index,
    estimated: Math.max(1, Math.floor(estimateBytes ? estimateBytes(item) : 1)),
  }));
  let activeCount = 0;
  let activeBytes = 0;

  await new Promise((resolve, reject) => {
    let settled = false;
    const findLaunchIndex = () => {
      if (pending.length === 0 || activeCount >= concurrency) {
        return -1;
      }
      if (activeCount === 0) {
        return 0;
      }

      const remaining = Math.max(0, budget - activeBytes);
      let bestIndex = -1;
      let bestBytes = 0;
      for (let i = 0; i < pending.length; i++) {
        const estimated = pending[i].estimated;
        if (estimated > remaining) {
          continue;
        }
        if (bestIndex < 0 || estimated > bestBytes) {
          bestIndex = i;
          bestBytes = estimated;
        }
      }
      return bestIndex;
    };

    const maybeLaunch = () => {
      if (settled) {
        return;
      }

      while (pending.length > 0 && activeCount < concurrency) {
        const launchIndex = findLaunchIndex();
        if (launchIndex < 0) {
          break;
        }

        const { item, index, estimated } = pending.splice(launchIndex, 1)[0];
        activeCount += 1;
        activeBytes += estimated;

        Promise.resolve()
          .then(() => onItem(item, index))
          .then(
            () => {
              activeCount -= 1;
              activeBytes -= estimated;
              if (pending.length === 0 && activeCount === 0) {
                settled = true;
                resolve();
                return;
              }
              maybeLaunch();
            },
            (err) => {
              settled = true;
              reject(err);
            },
          );
      }

      if (pending.length === 0 && activeCount === 0) {
        settled = true;
        resolve();
      }
    };

    maybeLaunch();
  });
}

function estimateBuildNodeInputRows(node) {
  if (node.leaf) {
    const bucketRows =
      Number.isInteger(node.bucketRowCount) && node.bucketRowCount >= 0
        ? node.bucketRowCount
        : null;
    return Math.max(1, bucketRows ?? node.count ?? 1);
  }
  let total = 0;
  for (const child of node.children) {
    total += estimateHandoffSourceRows(child);
  }
  return Math.max(1, total > 0 ? total : (node.count ?? 1));
}

function estimateHandoffSourceRows(node) {
  if (!node || node.handoffConsumed) {
    return 0;
  }
  if (Number.isInteger(node.handoffRowCount) && node.handoffRowCount >= 0) {
    return node.handoffRowCount;
  }
  if (node.virtual && !node.handoffPath) {
    return node.children.reduce(
      (sum, child) => sum + estimateHandoffSourceRows(child),
      0,
    );
  }
  return Number.isInteger(node.count) && node.count > 0 ? node.count : 0;
}

function estimateBuildNodeMemoryBytes(node, ctx) {
  const inputRows = estimateBuildNodeInputRows(node);
  const coeffCount = ctx.layout.coeffCount;
  const rowByteSize = bucketRowByteSize(HANDOFF_BUCKET_ENCODING, coeffCount);
  const spzRowBytes = spzBytesPerBucketRow(coeffCount);
  const base = BUILD_MIN_TASK_MEMORY_BYTES;

  if (node.leaf) {
    return base + inputRows * (spzRowBytes * 2 + rowByteSize * 0.25);
  }

  if (node.virtual) {
    return base + inputRows * rowByteSize;
  }

  const targetRows = resolveNodeContentTarget(node, ctx, inputRows);
  if (inputRows <= targetRows) {
    return base + inputRows * (spzRowBytes * 2 + rowByteSize);
  }

  return (
    base +
    inputRows * (rowByteSize + 96) +
    targetRows * (rowByteSize + spzRowBytes * 2)
  );
}

async function processBuildNodes(nodes, concurrency, ctx) {
  await runWithConcurrencyBudget(
    nodes,
    concurrency,
    ctx.buildMemoryBudgetBytes,
    (node) => estimateBuildNodeMemoryBytes(node, ctx),
    async (node) => {
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
    },
  );
}

function tickBuildProgress(ctx, node, status) {
  if (!ctx || !ctx.buildProgress || !node) {
    return;
  }
  const kind = node.virtual ? 'virtual' : node.leaf ? 'leaf' : 'internal';
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
  ensure(
    !node.virtual,
    `Virtual node ${node.key} cannot be emitted as a tile.`,
  );
  const error =
    rootGeometricError *
    geometricErrorScaleForDepth(node.depth, lodMaxDepth, samplingRatePerLevel);
  const children = buildEmittedTileChildren(
    node,
    rootGeometricError,
    lodMaxDepth,
    samplingRatePerLevel,
    nodesByKey,
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
  tileNode.orientedBox = node.orientedBox ? node.orientedBox.slice() : null;
  nodesByKey.set(tileNode.key(), tileNode);
  return tileNode;
}

function buildEmittedTileChildren(
  node,
  rootGeometricError,
  lodMaxDepth,
  samplingRatePerLevel,
  nodesByKey,
) {
  const children = [];
  const directRealChildren = node.children.filter((child) => !child.virtual);
  const virtualChildren = node.children.filter((child) => child.virtual);
  for (const child of directRealChildren) {
    children.push(
      buildTileNodeTree(
        child,
        rootGeometricError,
        lodMaxDepth,
        samplingRatePerLevel,
        nodesByKey,
      ),
    );
  }
  for (const child of virtualChildren) {
    children.push(
      ...buildEmittedTileChildren(
        child,
        rootGeometricError,
        lodMaxDepth,
        samplingRatePerLevel,
        nodesByKey,
      ),
    );
  }
  return children;
}

function tileToJson(node, sourceCoordinateSystem = null) {
  const box = node.orientedBox || node.bounds.toBoxArray();
  const obj = {
    boundingVolume: {
      box: applyContentBoxTransform(box, sourceCoordinateSystem),
    },
    geometricError: node.error,
    refine: 'REPLACE',
    content: { uri: node.contentUri },
  };
  if (node.children.length > 0) {
    obj.children = node.children.map((child) =>
      tileToJson(child, sourceCoordinateSystem),
    );
  }
  return obj;
}

function makeBuildSummary(
  args,
  header,
  layout,
  rootNode,
  rootGeometricError,
  rootGeometricErrorSource,
  nodeCount,
  maxDepth,
  availableLevels,
  physicalMaxLevel,
  physicalLevels,
  partitionNodeCount,
  virtualNodeCount,
  checkpointInfo,
  memoryPlan,
  timingsMs = {},
  peakRssBytes = null,
) {
  const samplingDivisorsByDepth = {};
  const samplingRatesByDepth = {};
  const geometricErrorScaleByDepth = {};
  const geometricErrorByDepth = {};
  const effectiveMaxDepth = maxDepth;
  const resolvedMemoryPlan = memoryPlan || makeMemoryBudgetPlan(args);

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
    memory_budget_gb: args.memoryBudget,
    memory_budget_plan: serializeMemoryBudgetPlan(resolvedMemoryPlan),
    build_concurrency: resolvedMemoryPlan.buildConcurrency,
    content_workers: resolvedMemoryPlan.contentWorkers,
    partition_write_concurrency: resolvedMemoryPlan.partitionWriteConcurrency,
    timings_ms: timingsMs,
    peak_rss_bytes:
      Number.isFinite(peakRssBytes) && peakRssBytes > 0
        ? Math.floor(peakRssBytes)
        : null,
    sampling_rate_per_level: args.samplingRatePerLevel,
    tiling_strategy: TILING_STRATEGY_KD_TREE,
    kd_tree_split_direction: kdTreeSplitDirectionForArgs(args),
    kd_tree_covariance_weighting: KD_TREE_COVARIANCE_WEIGHTING,
    kd_tree_split_plane: KD_TREE_SPLIT_PLANE,
    kd_tree_split_balance: kdTreeSplitBalanceForArgs(args),
    oriented_bounding_boxes: useOrientedBoundingBoxes(args),
    tile_bounding_volume_mode: tileBoundingVolumeModeForArgs(args),
    split_weight_formula: SPLIT_WEIGHT_FORMULA,
    kd_tree_split_histogram_bins: ADAPTIVE_SPLIT_HISTOGRAM_BINS,
    kd_tree_max_long_width_ratio: ADAPTIVE_MAX_LONG_WIDTH_RATIO,
    kd_tree_long_tile_split_mode: LONG_TILE_SPLIT_MODE,
    kd_tree_long_tile_split_virtual: true,
    content_codec: 'spz_stream',
    spz_version: SPZ_STREAM_VERSION,
    spz_sh1_bits: args.spzSh1Bits,
    spz_sh_rest_bits: args.spzShRestBits,
    spz_compression_level: args.spzCompressionLevel,
    source_coordinate_system:
      args.resolvedSourceCoordinateSystem || DEFAULT_SOURCE_COORDINATE_SYSTEM,
    source_coordinate_system_source:
      args.resolvedSourceCoordinateSystemSource || 'default',
    source_coordinate_system_reason:
      args.resolvedSourceCoordinateSystemReason || null,
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
    partition_node_count: partitionNodeCount,
    virtual_node_count: virtualNodeCount,
    available_levels: availableLevels,
    effective_max_depth: effectiveMaxDepth,
    physical_levels: physicalLevels,
    physical_max_level: physicalMaxLevel,
    root_geometric_error_source: rootGeometricErrorSource,
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
  const timingsMs = {};
  const conversionStartedAt = Date.now();
  let memoryPlan = makeMemoryBudgetPlan(args);
  const buildConcurrency = memoryPlan.buildConcurrency;
  const contentWorkers = memoryPlan.contentWorkers;
  const partitionWriteConcurrency = memoryPlan.partitionWriteConcurrency;
  const params = {
    outputDir: outputDirAbs,
    colorSpace: args.colorSpace,
    samplingRatePerLevel: args.samplingRatePerLevel,
    sampleMode: args.sampleMode,
    spzSh1Bits: args.spzSh1Bits,
    spzShRestBits: args.spzShRestBits,
    spzCompressionLevel: args.spzCompressionLevel,
    minGeometricError: args.minGeometricError,
    bucketChunkBytes: memoryPlan.bucketChunkBytes,
    sourceCoordinateSystem: DEFAULT_SOURCE_COORDINATE_SYSTEM,
    contentWorkerPool:
      contentWorkers > 0
        ? new SpzContentWorkerPool(contentWorkers, DEFAULT_WORKER_SCRIPT)
        : null,
  };

  if (fs.existsSync(outputDirAbs) && args.clean) {
    const cleanStartedAt = Date.now();
    console.log(`[info] cleaning output directory: ${outputDirAbs}`);
    await fs.promises.rm(outputDirAbs, { recursive: true, force: true });
    timingsMs.clean_output_ms = elapsedMsSince(cleanStartedAt);
    console.log(
      `[info] cleaned output directory | ms=${timingsMs.clean_output_ms}`,
    );
  }
  await fs.promises.mkdir(outputDirAbs, { recursive: true });
  if (fs.existsSync(path.join(outputDirAbs, 'viewer.html'))) {
    await removeFileIfExists(path.join(outputDirAbs, 'viewer.html'));
  }
  await fs.promises.rm(path.join(outputDirAbs, 'subtrees'), {
    recursive: true,
    force: true,
  });

  const inputStat = await fs.promises.stat(inputPath);
  const handle = await fs.promises.open(inputPath, 'r');
  let checkpointInfo = { reused: false, stage: null };
  let pipelineState = null;
  let success = false;
  try {
    console.log(`[info] scanning PLY header: ${inputPath}`);
    const headerStartedAt = Date.now();
    const header = await _readPlyHeaderFromHandle(handle, inputPath);
    const sourceCoordinateSystemDetection =
      detectSourceCoordinateSystemFromPlyHeader(header);
    args.resolvedSourceCoordinateSystem =
      sourceCoordinateSystemDetection.sourceCoordinateSystem;
    args.resolvedSourceCoordinateSystemSource =
      sourceCoordinateSystemDetection.source;
    args.resolvedSourceCoordinateSystemReason =
      sourceCoordinateSystemDetection.reason;
    params.sourceCoordinateSystem =
      sourceCoordinateSystemDetection.sourceCoordinateSystem;
    console.log(
      `[info] source coordinate system: ${sourceCoordinateSystemDetection.sourceCoordinateSystem} | source=${sourceCoordinateSystemDetection.source}`,
    );

    const layout = _buildGaussianPlyLayout(
      header.vertexProps,
      inputPath,
      args.inputConvention,
      args.linearScaleInput,
    );
    timingsMs.header_ms = elapsedMsSince(headerStartedAt);
    const fingerprint = makePipelineFingerprint(
      inputPath,
      inputStat,
      args,
      sourceCoordinateSystemDetection.sourceCoordinateSystem,
    );

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
          `[info] reusing checkpoint | stage=${checkpointInfo.stage}`,
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

    memoryPlan = makeMemoryBudgetPlan(args, { header, layout });
    params.bucketChunkBytes = memoryPlan.bucketChunkBytes;

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
      try {
        console.log(
          `[info] scan 1/4 | vertices=${header.vertexCount} | sh_degree=${layout.degree} | staging positions`,
        );
        const scanStartedAt = Date.now();
        const scanProgress = new ConsoleProgressBar(
          'scan 1/4',
          header.vertexCount,
        );
        scanProgress.update(0, 'staging positions');
        if (memoryPlan.inMemoryPositions) {
          const staged = await scanGlobalBoundsAndStagePositionsInMemory(
            handle,
            inputPath,
            header,
            layout,
            {
              chunkBytes: memoryPlan.scanChunkBytes,
              progress: scanProgress,
            },
          );
          rootBounds = staged.bounds;
          timingsMs.scan_positions_ms = elapsedMsSince(scanStartedAt);
          scanProgress.done(`ms=${timingsMs.scan_positions_ms}`);

          console.log(
            `[info] scan 2/4 | building ${splitBasisLabelForArgs(args)} visual-cost-balanced k-d tiling tree from memory`,
          );
          const tilingStartedAt = Date.now();
          const tilingProgress = new ConsoleProgressBar(
            'tiling',
            header.vertexCount,
          );
          rootNodeMeta = await buildAdaptiveNodeTreeFromPositions(
            {
              positions: staged.positions,
              weights: staged.weights,
              vertexCount: header.vertexCount,
            },
            rootBounds,
            args.maxDepth,
            args.leafLimit,
            {
              progress: tilingProgress,
              orientedBoundingBoxes: useOrientedBoundingBoxes(args),
            },
          );
          timingsMs.build_tiling_tree_ms = elapsedMsSince(tilingStartedAt);
          tilingProgress.done(`ms=${timingsMs.build_tiling_tree_ms}`);
          staged.positions = null;
          staged.weights = null;
        } else {
          rootBounds = await scanGlobalBoundsAndWritePositions(
            handle,
            inputPath,
            header,
            layout,
            positionsPath,
            {
              chunkBytes: memoryPlan.scanChunkBytes,
              positionTmpBufferBytes: memoryPlan.positionTmpBufferBytes,
              progress: scanProgress,
            },
          );
          timingsMs.scan_positions_ms = elapsedMsSince(scanStartedAt);
          scanProgress.done(`ms=${timingsMs.scan_positions_ms}`);

          console.log(
            `[info] scan 2/4 | building ${splitBasisLabelForArgs(args)} visual-cost-balanced k-d tiling tree from positions`,
          );
          const tilingStartedAt = Date.now();
          rootNodeMeta = await buildAdaptiveNodeTreeFromPositions(
            {
              positionsPath,
              vertexCount: header.vertexCount,
              chunkBytes: memoryPlan.scanChunkBytes,
            },
            rootBounds,
            args.maxDepth,
            args.leafLimit,
            { orientedBoundingBoxes: useOrientedBoundingBoxes(args) },
          );
          timingsMs.build_tiling_tree_ms = elapsedMsSince(tilingStartedAt);
        }
      } finally {
        await removeFileIfExists(positionsPath);
      }
      ensure(!!rootNodeMeta, 'Failed to build k-d tiling tree.');
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
    memoryPlan = makeMemoryBudgetPlan(args, { header, layout });
    params.bucketChunkBytes = memoryPlan.bucketChunkBytes;
    const lodMaxDepth = Math.max(
      0,
      Math.min(args.maxDepth, treeStats.maxDepth),
    );
    assignVirtualSegmentContentTargets(
      rootNodeMeta,
      lodMaxDepth,
      args.samplingRatePerLevel,
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
      const partitionProgress = new ConsoleProgressBar(
        'partition',
        header.vertexCount,
      );
      const partitionStartedAt = Date.now();
      const partitionResult = await partitionLeafBuckets(
        handle,
        inputPath,
        header,
        layout,
        rootNodeMeta,
        tempDir,
        {
          progress: partitionProgress,
          memoryPlan,
          writeConcurrency: partitionWriteConcurrency,
        },
      );
      timingsMs.partition_ms = elapsedMsSince(partitionStartedAt);
      partitionProgress.done(`rows=${partitionResult.rowCount}`);
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
      nodeConcurrency: buildConcurrency,
      leafNodeConcurrency: buildConcurrency,
      buildMemoryBudgetBytes: memoryPlan.usableBudgetBytes,
      bucketChunkBytes: memoryPlan.bucketChunkBytes,
      simplifyScratchBytes: memoryPlan.simplifyScratchBytes,
      bucketEntryCacheBytes: memoryPlan.bucketEntryCacheBytes,
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
    const buildStartedAt = Date.now();
    await buildPartitionedBottomUp(rootNodeMeta, ctx);
    timingsMs.build_tiles_ms = elapsedMsSince(buildStartedAt);
    ctx.buildProgress.done(
      `nodes=${treeStats.nodes.length} levels=${treeStats.maxDepth + 1} logical/${treeStats.maxLevel + 1} physical`,
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

    const availableLevels = treeStats.maxDepth + 1;
    const physicalLevels = treeStats.maxLevel + 1;
    const tileset = applyTilesetGltfContentExtensions({
      asset: makeTilesetAsset(),
      geometricError: rootTileNode.error,
      root: applyRootTransform(
        tileToJson(rootTileNode, args.resolvedSourceCoordinateSystem),
        args.transform,
      ),
    });

    const writeMetadataStartedAt = Date.now();
    await fs.promises.writeFile(
      path.join(outputDirAbs, 'tileset.json'),
      JSON.stringify(tileset),
      'utf8',
    );
    timingsMs.write_tileset_ms = elapsedMsSince(writeMetadataStartedAt);
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
          treeStats.maxDepth,
          availableLevels,
          treeStats.maxLevel,
          physicalLevels,
          treeStats.nodes.length,
          treeStats.virtualNodes.length,
          checkpointInfo,
          memoryPlan,
          {
            ...timingsMs,
            total_ms: elapsedMsSince(conversionStartedAt),
          },
          currentPeakRssBytes(),
        ),
      ),
      'utf8',
    );

    console.log(
      `[info] nodes=${nodesByKey.size} | levels=${availableLevels} logical/${physicalLevels} physical | splats=${header.vertexCount}`,
    );
    success = true;
    return {
      splatCount: header.vertexCount,
      shDegree: layout.degree,
      nodeCount: nodesByKey.size,
      levels: availableLevels,
      physicalLevels,
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
  _writeSimplifiedBucketGlbTaskOutput: writeSimplifiedBucketGlbTaskOutput,
};
