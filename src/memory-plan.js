const os = require('os');

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
const PARTITION_ARENA_COUNT = 2;
const POSITION_ROW_FLOAT_COUNT = 4;
const POSITION_ROW_BYTE_SIZE =
  POSITION_ROW_FLOAT_COUNT * Float32Array.BYTES_PER_ELEMENT;
const POSITION_INDEX_ROW_BYTE_SIZE = Uint32Array.BYTES_PER_ELEMENT;
const POSITION_TMP_BUFFER_BYTES = 256 * 1024;

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
  const includePositionWeights = !!(
    args && args.orientedBoundingBoxes === true
  );
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
  const positionFloatCount = header
    ? header.vertexCount * (includePositionWeights ? 4 : 3)
    : null;
  const positionBytes =
    positionFloatCount == null
      ? null
      : positionFloatCount * Float32Array.BYTES_PER_ELEMENT;
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

module.exports = {
  DEFAULT_MEMORY_BUDGET_BYTES,
  MAX_PARTITION_ARENA_BYTES,
  availableWorkerLimit,
  deriveBuildConcurrency,
  deriveContentWorkerCount,
  derivePartitionWriteConcurrency,
  makeMemoryBudgetPlan,
  memoryBudgetBytesFromArgs,
  serializeMemoryBudgetPlan,
};
