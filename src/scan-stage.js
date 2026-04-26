const fs = require('fs');
const path = require('path');

const {
  Bounds,
  ensure,
  _forEachGaussianPlyPosition,
  _forEachGaussianPlyCoreRecord,
  _forEachGaussianPlyCanonicalRecord,
} = require('./parser');
const { writeThreeSigmaExtentComponents } = require('./builder');
const {
  DEFAULT_MEMORY_BUDGET_BYTES,
  MAX_PARTITION_ARENA_BYTES,
  derivePartitionWriteConcurrency,
} = require('./memory-plan');
const { removeFileIfExists } = require('./pipeline-state');
const { canonicalNodePath } = require('./pipeline-paths');
const { makeRowScratch, writeArenaRunsToHandle } = require('./bucket-io');
const {
  nodeBuildState,
  existingNodeBuildState,
  pruneNodeBuildState,
  resolveLeafNodeForPoint,
  makeOrientedBoxStats,
  updateWeightedPositionMomentStats,
  orthonormalBasisFromMomentStats,
  normalizeSplitWeight,
} = require('./adaptive-tiler');
const { runWithConcurrency } = require('./concurrency');

const LEAF_BUCKET_DIR = 'leaf';
const PARTITION_ARENA_COUNT = 2;
const PARTITION_LEAF_HANDLE_LIMIT = 256;
const SCAN_PROGRESS_ROW_INTERVAL = 8192;
const PARTITION_PROGRESS_ROW_INTERVAL = 8192;
const POSITION_TMP_BUFFER_BYTES = 256 * 1024;
const POSITION_ROW_FLOAT_COUNT = 4;
const POSITION_ROW_BYTE_SIZE =
  POSITION_ROW_FLOAT_COUNT * Float32Array.BYTES_PER_ELEMENT;
const IS_LITTLE_ENDIAN = (() => {
  const probe = new Uint8Array(new Uint16Array([0x0102]).buffer);
  return probe[0] === 0x02;
})();

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
  // Budget two read arenas; non-contiguous leaf runs are written with writev.
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
    const build = nodeBuildState(leaf);
    if (build.partitionTouched) {
      return;
    }
    build.partitionTouched = true;
    build.partitionWriteChain = Promise.resolve();
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
      const build = nodeBuildState(leaf);
      const writePromise = build.partitionWriteChain
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
      build.partitionWriteChain = writePromise;
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
        nodeBuildState(leaf).partitionWriteChain.catch((err) => {
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
      const build = existingNodeBuildState(leaf);
      if (build) {
        delete build.partitionTouched;
        delete build.partitionWriteChain;
        pruneNodeBuildState(leaf);
      }
    }
  }
  arenas.length = 0;
  return {
    rowCount: processedRows,
    leafCount: touchedLeaves.length,
  };
}

function updatePositionBounds(minimum, maximum, x, y, z) {
  if (x < minimum[0]) minimum[0] = x;
  if (y < minimum[1]) minimum[1] = y;
  if (z < minimum[2]) minimum[2] = z;
  if (x > maximum[0]) maximum[0] = x;
  if (y > maximum[1]) maximum[1] = y;
  if (z > maximum[2]) maximum[2] = z;
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
  const includeWeights = options.includeWeights === true;
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
      ? new Float32Array(
          buffer.buffer,
          buffer.byteOffset,
          rowsPerBuffer * POSITION_ROW_FLOAT_COUNT,
        )
      : null;
  let bufferedRows = 0;
  let count = 0;
  const scratch = includeWeights ? makeRowScratch(0) : null;
  const extentScratch = includeWeights ? new Float32Array(3) : null;
  const rootBasisStats = includeWeights ? makeOrientedBoxStats(null) : null;
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
    const stagePosition = (_rowIndex, x, y, z, weight = 1.0) => {
        const fx = Math.fround(x);
        const fy = Math.fround(y);
        const fz = Math.fround(z);
        const fw = Math.fround(weight);
        updatePositionBounds(minimum, maximum, fx, fy, fz);
        if (rootBasisStats) {
          updateWeightedPositionMomentStats(rootBasisStats, fx, fy, fz, fw);
        }

        if (floatView) {
          const base = bufferedRows * POSITION_ROW_FLOAT_COUNT;
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
    };

    if (includeWeights) {
      await _forEachGaussianPlyCoreRecord(
        handle,
        filePath,
        header,
        layout,
        (_rowIndex, coreFloats) => {
          scratch.position[0] = coreFloats[0];
          scratch.position[1] = coreFloats[1];
          scratch.position[2] = coreFloats[2];
          scratch.scaleLog[0] = coreFloats[3];
          scratch.scaleLog[1] = coreFloats[4];
          scratch.scaleLog[2] = coreFloats[5];
          scratch.quat[0] = coreFloats[6];
          scratch.quat[1] = coreFloats[7];
          scratch.quat[2] = coreFloats[8];
          scratch.quat[3] = coreFloats[9];
          scratch.opacity = coreFloats[10];
          stagePosition(
            _rowIndex,
            coreFloats[0],
            coreFloats[1],
            coreFloats[2],
            splitWeightFromScratch(scratch, extentScratch),
          );
        },
        { chunkBytes: options.chunkBytes },
      );
    } else {
      await _forEachGaussianPlyPosition(
        handle,
        filePath,
        header,
        layout,
        stagePosition,
        { chunkBytes: options.chunkBytes },
      );
    }
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
  return {
    bounds: new Bounds(minimum, maximum),
    rootBasisAxes: rootBasisStats
      ? orthonormalBasisFromMomentStats(rootBasisStats)
      : null,
  };
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
  const includeWeights = options.includeWeights === true;
  const weights = includeWeights ? new Float32Array(header.vertexCount) : null;
  let count = 0;
  const scratch = includeWeights ? makeRowScratch(0) : null;
  const extentScratch = includeWeights ? new Float32Array(3) : null;
  const rootBasisStats = includeWeights ? makeOrientedBoxStats(null) : null;
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

  const stagePosition = (rowIndex, x, y, z, weight = 1.0) => {
      const fx = Math.fround(x);
      const fy = Math.fround(y);
      const fz = Math.fround(z);
      updatePositionBounds(minimum, maximum, fx, fy, fz);

      const base = rowIndex * 3;
      positions[base + 0] = fx;
      positions[base + 1] = fy;
      positions[base + 2] = fz;
      if (weights) {
        const fw = Math.fround(weight);
        weights[rowIndex] = fw;
        if (rootBasisStats) {
          updateWeightedPositionMomentStats(rootBasisStats, fx, fy, fz, fw);
        }
      }
      count += 1;
      updateProgress();
  };

  if (includeWeights) {
    await _forEachGaussianPlyCoreRecord(
      handle,
      filePath,
      header,
      layout,
      (rowIndex, coreFloats) => {
        scratch.position[0] = coreFloats[0];
        scratch.position[1] = coreFloats[1];
        scratch.position[2] = coreFloats[2];
        scratch.scaleLog[0] = coreFloats[3];
        scratch.scaleLog[1] = coreFloats[4];
        scratch.scaleLog[2] = coreFloats[5];
        scratch.quat[0] = coreFloats[6];
        scratch.quat[1] = coreFloats[7];
        scratch.quat[2] = coreFloats[8];
        scratch.quat[3] = coreFloats[9];
        scratch.opacity = coreFloats[10];
        stagePosition(
          rowIndex,
          coreFloats[0],
          coreFloats[1],
          coreFloats[2],
          splitWeightFromScratch(scratch, extentScratch),
        );
      },
      { chunkBytes: options.chunkBytes },
    );
  } else {
    await _forEachGaussianPlyPosition(
      handle,
      filePath,
      header,
      layout,
      stagePosition,
      { chunkBytes: options.chunkBytes },
    );
  }
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
    rootBasisAxes: rootBasisStats
      ? orthonormalBasisFromMomentStats(rootBasisStats)
      : null,
  };
}

module.exports = {
  partitionLeafBuckets,
  scanGlobalBoundsAndWritePositions,
  scanGlobalBoundsAndStagePositionsInMemory,
};
