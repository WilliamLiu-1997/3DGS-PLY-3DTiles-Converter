const fs = require('fs');
const path = require('path');

const {
  ConversionError,
  GaussianCloud,
  Bounds,
  ensure,
  shDegreeFromCoeffCount,
  _canonicalGaussianRowByteSize,
} = require('./parser');
const { writeThreeSigmaExtentComponents } = require('./builder');
const { removeFileIfExists } = require('./pipeline-state');

const LEAF_BUCKET_ENCODING = 'canonical32';
const HANDOFF_BUCKET_ENCODING = 'canonical32';
const WRITEV_BATCH_CHUNKS = 1024;
const IS_LITTLE_ENDIAN = (() => {
  const probe = new Uint8Array(new Uint16Array([0x0102]).buffer);
  return probe[0] === 0x02;
})();

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

  const chunks = [];
  for (let i = 0; i < entry.runs.length; i += 2) {
    const start = entry.runs[i];
    const byteLength = entry.runs[i + 1];
    chunks.push(sourceBuffer.subarray(start, start + byteLength));
  }
  await writeChunksToHandle(handle, filePath, chunks);
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

async function forEachBucketSpecChunk(
  fileSpec,
  coeffCount,
  rowBase,
  onChunk,
  options = {},
) {
  if (!fileSpec || !fileSpec.filePath) {
    return rowBase;
  }
  const rowByteSize = bucketRowByteSize(fileSpec.encoding, coeffCount);
  ensure(
    Number.isInteger(fileSpec.rowCount) && fileSpec.rowCount >= 0,
    `Bucket row count metadata is missing: ${fileSpec.filePath}`,
  );
  const totalBytes = fileSpec.rowCount * rowByteSize;
  if (totalBytes === 0) {
    return rowBase;
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
    const maybePromise = onChunk({
      view,
      byteOffset: 0,
      rowBase,
      rowCount: fileSpec.rowCount,
      rowByteSize,
      encoding: fileSpec.encoding,
      filePath: fileSpec.filePath,
      floatView,
      floatBase: 0,
    });
    if (maybePromise && typeof maybePromise.then === 'function') {
      await maybePromise;
    }
    return rowBase + fileSpec.rowCount;
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
    let fileRowBase = rowBase;
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
      const rowCount = bytesRead / rowByteSize;
      const maybePromise = onChunk({
        view,
        byteOffset: 0,
        rowBase: fileRowBase,
        rowCount,
        rowByteSize,
        encoding: fileSpec.encoding,
        filePath: fileSpec.filePath,
        floatView,
        floatBase: 0,
      });
      if (maybePromise && typeof maybePromise.then === 'function') {
        await maybePromise;
      }
      fileRowBase += rowCount;
    }
    return fileRowBase;
  } finally {
    await handle.close();
  }
}

async function forEachBucketChunk(entries, coeffCount, onChunk, options = {}) {
  let rowBase = 0;
  for (const entry of entries) {
    rowBase = await forEachBucketSpecChunk(
      entry,
      coeffCount,
      rowBase,
      onChunk,
      options,
    );
  }
}

async function forEachBucketSpecRow(fileSpec, coeffCount, onRow, options = {}) {
  await forEachBucketSpecChunk(
    fileSpec,
    coeffCount,
    0,
    async (chunk) => {
      const {
        view,
        byteOffset,
        rowCount,
        rowByteSize,
        encoding,
        filePath,
        floatView,
        floatBase,
      } = chunk;
      const floatsPerRow = rowByteSize >>> 2;
      for (let row = 0; row < rowCount; row++) {
        const offset = byteOffset + row * rowByteSize;
        const maybePromise = onRow(
          view,
          offset,
          encoding,
          filePath,
          floatView,
          floatView ? floatBase + row * floatsPerRow : 0,
        );
        if (maybePromise && typeof maybePromise.then === 'function') {
          await maybePromise;
        }
      }
    },
    options,
  );
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

  await forEachBucketChunk(
    entries,
    coeffCount,
    (chunk) => {
      const {
        view,
        byteOffset,
        rowCount: chunkRowCount,
        rowByteSize,
        encoding,
        floatView,
        floatBase,
      } = chunk;
      const floatsPerRow = rowByteSize >>> 2;
      for (let row = 0; row < chunkRowCount; row++) {
        readBucketCoreRowIntoScratch(
          encoding,
          view,
          byteOffset + row * rowByteSize,
          scratch,
          floatView,
          floatView ? floatBase + row * floatsPerRow : 0,
        );
        accumulateBoundsFromScratchRow(scratch, minimum, maximum, extentScratch);
      }
      rowCount += chunkRowCount;
    },
    { chunkBytes: options.bucketChunkBytes },
  );

  ensure(rowCount > 0, 'Cannot compute bounds for an empty bucket input.');
  return new Bounds(minimum, maximum);
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

  await forEachBucketChunk(
    entries,
    coeffCount,
    (chunk) => {
      const {
        view,
        byteOffset,
        rowCount,
        rowByteSize,
        encoding,
        floatView,
        floatBase,
      } = chunk;
      const floatsPerRow = rowByteSize >>> 2;
      for (let row = 0; row < rowCount; row++) {
        readBucketRowIntoScratch(
          encoding,
          view,
          byteOffset + row * rowByteSize,
          coeffCount,
          scratch,
          floatView,
          floatView ? floatBase + row * floatsPerRow : 0,
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
      }
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

module.exports = {
  LEAF_BUCKET_ENCODING,
  HANDOFF_BUCKET_ENCODING,
  materializeLinkedHandoffFile,
  bucketRowByteSize,
  makeBucketFileSpec,
  makeBucketAggregateSpec,
  leafBucketSpec,
  fileHandoffBucketSpec,
  isActiveHandoffSource,
  collectActiveHandoffSourceSpecs,
  handoffBucketSpec,
  flattenBucketSpec,
  flattenBucketSpecs,
  makeRowScratch,
  readBucketRowIntoScratch,
  readBucketCoreRowIntoScratch,
  appendBufferedBatches,
  appendChunksToFile,
  writeChunksToHandle,
  writeArenaRunsToHandle,
  collectBucketEntries,
  forEachBucketChunk,
  forEachBucketSpecRow,
  forEachBucketEntryRow,
  cacheBucketEntriesIfAffordable,
  accumulateBoundsFromScratchRow,
  computeBucketEntriesBounds,
  writeBufferToHandle,
  appendBucketEntryToHandle,
  materializeCanonicalEntriesFile,
  loadBucketCloudFromEntries,
  writeCanonicalCloudFile,
};
