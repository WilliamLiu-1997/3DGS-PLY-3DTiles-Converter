const fs = require('fs');
const path = require('path');

const C0 = 0.28209479177387814;

const PLY_DTYPE_MAP = {
  char: { size: 1, view: 'Int8' },
  uchar: { size: 1, view: 'Uint8' },
  int8: { size: 1, view: 'Int8' },
  uint8: { size: 1, view: 'Uint8' },
  short: { size: 2, view: 'Int16' },
  ushort: { size: 2, view: 'Uint16' },
  int16: { size: 2, view: 'Int16' },
  uint16: { size: 2, view: 'Uint16' },
  int: { size: 4, view: 'Int32' },
  uint: { size: 4, view: 'Uint32' },
  int32: { size: 4, view: 'Int32' },
  uint32: { size: 4, view: 'Uint32' },
  float: { size: 4, view: 'Float32' },
  float32: { size: 4, view: 'Float32' },
  double: { size: 8, view: 'Float64' },
  float64: { size: 8, view: 'Float64' },
};

const MAX_PLY_HEADER_BYTES = 8 * 1024 * 1024;
const HEADER_READ_CHUNK_SIZE = 64 * 1024;
const STREAM_READ_CHUNK_SIZE = 8 * 1024 * 1024;
const IS_LITTLE_ENDIAN = (() => {
  const probe = new Uint8Array(new Uint16Array([0x0102]).buffer);
  return probe[0] === 0x02;
})();

function streamChunkBytesFromOptions(options = {}) {
  const value = options && Number.isFinite(options.chunkBytes)
    ? Math.floor(options.chunkBytes)
    : STREAM_READ_CHUNK_SIZE;
  return Math.max(1, value);
}

const PLY_KIND_SKIP = 0;
const PLY_KIND_POSITION = 1;
const PLY_KIND_SH0 = 2;
const PLY_KIND_SCALE = 3;
const PLY_KIND_ROT = 4;
const PLY_KIND_OPACITY = 5;
const PLY_KIND_REST = 6;

class ConversionError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ConversionError';
  }
}

function clamp(value, minVal, maxVal) {
  return Math.max(minVal, Math.min(maxVal, value));
}

function roundHalfToEven(value) {
  const floor = Math.floor(value);
  const frac = value - floor;
  const eps = 1e-12;
  if (frac > 0.5 + eps) {
    return floor + 1;
  }
  if (frac < 0.5 - eps) {
    return floor;
  }
  return floor % 2 === 0 ? floor : floor + 1;
}

function ensure(cond, message) {
  if (!cond) {
    throw new ConversionError(message);
  }
}

function sigmoid(x) {
  const v = Math.max(-60.0, Math.min(60.0, x));
  return 1.0 / (1.0 + Math.exp(-v));
}

class GaussianCloud {
  constructor(
    positions,
    scaleLog,
    quatsXYZW,
    opacity,
    shCoeffs,
    color0 = null,
  ) {
    this.positions = positions;
    this.scaleLog = scaleLog;
    this.quatsXYZW = quatsXYZW;
    this.opacity = opacity;
    this.shCoeffs = shCoeffs;
    this.color0 = color0;
    this._shDegree = null;
  }

  get length() {
    return this.positions.length / 3;
  }

  get shDegree() {
    if (this._shDegree != null) {
      return this._shDegree;
    }
    const coeffCount = this.shCoeffs.length / (this.length * 3);
    return shDegreeFromCoeffCount(coeffCount);
  }

  withoutColor0() {
    const out = new GaussianCloud(
      this.positions,
      this.scaleLog,
      this.quatsXYZW,
      this.opacity,
      this.shCoeffs,
      null,
    );
    out._shDegree = this.shDegree;
    return out;
  }

  subset(indices, copyColor0 = true) {
    const n = indices.length;
    const positions = new Float32Array(n * 3);
    const scaleLog = new Float32Array(n * 3);
    const quats = new Float32Array(n * 4);
    const opacity = new Float32Array(n);
    const coeffCount = this.shCoeffs.length / (this.length * 3);
    const coeffStride = coeffCount * 3;
    const shCoeffs = new Float32Array(n * coeffStride);
    const keepColor0 = copyColor0 && this.color0 != null;
    const color0 = keepColor0 ? new Float32Array(n * 3) : null;

    for (let i = 0; i < n; i++) {
      const src = indices[i];
      const s3 = src * 3;
      const d3 = i * 3;
      const s4 = src * 4;
      const d4 = i * 4;
      positions[d3 + 0] = this.positions[s3 + 0];
      positions[d3 + 1] = this.positions[s3 + 1];
      positions[d3 + 2] = this.positions[s3 + 2];
      scaleLog[d3 + 0] = this.scaleLog[s3 + 0];
      scaleLog[d3 + 1] = this.scaleLog[s3 + 1];
      scaleLog[d3 + 2] = this.scaleLog[s3 + 2];
      if (keepColor0) {
        color0[d3 + 0] = this.color0[s3 + 0];
        color0[d3 + 1] = this.color0[s3 + 1];
        color0[d3 + 2] = this.color0[s3 + 2];
      }
      quats[d4 + 0] = this.quatsXYZW[s4 + 0];
      quats[d4 + 1] = this.quatsXYZW[s4 + 1];
      quats[d4 + 2] = this.quatsXYZW[s4 + 2];
      quats[d4 + 3] = this.quatsXYZW[s4 + 3];
      opacity[i] = this.opacity[src];

      const sCoeffBase = src * coeffStride;
      const dCoeffBase = i * coeffStride;
      for (let c = 0; c < coeffStride; c++) {
        shCoeffs[dCoeffBase + c] = this.shCoeffs[sCoeffBase + c];
      }
    }

    const out = new GaussianCloud(
      positions,
      scaleLog,
      quats,
      opacity,
      shCoeffs,
      color0,
    );
    out._shDegree = this.shDegree;
    return out;
  }
}

class Bounds {
  constructor(minimum, maximum) {
    this.minimum = minimum;
    this.maximum = maximum;
  }

  center() {
    return [
      (this.minimum[0] + this.maximum[0]) * 0.5,
      (this.minimum[1] + this.maximum[1]) * 0.5,
      (this.minimum[2] + this.maximum[2]) * 0.5,
    ];
  }

  extents() {
    return [
      this.maximum[0] - this.minimum[0],
      this.maximum[1] - this.minimum[1],
      this.maximum[2] - this.minimum[2],
    ];
  }

  toBoxArray() {
    const c = this.center();
    const h = this.extents().map((v) => Math.max(v * 0.5, 1e-6));
    return [c[0], c[1], c[2], h[0], 0.0, 0.0, 0.0, h[1], 0.0, 0.0, 0.0, h[2]];
  }
}

class TileNode {
  constructor(level, x, y, z, bounds, error, contentUri, children) {
    this.level = level;
    this.x = x;
    this.y = y;
    this.z = z;
    this.bounds = bounds;
    this.error = error;
    this.contentUri = contentUri;
    this.children = children;
  }

  key() {
    return `${this.level}/${this.x}/${this.y}/${this.z}`;
  }
}

function findLineEnd(buffer, start) {
  for (let i = start; i < buffer.length; i++) {
    if (buffer[i] === 0x0a || buffer[i] === 0x0d) {
      return i;
    }
  }
  return -1;
}

function canonicalGaussianRowFloatCount(coeffCount) {
  return 11 + coeffCount * 3;
}

function canonicalGaussianRowByteSize(coeffCount) {
  return canonicalGaussianRowFloatCount(coeffCount) * 4;
}

async function readPlyHeaderFromHandle(handle, filePath) {
  let format = null;
  let vertexCount = null;
  const vertexProps = [];
  let currentElement = null;
  let sawMagic = false;
  let cursor = 0;
  let fileOffset = 0;
  let headerBuffer = Buffer.alloc(0);
  let sawEndHeader = false;

  while (!sawEndHeader) {
    const chunk = Buffer.allocUnsafe(HEADER_READ_CHUNK_SIZE);
    const { bytesRead } = await handle.read(
      chunk,
      0,
      chunk.length,
      fileOffset,
    );
    ensure(bytesRead > 0, `PLY header is incomplete in ${filePath}.`);
    headerBuffer = Buffer.concat([headerBuffer, chunk.subarray(0, bytesRead)]);
    fileOffset += bytesRead;
    ensure(
      headerBuffer.length <= MAX_PLY_HEADER_BYTES,
      `PLY header exceeds ${MAX_PLY_HEADER_BYTES} bytes in ${filePath}.`,
    );

    while (true) {
      const lineEnd = findLineEnd(headerBuffer, cursor);
      if (lineEnd < 0) {
        break;
      }

      const line = headerBuffer.toString('ascii', cursor, lineEnd).trim();
      let nextCursor = lineEnd;
      while (
        nextCursor < headerBuffer.length &&
        (headerBuffer[nextCursor] === 0x0a || headerBuffer[nextCursor] === 0x0d)
      ) {
        nextCursor += 1;
      }
      cursor = nextCursor;

      if (!sawMagic) {
        ensure(line === 'ply', `${filePath} is not a valid PLY file.`);
        sawMagic = true;
        continue;
      }

      if (line === 'end_header') {
        sawEndHeader = true;
        break;
      }

      if (!line || line.startsWith('comment')) {
        continue;
      }

      const parts = line.split(/\s+/);
      if (parts[0] === 'format') {
        format = parts[1];
      } else if (parts[0] === 'element') {
        currentElement = parts[1];
        if (currentElement === 'vertex') {
          vertexCount = Number(parts[2]);
        }
      } else if (parts[0] === 'property' && currentElement === 'vertex') {
        if (parts[1] === 'list') {
          throw new ConversionError(
            'List property in vertex data is not supported.',
          );
        }
        vertexProps.push({ name: parts[2], type: parts[1] });
      }
    }
  }

  ensure(sawMagic, `${filePath} is not a valid PLY file.`);
  ensure(sawEndHeader, `PLY header is incomplete in ${filePath}.`);
  ensure(
    format === 'binary_little_endian' || format === 'ascii',
    `Only ascii or binary_little_endian PLY is supported; got ${format}.`,
  );
  ensure(
    vertexCount != null && vertexProps.length > 0,
    'No vertex element or vertex properties found in PLY header.',
  );
  return { format, vertexCount, vertexProps, dataOffset: cursor };
}

async function readExact(handle, buffer, length, position, eofMessage) {
  let offset = 0;
  let readPosition = position;
  while (offset < length) {
    const { bytesRead } = await handle.read(
      buffer,
      offset,
      length - offset,
      readPosition,
    );
    if (bytesRead === 0) {
      throw new ConversionError(eofMessage);
    }
    offset += bytesRead;
    if (readPosition != null) {
      readPosition += bytesRead;
    }
  }
}

function shDegreeFromCoeffCount(totalCoeffs) {
  const mapping = { 1: 0, 4: 1, 9: 2, 16: 3 };
  ensure(
    Object.prototype.hasOwnProperty.call(mapping, totalCoeffs),
    `Unsupported SH coefficient count ${totalCoeffs}.`,
  );
  return mapping[totalCoeffs];
}

function inferShDegreeFromRestCount(restScalarCount) {
  if (restScalarCount === 0) {
    return 0;
  }
  for (let degree = 1; degree <= 3; degree++) {
    const expected = 3 * ((degree + 1) ** 2 - 1);
    if (expected === restScalarCount) {
      return degree;
    }
  }
  throw new ConversionError(
    `Cannot infer SH degree from rest count ${restScalarCount}.`,
  );
}

function parseFieldIndex(name) {
  const parts = name.split('_');
  const idx = Number(parts[parts.length - 1]);
  ensure(Number.isFinite(idx), `Invalid SH field name: ${name}`);
  return idx;
}

function readPlyScalar(view, offset, typeInfo) {
  if (typeInfo.view === 'Int8') {
    return view.getInt8(offset);
  }
  if (typeInfo.view === 'Uint8') {
    return view.getUint8(offset);
  }
  if (typeInfo.view === 'Int16') {
    return view.getInt16(offset, true);
  }
  if (typeInfo.view === 'Uint16') {
    return view.getUint16(offset, true);
  }
  if (typeInfo.view === 'Int32') {
    return view.getInt32(offset, true);
  }
  if (typeInfo.view === 'Uint32') {
    return view.getUint32(offset, true);
  }
  if (typeInfo.view === 'Float32') {
    return view.getFloat32(offset, true);
  }
  if (typeInfo.view === 'Float64') {
    return view.getFloat64(offset, true);
  }
  throw new ConversionError(`Unsupported binary reader view: ${typeInfo.view}`);
}

function writeNormalizedQuaternionToView(
  view,
  inputConvention,
  r0,
  r1,
  r2,
  r3,
) {
  let x;
  let y;
  let z;
  let w;
  if (inputConvention === 'graphdeco') {
    x = r1;
    y = r2;
    z = r3;
    w = r0;
  } else if (inputConvention === 'khr_native') {
    x = r0;
    y = r1;
    z = r2;
    w = r3;
  } else {
    throw new ConversionError(`Unknown input_convention: ${inputConvention}`);
  }

  let n = Math.sqrt(x * x + y * y + z * z + w * w);
  if (n < 1e-12) {
    n = 1.0;
  }
  const sign = w < 0.0 ? -1.0 : 1.0;
  const inv = sign / n;
  view.setFloat32(24, x * inv, true);
  view.setFloat32(28, y * inv, true);
  view.setFloat32(32, z * inv, true);
  view.setFloat32(36, w * inv, true);
}

function buildGaussianPlyLayout(
  vertexProps,
  filePath,
  inputConvention,
  linearScaleInput,
) {
  const propNames = vertexProps.map((p) => p.name);
  const required = ['x', 'y', 'z', 'opacity', 'f_dc_0', 'f_dc_1', 'f_dc_2'];
  const missing = required.filter((name) => !propNames.includes(name));
  ensure(
    missing.length === 0,
    `Missing required fields: ${missing.join(', ')}`,
  );

  const scaleNames = propNames
    .filter((name) => name.startsWith('scale_'))
    .sort((a, b) => Number(a.split('_')[1]) - Number(b.split('_')[1]));
  const rotNames = propNames
    .filter((name) => name.startsWith('rot_'))
    .sort((a, b) => Number(a.split('_')[1]) - Number(b.split('_')[1]));
  const restNames = propNames
    .filter((name) => name.startsWith('f_rest_'))
    .sort((a, b) => parseFieldIndex(a) - parseFieldIndex(b));

  ensure(
    scaleNames.length === 3,
    `Expected 3 scale_* fields, got ${scaleNames.length}.`,
  );
  ensure(
    rotNames.length === 4,
    `Expected 4 rot_* fields, got ${rotNames.length}.`,
  );

  const degree = inferShDegreeFromRestCount(restNames.length);
  const extraDim = { 0: 0, 1: 3, 2: 8, 3: 15 }[degree];
  const coeffCount = 1 + extraDim;

  const scaleIndexByName = Object.create(null);
  for (let i = 0; i < scaleNames.length; i++) {
    scaleIndexByName[scaleNames[i]] = i;
  }
  const rotIndexByName = Object.create(null);
  for (let i = 0; i < rotNames.length; i++) {
    rotIndexByName[rotNames[i]] = i;
  }
  const restIndexByName = Object.create(null);
  for (let i = 0; i < restNames.length; i++) {
    restIndexByName[restNames[i]] = i;
  }

  const propertyPlan = [];
  const binaryPositionProps = new Array(3).fill(null);
  let sourceRecordSize = 0;
  let binaryFloat32Direct = true;
  for (const prop of vertexProps) {
    const typeInfo = PLY_DTYPE_MAP[prop.type];
    ensure(!!typeInfo, `Unsupported PLY property type: ${prop.type}`);
    const floatOffset = sourceRecordSize >>> 2;
    if (typeInfo.view !== 'Float32' || (sourceRecordSize & 3) !== 0) {
      binaryFloat32Direct = false;
    }
    const name = prop.name;
    let plan = null;
    if (name === 'x') plan = { kind: PLY_KIND_POSITION, index: 0, typeInfo };
    else if (name === 'y') {
      plan = { kind: PLY_KIND_POSITION, index: 1, typeInfo };
    } else if (name === 'z') {
      plan = { kind: PLY_KIND_POSITION, index: 2, typeInfo };
    } else if (name === 'opacity') {
      plan = { kind: PLY_KIND_OPACITY, typeInfo };
    } else if (name === 'f_dc_0') {
      plan = { kind: PLY_KIND_SH0, index: 0, typeInfo };
    } else if (name === 'f_dc_1') {
      plan = { kind: PLY_KIND_SH0, index: 1, typeInfo };
    } else if (name === 'f_dc_2') {
      plan = { kind: PLY_KIND_SH0, index: 2, typeInfo };
    }

    const scaleIdx = scaleIndexByName[name];
    if (!plan && scaleIdx != null) {
      plan = { kind: PLY_KIND_SCALE, index: scaleIdx, typeInfo };
    }

    const rotIdx = rotIndexByName[name];
    if (!plan && rotIdx != null) {
      plan = { kind: PLY_KIND_ROT, index: rotIdx, typeInfo };
    }

    const restIdx = restIndexByName[name];
    if (!plan && restIdx != null) {
      const coeff = restIdx % extraDim;
      const channel = Math.floor(restIdx / extraDim);
      plan = {
        kind: PLY_KIND_REST,
        index: 3 * (1 + coeff) + channel,
        typeInfo,
      };
    }

    if (!plan) {
      plan = { kind: PLY_KIND_SKIP, typeInfo };
    }
    plan.floatOffset = floatOffset;
    if (plan.kind === PLY_KIND_POSITION) {
      binaryPositionProps[plan.index] = {
        offset: sourceRecordSize,
        floatOffset,
        typeInfo,
      };
    }
    propertyPlan.push(plan);
    sourceRecordSize += typeInfo.size;
  }
  ensure(sourceRecordSize > 0, `PLY vertex record is empty in ${filePath}.`);

  return {
    inputConvention,
    linearScaleInput,
    degree,
    coeffCount,
    extraDim,
    propertyPlan,
    fieldCount: propertyPlan.length,
    sourceRecordSize,
    binaryPositionProps,
    binaryFloat32Direct:
      binaryFloat32Direct && (sourceRecordSize & 3) === 0,
    canonicalFloatCount: canonicalGaussianRowFloatCount(coeffCount),
    canonicalByteSize: canonicalGaussianRowByteSize(coeffCount),
  };
}

function writeCanonicalScalarToView(view, layout, plan, value) {
  switch (plan.kind) {
    case PLY_KIND_POSITION:
      view.setFloat32(plan.index * 4, value, true);
      break;
    case PLY_KIND_SH0:
      view.setFloat32((11 + plan.index) * 4, value, true);
      break;
    case PLY_KIND_SCALE:
      view.setFloat32(
        (3 + plan.index) * 4,
        layout.linearScaleInput ? Math.log(Math.max(value, 1e-8)) : value,
        true,
      );
      break;
    case PLY_KIND_OPACITY:
      view.setFloat32(
        40,
        layout.inputConvention === 'graphdeco'
          ? sigmoid(value)
          : clamp(value, 0.0, 1.0),
        true,
      );
      break;
    case PLY_KIND_REST:
      view.setFloat32((11 + plan.index) * 4, value, true);
      break;
    default:
      break;
  }
}

function writeCanonicalScalarToFloats(out, layout, plan, value) {
  switch (plan.kind) {
    case PLY_KIND_POSITION:
      out[plan.index] = value;
      break;
    case PLY_KIND_SH0:
      out[11 + plan.index] = value;
      break;
    case PLY_KIND_SCALE:
      out[3 + plan.index] = layout.linearScaleInput
        ? Math.log(Math.max(value, 1e-8))
        : value;
      break;
    case PLY_KIND_OPACITY:
      out[10] =
        layout.inputConvention === 'graphdeco'
          ? sigmoid(value)
          : clamp(value, 0.0, 1.0);
      break;
    case PLY_KIND_REST:
      out[11 + plan.index] = value;
      break;
    default:
      break;
  }
}

function decodeBinaryGaussianRecordToCanonical(sourceView, offset, layout, rowView) {
  let r0 = 0.0;
  let r1 = 0.0;
  let r2 = 0.0;
  let r3 = 1.0;
  for (let p = 0; p < layout.propertyPlan.length; p++) {
    const plan = layout.propertyPlan[p];
    if (plan.kind === PLY_KIND_SKIP) {
      offset += plan.typeInfo.size;
      continue;
    }
    const value = readPlyScalar(sourceView, offset, plan.typeInfo);
    offset += plan.typeInfo.size;
    if (plan.kind === PLY_KIND_ROT) {
      if (plan.index === 0) r0 = value;
      else if (plan.index === 1) r1 = value;
      else if (plan.index === 2) r2 = value;
      else r3 = value;
    } else {
      writeCanonicalScalarToView(rowView, layout, plan, value);
    }
  }
  writeNormalizedQuaternionToView(
    rowView,
    layout.inputConvention,
    r0,
    r1,
    r2,
    r3,
  );
  return offset;
}

function writeNormalizedQuaternionToFloats(
  out,
  inputConvention,
  r0,
  r1,
  r2,
  r3,
) {
  let x;
  let y;
  let z;
  let w;
  if (inputConvention === 'graphdeco') {
    x = r1;
    y = r2;
    z = r3;
    w = r0;
  } else if (inputConvention === 'khr_native') {
    x = r0;
    y = r1;
    z = r2;
    w = r3;
  } else {
    throw new ConversionError(`Unknown input_convention: ${inputConvention}`);
  }

  let n = Math.sqrt(x * x + y * y + z * z + w * w);
  if (n < 1e-12) {
    n = 1.0;
  }
  const sign = w < 0.0 ? -1.0 : 1.0;
  const inv = sign / n;
  out[6] = x * inv;
  out[7] = y * inv;
  out[8] = z * inv;
  out[9] = w * inv;
}

function decodeBinaryFloat32GaussianRecordToCanonical(
  sourceFloats,
  rowFloatOffset,
  layout,
  rowFloats,
) {
  let r0 = 0.0;
  let r1 = 0.0;
  let r2 = 0.0;
  let r3 = 1.0;
  for (let p = 0; p < layout.propertyPlan.length; p++) {
    const plan = layout.propertyPlan[p];
    if (plan.kind === PLY_KIND_SKIP) {
      continue;
    }
    const value = sourceFloats[rowFloatOffset + plan.floatOffset];
    if (plan.kind === PLY_KIND_ROT) {
      if (plan.index === 0) r0 = value;
      else if (plan.index === 1) r1 = value;
      else if (plan.index === 2) r2 = value;
      else r3 = value;
    } else {
      writeCanonicalScalarToFloats(rowFloats, layout, plan, value);
    }
  }
  writeNormalizedQuaternionToFloats(
    rowFloats,
    layout.inputConvention,
    r0,
    r1,
    r2,
    r3,
  );
}

async function forEachBinaryGaussianPlyPosition(
  handle,
  filePath,
  header,
  layout,
  onPosition,
  options = {},
) {
  const positionProps = layout.binaryPositionProps;
  ensure(
    Array.isArray(positionProps) &&
      positionProps[0] &&
      positionProps[1] &&
      positionProps[2],
    `Binary PLY layout is missing direct position offsets in ${filePath}.`,
  );
  const rowsPerChunk = Math.max(
    1,
    Math.floor(streamChunkBytesFromOptions(options) / layout.sourceRecordSize),
  );
  const chunkBytes = rowsPerChunk * layout.sourceRecordSize;
  const chunks = [Buffer.allocUnsafe(chunkBytes), Buffer.allocUnsafe(chunkBytes)];
  const canUseFastPath =
    layout.binaryFloat32Direct &&
    IS_LITTLE_ENDIAN &&
    chunks.every((chunk) => (chunk.byteOffset & 3) === 0);
  const floatsPerRow = layout.sourceRecordSize >>> 2;
  let nextRowBase = 0;
  let nextFileOffset = header.dataOffset;
  let nextChunkIndex = 0;

  const readNextChunk = () => {
    if (nextRowBase >= header.vertexCount) {
      return null;
    }
    const rowBase = nextRowBase;
    const rowCount = Math.min(rowsPerChunk, header.vertexCount - rowBase);
    const byteCount = rowCount * layout.sourceRecordSize;
    const fileOffset = nextFileOffset;
    const chunk = chunks[nextChunkIndex];
    nextRowBase += rowCount;
    nextFileOffset += byteCount;
    nextChunkIndex = 1 - nextChunkIndex;
    return readExact(
      handle,
      chunk,
      byteCount,
      fileOffset,
      `Binary PLY payload ended early in ${filePath}.`,
    ).then(() => ({ chunk, rowBase, rowCount, byteCount }));
  };

  let chunkPromise = readNextChunk();
  while (chunkPromise) {
    const { chunk, rowBase, rowCount, byteCount } = await chunkPromise;
    const nextChunkPromise = readNextChunk();
    try {
      if (canUseFastPath) {
        const sourceFloats = new Float32Array(
          chunk.buffer,
          chunk.byteOffset,
          byteCount >>> 2,
        );
        const xOff = positionProps[0].floatOffset;
        const yOff = positionProps[1].floatOffset;
        const zOff = positionProps[2].floatOffset;
        for (let i = 0; i < rowCount; i++) {
          const rowFloatOffset = i * floatsPerRow;
          onPosition(
            rowBase + i,
            sourceFloats[rowFloatOffset + xOff],
            sourceFloats[rowFloatOffset + yOff],
            sourceFloats[rowFloatOffset + zOff],
          );
        }
      } else {
        const view = new DataView(chunk.buffer, chunk.byteOffset, byteCount);
        for (let i = 0; i < rowCount; i++) {
          const rowOffset = i * layout.sourceRecordSize;
          const x = readPlyScalar(
            view,
            rowOffset + positionProps[0].offset,
            positionProps[0].typeInfo,
          );
          const y = readPlyScalar(
            view,
            rowOffset + positionProps[1].offset,
            positionProps[1].typeInfo,
          );
          const z = readPlyScalar(
            view,
            rowOffset + positionProps[2].offset,
            positionProps[2].typeInfo,
          );
          onPosition(rowBase + i, x, y, z);
        }
      }
    } catch (err) {
      if (nextChunkPromise) {
        try {
          await nextChunkPromise;
        } catch {}
      }
      throw err;
    }
    chunkPromise = nextChunkPromise;
  }
}

async function forEachBinaryGaussianPlyCanonicalRecord(
  handle,
  filePath,
  header,
  layout,
  onRecord,
  options = {},
) {
  const rowsPerChunk = Math.max(
    1,
    Math.floor(streamChunkBytesFromOptions(options) / layout.sourceRecordSize),
  );
  const chunkBytes = rowsPerChunk * layout.sourceRecordSize;
  const chunks = [Buffer.allocUnsafe(chunkBytes), Buffer.allocUnsafe(chunkBytes)];
  const rowBuffer = Buffer.allocUnsafe(layout.canonicalByteSize);
  const rowView = new DataView(
    rowBuffer.buffer,
    rowBuffer.byteOffset,
    rowBuffer.byteLength,
  );
  const canUseFastPath =
    layout.binaryFloat32Direct &&
    IS_LITTLE_ENDIAN &&
    chunks.every((chunk) => (chunk.byteOffset & 3) === 0) &&
    (rowBuffer.byteOffset & 3) === 0;
  const floatsPerRow = layout.sourceRecordSize >>> 2;
  const rowFloats = canUseFastPath
    ? new Float32Array(
        rowBuffer.buffer,
        rowBuffer.byteOffset,
        layout.canonicalFloatCount,
      )
    : null;
  let nextRowBase = 0;
  let nextFileOffset = header.dataOffset;
  let nextChunkIndex = 0;

  const readNextChunk = () => {
    if (nextRowBase >= header.vertexCount) {
      return null;
    }
    const rowBase = nextRowBase;
    const rowCount = Math.min(rowsPerChunk, header.vertexCount - rowBase);
    const byteCount = rowCount * layout.sourceRecordSize;
    const fileOffset = nextFileOffset;
    const chunk = chunks[nextChunkIndex];
    nextRowBase += rowCount;
    nextFileOffset += byteCount;
    nextChunkIndex = 1 - nextChunkIndex;
    return readExact(
      handle,
      chunk,
      byteCount,
      fileOffset,
      `Binary PLY payload ended early in ${filePath}.`,
    ).then(() => ({ chunk, rowBase, rowCount, byteCount }));
  };

  let chunkPromise = readNextChunk();
  while (chunkPromise) {
    const { chunk, rowBase, rowCount, byteCount } = await chunkPromise;
    const nextChunkPromise = readNextChunk();
    try {
      if (canUseFastPath) {
        const sourceFloats = new Float32Array(
          chunk.buffer,
          chunk.byteOffset,
          byteCount >>> 2,
        );
        for (let i = 0; i < rowCount; i++) {
          decodeBinaryFloat32GaussianRecordToCanonical(
            sourceFloats,
            i * floatsPerRow,
            layout,
            rowFloats,
          );
          const maybePromise = onRecord(
            rowBase + i,
            rowBuffer,
            rowView,
            rowFloats,
          );
          if (maybePromise && typeof maybePromise.then === 'function') {
            await maybePromise;
          }
        }
      } else {
        const view = new DataView(chunk.buffer, chunk.byteOffset, byteCount);
        let offset = 0;
        for (let i = 0; i < rowCount; i++) {
          offset = decodeBinaryGaussianRecordToCanonical(
            view,
            offset,
            layout,
            rowView,
          );
          const maybePromise = onRecord(rowBase + i, rowBuffer, rowView);
          if (maybePromise && typeof maybePromise.then === 'function') {
            await maybePromise;
          }
        }
      }
    } catch (err) {
      if (nextChunkPromise) {
        try {
          await nextChunkPromise;
        } catch {}
      }
      throw err;
    }
    chunkPromise = nextChunkPromise;
  }
}

async function forEachAsciiGaussianPlyPosition(
  handle,
  filePath,
  header,
  layout,
  onPosition,
  options = {},
) {
  const expected = header.vertexCount * layout.fieldCount;
  const chunk = Buffer.allocUnsafe(streamChunkBytesFromOptions(options));
  let fileOffset = header.dataOffset;
  let tokenCarry = '';
  let rowIndex = 0;
  let propIndex = 0;
  let count = 0;
  let x = 0.0;
  let y = 0.0;
  let z = 0.0;

  const processToken = (token) => {
    if (rowIndex >= header.vertexCount) {
      return;
    }
    const plan = layout.propertyPlan[propIndex];
    if (plan.kind === PLY_KIND_POSITION) {
      const value = Number.parseFloat(token);
      if (plan.index === 0) x = value;
      else if (plan.index === 1) y = value;
      else z = value;
    }
    propIndex += 1;
    count += 1;
    if (propIndex === layout.fieldCount) {
      onPosition(rowIndex, x, y, z);
      rowIndex += 1;
      propIndex = 0;
      x = 0.0;
      y = 0.0;
      z = 0.0;
    }
  };

  while (rowIndex < header.vertexCount) {
    const { bytesRead } = await handle.read(chunk, 0, chunk.length, fileOffset);
    if (bytesRead === 0) {
      break;
    }
    fileOffset += bytesRead;

    const text = tokenCarry + chunk.toString('ascii', 0, bytesRead);
    let tokenStart = -1;
    for (let i = 0; i < text.length && rowIndex < header.vertexCount; i++) {
      if (text.charCodeAt(i) > 0x20) {
        if (tokenStart < 0) {
          tokenStart = i;
        }
      } else if (tokenStart >= 0) {
        processToken(text.slice(tokenStart, i));
        tokenStart = -1;
      }
    }
    tokenCarry =
      tokenStart >= 0 && rowIndex < header.vertexCount
        ? text.slice(tokenStart)
        : '';
  }

  if (rowIndex < header.vertexCount && tokenCarry) {
    processToken(tokenCarry);
  }

  ensure(
    count >= expected && rowIndex === header.vertexCount && propIndex === 0,
    `ASCII PLY rows mismatch. Expected at least ${expected} values, got ${count}.`,
  );
}

async function forEachAsciiGaussianPlyCanonicalRecord(
  handle,
  filePath,
  header,
  layout,
  onRecord,
  options = {},
) {
  const expected = header.vertexCount * layout.fieldCount;
  const chunk = Buffer.allocUnsafe(streamChunkBytesFromOptions(options));
  const rowBuffer = Buffer.allocUnsafe(layout.canonicalByteSize);
  const rowView = new DataView(
    rowBuffer.buffer,
    rowBuffer.byteOffset,
    rowBuffer.byteLength,
  );
  let fileOffset = header.dataOffset;
  let tokenCarry = '';
  let rowIndex = 0;
  let propIndex = 0;
  let count = 0;
  let r0 = 0.0;
  let r1 = 0.0;
  let r2 = 0.0;
  let r3 = 1.0;

  const processToken = (token) => {
    if (rowIndex >= header.vertexCount) {
      return;
    }
    const plan = layout.propertyPlan[propIndex];
    if (plan.kind !== PLY_KIND_SKIP) {
      const value = Number.parseFloat(token);
      if (plan.kind === PLY_KIND_ROT) {
        if (plan.index === 0) r0 = value;
        else if (plan.index === 1) r1 = value;
        else if (plan.index === 2) r2 = value;
        else r3 = value;
      } else {
        writeCanonicalScalarToView(rowView, layout, plan, value);
      }
    }
    propIndex += 1;
    count += 1;
    if (propIndex === layout.fieldCount) {
      writeNormalizedQuaternionToView(
        rowView,
        layout.inputConvention,
        r0,
        r1,
        r2,
        r3,
      );
      const maybePromise = onRecord(rowIndex, rowBuffer, rowView);
      rowIndex += 1;
      propIndex = 0;
      r0 = 0.0;
      r1 = 0.0;
      r2 = 0.0;
      r3 = 1.0;
      return maybePromise;
    }
    return null;
  };

  while (rowIndex < header.vertexCount) {
    const { bytesRead } = await handle.read(chunk, 0, chunk.length, fileOffset);
    if (bytesRead === 0) {
      break;
    }
    fileOffset += bytesRead;

    const text = tokenCarry + chunk.toString('ascii', 0, bytesRead);
    let tokenStart = -1;
    for (let i = 0; i < text.length && rowIndex < header.vertexCount; i++) {
      if (text.charCodeAt(i) > 0x20) {
        if (tokenStart < 0) {
          tokenStart = i;
        }
      } else if (tokenStart >= 0) {
        const maybePromise = processToken(text.slice(tokenStart, i));
        if (maybePromise && typeof maybePromise.then === 'function') {
          await maybePromise;
        }
        tokenStart = -1;
      }
    }
    tokenCarry =
      tokenStart >= 0 && rowIndex < header.vertexCount
        ? text.slice(tokenStart)
        : '';
  }

  if (rowIndex < header.vertexCount && tokenCarry) {
    const maybePromise = processToken(tokenCarry);
    if (maybePromise && typeof maybePromise.then === 'function') {
      await maybePromise;
    }
  }

  ensure(
    count >= expected && rowIndex === header.vertexCount && propIndex === 0,
    `ASCII PLY rows mismatch. Expected at least ${expected} values, got ${count}.`,
  );
}

async function forEachGaussianPlyPosition(
  handle,
  filePath,
  header,
  layout,
  onPosition,
  options = {},
) {
  if (header.format === 'binary_little_endian') {
    await forEachBinaryGaussianPlyPosition(
      handle,
      filePath,
      header,
      layout,
      onPosition,
      options,
    );
    return;
  }
  await forEachAsciiGaussianPlyPosition(
    handle,
    filePath,
    header,
    layout,
    onPosition,
    options,
  );
}

async function forEachGaussianPlyCanonicalRecord(
  handle,
  filePath,
  header,
  layout,
  onRecord,
  options = {},
) {
  if (header.format === 'binary_little_endian') {
    await forEachBinaryGaussianPlyCanonicalRecord(
      handle,
      filePath,
      header,
      layout,
      onRecord,
      options,
    );
    return;
  }
  await forEachAsciiGaussianPlyCanonicalRecord(
    handle,
    filePath,
    header,
    layout,
    onRecord,
    options,
  );
}

function writeGraphdecoLikePly(filePath, cloud) {
  const n = cloud.length;
  const degree = cloud.shDegree;
  const coeffsPerChannel = (degree + 1) ** 2 - 1;
  const names = ['x', 'y', 'z', 'nx', 'ny', 'nz', 'f_dc_0', 'f_dc_1', 'f_dc_2'];
  for (let i = 0; i < 3 * coeffsPerChannel; i++) {
    names.push(`f_rest_${i}`);
  }
  names.push('opacity');
  for (let i = 0; i < 3; i++) names.push(`scale_${i}`);
  for (let i = 0; i < 4; i++) names.push(`rot_${i}`);

  const header = [
    'ply',
    'format binary_little_endian 1.0',
    `element vertex ${n}`,
  ];
  for (const n of names) {
    header.push(`property float ${n}`);
  }
  header.push('end_header');

  const recordSize = names.length * 4;
  const floatsPerRow = recordSize >>> 2;
  const payload = Buffer.alloc(n * recordSize);
  const payloadByteOffset = payload.byteOffset;
  const canUseFastPath =
    IS_LITTLE_ENDIAN && (payloadByteOffset & 3) === 0;

  if (canUseFastPath) {
    const fv = new Float32Array(
      payload.buffer,
      payloadByteOffset,
      n * floatsPerRow,
    );
    const shExtraCount = coeffsPerChannel * 3;
    for (let i = 0; i < n; i++) {
      const basePos = i * 3;
      const baseScale = i * 3;
      const baseRot = i * 4;
      const baseCoeff = i * (1 + coeffsPerChannel) * 3;
      const rowOff = i * floatsPerRow;
      fv[rowOff + 0] = cloud.positions[basePos + 0];
      fv[rowOff + 1] = cloud.positions[basePos + 1];
      fv[rowOff + 2] = cloud.positions[basePos + 2];
      // normals (3 zeros) are already zeroed by Buffer.alloc, skip
      fv[rowOff + 6] = cloud.shCoeffs[baseCoeff + 0];
      fv[rowOff + 7] = cloud.shCoeffs[baseCoeff + 1];
      fv[rowOff + 8] = cloud.shCoeffs[baseCoeff + 2];
      if (shExtraCount > 0) {
        fv.set(
          cloud.shCoeffs.subarray(baseCoeff + 3, baseCoeff + 3 + shExtraCount),
          rowOff + 9,
        );
      }
      const opacityOff = rowOff + 9 + shExtraCount;
      const p = cloud.opacity[i];
      fv[opacityOff] = Math.log(
        clamp(p, 1e-7, 1.0 - 1e-7) / clamp(1.0 - p, 1e-7, 1.0),
      );
      fv[opacityOff + 1] = cloud.scaleLog[baseScale + 0];
      fv[opacityOff + 2] = cloud.scaleLog[baseScale + 1];
      fv[opacityOff + 3] = cloud.scaleLog[baseScale + 2];
      fv[opacityOff + 4] = cloud.quatsXYZW[baseRot + 3];
      fv[opacityOff + 5] = cloud.quatsXYZW[baseRot + 0];
      fv[opacityOff + 6] = cloud.quatsXYZW[baseRot + 1];
      fv[opacityOff + 7] = cloud.quatsXYZW[baseRot + 2];
    }
  } else {
    const dv = new DataView(
      payload.buffer,
      payloadByteOffset,
      payload.byteLength,
    );
    let off = 0;
    for (let i = 0; i < n; i++) {
      const basePos = i * 3;
      const baseScale = i * 3;
      const baseRot = i * 4;
      const baseCoeff = i * (1 + coeffsPerChannel) * 3;
      dv.setFloat32(off, cloud.positions[basePos + 0], true);
      off += 4;
      dv.setFloat32(off, cloud.positions[basePos + 1], true);
      off += 4;
      dv.setFloat32(off, cloud.positions[basePos + 2], true);
      off += 4;
      dv.setFloat32(off, 0.0, true);
      off += 4;
      dv.setFloat32(off, 0.0, true);
      off += 4;
      dv.setFloat32(off, 0.0, true);
      off += 4;
      dv.setFloat32(off, cloud.shCoeffs[baseCoeff + 0], true);
      off += 4;
      dv.setFloat32(off, cloud.shCoeffs[baseCoeff + 1], true);
      off += 4;
      dv.setFloat32(off, cloud.shCoeffs[baseCoeff + 2], true);
      off += 4;

      if (coeffsPerChannel > 0) {
        const shExtraBase = baseCoeff + 3;
        for (let c = 0; c < coeffsPerChannel * 3; c++) {
          dv.setFloat32(off, cloud.shCoeffs[shExtraBase + c], true);
          off += 4;
        }
      }

      const p = cloud.opacity[i];
      const num = Math.log(
        clamp(p, 1e-7, 1.0 - 1e-7) / clamp(1.0 - p, 1e-7, 1.0),
      );
      dv.setFloat32(off, num, true);
      off += 4;
      dv.setFloat32(off, cloud.scaleLog[baseScale + 0], true);
      off += 4;
      dv.setFloat32(off, cloud.scaleLog[baseScale + 1], true);
      off += 4;
      dv.setFloat32(off, cloud.scaleLog[baseScale + 2], true);
      off += 4;
      dv.setFloat32(off, cloud.quatsXYZW[baseRot + 3], true);
      off += 4;
      dv.setFloat32(off, cloud.quatsXYZW[baseRot + 0], true);
      off += 4;
      dv.setFloat32(off, cloud.quatsXYZW[baseRot + 1], true);
      off += 4;
      dv.setFloat32(off, cloud.quatsXYZW[baseRot + 2], true);
      off += 4;
    }
  }

  const head = Buffer.from(header.join('\n') + '\n', 'ascii');
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, Buffer.concat([head, payload]));
}

function mulberry32(seed) {
  let s = seed >>> 0;
  return () => {
    s = (s + 0x6d2b79f5) >>> 0;
    let t = Math.imul(s ^ (s >>> 15), s | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function randn(rng) {
  let u = 0;
  let v = 0;
  while (u === 0) {
    u = rng();
  }
  while (v === 0) {
    v = rng();
  }
  return Math.sqrt(-2.0 * Math.log(u)) * Math.cos(2.0 * Math.PI * v);
}

function makeSelfTestCloud(n = 1000000, seed = 7) {
  const count = Math.max(1, Math.floor(n));
  const rng = mulberry32(seed);
  const centers = [
    [-1.8, -0.5, 0.2],
    [0.0, 0.8, -0.1],
    [1.6, -0.2, 0.4],
  ];
  const clusterColors = [
    [0.95, 0.25, 0.2],
    [0.15, 0.75, 0.25],
    [0.2, 0.35, 0.95],
  ];
  const baseCount = Math.floor(count / 3);
  const counts = [baseCount, baseCount, count - 2 * baseCount];

  const positions = new Float32Array(count * 3);
  const scaleLog = new Float32Array(count * 3);
  const quats = new Float32Array(count * 4);
  const opacity = new Float32Array(count);
  const color0 = new Float32Array(count * 3);
  const shCoeffs = new Float32Array(count * 3);
  const noiseScale = [0.22, 0.12, 0.16];

  let cursor = 0;
  for (let cluster = 0; cluster < 3; cluster++) {
    const center = centers[cluster];
    const rgb = clusterColors[cluster];
    const clusterCount = counts[cluster];
    for (let i = 0; i < clusterCount; i++) {
      const idx = cursor + i;
      positions[idx * 3 + 0] = center[0] + randn(rng) * noiseScale[0];
      positions[idx * 3 + 1] = center[1] + randn(rng) * noiseScale[1];
      positions[idx * 3 + 2] = center[2] + randn(rng) * noiseScale[2];

      color0[idx * 3 + 0] = clamp(rgb[0] + randn(rng) * 0.03, 0.0, 1.0);
      color0[idx * 3 + 1] = clamp(rgb[1] + randn(rng) * 0.03, 0.0, 1.0);
      color0[idx * 3 + 2] = clamp(rgb[2] + randn(rng) * 0.03, 0.0, 1.0);

      scaleLog[idx * 3 + 0] = Math.log(
        clamp(0.015 + rng() * (0.05 - 0.015), 1e-8, 1.0),
      );
      scaleLog[idx * 3 + 1] = Math.log(
        clamp(0.015 + rng() * (0.05 - 0.015), 1e-8, 1.0),
      );
      scaleLog[idx * 3 + 2] = Math.log(
        clamp(0.015 + rng() * (0.05 - 0.015), 1e-8, 1.0),
      );

      const ax = randn(rng);
      const ay = randn(rng);
      const az = randn(rng);
      let axisLen = Math.sqrt(ax * ax + ay * ay + az * az);
      if (axisLen < 1e-12) {
        axisLen = 1.0;
      }
      const nx = ax / axisLen;
      const ny = ay / axisLen;
      const nz = az / axisLen;
      const ang = rng() * Math.PI;
      const half = ang * 0.5;
      const s = Math.sin(half);
      const c = Math.cos(half);
      quats[idx * 4 + 0] = nx * s;
      quats[idx * 4 + 1] = ny * s;
      quats[idx * 4 + 2] = nz * s;
      quats[idx * 4 + 3] = c;

      opacity[idx] = 0.25 + rng() * 0.65;
      shCoeffs[idx * 3 + 0] = (color0[idx * 3 + 0] - 0.5) / C0;
      shCoeffs[idx * 3 + 1] = (color0[idx * 3 + 1] - 0.5) / C0;
      shCoeffs[idx * 3 + 2] = (color0[idx * 3 + 2] - 0.5) / C0;
    }
    cursor += clusterCount;
  }

  const out = new GaussianCloud(
    positions,
    scaleLog,
    quats,
    opacity,
    shCoeffs,
    color0,
  );
  out._shDegree = 0;
  return out;
}

module.exports = {
  ConversionError,
  GaussianCloud,
  Bounds,
  TileNode,
  clamp,
  roundHalfToEven,
  ensure,
  sigmoid,
  shDegreeFromCoeffCount,
  inferShDegreeFromRestCount,
  makeSelfTestCloud,
  writeGraphdecoLikePly,
  _canonicalGaussianRowFloatCount: canonicalGaussianRowFloatCount,
  _canonicalGaussianRowByteSize: canonicalGaussianRowByteSize,
  _readPlyHeaderFromHandle: readPlyHeaderFromHandle,
  _buildGaussianPlyLayout: buildGaussianPlyLayout,
  _forEachGaussianPlyPosition: forEachGaussianPlyPosition,
  _forEachGaussianPlyCanonicalRecord: forEachGaussianPlyCanonicalRecord,
};
