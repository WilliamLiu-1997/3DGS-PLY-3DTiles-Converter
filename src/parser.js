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

function serializeBounds(bounds) {
  return {
    minimum: [bounds.minimum[0], bounds.minimum[1], bounds.minimum[2]],
    maximum: [bounds.maximum[0], bounds.maximum[1], bounds.maximum[2]],
  };
}

function deserializeBounds(serialized) {
  return new Bounds(
    [serialized.minimum[0], serialized.minimum[1], serialized.minimum[2]],
    [serialized.maximum[0], serialized.maximum[1], serialized.maximum[2]],
  );
}

function serializeTileNode(node) {
  return {
    level: node.level,
    x: node.x,
    y: node.y,
    z: node.z,
    bounds: serializeBounds(node.bounds),
    error: node.error,
    contentUri: node.contentUri,
    children: node.children.map((child) => serializeTileNode(child)),
  };
}

function deserializeTileNode(serialized) {
  return new TileNode(
    serialized.level,
    serialized.x,
    serialized.y,
    serialized.z,
    deserializeBounds(serialized.bounds),
    serialized.error,
    serialized.contentUri,
    serialized.children.map((child) => deserializeTileNode(child)),
  );
}

function readLineAscii(buffer, start) {
  let end = start;
  while (end < buffer.length && buffer[end] !== 0x0a && buffer[end] !== 0x0d) {
    end += 1;
  }
  const line = buffer.slice(start, end).toString('ascii').trim();
  let next = end;
  while (
    next < buffer.length &&
    (buffer[next] === 0x0a || buffer[next] === 0x0d)
  ) {
    next += 1;
  }
  return [line, next];
}

function readPlyHeader(fileBuffer, filePath) {
  let cursor = 0;
  const [firstLine, afterFirst] = readLineAscii(fileBuffer, cursor);
  cursor = afterFirst;
  ensure(firstLine === 'ply', `${filePath} is not a valid PLY file.`);

  let format = null;
  let vertexCount = null;
  const vertexProps = [];
  let currentElement = null;

  while (cursor < fileBuffer.length) {
    const [line, nextCursor] = readLineAscii(fileBuffer, cursor);
    cursor = nextCursor;
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
    } else if (parts[0] === 'end_header') {
      break;
    }
  }

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

function writeNormalizedQuaternion(
  out,
  outOff,
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
  out[outOff + 0] = x * inv;
  out[outOff + 1] = y * inv;
  out[outOff + 2] = z * inv;
  out[outOff + 3] = w * inv;
}

function parseCommonGaussianPly(
  filePath,
  inputConvention,
  colorSpace,
  linearScaleInput,
) {
  const fileBuffer = fs.readFileSync(filePath);
  const {
    format,
    vertexCount: n,
    vertexProps,
    dataOffset,
  } = readPlyHeader(fileBuffer, filePath);
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
  const positions = new Float32Array(n * 3);
  const scaleLog = new Float32Array(n * 3);
  const quats = new Float32Array(n * 4);
  const opacity = new Float32Array(n);
  const color0 = new Float32Array(n * 3);
  const shCoeffs = new Float32Array(n * coeffCount * 3);

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

  const KIND_SKIP = 0;
  const KIND_POSITION = 1;
  const KIND_SH0 = 2;
  const KIND_SCALE = 3;
  const KIND_ROT = 4;
  const KIND_OPACITY = 5;
  const KIND_REST = 6;

  const propertyPlan = vertexProps.map((prop) => {
    const typeInfo = PLY_DTYPE_MAP[prop.type];
    ensure(!!typeInfo, `Unsupported PLY property type: ${prop.type}`);
    const name = prop.name;
    if (name === 'x') return { kind: KIND_POSITION, index: 0, typeInfo };
    if (name === 'y') return { kind: KIND_POSITION, index: 1, typeInfo };
    if (name === 'z') return { kind: KIND_POSITION, index: 2, typeInfo };
    if (name === 'opacity') return { kind: KIND_OPACITY, typeInfo };
    if (name === 'f_dc_0') return { kind: KIND_SH0, index: 0, typeInfo };
    if (name === 'f_dc_1') return { kind: KIND_SH0, index: 1, typeInfo };
    if (name === 'f_dc_2') return { kind: KIND_SH0, index: 2, typeInfo };

    const scaleIdx = scaleIndexByName[name];
    if (scaleIdx != null) {
      return { kind: KIND_SCALE, index: scaleIdx, typeInfo };
    }

    const rotIdx = rotIndexByName[name];
    if (rotIdx != null) {
      return { kind: KIND_ROT, index: rotIdx, typeInfo };
    }

    const restIdx = restIndexByName[name];
    if (restIdx != null) {
      const coeff = restIdx % extraDim;
      const channel = Math.floor(restIdx / extraDim);
      return {
        kind: KIND_REST,
        index: 3 * (1 + coeff) + channel,
        typeInfo,
      };
    }

    return { kind: KIND_SKIP, typeInfo };
  });

  const writeScalar = (rowIndex, plan, value) => {
    const p = rowIndex * 3;
    const coeffBase = rowIndex * coeffCount * 3;
    switch (plan.kind) {
      case KIND_POSITION:
        positions[p + plan.index] = value;
        break;
      case KIND_SH0:
        shCoeffs[coeffBase + plan.index] = value;
        color0[p + plan.index] = clamp(value * C0 + 0.5, 0.0, 1.0);
        break;
      case KIND_SCALE:
        scaleLog[p + plan.index] = linearScaleInput
          ? Math.log(Math.max(value, 1e-8))
          : value;
        break;
      case KIND_OPACITY:
        opacity[rowIndex] =
          inputConvention === 'graphdeco'
            ? sigmoid(value)
            : clamp(value, 0.0, 1.0);
        break;
      case KIND_REST:
        shCoeffs[coeffBase + plan.index] = value;
        break;
      default:
        break;
    }
  };

  if (format === 'binary_little_endian') {
    const data = fileBuffer.subarray(dataOffset);
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    let offset = 0;
    for (let i = 0; i < n; i++) {
      let r0 = 0.0;
      let r1 = 0.0;
      let r2 = 0.0;
      let r3 = 1.0;
      for (let p = 0; p < propertyPlan.length; p++) {
        const plan = propertyPlan[p];
        const value = readPlyScalar(view, offset, plan.typeInfo);
        offset += plan.typeInfo.size;
        if (plan.kind === KIND_ROT) {
          if (plan.index === 0) r0 = value;
          else if (plan.index === 1) r1 = value;
          else if (plan.index === 2) r2 = value;
          else r3 = value;
        } else {
          writeScalar(i, plan, value);
        }
      }
      writeNormalizedQuaternion(quats, i * 4, inputConvention, r0, r1, r2, r3);
    }
  } else {
    const bytes = fileBuffer.subarray(dataOffset);
    const expected = n * propertyPlan.length;
    let cursor = 0;
    let count = 0;
    for (let i = 0; i < n; i++) {
      let r0 = 0.0;
      let r1 = 0.0;
      let r2 = 0.0;
      let r3 = 1.0;
      for (let p = 0; p < propertyPlan.length; p++) {
        while (cursor < bytes.length) {
          const ch = bytes[cursor];
          if (ch > 0x20) {
            break;
          }
          cursor += 1;
        }

        if (cursor >= bytes.length) {
          throw new ConversionError(
            `ASCII PLY rows mismatch. Expected at least ${expected} values, got ${count}.`,
          );
        }

        const start = cursor;
        while (cursor < bytes.length && bytes[cursor] > 0x20) {
          cursor += 1;
        }
        const value = Number.parseFloat(bytes.toString('ascii', start, cursor));
        cursor += 1;
        count += 1;

        const plan = propertyPlan[p];
        if (plan.kind === KIND_ROT) {
          if (plan.index === 0) r0 = value;
          else if (plan.index === 1) r1 = value;
          else if (plan.index === 2) r2 = value;
          else r3 = value;
        } else {
          writeScalar(i, plan, value);
        }
      }
      writeNormalizedQuaternion(quats, i * 4, inputConvention, r0, r1, r2, r3);
    }
    ensure(
      count >= expected,
      `ASCII PLY rows mismatch. Expected at least ${expected} values, got ${count}.`,
    );
  }

  ensure(
    colorSpace === 'lin_rec709_display' || colorSpace === 'srgb_rec709_display',
    `Unsupported colorSpace: ${colorSpace}`,
  );

  const cloud = new GaussianCloud(
    positions,
    scaleLog,
    quats,
    opacity,
    shCoeffs,
    color0,
  );
  cloud._shDegree = degree;
  return cloud;
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
  const payload = Buffer.alloc(n * recordSize);
  const dv = new DataView(
    payload.buffer,
    payload.byteOffset,
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

function makeSelfTestCloud(n = 6000, seed = 7) {
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
  serializeBounds,
  deserializeBounds,
  serializeTileNode,
  deserializeTileNode,
  shDegreeFromCoeffCount,
  inferShDegreeFromRestCount,
  parseCommonGaussianPly,
  makeSelfTestCloud,
  writeGraphdecoLikePly,
};
