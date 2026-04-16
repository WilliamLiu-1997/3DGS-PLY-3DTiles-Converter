const zlib = require('zlib');

const { ConversionError, clamp, roundHalfToEven, ensure } = require('./parser');

const SPZ_COLOR_SCALE = 0.15;
const SPZ_MAGIC = 0x5053474e;
const SPZ_STREAM_VERSION = 3;
const SPZ_FRACTIONAL_BITS = 12;
const SPZ_FIXED24_LIMIT = (1 << 23) - 1;

function spzExtraDimForDegree(degree) {
  const mapping = { 0: 0, 1: 3, 2: 8, 3: 15 };
  if (!Object.prototype.hasOwnProperty.call(mapping, degree)) {
    throw new ConversionError(`SPZ v3 supports degree up to 3, got ${degree}.`);
  }
  return mapping[degree];
}

const SQRT_HALF = Math.sqrt(0.5);
const MAG_SCALE = 511; // 2**9 - 1

function packQuaternionSmallestThreeInto(quats, idx, out, outOff) {
  const qi = idx * 4;
  let x = quats[qi];
  let y = quats[qi + 1];
  let z = quats[qi + 2];
  let w = quats[qi + 3];
  const len2 = x * x + y * y + z * z + w * w;
  if (len2 < 1e-40) {
    x = 0.0;
    y = 0.0;
    z = 0.0;
    w = 1.0;
  } else {
    const inv = 1.0 / Math.sqrt(len2);
    x *= inv;
    y *= inv;
    z *= inv;
    w *= inv;
  }
  const ax = Math.abs(x);
  const ay = Math.abs(y);
  const az = Math.abs(z);
  const aw = Math.abs(w);
  let largest = 0;
  let lv = ax;
  if (ay > lv) {
    largest = 1;
    lv = ay;
  }
  if (az > lv) {
    largest = 2;
    lv = az;
  }
  if (aw > lv) {
    largest = 3;
  }
  const vals = [x, y, z, w];
  const negate = vals[largest] < 0.0;
  let comp = largest;
  const invS = SQRT_HALF;
  for (let i = 0; i < 4; i++) {
    if (i === largest) continue;
    const v = vals[i];
    const negbit = (v < 0.0) ^ negate ? 1 : 0;
    let mag = Math.floor(MAG_SCALE * (Math.abs(v) / invS) + 0.5);
    if (mag > MAG_SCALE) mag = MAG_SCALE;
    comp = (comp << 10) | (negbit << 9) | mag;
  }
  out[outOff] = comp & 0xff;
  out[outOff + 1] = (comp >>> 8) & 0xff;
  out[outOff + 2] = (comp >>> 16) & 0xff;
  out[outOff + 3] = (comp >>> 24) & 0xff;
}

function packCloudToSpz(cloudLocal, sh1Bits, shRestBits, translation = null) {
  if (
    !Number.isInteger(sh1Bits) ||
    !Number.isInteger(shRestBits) ||
    sh1Bits < 1 ||
    sh1Bits > 8 ||
    shRestBits < 1 ||
    shRestBits > 8
  ) {
    throw new ConversionError('SPZ SH quant bits must be in [1, 8].');
  }

  const n = cloudLocal.length;
  const extra = spzExtraDimForDegree(cloudLocal.shDegree);
  const expected = 1 + extra;
  const coeffCount = cloudLocal.shCoeffs.length / (n * 3);
  ensure(
    coeffCount === expected,
    `SPZ coefficient mismatch with degree=${cloudLocal.shDegree}; cloud=${coeffCount}.`,
  );

  // Pre-allocate payload buffer: positions(9) + alpha(1) + color(3) + scale(3) + quat(4) + extra SH
  const bytesPerPoint =
    9 + 1 + 3 + 3 + 4 + (extra > 0 ? (coeffCount - 1) * 3 : 0);
  const payload = new Uint8Array(n * bytesPerPoint);
  let off = 0;
  const tx = translation ? translation[0] : 0.0;
  const ty = translation ? translation[1] : 0.0;
  const tz = translation ? translation[2] : 0.0;

  const scale = 1 << SPZ_FRACTIONAL_BITS;
  for (let i = 0; i < n; i++) {
    const i3 = i * 3;
    const localX = translation
      ? Math.fround(cloudLocal.positions[i3 + 0] - tx)
      : cloudLocal.positions[i3 + 0];
    const fixedX = roundHalfToEven(localX * scale);
    ensure(
      Math.abs(fixedX) <= SPZ_FIXED24_LIMIT,
      'Tile local coordinates exceed SPZ 24-bit fixed-point range.',
    );
    payload[off++] = fixedX & 0xff;
    payload[off++] = (fixedX >>> 8) & 0xff;
    payload[off++] = (fixedX >>> 16) & 0xff;

    const localY = translation
      ? Math.fround(cloudLocal.positions[i3 + 1] - ty)
      : cloudLocal.positions[i3 + 1];
    const fixedY = roundHalfToEven(localY * scale);
    ensure(
      Math.abs(fixedY) <= SPZ_FIXED24_LIMIT,
      'Tile local coordinates exceed SPZ 24-bit fixed-point range.',
    );
    payload[off++] = fixedY & 0xff;
    payload[off++] = (fixedY >>> 8) & 0xff;
    payload[off++] = (fixedY >>> 16) & 0xff;

    const localZ = translation
      ? Math.fround(cloudLocal.positions[i3 + 2] - tz)
      : cloudLocal.positions[i3 + 2];
    const fixedZ = roundHalfToEven(localZ * scale);
    ensure(
      Math.abs(fixedZ) <= SPZ_FIXED24_LIMIT,
      'Tile local coordinates exceed SPZ 24-bit fixed-point range.',
    );
    payload[off++] = fixedZ & 0xff;
    payload[off++] = (fixedZ >>> 8) & 0xff;
    payload[off++] = (fixedZ >>> 16) & 0xff;
  }

  for (let i = 0; i < n; i++) {
    payload[off++] = Math.max(
      0,
      Math.min(
        255,
        roundHalfToEven(
          Math.min(1.0, Math.max(0.0, cloudLocal.opacity[i])) * 255.0,
        ),
      ),
    );
  }

  const colorScale255 = SPZ_COLOR_SCALE * 255.0;
  const half255 = 0.5 * 255.0;
  for (let i = 0; i < n; i++) {
    const pointBase = i * coeffCount * 3;
    payload[off++] = clamp(
      roundHalfToEven(
        cloudLocal.shCoeffs[pointBase + 0] * colorScale255 + half255,
      ),
      0,
      255,
    );
    payload[off++] = clamp(
      roundHalfToEven(
        cloudLocal.shCoeffs[pointBase + 1] * colorScale255 + half255,
      ),
      0,
      255,
    );
    payload[off++] = clamp(
      roundHalfToEven(
        cloudLocal.shCoeffs[pointBase + 2] * colorScale255 + half255,
      ),
      0,
      255,
    );
  }

  for (let i = 0; i < n; i++) {
    const i3 = i * 3;
    payload[off++] = clamp(
      roundHalfToEven((cloudLocal.scaleLog[i3 + 0] + 10.0) * 16.0),
      0,
      255,
    );
    payload[off++] = clamp(
      roundHalfToEven((cloudLocal.scaleLog[i3 + 1] + 10.0) * 16.0),
      0,
      255,
    );
    payload[off++] = clamp(
      roundHalfToEven((cloudLocal.scaleLog[i3 + 2] + 10.0) * 16.0),
      0,
      255,
    );
  }

  for (let i = 0; i < n; i++) {
    packQuaternionSmallestThreeInto(cloudLocal.quatsXYZW, i, payload, off);
    off += 4;
  }

  if (extra > 0) {
    // Pre-compute bucket values per coefficient index
    const buckets = new Uint8Array(coeffCount);
    const halfBuckets = new Float64Array(coeffCount);
    const invBuckets = new Float64Array(coeffCount);
    for (let coeff = 1; coeff < coeffCount; coeff++) {
      const bits = coeff <= 3 ? sh1Bits : shRestBits;
      const b = 1 << (8 - bits);
      buckets[coeff] = b;
      halfBuckets[coeff] = b / 2.0;
      invBuckets[coeff] = 1.0 / b;
    }
    for (let i = 0; i < n; i++) {
      const pointBase = i * coeffCount * 3;
      for (let coeff = 1; coeff < coeffCount; coeff++) {
        const bkt = buckets[coeff];
        const hb = halfBuckets[coeff];
        const ib = invBuckets[coeff];
        const coeffBase = pointBase + coeff * 3;
        const q0 = roundHalfToEven(
          cloudLocal.shCoeffs[coeffBase + 0] * 128.0 + 128.0,
        );
        const q1 = roundHalfToEven(
          cloudLocal.shCoeffs[coeffBase + 1] * 128.0 + 128.0,
        );
        const q2 = roundHalfToEven(
          cloudLocal.shCoeffs[coeffBase + 2] * 128.0 + 128.0,
        );
        payload[off++] = clamp(Math.floor((q0 + hb) * ib) * bkt, 0, 255);
        payload[off++] = clamp(Math.floor((q1 + hb) * ib) * bkt, 0, 255);
        payload[off++] = clamp(Math.floor((q2 + hb) * ib) * bkt, 0, 255);
      }
    }
  }

  const header = Buffer.alloc(13);
  header.writeUInt32LE(SPZ_MAGIC, 0);
  header.writeUInt32LE(SPZ_STREAM_VERSION, 4);
  header.writeUInt32LE(n, 8);
  header[12] = cloudLocal.shDegree;
  const tail = Buffer.from([SPZ_FRACTIONAL_BITS, 0, 0]);
  return zlib.gzipSync(
    Buffer.concat([
      header,
      tail,
      Buffer.from(payload.buffer, payload.byteOffset, off),
    ]),
    {
      level: 9,
    },
  );
}

function serializeCloudForWorkerTask(cloud) {
  return {
    shDegree: cloud.shDegree,
    positions: cloud.positions.buffer,
    scaleLog: cloud.scaleLog.buffer,
    quatsXYZW: cloud.quatsXYZW.buffer,
    opacity: cloud.opacity.buffer,
    shCoeffs: cloud.shCoeffs.buffer,
    color0: cloud.color0 ? cloud.color0.buffer : null,
    length: cloud.length,
  };
}

function transferListForCloud(cloud) {
  const transfer = [
    cloud.positions.buffer,
    cloud.scaleLog.buffer,
    cloud.quatsXYZW.buffer,
    cloud.opacity.buffer,
    cloud.shCoeffs.buffer,
  ];
  if (cloud.color0) {
    transfer.push(cloud.color0.buffer);
  }
  return transfer;
}

module.exports = {
  SPZ_STREAM_VERSION,
  packCloudToSpz,
  serializeCloudForWorkerTask,
  transferListForCloud,
};
