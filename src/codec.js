const zlib = require('zlib');

const { ConversionError, clamp, roundHalfToEven, ensure } = require('./parser');

const SPZ_COLOR_SCALE = 0.15;
const SPZ_MAGIC = 0x5053474e;
const SPZ_STREAM_VERSION = 3;
const SPZ_FRACTIONAL_BITS = 12;
const SPZ_FIXED24_LIMIT = (1 << 23) - 1;
const SPZ_HEADER_BYTES = 16;
const GZIP_SPZ_OPTIONS = { level: 9, memLevel: 9 };

function spzExtraDimForDegree(degree) {
  const mapping = { 0: 0, 1: 3, 2: 8, 3: 15 };
  if (!Object.prototype.hasOwnProperty.call(mapping, degree)) {
    throw new ConversionError(`SPZ v3 supports degree up to 3, got ${degree}.`);
  }
  return mapping[degree];
}

function validateSpzQuantBits(sh1Bits, shRestBits) {
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
}

function spzCoeffCountForDegree(shDegree) {
  return 1 + spzExtraDimForDegree(shDegree);
}

function validateSpzCoeffCount(shDegree, coeffCount) {
  const expected = spzCoeffCountForDegree(shDegree);
  ensure(
    coeffCount === expected,
    `SPZ coefficient mismatch with degree=${shDegree}; cloud=${coeffCount}.`,
  );
}

function spzBytesPerPoint(coeffCount) {
  return 9 + 1 + 3 + 3 + 4 + (coeffCount > 1 ? (coeffCount - 1) * 3 : 0);
}

function makeSpzPacketLayout(pointCount, coeffCount) {
  const bytesPerPoint = spzBytesPerPoint(coeffCount);
  const positionsOffset = SPZ_HEADER_BYTES;
  const opacityOffset = positionsOffset + pointCount * 9;
  const colorOffset = opacityOffset + pointCount;
  const scaleOffset = colorOffset + pointCount * 3;
  const quatOffset = scaleOffset + pointCount * 3;
  const extraShOffset = quatOffset + pointCount * 4;
  const extraBytesPerPoint = coeffCount > 1 ? (coeffCount - 1) * 3 : 0;
  return {
    packet: Buffer.allocUnsafe(SPZ_HEADER_BYTES + pointCount * bytesPerPoint),
    bytesPerPoint,
    positionsOffset,
    opacityOffset,
    colorOffset,
    scaleOffset,
    quatOffset,
    extraShOffset,
    extraBytesPerPoint,
  };
}

function writeSpzPacketHeader(packet, pointCount, shDegree) {
  packet.writeUInt32LE(SPZ_MAGIC, 0);
  packet.writeUInt32LE(SPZ_STREAM_VERSION, 4);
  packet.writeUInt32LE(pointCount, 8);
  packet[12] = shDegree;
  packet[13] = SPZ_FRACTIONAL_BITS;
  packet[14] = 0;
  packet[15] = 0;
}

function writeFixed24Into(out, outOff, fixed) {
  out[outOff + 0] = fixed & 0xff;
  out[outOff + 1] = (fixed >>> 8) & 0xff;
  out[outOff + 2] = (fixed >>> 16) & 0xff;
}

function quantizeSpzPosition(localValue) {
  const fixed = roundHalfToEven(localValue * (1 << SPZ_FRACTIONAL_BITS));
  ensure(
    Math.abs(fixed) <= SPZ_FIXED24_LIMIT,
    'Tile local coordinates exceed SPZ 24-bit fixed-point range.',
  );
  return fixed;
}

function quantizeSpzOpacity(opacity) {
  return Math.max(
    0,
    Math.min(
      255,
      roundHalfToEven(Math.min(1.0, Math.max(0.0, opacity)) * 255.0),
    ),
  );
}

function quantizeSpzColor(coeff) {
  return clamp(
    roundHalfToEven(coeff * SPZ_COLOR_SCALE * 255.0 + 0.5 * 255.0),
    0,
    255,
  );
}

function quantizeSpzScale(scaleLog) {
  return clamp(roundHalfToEven((scaleLog + 10.0) * 16.0), 0, 255);
}

function makeSpzShQuantBuckets(coeffCount, sh1Bits, shRestBits) {
  validateSpzQuantBits(sh1Bits, shRestBits);
  const buckets = new Uint8Array(coeffCount);
  const halfBuckets = new Float64Array(coeffCount);
  const invBuckets = new Float64Array(coeffCount);
  for (let coeff = 1; coeff < coeffCount; coeff++) {
    const bits = coeff <= 3 ? sh1Bits : shRestBits;
    const bucket = 1 << (8 - bits);
    buckets[coeff] = bucket;
    halfBuckets[coeff] = bucket / 2.0;
    invBuckets[coeff] = 1.0 / bucket;
  }
  return { buckets, halfBuckets, invBuckets };
}

function quantizeSpzExtraSh(coeff, bucket, halfBucket, invBucket) {
  const q = roundHalfToEven(coeff * 128.0 + 128.0);
  return clamp(Math.floor((q + halfBucket) * invBucket) * bucket, 0, 255);
}

function gzipSpzPacket(packet, byteLength = packet.length) {
  return zlib.gzipSync(packet.subarray(0, byteLength), GZIP_SPZ_OPTIONS);
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
  const negate =
    largest === 0 ? x < 0.0 :
    largest === 1 ? y < 0.0 :
    largest === 2 ? z < 0.0 :
    w < 0.0;
  let comp = largest;

  if (largest !== 0) {
    const v = x;
    const negbit = (v < 0.0) ^ negate ? 1 : 0;
    let mag = Math.floor(MAG_SCALE * (Math.abs(v) / SQRT_HALF) + 0.5);
    if (mag > MAG_SCALE) mag = MAG_SCALE;
    comp = (comp << 10) | (negbit << 9) | mag;
  }
  if (largest !== 1) {
    const v = y;
    const negbit = (v < 0.0) ^ negate ? 1 : 0;
    let mag = Math.floor(MAG_SCALE * (Math.abs(v) / SQRT_HALF) + 0.5);
    if (mag > MAG_SCALE) mag = MAG_SCALE;
    comp = (comp << 10) | (negbit << 9) | mag;
  }
  if (largest !== 2) {
    const v = z;
    const negbit = (v < 0.0) ^ negate ? 1 : 0;
    let mag = Math.floor(MAG_SCALE * (Math.abs(v) / SQRT_HALF) + 0.5);
    if (mag > MAG_SCALE) mag = MAG_SCALE;
    comp = (comp << 10) | (negbit << 9) | mag;
  }
  if (largest !== 3) {
    const v = w;
    const negbit = (v < 0.0) ^ negate ? 1 : 0;
    let mag = Math.floor(MAG_SCALE * (Math.abs(v) / SQRT_HALF) + 0.5);
    if (mag > MAG_SCALE) mag = MAG_SCALE;
    comp = (comp << 10) | (negbit << 9) | mag;
  }
  out[outOff] = comp & 0xff;
  out[outOff + 1] = (comp >>> 8) & 0xff;
  out[outOff + 2] = (comp >>> 16) & 0xff;
  out[outOff + 3] = (comp >>> 24) & 0xff;
}

function packCloudToSpz(cloudLocal, sh1Bits, shRestBits, translation = null) {
  validateSpzQuantBits(sh1Bits, shRestBits);

  const n = cloudLocal.length;
  const coeffCount = cloudLocal.shCoeffs.length / (n * 3);
  validateSpzCoeffCount(cloudLocal.shDegree, coeffCount);

  const extra = coeffCount - 1;
  const layout = makeSpzPacketLayout(n, coeffCount);
  const { packet } = layout;
  const tx = translation ? translation[0] : 0.0;
  const ty = translation ? translation[1] : 0.0;
  const tz = translation ? translation[2] : 0.0;
  const shBuckets =
    extra > 0 ? makeSpzShQuantBuckets(coeffCount, sh1Bits, shRestBits) : null;

  for (let i = 0; i < n; i++) {
    const i3 = i * 3;
    const pointBase = i * coeffCount * 3;

    const localX = translation
      ? Math.fround(cloudLocal.positions[i3 + 0] - tx)
      : cloudLocal.positions[i3 + 0];
    const posBase = layout.positionsOffset + i * 9;
    writeFixed24Into(packet, posBase + 0, quantizeSpzPosition(localX));

    const localY = translation
      ? Math.fround(cloudLocal.positions[i3 + 1] - ty)
      : cloudLocal.positions[i3 + 1];
    writeFixed24Into(packet, posBase + 3, quantizeSpzPosition(localY));

    const localZ = translation
      ? Math.fround(cloudLocal.positions[i3 + 2] - tz)
      : cloudLocal.positions[i3 + 2];
    writeFixed24Into(packet, posBase + 6, quantizeSpzPosition(localZ));

    packet[layout.opacityOffset + i] = quantizeSpzOpacity(cloudLocal.opacity[i]);

    const colorBase = layout.colorOffset + i * 3;
    packet[colorBase + 0] = quantizeSpzColor(cloudLocal.shCoeffs[pointBase + 0]);
    packet[colorBase + 1] = quantizeSpzColor(cloudLocal.shCoeffs[pointBase + 1]);
    packet[colorBase + 2] = quantizeSpzColor(cloudLocal.shCoeffs[pointBase + 2]);

    const scaleBase = layout.scaleOffset + i * 3;
    packet[scaleBase + 0] = quantizeSpzScale(cloudLocal.scaleLog[i3 + 0]);
    packet[scaleBase + 1] = quantizeSpzScale(cloudLocal.scaleLog[i3 + 1]);
    packet[scaleBase + 2] = quantizeSpzScale(cloudLocal.scaleLog[i3 + 2]);

    packQuaternionSmallestThreeInto(
      cloudLocal.quatsXYZW,
      i,
      packet,
      layout.quatOffset + i * 4,
    );

    if (extra > 0) {
      let shBase = layout.extraShOffset + i * layout.extraBytesPerPoint;
      for (let coeff = 1; coeff < coeffCount; coeff++) {
        const bkt = shBuckets.buckets[coeff];
        const hb = shBuckets.halfBuckets[coeff];
        const ib = shBuckets.invBuckets[coeff];
        const coeffBase = pointBase + coeff * 3;
        packet[shBase++] = quantizeSpzExtraSh(
          cloudLocal.shCoeffs[coeffBase + 0],
          bkt,
          hb,
          ib,
        );
        packet[shBase++] = quantizeSpzExtraSh(
          cloudLocal.shCoeffs[coeffBase + 1],
          bkt,
          hb,
          ib,
        );
        packet[shBase++] = quantizeSpzExtraSh(
          cloudLocal.shCoeffs[coeffBase + 2],
          bkt,
          hb,
          ib,
        );
      }
    }
  }

  writeSpzPacketHeader(packet, n, cloudLocal.shDegree);
  return gzipSpzPacket(packet);
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
  SPZ_COLOR_SCALE,
  SPZ_MAGIC,
  SPZ_STREAM_VERSION,
  SPZ_FRACTIONAL_BITS,
  SPZ_FIXED24_LIMIT,
  SPZ_HEADER_BYTES,
  gzipSpzPacket,
  makeSpzPacketLayout,
  makeSpzShQuantBuckets,
  quantizeSpzColor,
  quantizeSpzExtraSh,
  quantizeSpzOpacity,
  quantizeSpzPosition,
  quantizeSpzScale,
  validateSpzCoeffCount,
  validateSpzQuantBits,
  writeFixed24Into,
  writeSpzPacketHeader,
  spzExtraDimForDegree,
  packQuaternionSmallestThreeInto,
  packCloudToSpz,
  serializeCloudForWorkerTask,
  transferListForCloud,
};
