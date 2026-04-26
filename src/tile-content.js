const fs = require('fs');
const path = require('path');

const { GaussianCloud, ensure, shDegreeFromCoeffCount } = require('./parser');
const {
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
  computeBounds,
  computeThreeSigmaAabbDiagonalRadiusAt,
  computeThreeSigmaAabbDiagonalRadius,
  normalizeSplatTargetCount,
  planSimplifyCloudVoxel,
} = require('./builder');
const {
  LEAF_BUCKET_ENCODING,
  HANDOFF_BUCKET_ENCODING,
  makeRowScratch,
  readBucketRowIntoScratch,
  readBucketCoreRowIntoScratch,
  forEachBucketChunk,
  cacheBucketEntriesIfAffordable,
  computeBucketEntriesBounds,
  loadBucketCloudFromEntries,
  writeCanonicalCloudFile,
} = require('./bucket-io');
const { serializeBoundsState, deserializeBoundsState } = require('./pipeline-state');
const { contentRelPath } = require('./pipeline-paths');

const SPZ_CLOUD_ASYNC_WRITE_THRESHOLD = 4096;
const SPZ_BUCKET_ASYNC_WRITE_THRESHOLD = 4096;
const MERGE_SH_COEFF_BLOCK = 12;

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
        const base = byteOffset + row * rowByteSize;
        const currentFloatBase = floatView
          ? floatBase + row * floatsPerRow
          : 0;
        if (
          floatView &&
          (encoding === LEAF_BUCKET_ENCODING ||
            encoding === HANDOFF_BUCKET_ENCODING)
        ) {
          const src = floatView;
          const off = currentFloatBase;
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
          continue;
        }

      readBucketRowIntoScratch(
        encoding,
        view,
        base,
        coeffCount,
        scratch,
        floatView,
        currentFloatBase,
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
      }
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

  await forEachBucketChunk(
    entries,
    coeffCount,
    (chunk) => {
      const {
        view,
        byteOffset,
        rowBase,
        rowCount,
        rowByteSize,
        encoding,
        floatView,
        floatBase,
      } = chunk;
      const floatsPerRow = rowByteSize >>> 2;
      for (let row = 0; row < rowCount; row++) {
        const sourceRowIndex = rowBase + row;
        const slots = wantedRows.get(sourceRowIndex);
        if (slots) {
          readBucketRowIntoScratch(
            encoding,
            view,
            byteOffset + row * rowByteSize,
            coeffCount,
            scratch,
            floatView,
            floatView ? floatBase + row * floatsPerRow : 0,
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
      }
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
        readBucketCoreRowIntoScratch(
          encoding,
          view,
          byteOffset + row * rowByteSize,
          scratch,
          floatView,
          floatView ? floatBase + row * floatsPerRow : 0,
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
      }
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
        readBucketCoreRowIntoScratch(
          encoding,
          view,
          byteOffset + row * rowByteSize,
          scratch,
          floatView,
          floatView ? floatBase + row * floatsPerRow : 0,
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
      }
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
          const slot = assignment[rowIndex];
          if (
            !Number.isFinite(weightSums[slot]) ||
            weightSums[slot] <= 1e-12 ||
            counts[slot] === 0
          ) {
            rowIndex += 1;
            continue;
          }
          readBucketRowIntoScratch(
            encoding,
            view,
            byteOffset + row * rowByteSize,
            coeffCount,
            scratch,
            floatView,
            floatView ? floatBase + row * floatsPerRow : 0,
          );
          const radius = rowRadiusOrScratch(inputRadius, rowIndex, scratch);
          const weight = mergeAggregationWeight(
            scratch.opacity,
            radius,
            voxelDiag,
          );
          const blockBase = slot * blockWidth;
          for (let c = 0; c < blockWidth; c++) {
            weightedShBlock[blockBase + c] +=
              scratch.sh[coeffStart + c] * weight;
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
        }
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
          readBucketCoreRowIntoScratch(
            encoding,
            view,
            byteOffset + row * rowByteSize,
            scratch,
            floatView,
            floatView ? floatBase + row * floatsPerRow : 0,
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
        }
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
        readBucketCoreRowIntoScratch(
          encoding,
          view,
          byteOffset + row * rowByteSize,
          scratch,
          floatView,
          floatView ? floatBase + row * floatsPerRow : 0,
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
      }
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

module.exports = {
  packBucketEntriesToSpz,
  streamSimplifyBucketEntriesExact,
  spzBytesPerBucketRow,
  safeNodeBoundsTranslation,
  writeContentFile,
  writeCloudGlbOutput,
  writeBucketGlbTaskOutput,
  writeSimplifiedBucketGlbTaskOutput,
  writeSimplifiedBucketContentFile,
  writeBucketContentFile,
};
