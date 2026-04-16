const fs = require('fs');
const path = require('path');
const { Worker } = require('worker_threads');

const {
  ConversionError,
  Bounds,
  TileNode,
  roundHalfToEven,
  serializeBounds,
  deserializeBounds,
  serializeTileNode,
  deserializeTileNode,
} = require('./parser');

const {
  SPZ_STREAM_VERSION,
  packCloudToSpz,
  serializeCloudForWorkerTask,
  transferListForCloud,
} = require('./codec');

const { GltfBuilder } = require('./gltf');

const SPZ_ASYNC_WRITE_THRESHOLD = 65536;
const SUBTREE_BUILD_ASYNC_THRESHOLD = 32768;
const SUBTREE_BUILD_ASYNC_MAX_DEPTH = 2;
const SUBTREE_MAGIC = 0x74627573;
const SUBTREE_VERSION = 1;
const SOURCE_REPOSITORY = '3DGS-PLY-3DTiles-Converter';
const DEFAULT_WORKER_SCRIPT = path.join(__dirname, 'convert-core.js');

class ConsoleProgressBar {
  constructor(label, total = 0, width = 28) {
    this.label = label;
    this.width = Math.max(10, width);
    this.current = 0;
    this.total =
      Number.isFinite(total) && total > 0 ? Math.max(1, Math.floor(total)) : 0;
    this.enabled =
      process.stdout && process.stdout.isTTY && process.stderr.isTTY;
    this._spinner = ['-', '\\', '|', '/'];
    this._spinnerPos = 0;
    this._last = 0;
    this._lastMessage = '';
    this._done = false;
  }

  setTotal(total) {
    this.total =
      Number.isFinite(total) && total > 0 ? Math.max(1, Math.floor(total)) : 0;
    this._render();
  }

  update(current, message = '') {
    this.current = Math.max(this.current, current);
    if (message) {
      this._lastMessage = message;
    }
    this._render();
  }

  tick(message = '') {
    this.update(this.current + 1, message);
  }

  done(message = '') {
    if (this._done) {
      return;
    }
    this._done = true;
    if (message) {
      this._lastMessage = message;
    }
    if (this.enabled) {
      this.current = this.total > 0 ? this.total : this.current;
      this._render(true);
      process.stdout.write('\n');
    } else if (this._lastMessage) {
      console.log(`[${this.label}] ${this._lastMessage}`);
    }
  }

  _render(force = false) {
    if (!this.enabled) {
      return;
    }
    const now = Date.now();
    if (!force && now - this._last < 45 && this.current > 0) {
      return;
    }
    this._last = now;
    const ratio = this.total > 0 ? Math.min(1, this.current / this.total) : 0.0;
    const done = this.total > 0 ? Math.floor(ratio * this.width) : 0;
    const fill = done > 0 ? '='.repeat(done) : '';
    const remain =
      this.total > 0 ? Math.max(0, this.width - done - 1) : this.width - 1;

    let bar;
    if (this.total > 0) {
      bar = `[${fill}${done === this.width ? '' : '>'}${' '.repeat(remain)}]`;
      const percent = `${Math.round(ratio * 100)
        .toString()
        .padStart(3, ' ')}%`;
      this._renderLine(
        `${this.label} ${bar} ${percent} (${this.current}/${this.total}) ${this._lastMessage}`,
      );
    } else {
      const spin = this._spinner[this._spinnerPos];
      this._spinnerPos = (this._spinnerPos + 1) & 3;
      bar = `[${spin}${' '.repeat(this.width - 1)}]`;
      this._renderLine(
        `${this.label} ${bar} (${this.current}) ${this._lastMessage}`,
      );
    }
  }

  _renderLine(text) {
    if (!this.enabled) return;
    process.stdout.write(
      `\r${text}${' '.repeat(Math.max(0, 120 - text.length))}`,
    );
  }
}

class SpzContentWorkerPool {
  constructor(workerCount, workerScriptPath = DEFAULT_WORKER_SCRIPT) {
    this.workers = [];
    this.workerCount = Math.max(1, Math.floor(workerCount));
    this.workerScriptPath = workerScriptPath;
    this.taskQueue = [];
    this.activeTasks = 0;
    this.closed = false;
    this.idleResolvers = [];
    this._lastErr = null;

    for (let i = 0; i < this.workerCount; i++) {
      const worker = new Worker(this.workerScriptPath);
      worker._busy = false;
      worker._job = null;
      worker.on('message', (msg) => this._handleMessage(worker, msg));
      worker.on('error', (err) => this._handleError(worker, err));
      worker.on('exit', (code) => {
        if (code !== 0 && worker._job) {
          this._handleError(
            worker,
            new Error(`Spz worker exited with code ${code}`),
          );
        }
        worker._busy = false;
        worker._job = null;
      });
      this.workers.push(worker);
    }
  }

  submit(task, transfer = []) {
    if (this.closed) {
      return Promise.reject(
        new ConversionError('SpzContentWorkerPool is already closed.'),
      );
    }
    if (this._lastErr) {
      return Promise.reject(this._lastErr);
    }
    return new Promise((resolve, reject) => {
      this.taskQueue.push({ task, transfer, resolve, reject });
      this._drain();
    });
  }

  _drain() {
    if (this.closed || this._lastErr) {
      return;
    }

    for (const worker of this.workers) {
      if (worker._busy) {
        continue;
      }
      const job = this.taskQueue.shift();
      if (!job) {
        break;
      }

      worker._busy = true;
      worker._job = job;
      this.activeTasks += 1;
      if (job.transfer && job.transfer.length > 0) {
        worker.postMessage(
          {
            type: 'worker-task',
            task: job.task,
          },
          job.transfer,
        );
      } else {
        worker.postMessage({
          type: 'worker-task',
          task: job.task,
        });
      }
    }

    if (this.taskQueue.length === 0 && this.activeTasks === 0) {
      this._notifyIdle();
    }
  }

  _handleMessage(worker, msg) {
    const job = worker._job;
    worker._job = null;
    worker._busy = false;
    this.activeTasks -= 1;
    if (!job) {
      this._drain();
      return;
    }

    if (msg && msg.error) {
      this._lastErr = new ConversionError(msg.error);
      job.reject(this._lastErr);
    } else {
      job.resolve(msg && msg.result ? msg.result : undefined);
    }

    this._drain();
  }

  _handleError(worker, err) {
    const job = worker._job;
    worker._job = null;
    worker._busy = false;
    this.activeTasks = Math.max(0, this.activeTasks - 1);

    if (!this._lastErr) {
      this._lastErr =
        err instanceof Error ? err : new ConversionError(String(err));
    }

    if (job) {
      job.reject(this._lastErr);
    }
    this._drain();
  }

  _notifyIdle() {
    while (this.idleResolvers.length > 0) {
      const resolve = this.idleResolvers.pop();
      resolve();
    }
  }

  waitForIdle() {
    if (
      this.taskQueue.length === 0 &&
      this.activeTasks === 0 &&
      !this._lastErr
    ) {
      return Promise.resolve();
    }
    return new Promise((resolve, reject) => {
      this.idleResolvers.push(() => {
        if (this._lastErr) {
          reject(this._lastErr);
        } else {
          resolve();
        }
      });
    });
  }

  async close() {
    this.closed = true;
    if (this._lastErr) {
      this._notifyIdle();
      throw this._lastErr;
    }
    await this.waitForIdle();
    await Promise.all(this.workers.map((worker) => worker.terminate()));
  }
}

function computeThreeSigmaExtents(scaleLog, quatsXYZW) {
  const n = scaleLog.length / 3;
  const out = new Float32Array(n * 3);
  for (let i = 0; i < n; i++) {
    const qi = i * 4;
    let x = quatsXYZW[qi + 0];
    let y = quatsXYZW[qi + 1];
    let z = quatsXYZW[qi + 2];
    let w = quatsXYZW[qi + 3];

    const len2 = x * x + y * y + z * z + w * w;
    if (len2 < 1e-20) {
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

    const xx = x * x;
    const yy = y * y;
    const zz = z * z;
    const xy = x * y;
    const xz = x * z;
    const yz = y * z;
    const wx = w * x;
    const wy = w * y;
    const wz = w * z;

    const s0 = Math.exp(scaleLog[i * 3 + 0]);
    const s1 = Math.exp(scaleLog[i * 3 + 1]);
    const s2 = Math.exp(scaleLog[i * 3 + 2]);
    const s2x = s0 * s0;
    const s2y = s1 * s1;
    const s2z = s2 * s2;

    const c00 =
      (1.0 - 2.0 * (yy + zz)) * (1.0 - 2.0 * (yy + zz)) * s2x +
      2.0 * (xy - wz) * (2.0 * (xy - wz)) * s2y +
      2.0 * (xz + wy) * (2.0 * (xz + wy)) * s2z;
    const c11 =
      2.0 * (xy + wz) * (2.0 * (xy + wz)) * s2x +
      (1.0 - 2.0 * (xx + zz)) * (1.0 - 2.0 * (xx + zz)) * s2y +
      2.0 * (yz - wx) * (2.0 * (yz - wx)) * s2z;
    const c22 =
      2.0 * (xz - wy) * (2.0 * (xz - wy)) * s2x +
      2.0 * (yz + wx) * (2.0 * (yz + wx)) * s2y +
      (1.0 - 2.0 * (xx + yy)) * (1.0 - 2.0 * (xx + yy)) * s2z;

    out[i * 3 + 0] = 3.0 * Math.sqrt(Math.max(c00, 1e-20));
    out[i * 3 + 1] = 3.0 * Math.sqrt(Math.max(c11, 1e-20));
    out[i * 3 + 2] = 3.0 * Math.sqrt(Math.max(c22, 1e-20));
  }
  return out;
}

function computeThreeSigmaAabbDiagonalRadius(scaleLog, quatsXYZW) {
  const n = scaleLog.length / 3;
  const out = new Float32Array(n);
  for (let i = 0; i < n; i++) {
    const qi = i * 4;
    let x = quatsXYZW[qi + 0];
    let y = quatsXYZW[qi + 1];
    let z = quatsXYZW[qi + 2];
    let w = quatsXYZW[qi + 3];

    const len2 = x * x + y * y + z * z + w * w;
    if (len2 < 1e-20) {
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

    const xx = x * x;
    const yy = y * y;
    const zz = z * z;
    const xy = x * y;
    const xz = x * z;
    const yz = y * z;
    const wx = w * x;
    const wy = w * y;
    const wz = w * z;

    const s0 = Math.exp(scaleLog[i * 3 + 0]);
    const s1 = Math.exp(scaleLog[i * 3 + 1]);
    const s2 = Math.exp(scaleLog[i * 3 + 2]);
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

    const c00 = r00 * r00 * s2x + r10 * r10 * s2y + r20 * r20 * s2z;
    const c11 = r01 * r01 * s2x + r11 * r11 * s2y + r21 * r21 * s2z;
    const c22 = r02 * r02 * s2x + r12 * r12 * s2y + r22 * r22 * s2z;

    const ex = 3.0 * Math.sqrt(Math.max(c00, 1e-20));
    const ey = 3.0 * Math.sqrt(Math.max(c11, 1e-20));
    const ez = 3.0 * Math.sqrt(Math.max(c22, 1e-20));
    out[i] = Math.sqrt(ex * ex + ey * ey + ez * ez);
  }
  return out;
}

function computeBounds(cloud) {
  const n = cloud.length;
  const ext = computeThreeSigmaExtents(cloud.scaleLog, cloud.quatsXYZW);
  const minimum = [Infinity, Infinity, Infinity];
  const maximum = [-Infinity, -Infinity, -Infinity];
  for (let i = 0; i < n; i++) {
    const pi = i * 3;
    const ei = i * 3;
    const p0 = cloud.positions[pi + 0];
    const p1 = cloud.positions[pi + 1];
    const p2 = cloud.positions[pi + 2];
    const ex = ext[ei + 0];
    const ey = ext[ei + 1];
    const ez = ext[ei + 2];
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
  return new Bounds(minimum, maximum);
}

function childBounds(parent, octant) {
  const c = parent.center();
  const mn = parent.minimum.slice();
  const mx = parent.maximum.slice();
  if (octant & 1) {
    mn[0] = c[0];
  } else {
    mx[0] = c[0];
  }
  if (octant & 2) {
    mn[1] = c[1];
  } else {
    mx[1] = c[1];
  }
  if (octant & 4) {
    mn[2] = c[2];
  } else {
    mx[2] = c[2];
  }
  return new Bounds(mn, mx);
}

function chooseGridDims(bounds, targetCount) {
  const ext = bounds.extents().map((v) => Math.max(v, 1e-6));
  const maxExt = Math.max(ext[0], ext[1], ext[2]);
  const ratios = ext.map((v) => v / maxExt);
  const base = Math.max(1.0, Math.pow(targetCount, 1.0 / 3.0));
  let dims = ratios.map((r) => Math.max(1, roundHalfToEven(base * r)));
  const prod = Math.max(1, dims[0] * dims[1] * dims[2]);
  const scale = Math.pow(targetCount / prod, 1.0 / 3.0);
  dims = dims.map((d) => Math.max(1, roundHalfToEven(d * scale)));
  return dims;
}

function groupMembersFromInverse(inverse, numGroups) {
  const groups = Array.from({ length: numGroups }, () => []);
  for (let i = 0; i < inverse.length; i++) {
    groups[inverse[i]].push(i);
  }
  return groups;
}

function percent95(arr) {
  if (!arr.length) {
    return 0.0;
  }
  const sorted = arr.slice();
  sorted.sort((a, b) => a - b);
  const pos = 0.95 * (sorted.length - 1);
  const lo = Math.floor(pos);
  const hi = Math.min(sorted.length - 1, lo + 1);
  const frac = pos - lo;
  return frac === 0 ? sorted[lo] : sorted[lo] * (1 - frac) + sorted[hi] * frac;
}

function estimateVoxelGroupingError(cloud, targetCount, bounds = null) {
  const n = cloud.length;
  const target = Math.max(1, Math.min(Math.floor(targetCount), n));
  if (n <= target) {
    return 0.0;
  }

  const activeBounds = bounds || computeBounds(cloud);
  let dims = chooseGridDims(activeBounds, target);
  const mins = activeBounds.minimum;
  const ext = activeBounds.extents().map((v) => Math.max(v, 1e-6));
  const pos = cloud.positions;
  const m0 = mins[0];
  const m1 = mins[1];
  const m2 = mins[2];
  const invExt0 = 1.0 / ext[0];
  const invExt1 = 1.0 / ext[1];
  const invExt2 = 1.0 / ext[2];
  const map = new Map();
  let groups = [];

  for (let iter = 0; iter < 24; iter++) {
    map.clear();
    const d0 = dims[0];
    const d1 = dims[1];
    const d2 = dims[2];
    const d0m1 = d0 - 1;
    const d1m1 = d1 - 1;
    const d2m1 = d2 - 1;

    for (let i = 0; i < n; i++) {
      const i3 = i * 3;
      const uvw0 = Math.max(0.0, Math.min(0.999999, (pos[i3] - m0) * invExt0));
      const uvw1 = Math.max(
        0.0,
        Math.min(0.999999, (pos[i3 + 1] - m1) * invExt1),
      );
      const uvw2 = Math.max(
        0.0,
        Math.min(0.999999, (pos[i3 + 2] - m2) * invExt2),
      );
      const iIdx = Math.min(d0m1, Math.floor(uvw0 * d0));
      const jIdx = Math.min(d1m1, Math.floor(uvw1 * d1));
      const kIdx = Math.min(d2m1, Math.floor(uvw2 * d2));
      const flat = iIdx + d0 * (jIdx + d1 * kIdx);
      let bucket = map.get(flat);
      if (!bucket) {
        bucket = [];
        map.set(flat, bucket);
      }
      bucket.push(i);
    }

    const keys = Array.from(map.keys()).sort((a, b) => a - b);
    groups = keys.map((k) => map.get(k));
    if (
      groups.length <= target ||
      (dims[0] === 1 && dims[1] === 1 && dims[2] === 1)
    ) {
      break;
    }

    dims = [
      Math.max(1, Math.floor(dims[0] * 0.85)),
      Math.max(1, Math.floor(dims[1] * 0.85)),
      Math.max(1, Math.floor(dims[2] * 0.85)),
    ];
  }

  const radii = computeThreeSigmaAabbDiagonalRadius(
    cloud.scaleLog,
    cloud.quatsXYZW,
  );
  const err = new Float32Array(n);

  for (const idxs of groups) {
    if (!idxs || idxs.length === 0) {
      continue;
    }

    let sumW = 0.0;
    let cx = 0.0;
    let cy = 0.0;
    let cz = 0.0;
    let fallbackX = 0.0;
    let fallbackY = 0.0;
    let fallbackZ = 0.0;
    let groupRadiusMax = 0.0;
    let groupRadiusWeighted = 0.0;
    let groupRadiusFallback = 0.0;

    for (let i = 0; i < idxs.length; i++) {
      const idx = idxs[i];
      const i3 = idx * 3;
      const weight = Math.max(cloud.opacity[idx], 1e-4);
      const radius = radii[idx];
      fallbackX += pos[i3 + 0];
      fallbackY += pos[i3 + 1];
      fallbackZ += pos[i3 + 2];
      sumW += weight;
      cx += pos[i3 + 0] * weight;
      cy += pos[i3 + 1] * weight;
      cz += pos[i3 + 2] * weight;
      groupRadiusWeighted += radius * weight;
      groupRadiusFallback += radius;
      if (radius > groupRadiusMax) {
        groupRadiusMax = radius;
      }
    }

    if (sumW > 1e-12 && Number.isFinite(sumW)) {
      cx /= sumW;
      cy /= sumW;
      cz /= sumW;
    } else {
      const invCount = 1.0 / idxs.length;
      cx = fallbackX * invCount;
      cy = fallbackY * invCount;
      cz = fallbackZ * invCount;
    }

    const groupRadiusMean =
      sumW > 1e-12 && Number.isFinite(sumW)
        ? groupRadiusWeighted / sumW
        : groupRadiusFallback / idxs.length;
    const groupRadius =
      groupRadiusMax * 0.75 + Math.max(0.0, groupRadiusMean) * 0.25;

    for (let i = 0; i < idxs.length; i++) {
      const idx = idxs[i];
      const i3 = idx * 3;
      const dx = pos[i3 + 0] - cx;
      const dy = pos[i3 + 1] - cy;
      const dz = pos[i3 + 2] - cz;
      err[idx] =
        Math.sqrt(dx * dx + dy * dy + dz * dz) + radii[idx] + groupRadius;
    }
  }

  return percent95(err);
}

function representativeIndexForGroup(
  cloud,
  idxs,
  weights,
  radii,
  voxelDiagSq,
  radiusBias,
  excludeA = -1,
  excludeB = -1,
) {
  if (!idxs || idxs.length === 0) {
    return -1;
  }

  const pos = cloud.positions;
  let sumW = 0.0;
  let cx = 0.0;
  let cy = 0.0;
  let cz = 0.0;
  let fallback = -1;
  let fallbackWeight = -Infinity;
  let fallbackRadius = radiusBias >= 0.0 ? Infinity : -Infinity;
  let validCount = 0;

  for (let i = 0; i < idxs.length; i++) {
    const idx = idxs[i];
    if (idx === excludeA || idx === excludeB) {
      continue;
    }
    validCount += 1;
    const w = Math.max(weights[idx], 1e-12);
    const i3 = idx * 3;
    sumW += w;
    cx += pos[i3 + 0] * w;
    cy += pos[i3 + 1] * w;
    cz += pos[i3 + 2] * w;
    if (
      fallback < 0 ||
      w > fallbackWeight + 1e-12 ||
      (Math.abs(w - fallbackWeight) <= 1e-12 &&
        ((radiusBias >= 0.0 && radii[idx] < fallbackRadius) ||
          (radiusBias < 0.0 && radii[idx] > fallbackRadius)))
    ) {
      fallback = idx;
      fallbackWeight = w;
      fallbackRadius = radii[idx];
    }
  }

  if (validCount <= 0 || fallback < 0) {
    return -1;
  }
  if (validCount === 1) {
    return fallback;
  }
  if (!Number.isFinite(sumW) || sumW <= 0.0) {
    return fallback;
  }

  cx /= sumW;
  cy /= sumW;
  cz /= sumW;

  let rep = fallback;
  let bestCost = Infinity;
  let bestWeight = fallbackWeight;
  let bestRadius = fallbackRadius;
  const invVoxelDiagSq = 1.0 / Math.max(voxelDiagSq, 1e-12);
  for (let i = 0; i < idxs.length; i++) {
    const idx = idxs[i];
    if (idx === excludeA || idx === excludeB) {
      continue;
    }
    const i3 = idx * 3;
    const dx = pos[i3 + 0] - cx;
    const dy = pos[i3 + 1] - cy;
    const dz = pos[i3 + 2] - cz;
    const dist2 = dx * dx + dy * dy + dz * dz;
    const radius = radii[idx];
    const w = weights[idx];
    const cost =
      dist2 * invVoxelDiagSq + radiusBias * radius * radius * invVoxelDiagSq;
    if (
      cost < bestCost - 1e-12 ||
      (Math.abs(cost - bestCost) <= 1e-12 &&
        (w > bestWeight + 1e-12 ||
          (Math.abs(w - bestWeight) <= 1e-12 &&
            ((radiusBias >= 0.0 && radius < bestRadius) ||
              (radiusBias < 0.0 && radius > bestRadius)))))
    ) {
      rep = idx;
      bestCost = cost;
      bestWeight = w;
      bestRadius = radius;
    }
  }
  return rep;
}

function simplifyCloudVoxel(cloud, targetCount, bounds = null) {
  const n = cloud.length;
  const target = Math.max(1, Math.min(Math.floor(targetCount), n));
  if (n <= target) {
    const assign = new Int32Array(n);
    for (let i = 0; i < n; i++) {
      assign[i] = i;
    }
    return [cloud, assign, 0.0];
  }

  const activeBounds = bounds || computeBounds(cloud);
  let dims = chooseGridDims(activeBounds, target);
  const mins = activeBounds.minimum;
  const ext = activeBounds.extents().map((v) => Math.max(v, 1e-6));

  let inverse = new Int32Array(n);
  let uniqueSize = 0;
  let groups = null;
  const pos = cloud.positions;
  const m0 = mins[0];
  const m1 = mins[1];
  const m2 = mins[2];
  const invExt0 = 1.0 / ext[0];
  const invExt1 = 1.0 / ext[1];
  const invExt2 = 1.0 / ext[2];
  const map = new Map();
  for (let iter = 0; iter < 24; iter++) {
    map.clear();
    const d0 = dims[0];
    const d1 = dims[1];
    const d2 = dims[2];
    const d0m1 = d0 - 1;
    const d1m1 = d1 - 1;
    const d2m1 = d2 - 1;
    for (let i = 0; i < n; i++) {
      const i3 = i * 3;
      const uvw0 = Math.max(0.0, Math.min(0.999999, (pos[i3] - m0) * invExt0));
      const uvw1 = Math.max(
        0.0,
        Math.min(0.999999, (pos[i3 + 1] - m1) * invExt1),
      );
      const uvw2 = Math.max(
        0.0,
        Math.min(0.999999, (pos[i3 + 2] - m2) * invExt2),
      );

      const iIdx = Math.min(d0m1, Math.floor(uvw0 * d0));
      const jIdx = Math.min(d1m1, Math.floor(uvw1 * d1));
      const kIdx = Math.min(d2m1, Math.floor(uvw2 * d2));
      const flat = iIdx + d0 * (jIdx + d1 * kIdx);
      let bucket = map.get(flat);
      if (!bucket) {
        bucket = [];
        map.set(flat, bucket);
      }
      bucket.push(i);
    }

    const keys = Array.from(map.keys()).sort((a, b) => a - b);
    groups = keys.map((k) => map.get(k));
    uniqueSize = groups.length;
    for (let g = 0; g < groups.length; g++) {
      for (const idx of groups[g]) {
        inverse[idx] = g;
      }
    }
    if (
      uniqueSize <= target ||
      (dims[0] === 1 && dims[1] === 1 && dims[2] === 1)
    ) {
      break;
    }
    dims = [
      Math.max(1, Math.floor(dims[0] * 0.85)),
      Math.max(1, Math.floor(dims[1] * 0.85)),
      Math.max(1, Math.floor(dims[2] * 0.85)),
    ];
  }

  const selectedGroups = groups || groupMembersFromInverse(inverse, uniqueSize);
  const origRadius = computeThreeSigmaAabbDiagonalRadius(
    cloud.scaleLog,
    cloud.quatsXYZW,
  );
  const voxelSize0 = ext[0] / Math.max(1, dims[0]);
  const voxelSize1 = ext[1] / Math.max(1, dims[1]);
  const voxelSize2 = ext[2] / Math.max(1, dims[2]);
  const voxelDiagSq =
    voxelSize0 * voxelSize0 + voxelSize1 * voxelSize1 + voxelSize2 * voxelSize2;
  const voxelDiag = Math.max(Math.sqrt(voxelDiagSq), 1e-6);
  const detailWeights = new Float64Array(n);
  const coarseWeights = new Float64Array(n);
  for (let i = 0; i < n; i++) {
    const opacity = Math.max(cloud.opacity[i], 1e-4);
    const radiusNorm = Math.max(origRadius[i] / voxelDiag, 0.35);
    detailWeights[i] = opacity / Math.sqrt(radiusNorm);
    coarseWeights[i] = opacity * Math.sqrt(radiusNorm);
  }

  const selected = [];
  const selectedSlotByOrig = new Int32Array(n);
  selectedSlotByOrig.fill(-1);
  const assignment = new Int32Array(n);
  const taken = new Uint8Array(n);
  const secondaryCandidates = [];
  const tertiaryDetailCandidates = [];
  const tertiaryCoarseCandidates = [];
  const posAll = cloud.positions;
  let coarseSelectedCount = 0;
  let detailSelectedCount = 0;

  for (const idxs of selectedGroups) {
    if (idxs.length === 0) {
      continue;
    }
    const coarseRep = representativeIndexForGroup(
      cloud,
      idxs,
      coarseWeights,
      origRadius,
      voxelDiagSq,
      -0.15,
    );
    const coarseOutIdx = selected.length;
    selected.push(coarseRep);
    selectedSlotByOrig[coarseRep] = coarseOutIdx;
    taken[coarseRep] = 1;
    coarseSelectedCount += 1;

    const detailRep = representativeIndexForGroup(
      cloud,
      idxs,
      detailWeights,
      origRadius,
      voxelDiagSq,
      0.35,
    );
    if (detailRep >= 0 && detailRep !== coarseRep) {
      const c3 = coarseRep * 3;
      const d3 = detailRep * 3;
      const dx = posAll[d3 + 0] - posAll[c3 + 0];
      const dy = posAll[d3 + 1] - posAll[c3 + 1];
      const dz = posAll[d3 + 2] - posAll[c3 + 2];
      const sepNorm = Math.sqrt(dx * dx + dy * dy + dz * dz) / voxelDiag;
      const radiusRatio =
        Math.max(origRadius[coarseRep], 1e-6) /
        Math.max(origRadius[detailRep], 1e-6);
      const priority =
        detailWeights[detailRep] *
        (1.0 + sepNorm) *
        (1.0 + Math.max(0.0, Math.log2(Math.max(radiusRatio, 1.0))));
      secondaryCandidates.push({
        rep: detailRep,
        priority,
      });
    }

    const extraCoarseRep = representativeIndexForGroup(
      cloud,
      idxs,
      coarseWeights,
      origRadius,
      voxelDiagSq,
      -0.15,
      coarseRep,
      detailRep,
    );
    if (extraCoarseRep >= 0) {
      tertiaryCoarseCandidates.push({
        rep: extraCoarseRep,
        priority: coarseWeights[extraCoarseRep],
      });
    }

    const extraDetailRep = representativeIndexForGroup(
      cloud,
      idxs,
      detailWeights,
      origRadius,
      voxelDiagSq,
      0.35,
      coarseRep,
      detailRep,
    );
    if (extraDetailRep >= 0) {
      tertiaryDetailCandidates.push({
        rep: extraDetailRep,
        priority: detailWeights[extraDetailRep],
      });
    }
  }

  if (selected.length < target && secondaryCandidates.length > 0) {
    secondaryCandidates.sort((a, b) => {
      if (b.priority !== a.priority) {
        return b.priority - a.priority;
      }
      return a.rep - b.rep;
    });
    for (
      let i = 0;
      i < secondaryCandidates.length && selected.length < target;
      i++
    ) {
      const rep = secondaryCandidates[i].rep;
      if (taken[rep]) {
        continue;
      }
      const outIdx = selected.length;
      selected.push(rep);
      selectedSlotByOrig[rep] = outIdx;
      taken[rep] = 1;
      detailSelectedCount += 1;
    }
  }

  if (selected.length < target) {
    tertiaryDetailCandidates.sort((a, b) => {
      if (b.priority !== a.priority) {
        return b.priority - a.priority;
      }
      return a.rep - b.rep;
    });
    tertiaryCoarseCandidates.sort((a, b) => {
      if (b.priority !== a.priority) {
        return b.priority - a.priority;
      }
      return a.rep - b.rep;
    });

    let detailCursor = 0;
    let coarseCursor = 0;
    while (
      selected.length < target &&
      (detailCursor < tertiaryDetailCandidates.length ||
        coarseCursor < tertiaryCoarseCandidates.length)
    ) {
      const preferDetail = detailSelectedCount <= coarseSelectedCount;
      let picked = false;

      if (preferDetail) {
        while (detailCursor < tertiaryDetailCandidates.length) {
          const rep = tertiaryDetailCandidates[detailCursor++].rep;
          if (taken[rep]) {
            continue;
          }
          const outIdx = selected.length;
          selected.push(rep);
          selectedSlotByOrig[rep] = outIdx;
          taken[rep] = 1;
          detailSelectedCount += 1;
          picked = true;
          break;
        }
      } else {
        while (coarseCursor < tertiaryCoarseCandidates.length) {
          const rep = tertiaryCoarseCandidates[coarseCursor++].rep;
          if (taken[rep]) {
            continue;
          }
          const outIdx = selected.length;
          selected.push(rep);
          selectedSlotByOrig[rep] = outIdx;
          taken[rep] = 1;
          coarseSelectedCount += 1;
          picked = true;
          break;
        }
      }

      if (picked) {
        continue;
      }

      while (detailCursor < tertiaryDetailCandidates.length) {
        const rep = tertiaryDetailCandidates[detailCursor++].rep;
        if (taken[rep]) {
          continue;
        }
        const outIdx = selected.length;
        selected.push(rep);
        selectedSlotByOrig[rep] = outIdx;
        taken[rep] = 1;
        detailSelectedCount += 1;
        picked = true;
        break;
      }
      if (picked || selected.length >= target) {
        continue;
      }

      while (coarseCursor < tertiaryCoarseCandidates.length) {
        const rep = tertiaryCoarseCandidates[coarseCursor++].rep;
        if (taken[rep]) {
          continue;
        }
        const outIdx = selected.length;
        selected.push(rep);
        selectedSlotByOrig[rep] = outIdx;
        taken[rep] = 1;
        coarseSelectedCount += 1;
        picked = true;
        break;
      }
      if (!picked) {
        break;
      }
    }
  }

  if (selected.length < target) {
    const remain = [];
    for (let i = 0; i < n; i++) {
      if (!taken[i]) {
        remain.push(i);
      }
    }
    const remainDetail = remain.slice().sort((a, b) => {
      const w = detailWeights[b] - detailWeights[a];
      if (w !== 0) return w;
      const r = origRadius[a] - origRadius[b];
      return r !== 0 ? r : b - a;
    });
    const remainCoarse = remain.slice().sort((a, b) => {
      const w = coarseWeights[b] - coarseWeights[a];
      if (w !== 0) return w;
      const r = origRadius[b] - origRadius[a];
      return r !== 0 ? r : b - a;
    });

    let detailCursor = 0;
    let coarseCursor = 0;
    while (
      selected.length < target &&
      (detailCursor < remainDetail.length || coarseCursor < remainCoarse.length)
    ) {
      const preferDetail = detailSelectedCount <= coarseSelectedCount;
      let picked = false;

      if (preferDetail) {
        while (detailCursor < remainDetail.length) {
          const rep = remainDetail[detailCursor++];
          if (taken[rep]) {
            continue;
          }
          const outIdx = selected.length;
          selected.push(rep);
          selectedSlotByOrig[rep] = outIdx;
          taken[rep] = 1;
          detailSelectedCount += 1;
          picked = true;
          break;
        }
      } else {
        while (coarseCursor < remainCoarse.length) {
          const rep = remainCoarse[coarseCursor++];
          if (taken[rep]) {
            continue;
          }
          const outIdx = selected.length;
          selected.push(rep);
          selectedSlotByOrig[rep] = outIdx;
          taken[rep] = 1;
          coarseSelectedCount += 1;
          picked = true;
          break;
        }
      }

      if (picked) {
        continue;
      }

      while (detailCursor < remainDetail.length) {
        const rep = remainDetail[detailCursor++];
        if (taken[rep]) {
          continue;
        }
        const outIdx = selected.length;
        selected.push(rep);
        selectedSlotByOrig[rep] = outIdx;
        taken[rep] = 1;
        detailSelectedCount += 1;
        picked = true;
        break;
      }
      if (picked || selected.length >= target) {
        continue;
      }

      while (coarseCursor < remainCoarse.length) {
        const rep = remainCoarse[coarseCursor++];
        if (taken[rep]) {
          continue;
        }
        const outIdx = selected.length;
        selected.push(rep);
        selectedSlotByOrig[rep] = outIdx;
        taken[rep] = 1;
        coarseSelectedCount += 1;
        picked = true;
        break;
      }
      if (!picked) {
        break;
      }
    }
  }

  const selectedCloud = cloud.subset(selected, false);

  const keptRadius = new Float32Array(selected.length);
  for (let i = 0; i < selected.length; i++) {
    keptRadius[i] = origRadius[selected[i]];
  }

  for (const idxs of selectedGroups) {
    if (idxs.length === 0) {
      continue;
    }
    const reps = [];
    for (let i = 0; i < idxs.length; i++) {
      const slot = selectedSlotByOrig[idxs[i]];
      if (slot >= 0) {
        reps.push(slot);
      }
    }
    if (reps.length === 0) {
      continue;
    }
    if (reps.length === 1) {
      for (let i = 0; i < idxs.length; i++) {
        assignment[idxs[i]] = reps[0];
      }
      continue;
    }
    for (let i = 0; i < idxs.length; i++) {
      const orig = idxs[i];
      const p3 = orig * 3;
      let bestSlot = reps[0];
      let bestScore = Infinity;
      for (let r = 0; r < reps.length; r++) {
        const slot = reps[r];
        const s3 = slot * 3;
        const dx = posAll[p3 + 0] - selectedCloud.positions[s3 + 0];
        const dy = posAll[p3 + 1] - selectedCloud.positions[s3 + 1];
        const dz = posAll[p3 + 2] - selectedCloud.positions[s3 + 2];
        const score = Math.sqrt(dx * dx + dy * dy + dz * dz) + keptRadius[slot];
        if (score < bestScore) {
          bestScore = score;
          bestSlot = slot;
        }
      }
      assignment[orig] = bestSlot;
    }
  }

  const err = new Float32Array(n);
  for (let i = 0; i < n; i++) {
    const r = assignment[i];
    const px = cloud.positions[i * 3 + 0];
    const py = cloud.positions[i * 3 + 1];
    const pz = cloud.positions[i * 3 + 2];
    const sx = selectedCloud.positions[r * 3 + 0];
    const sy = selectedCloud.positions[r * 3 + 1];
    const sz = selectedCloud.positions[r * 3 + 2];
    const dx = px - sx;
    const dy = py - sy;
    const dz = pz - sz;
    const centerErr = Math.sqrt(dx * dx + dy * dy + dz * dz);
    err[i] = centerErr + origRadius[i] + keptRadius[r];
  }

  return [selectedCloud, assignment, percent95(err)];
}

function samplingDivisorForDepth(depth, maxDepth, samplingRatePerLevel) {
  const stepsFromFinest = Math.max(0, Math.floor(maxDepth) - Math.floor(depth));
  return (1.0 / samplingRatePerLevel) ** stepsFromFinest;
}

function lodMaxDepthForParams(params) {
  return params.lodMaxDepth != null ? params.lodMaxDepth : params.maxDepth;
}

function geometricErrorScaleForDepth(depth, maxDepth, samplingRatePerLevel) {
  const rootDivisor = samplingDivisorForDepth(
    0,
    maxDepth,
    samplingRatePerLevel,
  );
  const d = samplingDivisorForDepth(depth, maxDepth, samplingRatePerLevel);
  return d / rootDivisor;
}

function padLength(length, alignment) {
  const rem = length % alignment;
  return rem === 0 ? 0 : alignment - rem;
}

function targetSplatCountForParams(params, depth, splatCount) {
  const lodMaxDepth = lodMaxDepthForParams(params);
  const divisor = samplingDivisorForDepth(
    depth,
    lodMaxDepth,
    params.samplingRatePerLevel,
  );
  return Math.max(1, Math.min(splatCount, Math.ceil(splatCount / divisor)));
}

function constrainTargetSplatCount(
  targetCount,
  splatCount,
  occupiedChildCount = 0,
) {
  const minCoverage =
    occupiedChildCount > 1
      ? Math.max(1, Math.min(splatCount, occupiedChildCount))
      : 1;
  return Math.max(minCoverage, Math.min(splatCount, Math.floor(targetCount)));
}

function geometricErrorForParams(params, depth) {
  const lodMaxDepth = lodMaxDepthForParams(params);
  return (
    params.rootGeometricError *
    geometricErrorScaleForDepth(
      depth,
      lodMaxDepth,
      params.samplingRatePerLevel,
    )
  );
}

function rootGeometricErrorFromMinLevel(
  minGeometricError,
  maxDepth,
  samplingRatePerLevel,
) {
  const finestScale = geometricErrorScaleForDepth(
    maxDepth,
    maxDepth,
    samplingRatePerLevel,
  );
  if (!Number.isFinite(finestScale) || finestScale <= 0.0) {
    throw new ConversionError(
      `Invalid geometric error scale at maxDepth=${maxDepth}.`,
    );
  }
  return minGeometricError / finestScale;
}

function splitCloudOctants(cloud, cellBounds) {
  const c = cellBounds.center();
  const n = cloud.length;
  const cx = c[0];
  const cy = c[1];
  const cz = c[2];
  const pos = cloud.positions;
  const counts = new Uint32Array(8);
  for (let i = 0; i < n; i++) {
    const oct =
      (pos[i * 3] >= cx ? 1 : 0) |
      (pos[i * 3 + 1] >= cy ? 2 : 0) |
      (pos[i * 3 + 2] >= cz ? 4 : 0);
    counts[oct]++;
  }
  const out = {};
  const offsets = new Uint32Array(8);
  for (let oct = 0; oct < 8; oct++) {
    if (counts[oct] > 0) out[oct] = new Uint32Array(counts[oct]);
  }
  for (let i = 0; i < n; i++) {
    const oct =
      (pos[i * 3] >= cx ? 1 : 0) |
      (pos[i * 3 + 1] >= cy ? 2 : 0) |
      (pos[i * 3 + 2] >= cz ? 4 : 0);
    out[oct][offsets[oct]++] = i;
  }
  return out;
}

function writeContentFile(params, cloud, level, x, y, z) {
  const relPath = `tiles/${level}/${x}/${y}/${z}.glb`;
  const outPath = path.join(params.outDir, relPath);
  const translation = computeBounds(cloud).center();
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
    params.sourceUpAxis,
  );
  return relPath;
}

function buildSubtreeNodeLocal(
  params,
  cloud,
  cellBounds,
  depth,
  level,
  x,
  y,
  z,
) {
  const isLeafDepth = depth >= params.maxDepth;
  const childGroups = isLeafDepth ? {} : splitCloudOctants(cloud, cellBounds);
  const childKeys = Object.keys(childGroups);
  const isLeaf = isLeafDepth || cloud.length <= params.leafLimit;

  if (isLeaf) {
    const uri = writeContentFile(params, cloud, level, x, y, z);
    return new TileNode(
      level,
      x,
      y,
      z,
      cellBounds,
      geometricErrorForParams(params, depth),
      uri,
      [],
    );
  }

  const targetCount = constrainTargetSplatCount(
    targetSplatCountForParams(params, depth, cloud.length),
    cloud.length,
    childKeys.length,
  );
  const [lodCloud] = simplifyCloudVoxel(cloud, targetCount, cellBounds);
  const uri = writeContentFile(params, lodCloud, level, x, y, z);
  const children = [];
  for (let oct = 0; oct < 8; oct++) {
    const idx = childGroups[oct];
    if (!idx) continue;
    const childCloud = cloud.subset(idx, false);
    const cb = childBounds(cellBounds, oct);
    children.push(
      buildSubtreeNodeLocal(
        params,
        childCloud,
        cb,
        depth + 1,
        level + 1,
        (x << 1) | (oct & 1),
        (y << 1) | ((oct >> 1) & 1),
        (z << 1) | ((oct >> 2) & 1),
      ),
    );
  }

  return new TileNode(
    level,
    x,
    y,
    z,
    cellBounds,
    geometricErrorForParams(params, depth),
    uri,
    children,
  );
}

class OctreeTilesBuilder {
  constructor(params) {
    this.cloud = params.cloud;
    this.outDir = params.outDir;
    this.tilesDir = path.join(this.outDir, 'tiles');
    this.subtreesDir = path.join(this.outDir, 'subtrees');
    this.colorSpace = params.colorSpace;
    this.maxDepth = params.maxDepth;
    this.leafLimit = params.leafLimit;
    this.tilingMode = params.tilingMode;
    this.subtreeLevels = params.subtreeLevels;
    this.spzSh1Bits = params.spzSh1Bits;
    this.spzShRestBits = params.spzShRestBits;
    this.sourceUpAxis = params.sourceUpAxis;
    this.samplingRatePerLevel = params.samplingRatePerLevel;
    this.minGeometricError = params.minGeometricError;
    this.contentWorkers = params.contentWorkers || 0;
    this.workerScriptPath = params.workerScriptPath || DEFAULT_WORKER_SCRIPT;
    this.contentWorkerPool =
      this.contentWorkers > 0
        ? new SpzContentWorkerPool(this.contentWorkers, this.workerScriptPath)
        : null;
    this.contentWritePromises = [];
    this.nodes = new Map();
    this.maxNodeLevel = 0;
    this.rootCellBounds = computeBounds(this.cloud);
    this.lodMaxDepth = this.maxDepth;
    this.rootGeometricErrorSource = 'estimated_root';
    this.rootGeometricError = this.resolveRootGeometricError();
    this.buildPlan = null;
    this.scanWorkDone = 0;
    this.nodeBuildCount = 0;
    this.contentWriteCount = 0;
    this.subtreeWriteCount = 0;
    this.scanProgress = new ConsoleProgressBar('scan', this.scanWorkTotal());
    this.overallProgress = new ConsoleProgressBar('overall', 1);
    this._sharedNodeTableSeq = 0;
  }

  contentRelPath(level, x, y, z) {
    return `tiles/${level}/${x}/${y}/${z}.glb`;
  }

  scanWorkTotal() {
    return Math.max(1, this.cloud.length * Math.max(1, this.maxDepth + 1));
  }

  updateScanProgress(cloudLength, depth, level) {
    this.scanWorkDone = Math.min(
      this.scanWorkTotal(),
      this.scanWorkDone + Math.max(1, Math.floor(cloudLength)),
    );
    this.scanProgress.update(
      this.scanWorkDone,
      `depth=${depth} splats=${cloudLength} cellLevel=${level}`,
    );
  }

  overallCompletedCount() {
    return (
      this.nodeBuildCount + this.contentWriteCount + this.subtreeWriteCount
    );
  }

  overallTotalCount() {
    return this.buildPlan ? this.buildPlan.overallCount : 1;
  }

  progressSummary() {
    if (!this.buildPlan) {
      return [];
    }
    const parts = [
      `nodes=${this.nodeBuildCount}/${this.buildPlan.nodeCount}`,
      `content=${this.contentWriteCount}/${this.buildPlan.contentCount}`,
    ];
    if (this.buildPlan.subtreeCount > 0) {
      parts.push(
        `subtrees=${this.subtreeWriteCount}/${this.buildPlan.subtreeCount}`,
      );
    }
    return parts;
  }

  refreshOverallProgress(message = '') {
    const parts = [];
    if (message) {
      parts.push(message);
    }
    parts.push(...this.progressSummary());
    this.overallProgress.update(
      Math.min(this.overallCompletedCount(), this.overallTotalCount()),
      parts.join(' | '),
    );
  }

  finalizeOverallProgress(message = 'finalizing') {
    const total = this.overallTotalCount();
    this.overallProgress.setTotal(total);
    this.overallProgress.update(
      Math.min(this.overallCompletedCount(), total),
      [message, ...this.progressSummary()].filter(Boolean).join(' | '),
    );
  }

  log(msg) {
    console.log(msg);
  }

  targetSplatCountForDepth(depth, splatCount) {
    return targetSplatCountForParams(this, depth, splatCount);
  }

  geometricErrorForDepth(depth) {
    return geometricErrorForParams(this, depth);
  }

  syncLodMaxDepthToBuildPlan() {
    const planMaxLevel =
      this.buildPlan && Number.isFinite(this.buildPlan.maxLevel)
        ? this.buildPlan.maxLevel
        : this.maxDepth;
    this.lodMaxDepth = Math.max(0, Math.min(this.maxDepth, planMaxLevel));
    this.rootGeometricError = this.resolveRootGeometricError();
  }

  resolveRootGeometricError() {
    if (this.minGeometricError != null && this.minGeometricError > 0.0) {
      this.rootGeometricErrorSource = 'configured_min_geometric_error';
      return rootGeometricErrorFromMinLevel(
        this.minGeometricError,
        this.lodMaxDepth,
        this.samplingRatePerLevel,
      );
    }
    this.rootGeometricErrorSource = 'estimated_root_voxel_groups';
    return this.estimateRootGeometricError();
  }

  estimateRootGeometricError() {
    const ex = this.rootCellBounds.extents();
    const diag = Math.sqrt(ex[0] * ex[0] + ex[1] * ex[1] + ex[2] * ex[2]);
    if (this.cloud.length <= 1) {
      return Math.max(diag * 1e-6, 1e-6);
    }
    const target = this.targetSplatCountForDepth(0, this.cloud.length);
    if (target >= this.cloud.length) {
      return Math.max(diag * 0.125, diag * 1e-6, 1e-6);
    }
    const ownError = estimateVoxelGroupingError(
      this.cloud,
      target,
      this.rootCellBounds,
    );
    if (!Number.isFinite(ownError) || ownError <= 0.0) {
      return Math.max(diag * 0.125, 1e-6);
    }
    return Math.max(ownError, diag * 1e-6, 1e-6);
  }

  splitOctants(cloud, cellBounds) {
    return splitCloudOctants(cloud, cellBounds);
  }

  analyzeNode(cloud, cellBounds, depth) {
    const isLeafDepth = depth >= this.maxDepth;
    const childGroups = isLeafDepth ? {} : this.splitOctants(cloud, cellBounds);
    const childKeys = Object.keys(childGroups);
    return {
      childGroups,
      childKeys,
      isLeaf: isLeafDepth || cloud.length <= this.leafLimit,
    };
  }

  scanNode(cloud, cellBounds, depth, level, levelCounts) {
    this.updateScanProgress(cloud.length, depth, level);
    levelCounts[level] = (levelCounts[level] || 0) + 1;

    const { childGroups, isLeaf } = this.analyzeNode(cloud, cellBounds, depth);
    let nodeCount = 1;
    let maxLevel = level;

    if (isLeaf) {
      return { nodeCount, maxLevel };
    }

    for (let oct = 0; oct < 8; oct++) {
      const idx = childGroups[oct];
      if (!idx) {
        continue;
      }
      const childResult = this.scanNode(
        cloud.subset(idx, false),
        childBounds(cellBounds, oct),
        depth + 1,
        level + 1,
        levelCounts,
      );
      nodeCount += childResult.nodeCount;
      if (childResult.maxLevel > maxLevel) {
        maxLevel = childResult.maxLevel;
      }
    }

    return { nodeCount, maxLevel };
  }

  makeBuildPlan() {
    const levelCounts = [];
    const scanResult = this.scanNode(
      this.cloud,
      this.rootCellBounds,
      0,
      0,
      levelCounts,
    );
    const availableLevels = scanResult.maxLevel + 1;
    const effectiveSubtreeLevels =
      this.tilingMode === 'implicit'
        ? Math.max(
            1,
            Math.min(this.subtreeLevels, Math.max(1, availableLevels)),
          )
        : 0;
    let subtreeCount = 0;
    if (this.tilingMode === 'implicit') {
      for (let level = 0; level < levelCounts.length; level++) {
        if (
          (levelCounts[level] || 0) > 0 &&
          level % effectiveSubtreeLevels === 0
        ) {
          subtreeCount += levelCounts[level];
        }
      }
    }
    return {
      nodeCount: scanResult.nodeCount,
      contentCount: scanResult.nodeCount,
      subtreeCount,
      maxLevel: scanResult.maxLevel,
      availableLevels,
      effectiveSubtreeLevels,
      overallCount: scanResult.nodeCount * 2 + subtreeCount,
    };
  }

  markNodeCompleted(node, message = '') {
    this.nodeBuildCount += 1;
    this.refreshOverallProgress(
      message || `built cell=(${node.level},${node.x},${node.y},${node.z})`,
    );
  }

  markContentCompleted(relPath, splatCount, level = null) {
    this.contentWriteCount += 1;
    const parts = [];
    if (level != null) {
      parts.push(`depth=${level}`);
    }
    parts.push(`splats=${splatCount}`);
    parts.push(`uri=${relPath}`);
    this.refreshOverallProgress(`content ${parts.join(' ')}`);
  }

  markSubtreeCompleted(level, x, y, z) {
    this.subtreeWriteCount += 1;
    this.refreshOverallProgress(`subtree=(${level},${x},${y},${z})`);
  }

  writeContent(cloud, level, x, y, z) {
    const relPath = this.contentRelPath(level, x, y, z);
    const outPath = path.join(this.outDir, relPath);
    const translation = computeBounds(cloud).center();
    const splatCount = cloud.length;
    if (this.contentWorkerPool && cloud.length >= SPZ_ASYNC_WRITE_THRESHOLD) {
      const task = {
        kind: 'pack-spz',
        outPath,
        sh1Bits: this.spzSh1Bits,
        shRestBits: this.spzShRestBits,
        colorSpace: this.colorSpace,
        sourceUpAxis: this.sourceUpAxis,
        translation,
        cloud: serializeCloudForWorkerTask(cloud),
      };
      const transfer = transferListForCloud(cloud);
      return this.submitContentWriteTask(
        task,
        transfer,
        relPath,
        splatCount,
        level,
      );
    }
    this.refreshOverallProgress(`writing depth=${level} splats=${splatCount}`);
    const spzBytes = packCloudToSpz(
      cloud,
      this.spzSh1Bits,
      this.spzShRestBits,
      translation,
    );

    const builder = new GltfBuilder();
    builder.writeSpzStreamGlb(
      outPath,
      spzBytes,
      cloud,
      this.colorSpace,
      translation,
      this.sourceUpAxis,
    );

    this.markContentCompleted(relPath, splatCount, level);
    return relPath;
  }

  shouldBuildChildrenInWorkers(depth, cloudLength, childCount) {
    return (
      !!this.contentWorkerPool &&
      this.contentWorkerPool.workerCount > 1 &&
      depth <= SUBTREE_BUILD_ASYNC_MAX_DEPTH &&
      cloudLength >= SUBTREE_BUILD_ASYNC_THRESHOLD &&
      childCount > 1
    );
  }

  registerImportedSubtree(node) {
    let nodeCount = 0;
    const visit = (current) => {
      nodeCount += 1;
      this.nodes.set(current.key(), current);
      if (current.level > this.maxNodeLevel) {
        this.maxNodeLevel = current.level;
      }
      for (let i = 0; i < current.children.length; i++) {
        visit(current.children[i]);
      }
    };
    visit(node);
    this.nodeBuildCount += nodeCount;
    this.contentWriteCount += nodeCount;
    this.refreshOverallProgress(
      `merged subtree cell=(${node.level},${node.x},${node.y},${node.z}) nodes=${nodeCount}`,
    );
    return node;
  }

  buildSubtreeInWorker(cloud, cellBounds, depth, level, x, y, z) {
    const task = {
      kind: 'build-subtree',
      outDir: this.outDir,
      colorSpace: this.colorSpace,
      maxDepth: this.maxDepth,
      lodMaxDepth: this.lodMaxDepth,
      leafLimit: this.leafLimit,
      spzSh1Bits: this.spzSh1Bits,
      spzShRestBits: this.spzShRestBits,
      sourceUpAxis: this.sourceUpAxis,
      samplingRatePerLevel: this.samplingRatePerLevel,
      rootGeometricError: this.rootGeometricError,
      depth,
      level,
      x,
      y,
      z,
      cellBounds: serializeBounds(cellBounds),
      cloud: serializeCloudForWorkerTask(cloud),
    };
    return this.contentWorkerPool
      .submit(task, transferListForCloud(cloud))
      .then((result) => {
        if (!result || !result.root) {
          throw new ConversionError('Missing subtree build result.');
        }
        return this.registerImportedSubtree(deserializeTileNode(result.root));
      })
      .catch((err) => {
        if (err instanceof ConversionError) {
          throw err;
        }
        throw new ConversionError('Failed to build subtree in worker thread.');
      });
  }

  async close() {
    if (this.contentWorkerPool) {
      await this.contentWorkerPool.close();
      this.contentWorkerPool = null;
    }
  }

  async buildNode(cloud, cellBounds, depth, level, x, y, z) {
    this.refreshOverallProgress(
      `building depth=${depth} splats=${cloud.length} cell=(${level},${x},${y},${z})`,
    );

    const { childGroups, childKeys, isLeaf } = this.analyzeNode(
      cloud,
      cellBounds,
      depth,
    );

    if (isLeaf) {
      const uri = this.writeContent(cloud, level, x, y, z);
      const node = new TileNode(
        level,
        x,
        y,
        z,
        cellBounds,
        this.geometricErrorForDepth(depth),
        uri,
        [],
      );
      this.nodes.set(node.key(), node);
      if (level > this.maxNodeLevel) this.maxNodeLevel = level;
      this.markNodeCompleted(node);
      return node;
    }

    const targetCount = constrainTargetSplatCount(
      this.targetSplatCountForDepth(depth, cloud.length),
      cloud.length,
      childKeys.length,
    );
    const useAsyncSimplify =
      this.contentWorkerPool && cloud.length >= SPZ_ASYNC_WRITE_THRESHOLD;
    const useParallelChildBuild = this.shouldBuildChildrenInWorkers(
      depth,
      cloud.length,
      childKeys.length,
    );
    let uri;
    const children = [];
    if (useAsyncSimplify || useParallelChildBuild) {
      const childEntries = [];
      for (let oct = 0; oct < 8; oct++) {
        const idx = childGroups[oct];
        if (!idx) continue;
        const childCloud = cloud.subset(idx, false);
        const cb = childBounds(cellBounds, oct);
        childEntries.push({
          oct,
          cloud: childCloud,
          bounds: cb,
        });
      }

      if (useAsyncSimplify) {
        uri = this.writeSimplifiedContent(
          cloud,
          targetCount,
          cellBounds,
          level,
          x,
          y,
          z,
        );
      } else {
        const [lodCloud] = simplifyCloudVoxel(cloud, targetCount, cellBounds);
        uri = this.writeContent(lodCloud, level, x, y, z);
      }

      if (useParallelChildBuild) {
        const childrenByOct = new Array(8);
        const workItems = childEntries
          .slice()
          .sort((a, b) => b.cloud.length - a.cloud.length || a.oct - b.oct);
        const localEntry = workItems.shift();
        const tasks = [];
        if (localEntry) {
          tasks.push(
            this.buildNode(
              localEntry.cloud,
              localEntry.bounds,
              depth + 1,
              level + 1,
              (x << 1) | (localEntry.oct & 1),
              (y << 1) | ((localEntry.oct >> 1) & 1),
              (z << 1) | ((localEntry.oct >> 2) & 1),
            ).then((child) => {
              childrenByOct[localEntry.oct] = child;
            }),
          );
        }
        for (let i = 0; i < workItems.length; i++) {
          const entry = workItems[i];
          tasks.push(
            this.buildSubtreeInWorker(
              entry.cloud,
              entry.bounds,
              depth + 1,
              level + 1,
              (x << 1) | (entry.oct & 1),
              (y << 1) | ((entry.oct >> 1) & 1),
              (z << 1) | ((entry.oct >> 2) & 1),
            ).then((child) => {
              childrenByOct[entry.oct] = child;
            }),
          );
        }
        await Promise.all(tasks);
        for (let oct = 0; oct < 8; oct++) {
          if (childrenByOct[oct]) {
            children.push(childrenByOct[oct]);
          }
        }
      } else {
        for (let i = 0; i < childEntries.length; i++) {
          const entry = childEntries[i];
          const oct = entry.oct;
          const child = await this.buildNode(
            entry.cloud,
            entry.bounds,
            depth + 1,
            level + 1,
            (x << 1) | (oct & 1),
            (y << 1) | ((oct >> 1) & 1),
            (z << 1) | ((oct >> 2) & 1),
          );
          children.push(child);
        }
      }
    } else {
      const [lodCloud] = simplifyCloudVoxel(cloud, targetCount, cellBounds);
      uri = this.writeContent(lodCloud, level, x, y, z);
      for (let oct = 0; oct < 8; oct++) {
        const idx = childGroups[oct];
        if (!idx) continue;
        const childCloud = cloud.subset(idx, false);
        const cb = childBounds(cellBounds, oct);
        const child = await this.buildNode(
          childCloud,
          cb,
          depth + 1,
          level + 1,
          (x << 1) | (oct & 1),
          (y << 1) | ((oct >> 1) & 1),
          (z << 1) | ((oct >> 2) & 1),
        );
        children.push(child);
      }
    }

    const node = new TileNode(
      level,
      x,
      y,
      z,
      cellBounds,
      this.geometricErrorForDepth(depth),
      uri,
      children,
    );
    this.nodes.set(node.key(), node);
    if (level > this.maxNodeLevel) this.maxNodeLevel = level;
    this.markNodeCompleted(node);
    return node;
  }

  tileToJson(node) {
    const obj = {
      boundingVolume: { box: node.bounds.toBoxArray() },
      geometricError: node.error,
      refine: 'REPLACE',
      content: { uri: node.contentUri },
    };
    if (node.children.length > 0) {
      obj.children = node.children.map((c) => this.tileToJson(c));
    }
    return obj;
  }

  maxLevel() {
    return this.maxNodeLevel;
  }

  implicitRootError() {
    return this.rootGeometricError;
  }

  buildSharedNodeTable() {
    const nodeCount = this.nodes.size;
    const buffer = new SharedArrayBuffer(
      nodeCount * 4 * Int32Array.BYTES_PER_ELEMENT,
    );
    const packed = new Int32Array(buffer);
    let off = 0;
    for (const node of this.nodes.values()) {
      packed[off++] = node.level;
      packed[off++] = node.x;
      packed[off++] = node.y;
      packed[off++] = node.z;
    }
    this._sharedNodeTableSeq += 1;
    return {
      id: this._sharedNodeTableSeq,
      buffer,
      nodeCount,
    };
  }

  async build() {
    fs.mkdirSync(this.tilesDir, { recursive: true });
    this.log('[info] planning tile tree...');
    this.scanProgress.setTotal(this.scanWorkTotal());
    this.scanProgress.update(0, 'starting');
    this.buildPlan = this.makeBuildPlan();
    this.syncLodMaxDepthToBuildPlan();
    this.scanProgress.done(
      [
        `nodes=${this.buildPlan.nodeCount}`,
        `content=${this.buildPlan.contentCount}`,
        this.buildPlan.subtreeCount > 0
          ? `subtrees=${this.buildPlan.subtreeCount}`
          : null,
      ]
        .filter(Boolean)
        .join(' | '),
    );
    if (this.rootGeometricErrorSource === 'configured_min_geometric_error') {
      const minLevelLabel =
        this.lodMaxDepth === this.maxDepth
          ? `level=${this.lodMaxDepth}`
          : `level=${this.lodMaxDepth} actual (configured maxDepth=${this.maxDepth})`;
      this.log(
        `[info] root geometricError base=${this.rootGeometricError.toFixed(6)} | min(${minLevelLabel})=${this.minGeometricError.toFixed(6)} [configured]`,
      );
    } else {
      this.log(
        `[info] root geometricError base=${this.rootGeometricError.toFixed(6)}`,
      );
    }
    this.log(
      `[info] planned work | nodes=${this.buildPlan.nodeCount} | content=${this.buildPlan.contentCount}` +
        (this.buildPlan.subtreeCount > 0
          ? ` | subtrees=${this.buildPlan.subtreeCount}`
          : '') +
        ` | total=${this.buildPlan.overallCount}`,
    );
    this.overallProgress.setTotal(this.overallTotalCount());
    this.refreshOverallProgress('starting');

    const root = await this.buildNode(
      this.cloud,
      this.rootCellBounds,
      0,
      0,
      0,
      0,
      0,
    );
    if (this.contentWorkerPool) {
      await this.contentWorkerPool.waitForIdle();
    }
    if (this.contentWritePromises.length > 0) {
      await Promise.all(this.contentWritePromises);
    }

    let tileset;
    if (this.tilingMode === 'explicit') {
      tileset = {
        asset: { version: '1.1' },
        geometricError: root.error,
        root: this.tileToJson(root),
      };
    } else if (this.tilingMode === 'implicit') {
      tileset = await this.writeImplicitTileset();
    } else {
      throw new ConversionError(`Unknown tiling mode: ${this.tilingMode}`);
    }

    this.finalizeOverallProgress('finalizing');
    this.overallProgress.done(this.progressSummary().join(' | '));
    return [root, tileset];
  }

  async writeImplicitTileset() {
    const availableLevels = this.buildPlan
      ? this.buildPlan.availableLevels
      : this.maxLevel() + 1;
    const subtreeLevels = this.buildPlan
      ? this.buildPlan.effectiveSubtreeLevels
      : Math.max(1, Math.min(this.subtreeLevels, Math.max(1, availableLevels)));
    fs.mkdirSync(this.subtreesDir, { recursive: true });
    await this.writeAllSubtrees(availableLevels, subtreeLevels);
    const rootError = this.implicitRootError();
    return {
      asset: { version: '1.1' },
      geometricError: rootError,
      root: {
        boundingVolume: { box: this.rootCellBounds.toBoxArray() },
        refine: 'REPLACE',
        geometricError: rootError,
        content: { uri: 'tiles/{level}/{x}/{y}/{z}.glb' },
        implicitTiling: {
          subdivisionScheme: 'OCTREE',
          availableLevels,
          subtreeLevels,
          subtrees: { uri: 'subtrees/{level}/{x}/{y}/{z}.subtree' },
        },
      },
    };
  }

  async writeAllSubtrees(availableLevels, subtreeLevels) {
    const keys = [];
    for (const node of this.nodes.values()) {
      if (node.level % subtreeLevels === 0) {
        keys.push([node.level, node.x, node.y, node.z]);
      }
    }
    keys.sort((a, b) => {
      if (a[0] !== b[0]) return a[0] - b[0];
      if (a[1] !== b[1]) return a[1] - b[1];
      if (a[2] !== b[2]) return a[2] - b[2];
      return a[3] - b[3];
    });

    if (!this.contentWorkerPool || keys.length <= 1) {
      for (const [level, x, y, z] of keys) {
        this.writeOneSubtree(level, x, y, z, availableLevels, subtreeLevels);
      }
      return;
    }

    const nodeTable = this.buildSharedNodeTable();
    const tasks = [];
    for (const [level, x, y, z] of keys) {
      const subtreeDir = path.join(
        this.subtreesDir,
        String(level),
        String(x),
        String(y),
      );
      const subtreePath = path.join(subtreeDir, `${z}.subtree`);
      tasks.push(
        this.contentWorkerPool
          .submit({
            kind: 'write-subtree',
            subtreePath,
            level,
            x,
            y,
            z,
            availableLevels,
            subtreeLevels,
            nodeTableId: nodeTable.id,
            nodeTableBuffer: nodeTable.buffer,
            nodeCount: nodeTable.nodeCount,
          })
          .then(() => {
            this.markSubtreeCompleted(level, x, y, z);
          }),
      );
    }
    await Promise.all(tasks);
  }

  writeOneSubtree(level, x, y, z, availableLevels, subtreeLevels) {
    const subtreePath = path.join(
      this.subtreesDir,
      String(level),
      String(x),
      String(y),
      `${z}.subtree`,
    );
    const { subtree, blob } = buildSubtreeArtifact(
      level,
      x,
      y,
      z,
      availableLevels,
      subtreeLevels,
      (globalLevel, gx, gy, gz) =>
        this.nodes.has(`${globalLevel}/${gx}/${gy}/${gz}`),
    );
    writeSubtreeFile(subtreePath, subtree, blob);
    this.markSubtreeCompleted(level, x, y, z);
  }

  submitContentWriteTask(task, transfer, relPath, splatCount, level = null) {
    this.refreshOverallProgress(`queued splats=${splatCount} uri=${relPath}`);
    this.contentWritePromises.push(
      this.contentWorkerPool
        .submit(task, transfer)
        .then(() => {
          this.markContentCompleted(relPath, splatCount, level);
        })
        .catch(() => {
          throw new ConversionError(
            'Failed to write content in worker thread.',
          );
        }),
    );
    return relPath;
  }

  writeSimplifiedContent(cloud, targetCount, cellBounds, level, x, y, z) {
    if (!this.contentWorkerPool || cloud.length < SPZ_ASYNC_WRITE_THRESHOLD) {
      const [lodCloud] = simplifyCloudVoxel(cloud, targetCount, cellBounds);
      return this.writeContent(lodCloud, level, x, y, z);
    }
    const relPath = this.contentRelPath(level, x, y, z);
    const outPath = path.join(this.outDir, relPath);
    const splatCount = cloud.length;
    const task = {
      kind: 'simplify-pack-spz',
      outPath,
      targetCount,
      cellBounds: serializeBounds(cellBounds),
      sh1Bits: this.spzSh1Bits,
      shRestBits: this.spzShRestBits,
      colorSpace: this.colorSpace,
      sourceUpAxis: this.sourceUpAxis,
      cloud: serializeCloudForWorkerTask(cloud),
    };
    const transfer = transferListForCloud(cloud);
    return this.submitContentWriteTask(
      task,
      transfer,
      relPath,
      splatCount,
      level,
    );
  }
}

function morton3(x, y, z) {
  let result = 0;
  const v = x | y | z;
  if (v === 0) return 0;
  const maxBits = 32 - Math.clz32(v);
  for (let bit = 0; bit < maxBits; bit++) {
    result |= ((x >> bit) & 1) << (3 * bit);
    result |= ((y >> bit) & 1) << (3 * bit + 1);
    result |= ((z >> bit) & 1) << (3 * bit + 2);
  }
  return result;
}

function subtreeNodeCount(levels) {
  let total = 0;
  let width = 1;
  for (let i = 0; i < levels; i++) {
    total += width;
    width *= 8;
  }
  return total;
}

const MORTON_COORD_CACHE = new Map();

function iterMortonCoords(depth) {
  const cached = MORTON_COORD_CACHE.get(depth);
  if (cached) {
    return cached;
  }
  if (depth === 0) {
    const root = [[0, 0, 0]];
    MORTON_COORD_CACHE.set(depth, root);
    return root;
  }
  const side = 1 << depth;
  const out = [];
  for (let z = 0; z < side; z++) {
    for (let y = 0; y < side; y++) {
      for (let x = 0; x < side; x++) {
        out.push([x, y, z]);
      }
    }
  }
  out.sort((a, b) => morton3(a[0], a[1], a[2]) - morton3(b[0], b[1], b[2]));
  MORTON_COORD_CACHE.set(depth, out);
  return out;
}

function writeUint64LE(buffer, value, offset) {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new ConversionError(`Invalid UINT64 value: ${value}`);
  }
  const low = value >>> 0;
  const high = Math.floor(value / 4294967296) >>> 0;
  buffer.writeUInt32LE(low, offset);
  buffer.writeUInt32LE(high, offset + 4);
}

function packBitsLsb(bits) {
  const out = new Uint8Array(Math.ceil(bits.length / 8));
  for (let i = 0; i < bits.length; i++) {
    if (bits[i]) {
      out[i >> 3] |= 1 << (i & 7);
    }
  }
  return out;
}

function buildSubtreeArtifact(
  level,
  x,
  y,
  z,
  availableLevels,
  subtreeLevels,
  hasNode,
) {
  const tileBits = new Uint8Array(subtreeNodeCount(subtreeLevels));
  const childSubtreeBits = new Uint8Array(8 ** subtreeLevels);
  let tileBitCount = 0;
  let childSubtreeCount = 0;
  let tileOff = 0;
  let childOff = 0;

  for (let relDepth = 0; relDepth < subtreeLevels; relDepth++) {
    const globalLevel = level + relDepth;
    for (const [lx, ly, lz] of iterMortonCoords(relDepth)) {
      const gx = (x << relDepth) | lx;
      const gy = (y << relDepth) | ly;
      const gz = (z << relDepth) | lz;
      const exists =
        globalLevel < availableLevels && hasNode(globalLevel, gx, gy, gz)
          ? 1
          : 0;
      tileBits[tileOff++] = exists;
      tileBitCount += exists;
    }
  }

  const childGlobal = level + subtreeLevels;
  for (const [lx, ly, lz] of iterMortonCoords(subtreeLevels)) {
    const gx = (x << subtreeLevels) | lx;
    const gy = (y << subtreeLevels) | ly;
    const gz = (z << subtreeLevels) | lz;
    const exists =
      childGlobal < availableLevels && hasNode(childGlobal, gx, gy, gz) ? 1 : 0;
    childSubtreeBits[childOff++] = exists;
    childSubtreeCount += exists;
  }

  const blobParts = [];
  let blobLen = 0;
  const bufferViews = [];
  const addBufferView = (data) => {
    const pad = (8 - (blobLen % 8)) % 8;
    if (pad > 0) {
      blobParts.push(Buffer.alloc(pad));
      blobLen += pad;
    }
    const byteOffset = blobLen;
    const buf = Buffer.from(data);
    blobParts.push(buf);
    blobLen += buf.length;
    const bv = { buffer: 0, byteOffset, byteLength: buf.length };
    bufferViews.push(bv);
    return bufferViews.length - 1;
  };

  const availabilityObj = (bits, count) => {
    if (count === bits.length) {
      return { constant: 1 };
    }
    if (count === 0) {
      return { constant: 0 };
    }
    const packed = packBitsLsb(bits);
    return {
      bitstream: addBufferView(packed),
      availableCount: count,
    };
  };

  const tileAvailability = availabilityObj(tileBits, tileBitCount);
  const subtree = {
    tileAvailability,
    contentAvailability: [
      tileAvailability.constant != null
        ? { constant: tileAvailability.constant }
        : {
            bitstream: tileAvailability.bitstream,
            availableCount: tileAvailability.availableCount,
          },
    ],
    childSubtreeAvailability: availabilityObj(
      childSubtreeBits,
      childSubtreeCount,
    ),
  };

  let blob = null;
  if (bufferViews.length > 0) {
    subtree.buffers = [{ uri: '', byteLength: blobLen }];
    subtree.bufferViews = bufferViews;
    blob = Buffer.alloc(blobLen);
    let blobOffset = 0;
    for (const part of blobParts) {
      part.copy(blob, blobOffset);
      blobOffset += part.length;
    }
  }

  return { subtree, blob };
}

function buildSubtreeBinaryBuffer(subtree, blob) {
  const subtreeJson = JSON.parse(JSON.stringify(subtree));
  let binaryChunk = Buffer.alloc(0);

  if (blob && blob.length > 0) {
    const binaryPad = padLength(blob.length, 8);
    binaryChunk =
      binaryPad > 0 ? Buffer.concat([blob, Buffer.alloc(binaryPad)]) : blob;
    subtreeJson.buffers[0].byteLength = binaryChunk.length;
    delete subtreeJson.buffers[0].uri;
  }

  const jsonChunk = Buffer.from(JSON.stringify(subtreeJson), 'utf8');
  const jsonPad = padLength(jsonChunk.length, 8);
  const jsonChunkPadded =
    jsonPad > 0
      ? Buffer.concat([jsonChunk, Buffer.alloc(jsonPad, 0x20)])
      : jsonChunk;

  const header = Buffer.alloc(24);
  header.writeUInt32LE(SUBTREE_MAGIC, 0);
  header.writeUInt32LE(SUBTREE_VERSION, 4);
  writeUint64LE(header, jsonChunkPadded.length, 8);
  writeUint64LE(header, binaryChunk.length, 16);

  return Buffer.concat([header, jsonChunkPadded, binaryChunk]);
}

function writeSubtreeFile(subtreePath, subtree, blob) {
  fs.mkdirSync(path.dirname(subtreePath), { recursive: true });
  fs.writeFileSync(subtreePath, buildSubtreeBinaryBuffer(subtree, blob));
}

async function buildTilesetFromCloud(cloud, outDir, args) {
  const outputDir = path.resolve(outDir);
  if (fs.existsSync(outputDir) && args.clean) {
    fs.rmSync(outputDir, { recursive: true, force: true });
  }
  fs.mkdirSync(outputDir, { recursive: true });

  const viewerPath = path.join(outputDir, 'viewer.html');
  if (fs.existsSync(viewerPath)) {
    fs.unlinkSync(viewerPath);
  }

  console.log(
    `[info] building tileset | splats=${cloud.length} | mode=${args.tilingMode} | codec=spz_stream`,
  );

  const inputSplatCount = cloud.length;
  const inputShDegree = cloud.shDegree;
  const buildCloud = cloud.color0 ? cloud.withoutColor0() : cloud;

  const builder = new OctreeTilesBuilder({
    cloud: buildCloud,
    outDir: outputDir,
    colorSpace: args.colorSpace,
    maxDepth: args.maxDepth,
    leafLimit: args.leafLimit,
    tilingMode: args.tilingMode,
    subtreeLevels: args.subtreeLevels,
    spzSh1Bits: args.spzSh1Bits,
    spzShRestBits: args.spzShRestBits,
    sourceUpAxis: args.sourceUpAxis,
    samplingRatePerLevel: args.samplingRatePerLevel,
    minGeometricError:
      args.minGeometricError != null && args.minGeometricError > 0.0
        ? args.minGeometricError
        : null,
    contentWorkers: args.contentWorkers,
    workerScriptPath: args.workerScriptPath || DEFAULT_WORKER_SCRIPT,
  });

  try {
    const [, tileset] = await builder.build();
    fs.writeFileSync(
      path.join(outputDir, 'tileset.json'),
      JSON.stringify(tileset, null, 2),
      'utf8',
    );

    const samplingDivisorsByDepth = {};
    const samplingRatesByDepth = {};
    const geometricErrorScaleByDepth = {};
    const geometricErrorByDepth = {};
    for (let depth = 0; depth <= builder.lodMaxDepth; depth++) {
      const geometricScale = geometricErrorScaleForDepth(
        depth,
        builder.lodMaxDepth,
        args.samplingRatePerLevel,
      );
      samplingDivisorsByDepth[String(depth)] = samplingDivisorForDepth(
        depth,
        builder.lodMaxDepth,
        args.samplingRatePerLevel,
      );
      samplingRatesByDepth[String(depth)] =
        args.samplingRatePerLevel **
        Math.max(0, builder.lodMaxDepth - depth);
      geometricErrorScaleByDepth[String(depth)] = geometricScale;
      geometricErrorByDepth[String(depth)] =
        builder.rootGeometricError * geometricScale;
    }

    const root = builder.nodes.get('0/0/0/0');
    const summary = {
      input_splats: inputSplatCount,
      sh_degree: inputShDegree,
      max_depth: args.maxDepth,
      leaf_limit: args.leafLimit,
      color_space: args.colorSpace,
      sampling_rate_per_level: args.samplingRatePerLevel,
      tiling_mode: args.tilingMode,
      subtree_levels:
        args.tilingMode === 'implicit' ? args.subtreeLevels : null,
      content_codec: 'spz_stream',
      spz_version: SPZ_STREAM_VERSION,
      spz_sh1_bits: args.spzSh1Bits,
      spz_sh_rest_bits: args.spzShRestBits,
      source_up_axis: args.sourceUpAxis,
      configured_min_geometric_error:
        args.minGeometricError != null && args.minGeometricError > 0.0
          ? args.minGeometricError
          : null,
      node_count: builder.nodes.size,
      available_levels: builder.maxLevel() + 1,
      effective_max_depth: builder.lodMaxDepth,
      root_geometric_error_source: builder.rootGeometricErrorSource,
      implicit_root_geometric_error:
        args.tilingMode === 'implicit' ? builder.implicitRootError() : null,
      root_geometric_error: root ? root.error : null,
      min_geometric_error: builder.geometricErrorForDepth(builder.lodMaxDepth),
      geometric_error_scale_by_depth: geometricErrorScaleByDepth,
      geometric_error_by_depth: geometricErrorByDepth,
      sampling_rates_by_depth: samplingRatesByDepth,
      sampling_divisors_by_depth: samplingDivisorsByDepth,
      source: SOURCE_REPOSITORY,
    };
    fs.writeFileSync(
      path.join(outputDir, 'build_summary.json'),
      JSON.stringify(summary, null, 2),
      'utf8',
    );
    console.log(
      `[info] nodes=${builder.nodes.size} | levels=${builder.maxLevel() + 1}`,
    );
  } finally {
    await builder.close();
  }
}

module.exports = {
  SOURCE_REPOSITORY,
  ConsoleProgressBar,
  SpzContentWorkerPool,
  computeBounds,
  simplifyCloudVoxel,
  samplingDivisorForDepth,
  geometricErrorScaleForDepth,
  buildSubtreeNodeLocal,
  OctreeTilesBuilder,
  buildSubtreeArtifact,
  writeSubtreeFile,
  buildTilesetFromCloud,
};
