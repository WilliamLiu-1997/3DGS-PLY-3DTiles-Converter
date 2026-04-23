const fs = require('fs');
const path = require('path');
const { Worker } = require('worker_threads');

const { ConversionError, Bounds, roundHalfToEven } = require('./parser');

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

    if (this.total > 0) {
      const bar = `[${fill}${done === this.width ? '' : '>'}${' '.repeat(remain)}]`;
      const percent = `${Math.round(ratio * 100)
        .toString()
        .padStart(3, ' ')}%`;
      this._renderLine(
        `${this.label} ${bar} ${percent} (${this.current}/${this.total}) ${this._lastMessage}`,
      );
      return;
    }

    const spin = this._spinner[this._spinnerPos];
    this._spinnerPos = (this._spinnerPos + 1) & 3;
    const bar = `[${spin}${' '.repeat(this.width - 1)}]`;
    this._renderLine(
      `${this.label} ${bar} (${this.current}) ${this._lastMessage}`,
    );
  }

  _renderLine(text) {
    if (!this.enabled) return;
    if (
      typeof process.stdout.clearLine === 'function' &&
      typeof process.stdout.cursorTo === 'function'
    ) {
      process.stdout.clearLine(0);
      process.stdout.cursorTo(0);
      process.stdout.write(text);
      return;
    }
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

function normalizeSplatTargetCount(targetCount, splatCount) {
  return Math.max(1, Math.min(splatCount, Math.floor(targetCount)));
}

function defaultVoxelTargetCount(targetCount, splatCount) {
  const normalized = normalizeSplatTargetCount(targetCount, splatCount);
  return Math.max(1, Math.floor(normalized * 4));
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

function planSimplifyCloudVoxel(
  cloud,
  targetCount,
  bounds = null,
  voxelTargetCount = targetCount,
) {
  const n = cloud.length;
  const target = normalizeSplatTargetCount(targetCount, n);
  const voxelTarget = Math.max(
    target,
    normalizeSplatTargetCount(voxelTargetCount, n),
  );
  if (n <= target) {
    const selected = [];
    const assignment = new Int32Array(n);
    for (let i = 0; i < n; i++) {
      selected.push(i);
      assignment[i] = i;
    }
    const keptRadius = computeThreeSigmaAabbDiagonalRadius(
      cloud.scaleLog,
      cloud.quatsXYZW,
    );
    return {
      selected,
      assignment,
      keptRadius,
      origRadius: keptRadius,
      voxelDiag: 0.0,
    };
  }

  const activeBounds = bounds || computeBounds(cloud);
  let dims = chooseGridDims(activeBounds, voxelTarget);
  const mins = activeBounds.minimum;
  const ext = activeBounds.extents().map((v) => Math.max(v, 1e-6));

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

  const selectedGroups = groups || [];
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
      0.15,
    );
    if (detailRep >= 0 && detailRep !== coarseRep) {
      const c3 = coarseRep * 3;
      const d3 = detailRep * 3;
      const dx = pos[d3 + 0] - pos[c3 + 0];
      const dy = pos[d3 + 1] - pos[c3 + 1];
      const dz = pos[d3 + 2] - pos[c3 + 2];
      const sepNorm = Math.sqrt(dx * dx + dy * dy + dz * dz) / voxelDiag;
      const radiusRatio =
        Math.max(origRadius[coarseRep], 1e-6) /
        Math.max(origRadius[detailRep], 1e-6);
      secondaryCandidates.push({
        rep: detailRep,
        priority:
          detailWeights[detailRep] *
          (1.0 + sepNorm) *
          (1.0 + Math.max(0.0, Math.log2(Math.max(radiusRatio, 1.0)))),
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
      0.15,
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
        const rep = selected[slot];
        const s3 = rep * 3;
        const dx = pos[p3 + 0] - pos[s3 + 0];
        const dy = pos[p3 + 1] - pos[s3 + 1];
        const dz = pos[p3 + 2] - pos[s3 + 2];
        const score = Math.sqrt(dx * dx + dy * dy + dz * dz) + keptRadius[slot];
        if (score < bestScore) {
          bestScore = score;
          bestSlot = slot;
        }
      }
      assignment[orig] = bestSlot;
    }
  }

  return {
    selected,
    assignment,
    keptRadius,
    origRadius,
    voxelDiag,
  };
}

function samplingDivisorForDepth(depth, maxDepth, samplingRatePerLevel) {
  const stepsFromFinest = Math.max(0, Math.floor(maxDepth) - Math.floor(depth));
  return (1.0 / samplingRatePerLevel) ** stepsFromFinest;
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

function padLength(length, alignment) {
  const rem = length % alignment;
  return rem === 0 ? 0 : alignment - rem;
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

module.exports = {
  SOURCE_REPOSITORY,
  ConsoleProgressBar,
  SpzContentWorkerPool,
  computeBounds,
  computeThreeSigmaAabbDiagonalRadius,
  childBounds,
  chooseGridDims,
  normalizeSplatTargetCount,
  defaultVoxelTargetCount,
  constrainTargetSplatCount,
  percent95,
  planSimplifyCloudVoxel,
  samplingDivisorForDepth,
  geometricErrorScaleForDepth,
  rootGeometricErrorFromMinLevel,
  buildSubtreeArtifact,
  writeSubtreeFile,
};
