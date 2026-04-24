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
  }

  _createWorker() {
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
    return worker;
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

    while (this.taskQueue.length > 0 && this.workers.length < this.workerCount) {
      const idleCount = this.workers.reduce(
        (sum, worker) => sum + (worker._busy ? 0 : 1),
        0,
      );
      if (idleCount >= this.taskQueue.length) {
        break;
      }
      this._createWorker();
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

function writeThreeSigmaExtentComponents(
  scaleLog,
  scaleOffset,
  quatsXYZW,
  quatOffset,
  out,
  outOffset = 0,
) {
  let x = quatsXYZW[quatOffset + 0];
  let y = quatsXYZW[quatOffset + 1];
  let z = quatsXYZW[quatOffset + 2];
  let w = quatsXYZW[quatOffset + 3];

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

  const s0 = Math.exp(scaleLog[scaleOffset + 0]);
  const s1 = Math.exp(scaleLog[scaleOffset + 1]);
  const s2 = Math.exp(scaleLog[scaleOffset + 2]);
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

  out[outOffset + 0] = Math.fround(3.0 * Math.sqrt(Math.max(c00, 1e-20)));
  out[outOffset + 1] = Math.fround(3.0 * Math.sqrt(Math.max(c11, 1e-20)));
  out[outOffset + 2] = Math.fround(3.0 * Math.sqrt(Math.max(c22, 1e-20)));
  return out;
}

function computeThreeSigmaExtents(scaleLog, quatsXYZW) {
  const n = scaleLog.length / 3;
  const out = new Float32Array(n * 3);
  for (let i = 0; i < n; i++) {
    writeThreeSigmaExtentComponents(
      scaleLog,
      i * 3,
      quatsXYZW,
      i * 4,
      out,
      i * 3,
    );
  }
  return out;
}

function computeThreeSigmaAabbDiagonalRadius(scaleLog, quatsXYZW) {
  const n = scaleLog.length / 3;
  const out = new Float32Array(n);
  for (let i = 0; i < n; i++) {
    out[i] = computeThreeSigmaAabbDiagonalRadiusAt(
      scaleLog,
      i * 3,
      quatsXYZW,
      i * 4,
    );
  }
  return out;
}

function computeThreeSigmaAabbDiagonalRadiusAt(
  scaleLog,
  scaleOffset,
  quatsXYZW,
  quatOffset,
) {
  let x = quatsXYZW[quatOffset + 0];
  let y = quatsXYZW[quatOffset + 1];
  let z = quatsXYZW[quatOffset + 2];
  let w = quatsXYZW[quatOffset + 3];

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

  const s0 = Math.exp(scaleLog[scaleOffset + 0]);
  const s1 = Math.exp(scaleLog[scaleOffset + 1]);
  const s2 = Math.exp(scaleLog[scaleOffset + 2]);
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
  return Math.sqrt(ex * ex + ey * ey + ez * ez);
}

function computeBounds(cloud) {
  const n = cloud.length;
  const minimum = [Infinity, Infinity, Infinity];
  const maximum = [-Infinity, -Infinity, -Infinity];
  const extentScratch = new Float32Array(3);
  for (let i = 0; i < n; i++) {
    const pi = i * 3;
    writeThreeSigmaExtentComponents(
      cloud.scaleLog,
      pi,
      cloud.quatsXYZW,
      i * 4,
      extentScratch,
      0,
    );
    const p0 = cloud.positions[pi + 0];
    const p1 = cloud.positions[pi + 1];
    const p2 = cloud.positions[pi + 2];
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

function representativeIndexForGroupRange(
  cloud,
  groupIndices,
  start,
  end,
  weightAt,
  radii,
  voxelDiagSq,
  radiusBias,
  excludeA = -1,
  excludeB = -1,
) {
  if (end <= start) {
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

  for (let p = start; p < end; p++) {
    const idx = groupIndices[p];
    if (idx === excludeA || idx === excludeB) {
      continue;
    }
    validCount += 1;
    const w = Math.max(weightAt(idx), 1e-12);
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
  for (let p = start; p < end; p++) {
    const idx = groupIndices[p];
    if (idx === excludeA || idx === excludeB) {
      continue;
    }
    const i3 = idx * 3;
    const dx = pos[i3 + 0] - cx;
    const dy = pos[i3 + 1] - cy;
    const dz = pos[i3 + 2] - cz;
    const dist2 = dx * dx + dy * dy + dz * dz;
    const radius = radii[idx];
    const w = weightAt(idx);
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
  options = {},
) {
  const returnOrigRadius = options.returnOrigRadius !== false;
  const returnKeptRadius = options.returnKeptRadius !== false;
  const n = cloud.length;
  const target = normalizeSplatTargetCount(targetCount, n);
  const voxelTarget = normalizeSplatTargetCount(voxelTargetCount, n);
  if (n <= target) {
    const selected = [];
    const assignment = new Int32Array(n);
    for (let i = 0; i < n; i++) {
      selected.push(i);
      assignment[i] = i;
    }
    const keptRadius =
      cloud.origRadius ||
      computeThreeSigmaAabbDiagonalRadius(cloud.scaleLog, cloud.quatsXYZW);
    return {
      selected,
      assignment,
      keptRadius: returnKeptRadius ? keptRadius : null,
      origRadius: returnOrigRadius ? keptRadius : null,
      voxelDiag: 0.0,
    };
  }

  const activeBounds = bounds || computeBounds(cloud);
  let dims = chooseGridDims(activeBounds, voxelTarget);
  const mins = activeBounds.minimum;
  const ext = activeBounds.extents().map((v) => Math.max(v, 1e-6));

  const pos = cloud.positions;
  const m0 = mins[0];
  const m1 = mins[1];
  const m2 = mins[2];
  const invExt0 = 1.0 / ext[0];
  const invExt1 = 1.0 / ext[1];
  const invExt2 = 1.0 / ext[2];
  let groupKeys = [];
  let cellCounts = null;

  const flatForPoint = (idx, dimsIn) => {
    const i3 = idx * 3;
    const d0 = dimsIn[0];
    const d1 = dimsIn[1];
    const d2 = dimsIn[2];
    const uvw0 = Math.max(0.0, Math.min(0.999999, (pos[i3] - m0) * invExt0));
    const uvw1 = Math.max(
      0.0,
      Math.min(0.999999, (pos[i3 + 1] - m1) * invExt1),
    );
    const uvw2 = Math.max(
      0.0,
      Math.min(0.999999, (pos[i3 + 2] - m2) * invExt2),
    );
    const iIdx = Math.min(d0 - 1, Math.floor(uvw0 * d0));
    const jIdx = Math.min(d1 - 1, Math.floor(uvw1 * d1));
    const kIdx = Math.min(d2 - 1, Math.floor(uvw2 * d2));
    return iIdx + d0 * (jIdx + d1 * kIdx);
  };

  for (let iter = 0; iter < 24; iter++) {
    const cellCount = dims[0] * dims[1] * dims[2];
    cellCounts = new Uint32Array(cellCount);

    for (let i = 0; i < n; i++) {
      const flat = flatForPoint(i, dims);
      cellCounts[flat] += 1;
    }

    groupKeys = [];
    for (let flat = 0; flat < cellCount; flat++) {
      if (cellCounts[flat] > 0) {
        groupKeys.push(flat);
      }
    }
    if (
      groupKeys.length <= target ||
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

  const groupCount = groupKeys.length;
  const groupOffsets = new Uint32Array(groupCount + 1);
  for (let group = 0; group < groupCount; group++) {
    const key = groupKeys[group];
    groupOffsets[group + 1] = groupOffsets[group] + cellCounts[key];
    cellCounts[key] = group;
  }
  const groupIndices = new Int32Array(n);
  const groupCursors = new Uint32Array(groupOffsets);
  for (let i = 0; i < n; i++) {
    const group = cellCounts[flatForPoint(i, dims)];
    groupIndices[groupCursors[group]++] = i;
  }
  cellCounts = null;
  groupKeys = [];

  const origRadius =
    cloud.origRadius ||
    computeThreeSigmaAabbDiagonalRadius(cloud.scaleLog, cloud.quatsXYZW);
  const voxelSize0 = ext[0] / Math.max(1, dims[0]);
  const voxelSize1 = ext[1] / Math.max(1, dims[1]);
  const voxelSize2 = ext[2] / Math.max(1, dims[2]);
  const voxelDiagSq =
    voxelSize0 * voxelSize0 + voxelSize1 * voxelSize1 + voxelSize2 * voxelSize2;
  const voxelDiag = Math.max(Math.sqrt(voxelDiagSq), 1e-6);
  const coarseWeightAt = (i) => {
    const opacity = Math.max(cloud.opacity[i], 1e-4);
    const radiusNorm = Math.max(origRadius[i] / voxelDiag, 0.35);
    return opacity * Math.sqrt(radiusNorm);
  };

  const selected = [];
  const assignment = new Int32Array(n);
  assignment.fill(-1);
  const extraCoarseCandidates = [];

  for (let group = 0; group < groupCount; group++) {
    const start = groupOffsets[group];
    const end = groupOffsets[group + 1];
    if (end <= start) {
      continue;
    }
    const coarseRep = representativeIndexForGroupRange(
      cloud,
      groupIndices,
      start,
      end,
      coarseWeightAt,
      origRadius,
      voxelDiagSq,
      -0.15,
    );
    const coarseOutIdx = selected.length;
    selected.push(coarseRep);
    assignment[coarseRep] = coarseOutIdx;

    const extraCoarseRep = representativeIndexForGroupRange(
      cloud,
      groupIndices,
      start,
      end,
      coarseWeightAt,
      origRadius,
      voxelDiagSq,
      -0.15,
      coarseRep,
    );
    if (extraCoarseRep >= 0) {
      extraCoarseCandidates.push(extraCoarseRep);
    }
  }

  if (selected.length < target && extraCoarseCandidates.length > 0) {
    extraCoarseCandidates.sort((a, b) => {
      const priorityDiff = coarseWeightAt(b) - coarseWeightAt(a);
      if (priorityDiff !== 0) {
        return priorityDiff;
      }
      return a - b;
    });
    for (
      let i = 0;
      i < extraCoarseCandidates.length && selected.length < target;
      i++
    ) {
      const rep = extraCoarseCandidates[i];
      if (assignment[rep] >= 0) {
        continue;
      }
      const outIdx = selected.length;
      selected.push(rep);
      assignment[rep] = outIdx;
    }
  }

  if (selected.length < target) {
    const remain = [];
    for (let i = 0; i < n; i++) {
      if (assignment[i] < 0) {
        remain.push(i);
      }
    }
    remain.sort((a, b) => {
      const w = coarseWeightAt(b) - coarseWeightAt(a);
      if (w !== 0) return w;
      const r = origRadius[b] - origRadius[a];
      return r !== 0 ? r : b - a;
    });
    for (let i = 0; i < remain.length && selected.length < target; i++) {
      const rep = remain[i];
      if (assignment[rep] >= 0) {
        continue;
      }
      const outIdx = selected.length;
      selected.push(rep);
      assignment[rep] = outIdx;
    }
  }

  const keptRadius = new Float32Array(selected.length);
  for (let i = 0; i < selected.length; i++) {
    keptRadius[i] = origRadius[selected[i]];
  }

  for (let group = 0; group < groupCount; group++) {
    const start = groupOffsets[group];
    const end = groupOffsets[group + 1];
    if (end <= start) {
      continue;
    }
    const reps = [];
    for (let p = start; p < end; p++) {
      const slot = assignment[groupIndices[p]];
      if (slot >= 0) {
        reps.push(slot);
      }
    }
    if (reps.length === 0) {
      continue;
    }
    if (reps.length === 1) {
      for (let p = start; p < end; p++) {
        assignment[groupIndices[p]] = reps[0];
      }
      continue;
    }
    for (let p = start; p < end; p++) {
      const orig = groupIndices[p];
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
    keptRadius: returnKeptRadius ? keptRadius : null,
    origRadius: returnOrigRadius ? origRadius : null,
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
  let subtreeJson = subtree;
  let binaryChunk = Buffer.alloc(0);

  if (blob && blob.length > 0) {
    const binaryPad = padLength(blob.length, 8);
    binaryChunk =
      binaryPad > 0 ? Buffer.concat([blob, Buffer.alloc(binaryPad)]) : blob;
    const { uri: _uri, ...restBuf0 } = subtree.buffers[0];
    const newBuf0 = { ...restBuf0, byteLength: binaryChunk.length };
    subtreeJson = {
      ...subtree,
      buffers: [newBuf0, ...subtree.buffers.slice(1)],
    };
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
  computeThreeSigmaAabbDiagonalRadiusAt,
  computeThreeSigmaAabbDiagonalRadius,
  childBounds,
  chooseGridDims,
  normalizeSplatTargetCount,
  constrainTargetSplatCount,
  percent95,
  planSimplifyCloudVoxel,
  samplingDivisorForDepth,
  geometricErrorScaleForDepth,
  rootGeometricErrorFromMinLevel,
  writeThreeSigmaExtentComponents,
  buildSubtreeArtifact,
  writeSubtreeFile,
};
