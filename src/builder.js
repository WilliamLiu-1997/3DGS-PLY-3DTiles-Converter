const path = require('path');
const { Worker } = require('worker_threads');

const { ConversionError, Bounds, roundHalfToEven } = require('./parser');

const SOURCE_REPOSITORY = '3DGS-PLY-3DTiles-Converter';
const DEFAULT_WORKER_SCRIPT = path.join(__dirname, 'convert-core.js');

class ConsoleProgressBar {
  constructor(label, total = 0) {
    this.label = label;
    this.current = 0;
    this.total =
      Number.isFinite(total) && total > 0 ? Math.max(1, Math.floor(total)) : 0;
    this.enabled =
      process.stdout && process.stdout.isTTY && process.stderr.isTTY;
    this._spinner = ['-', '\\', '|', '/'];
    this._spinnerPos = 0;
    this._last = 0;
    this._lastMessage = '';
    this._lastStatus = '';
    this._lineActive = false;
    this._lastDetailMessage = '';
    this._done = false;
    this._onResize = () => this._render(true);
    this._resizeListenerAttached = false;
    this._attachResizeListener();
  }

  setTotal(total) {
    this.total =
      Number.isFinite(total) && total > 0 ? Math.max(1, Math.floor(total)) : 0;
    this._render();
  }

  reset(total = 0, message = '') {
    this.current = 0;
    this.total =
      Number.isFinite(total) && total > 0 ? Math.max(1, Math.floor(total)) : 0;
    this._done = false;
    if (message) {
      this._setMessage(message);
    }
    this._attachResizeListener();
    this._render(true);
  }

  update(current, message = '') {
    this.current = Math.max(this.current, current);
    if (message) {
      this._setMessage(message);
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
      this._setMessage(message);
    }
    if (this.enabled) {
      this.current = this.total > 0 ? this.total : this.current;
      this._render(true);
      process.stdout.write('\n');
      this._lineActive = false;
      this._detachResizeListener();
    } else if (this._lastMessage) {
      console.log(`[${this.label}] ${this._lastMessage}`);
    }
  }

  _render(force = false) {
    if (!this.enabled) {
      return;
    }
    const now = Date.now();
    if (!force && now - this._last < 100 && this.current > 0) {
      return;
    }
    this._last = now;
    const spin = this._spinner[this._spinnerPos];
    this._spinnerPos = (this._spinnerPos + 1) & 3;

    if (this.total > 0) {
      const ratio = Math.min(1, this.current / this.total);
      const percent = `${Math.round(ratio * 100)}%`;
      this._renderLine(
        `${this.label} ${spin} ${percent} (${this.current}/${this.total}) ${this._lastStatus}`,
      );
      return;
    }

    this._renderLine(
      `${this.label} ${spin} (${this.current}) ${this._lastStatus}`,
    );
  }

  _renderLine(text) {
    if (!this.enabled) return;
    const line = this._fitAsciiLine(text);
    if (
      typeof process.stdout.clearLine === 'function' &&
      typeof process.stdout.cursorTo === 'function'
    ) {
      process.stdout.cursorTo(0);
      process.stdout.clearLine(0);
      process.stdout.write(line);
      this._lineActive = true;
      return;
    }
    process.stdout.write(
      `\r${line}${' '.repeat(Math.max(0, this._maxLineLength() - line.length))}`,
    );
    this._lineActive = true;
  }

  _setMessage(message) {
    const text = String(message).replace(/[\r\n]+/g, ' ').trim();
    this._lastMessage = text;
    this._lastStatus = this._compactStatus(text);
  }

  logDetail(message) {
    const text = String(message).replace(/[\r\n]+/g, ' ').trim();
    if (!text || text === this._lastDetailMessage) {
      return;
    }
    if (this.enabled) {
      this._clearActiveLine();
      process.stdout.write(`[info] ${this.label} ${this._ascii(text)}\n`);
    } else {
      console.log(`[info] ${this.label} ${text}`);
    }
    this._lastDetailMessage = text;
  }

  _compactStatus(text) {
    if (!text) {
      return '';
    }
    if (text.includes('extra virtual long-tile work')) {
      const splits = text.match(/splits=([0-9,]+)/);
      const segments = text.match(/segments=([0-9,]+)/);
      return [
        'extra-work',
        splits ? `splits=${splits[1]}` : '',
        segments ? `segments=${segments[1]}` : '',
      ]
        .filter(Boolean)
        .join(' ');
    }
    const splitCandidates = text.match(/split candidates=([0-9,]+)/);
    if (splitCandidates) {
      return `split candidates=${splitCandidates[1]}`;
    }
    const bucketSplits = text.match(/bucket splits=([0-9,]+)/);
    if (bucketSplits) {
      return `bucket splits=${bucketSplits[1]}`;
    }
    const parts = text.split('|').map((part) => part.trim()).filter(Boolean);
    const status = parts.length > 1 ? parts[parts.length - 1] : text;
    return this._ascii(status).slice(0, 36).trim();
  }

  _clearActiveLine() {
    if (
      !this._lineActive ||
      typeof process.stdout.clearLine !== 'function' ||
      typeof process.stdout.cursorTo !== 'function'
    ) {
      return;
    }
    process.stdout.cursorTo(0);
    process.stdout.clearLine(0);
    this._lineActive = false;
  }

  _maxLineLength() {
    const columns =
      process.stdout && Number.isFinite(process.stdout.columns)
        ? Math.floor(process.stdout.columns)
        : 0;
    return columns > 1 ? columns - 1 : 120;
  }

  _fitAsciiLine(text) {
    const maxLineLength = this._maxLineLength();
    const line = this._ascii(text);
    if (line.length <= maxLineLength) {
      return line;
    }
    if (maxLineLength <= 3) {
      return line.slice(0, maxLineLength);
    }
    return `${line.slice(0, maxLineLength - 3)}...`;
  }

  _ascii(text) {
    return String(text)
      .replace(/[\r\n]+/g, ' ')
      .replace(/[^\x20-\x7e]/g, '?');
  }

  _attachResizeListener() {
    if (
      !this.enabled ||
      this._resizeListenerAttached ||
      !process.stdout ||
      typeof process.stdout.on !== 'function'
    ) {
      return;
    }
    process.stdout.on('resize', this._onResize);
    this._resizeListenerAttached = true;
  }

  _detachResizeListener() {
    if (
      !this._resizeListenerAttached ||
      !process.stdout ||
      typeof process.stdout.removeListener !== 'function'
    ) {
      return;
    }
    process.stdout.removeListener('resize', this._onResize);
    this._resizeListenerAttached = false;
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

module.exports = {
  SOURCE_REPOSITORY,
  ConsoleProgressBar,
  SpzContentWorkerPool,
  computeBounds,
  computeThreeSigmaAabbDiagonalRadiusAt,
  computeThreeSigmaAabbDiagonalRadius,
  chooseGridDims,
  normalizeSplatTargetCount,
  constrainTargetSplatCount,
  percent95,
  planSimplifyCloudVoxel,
  samplingDivisorForDepth,
  geometricErrorScaleForDepth,
  rootGeometricErrorFromMinLevel,
  writeThreeSigmaExtentComponents,
};
