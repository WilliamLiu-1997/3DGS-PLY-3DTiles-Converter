const fs = require('fs');
const path = require('path');

const { Bounds } = require('./parser');

async function pathExists(targetPath) {
  if (!targetPath) {
    return false;
  }
  try {
    await fs.promises.access(targetPath);
    return true;
  } catch {
    return false;
  }
}

async function removeFileIfExists(filePath) {
  if (!filePath) {
    return;
  }
  try {
    await fs.promises.unlink(filePath);
  } catch (err) {
    if (!err || err.code !== 'ENOENT') {
      throw err;
    }
  }
}

function makePipelineFingerprint(
  inputPath,
  inputStat,
  args,
  sourceCoordinateSystem,
  defaultSourceCoordinateSystem,
) {
  return {
    inputPath: path.resolve(inputPath),
    inputSize: inputStat.size,
    inputMtimeMs: inputStat.mtimeMs,
    inputConvention: args.inputConvention,
    linearScaleInput: args.linearScaleInput,
    sourceCoordinateSystem:
      sourceCoordinateSystem || defaultSourceCoordinateSystem,
    maxDepth: args.maxDepth,
    tileRefinement: args.tileRefinement,
    leafLimit: args.leafLimit,
    splitMidpointPenalty: args.splitMidpointPenalty,
    splitCountBalancePenalty: args.splitCountBalancePenalty,
    orientedBoundingBoxes: args.orientedBoundingBoxes === true,
    samplingRatePerLevel: args.samplingRatePerLevel,
    sampleMode: args.sampleMode,
    minGeometricError: args.minGeometricError,
    colorSpace: args.colorSpace,
    spzSh1Bits: args.spzSh1Bits,
    spzShRestBits: args.spzShRestBits,
    spzCompressionLevel: args.spzCompressionLevel,
  };
}

function fingerprintsMatch(lhs, rhs) {
  return JSON.stringify(lhs) === JSON.stringify(rhs);
}

function serializeBoundsState(bounds) {
  if (!bounds) {
    return null;
  }
  return {
    minimum: bounds.minimum.slice(),
    maximum: bounds.maximum.slice(),
  };
}

function deserializeBoundsState(value) {
  if (!value) {
    return null;
  }
  return new Bounds(value.minimum.slice(), value.maximum.slice());
}

function makeEmptyPipelineState(version, fingerprint) {
  return {
    version,
    fingerprint,
    stage: 'init',
    rootBounds: null,
    layout: null,
    rootNode: null,
    updatedAt: null,
  };
}

async function readPipelineState(tempDir, stateFile, bucketedStage) {
  const statePath = path.join(tempDir, stateFile);
  if (!(await pathExists(statePath))) {
    return null;
  }
  const text = await fs.promises.readFile(statePath, 'utf8');
  const state = JSON.parse(text);
  if (state && state.stage === 'partitioned') {
    state.stage = bucketedStage;
  }
  return state;
}

function enqueuePipelineStateSave(ctx, stage = null, { force = false } = {}) {
  if (stage) {
    ctx.pipelineState.stage = stage;
  }
  const now = Date.now();
  const saveIntervalMs =
    Number.isFinite(ctx.pipelineStateSaveIntervalMs) &&
    ctx.pipelineStateSaveIntervalMs >= 0
      ? ctx.pipelineStateSaveIntervalMs
      : 5000;
  const saveNodeInterval =
    Number.isInteger(ctx.pipelineStateSaveNodeInterval) &&
    ctx.pipelineStateSaveNodeInterval > 0
      ? ctx.pipelineStateSaveNodeInterval
      : 512;
  if (!force) {
    ctx.nodesSincePipelineStateSave =
      (ctx.nodesSincePipelineStateSave || 0) + 1;
    if (
      now - (ctx.lastPipelineStateSaveAt || 0) < saveIntervalMs &&
      ctx.nodesSincePipelineStateSave < saveNodeInterval
    ) {
      return ctx.savePromise;
    }
  }
  ctx.lastPipelineStateSaveAt = now;
  ctx.nodesSincePipelineStateSave = 0;
  const targetPath = path.join(
    ctx.tempDir,
    ctx.pipelineStateFile || 'pipeline-state.json',
  );
  ctx.savePromise = (ctx.savePromise || Promise.resolve()).then(() => {
    ctx.pipelineState.version =
      ctx.pipelineStateVersion || ctx.pipelineState.version;
    ctx.pipelineState.rootBounds = serializeBoundsState(ctx.rootBounds);
    ctx.pipelineState.rootNode = ctx.serializeNodeMeta
      ? ctx.serializeNodeMeta(ctx.rootNode)
      : ctx.pipelineState.rootNode;
    ctx.pipelineState.layout = ctx.layout
      ? {
          degree: ctx.layout.degree,
          coeffCount: ctx.layout.coeffCount,
        }
      : ctx.pipelineState.layout;
    ctx.pipelineState.updatedAt = new Date().toISOString();
    return fs.promises.writeFile(
      targetPath,
      JSON.stringify(ctx.pipelineState),
      'utf8',
    );
  });
  return ctx.savePromise;
}

module.exports = {
  deserializeBoundsState,
  enqueuePipelineStateSave,
  fingerprintsMatch,
  makeEmptyPipelineState,
  makePipelineFingerprint,
  pathExists,
  readPipelineState,
  removeFileIfExists,
  serializeBoundsState,
};
