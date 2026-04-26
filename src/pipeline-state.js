const fs = require('fs');
const path = require('path');

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

module.exports = {
  fingerprintsMatch,
  makeEmptyPipelineState,
  makePipelineFingerprint,
  pathExists,
  readPipelineState,
  removeFileIfExists,
};
