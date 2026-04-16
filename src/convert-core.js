#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { isMainThread, parentPort } = require('worker_threads');

const {
  ConversionError,
  GaussianCloud,
  Bounds,
  TileNode,
  parseCommonGaussianPly,
  makeSelfTestCloud,
  writeGraphdecoLikePly,
  deserializeBounds,
  serializeTileNode,
} = require('./parser');

const { packCloudToSpz } = require('./codec');

const { GltfBuilder } = require('./gltf');

const {
  usage,
  parseArgs,
  makeConversionArgs,
  normalizeToInt,
  normalizeToFloat,
} = require('./args');

const {
  computeBounds,
  simplifyCloudVoxel,
  buildSubtreeNodeLocal,
  OctreeTilesBuilder,
  buildSubtreeArtifact,
  writeSubtreeFile,
  buildTilesetFromCloud,
} = require('./builder');

async function convertPlyTo3DTiles(inputPath, outputDir, options = {}) {
  const args = makeConversionArgs(inputPath, outputDir, options, {
    requireInput: true,
  });
  const inPath = path.resolve(args.input);
  const outPath = path.resolve(args.output);
  const cloud = parseCommonGaussianPly(
    inPath,
    args.inputConvention,
    args.colorSpace,
    args.linearScaleInput,
  );
  await buildTilesetFromCloud(cloud, outPath, args);
  return {
    inputPath: inPath,
    outputDir: outPath,
    splatCount: cloud.length,
    shDegree: cloud.shDegree,
    args,
  };
}

async function convertCloudTo3DTiles(cloud, outputDir, options = {}) {
  const args = makeConversionArgs(null, outputDir, options, {
    requireInput: false,
  });
  const outPath = path.resolve(args.output);
  await buildTilesetFromCloud(cloud, outPath, args);
  return {
    outputDir: outPath,
    splatCount: cloud.length,
    shDegree: cloud.shDegree,
    args,
  };
}

function cloudFromWorkerTask(task) {
  const cloud = new GaussianCloud(
    new Float32Array(task.cloud.positions),
    new Float32Array(task.cloud.scaleLog),
    new Float32Array(task.cloud.quatsXYZW),
    new Float32Array(task.cloud.opacity),
    new Float32Array(task.cloud.shCoeffs),
    task.cloud.color0 ? new Float32Array(task.cloud.color0) : null,
  );
  cloud._shDegree = task.cloud.shDegree;
  return cloud;
}

function writeCloudGlbTaskOutput(task, cloud, translation) {
  const spzBytes = packCloudToSpz(
    cloud,
    task.sh1Bits,
    task.shRestBits,
    translation,
  );
  const builder = new GltfBuilder();
  builder.writeSpzStreamGlb(
    task.outPath,
    spzBytes,
    cloud,
    task.colorSpace,
    translation,
    task.sourceUpAxis,
  );
}

function runPackSpzWorkerTask(task) {
  const cloud = cloudFromWorkerTask(task);
  writeCloudGlbTaskOutput(task, cloud, task.translation);
  return true;
}

function runSimplifyPackSpzWorkerTask(task) {
  const cloud = cloudFromWorkerTask(task);
  const bounds = task.cellBounds ? deserializeBounds(task.cellBounds) : null;
  const [lodCloud] = simplifyCloudVoxel(cloud, task.targetCount, bounds);
  const translation = computeBounds(lodCloud).center();
  writeCloudGlbTaskOutput(task, lodCloud, translation);
  return true;
}

let cachedSubtreeNodeTableId = null;
let cachedSubtreeNodeLookup = null;

function getSubtreeNodeLookup(task) {
  if (
    cachedSubtreeNodeTableId === task.nodeTableId &&
    cachedSubtreeNodeLookup
  ) {
    return cachedSubtreeNodeLookup;
  }

  const packed = new Int32Array(task.nodeTableBuffer);
  const lookup = new Set();
  for (let i = 0; i < task.nodeCount; i++) {
    const off = i * 4;
    lookup.add(
      `${packed[off]}/${packed[off + 1]}/${packed[off + 2]}/${packed[off + 3]}`,
    );
  }
  cachedSubtreeNodeTableId = task.nodeTableId;
  cachedSubtreeNodeLookup = lookup;
  return lookup;
}

function runWriteSubtreeWorkerTask(task) {
  const lookup = getSubtreeNodeLookup(task);
  const { subtree, blob } = buildSubtreeArtifact(
    task.level,
    task.x,
    task.y,
    task.z,
    task.availableLevels,
    task.subtreeLevels,
    (level, x, y, z) => lookup.has(`${level}/${x}/${y}/${z}`),
  );
  writeSubtreeFile(task.subtreePath, subtree, blob);
  return true;
}

function runBuildSubtreeWorkerTask(task) {
  const params = {
    outDir: task.outDir,
    colorSpace: task.colorSpace,
    maxDepth: task.maxDepth,
    lodMaxDepth: task.lodMaxDepth,
    leafLimit: task.leafLimit,
    spzSh1Bits: task.spzSh1Bits,
    spzShRestBits: task.spzShRestBits,
    sourceUpAxis: task.sourceUpAxis,
    samplingRatePerLevel: task.samplingRatePerLevel,
    rootGeometricError: task.rootGeometricError,
  };
  const cloud = cloudFromWorkerTask(task);
  const node = buildSubtreeNodeLocal(
    params,
    cloud,
    deserializeBounds(task.cellBounds),
    task.depth,
    task.level,
    task.x,
    task.y,
    task.z,
  );
  return { root: serializeTileNode(node) };
}

function runWorkerTask(task) {
  if (!task || !task.kind) {
    throw new ConversionError('Missing worker task kind.');
  }
  if (task.kind === 'pack-spz') {
    return runPackSpzWorkerTask(task);
  }
  if (task.kind === 'simplify-pack-spz') {
    return runSimplifyPackSpzWorkerTask(task);
  }
  if (task.kind === 'write-subtree') {
    return runWriteSubtreeWorkerTask(task);
  }
  if (task.kind === 'build-subtree') {
    return runBuildSubtreeWorkerTask(task);
  }
  throw new ConversionError(`Unknown worker task kind: ${task.kind}`);
}

if (!isMainThread && parentPort) {
  parentPort.on('message', (msg) => {
    if (!msg || msg.type !== 'worker-task') {
      parentPort.postMessage({ error: 'Unknown worker message type.' });
      return;
    }
    try {
      parentPort.postMessage({ result: runWorkerTask(msg.task) });
    } catch (err) {
      parentPort.postMessage({
        error: err instanceof Error ? err.message : String(err),
      });
    }
  });
}

async function main(argv) {
  try {
    const args = parseArgs(argv);
    if (args.help) {
      console.log(usage());
      return 0;
    }

    const outDir = path.resolve(args.output);

    if (args.selfTest) {
      console.log(
        `[info] generating self-test cloud | splats=${args.selfTestCount}`,
      );
      const cloud = makeSelfTestCloud(args.selfTestCount);
      const samplePlyPath = path.join(outDir, '_self_test_input_graphdeco.ply');
      if (fs.existsSync(outDir) && args.clean) {
        fs.rmSync(outDir, { recursive: true, force: true });
      }
      const buildArgs = { ...args };
      buildArgs.clean = false;
      await buildTilesetFromCloud(cloud, outDir, buildArgs);
      writeGraphdecoLikePly(
        samplePlyPath,
        makeSelfTestCloud(args.selfTestCount),
      );
      console.log(`[ok] self-test completed: ${outDir}`);
      console.log(`[ok] sample input PLY: ${samplePlyPath}`);
      return 0;
    }

    if (!args.input) {
      throw new ConversionError(
        'Please provide an input PLY file, or use --self-test.',
      );
    }
    if (!fs.existsSync(args.input)) {
      throw new ConversionError(`Input file does not exist: ${args.input}`);
    }

    const inPath = path.resolve(args.input);
    console.log(`[info] reading PLY: ${inPath}`);
    const cloud = parseCommonGaussianPly(
      inPath,
      args.inputConvention,
      args.colorSpace,
      args.linearScaleInput,
    );
    console.log(
      `[info] parsed PLY | splats=${cloud.length} | sh_degree=${cloud.shDegree}`,
    );
    await buildTilesetFromCloud(cloud, outDir, args);
    console.log(`[ok] output completed: ${outDir}`);
    return 0;
  } catch (err) {
    if (err instanceof ConversionError) {
      console.error(`Conversion failed: ${err.message}`);
    } else if (err != null) {
      console.error(err.message || String(err));
    }
    return 2;
  }
}

if (require.main === module && isMainThread) {
  main(process.argv.slice(2))
    .then((code) => process.exit(code))
    .catch((err) => {
      if (err instanceof ConversionError) {
        console.error(`Conversion failed: ${err.message}`);
      } else if (err != null) {
        console.error(err.message || String(err));
      }
      process.exit(2);
    });
}

module.exports = {
  ConversionError,
  GaussianCloud,
  Bounds,
  TileNode,
  GltfBuilder,
  OctreeTilesBuilder,
  parseCommonGaussianPly,
  makeSelfTestCloud,
  writeGraphdecoLikePly,
  buildTilesetFromCloud,
  buildSubtreeArtifact,
  parseArgs,
  usage,
  main,
  makeConversionArgs,
  convertPlyTo3DTiles,
  convertCloudTo3DTiles,
  normalizeToInt,
  normalizeToFloat,
};
