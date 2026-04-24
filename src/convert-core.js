#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { isMainThread, parentPort } = require('worker_threads');

const {
  ConversionError,
  GaussianCloud,
  makeSelfTestCloud,
  writeGraphdecoLikePly,
} = require('./parser');

const { packCloudToSpz } = require('./codec');

const { GltfBuilder } = require('./gltf');

const {
  usage,
  parseArgs,
  makeConversionArgs,
} = require('./args');

const {
  convertPartitionedPlyTo3DTiles,
  _writeBucketGlbTaskOutput,
  _writeSimplifiedBucketGlbTaskOutput,
} = require('./partitioned-ply');

async function openTilesetInspector(outputDir) {
  const tilesetPath = path.join(outputDir, 'tileset.json');
  const { runInspector } = require('3dtiles-inspector');
  console.log(`[info] opening inspector: ${tilesetPath}`);
  await runInspector(tilesetPath);
}

async function convertPlyTo3DTiles(inputPath, outputDir, options = {}) {
  const args = makeConversionArgs(inputPath, outputDir, options, {
    requireInput: true,
  });
  const inPath = path.resolve(args.input);
  const outPath = path.resolve(args.output);
  const result = await convertPartitionedPlyTo3DTiles(
    inPath,
    outPath,
    args,
  );
  if (args.openInspector) {
    await openTilesetInspector(outPath);
  }
  return {
    inputPath: inPath,
    outputDir: outPath,
    splatCount: result.splatCount,
    shDegree: result.shDegree,
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
    { compressionLevel: task.compressionLevel },
  );
  const builder = new GltfBuilder();
  builder.writeSpzStreamGlb(
    task.outPath,
    spzBytes,
    cloud,
    task.colorSpace,
    translation,
  );
}

async function runPackSpzWorkerTask(task) {
  const cloud = cloudFromWorkerTask(task);
  writeCloudGlbTaskOutput(task, cloud, task.translation);
  return true;
}

async function runWorkerTask(task) {
  if (!task || !task.kind) {
    throw new ConversionError('Missing worker task kind.');
  }
  if (task.kind === 'pack-spz') {
    return runPackSpzWorkerTask(task);
  }
  if (task.kind === 'pack-bucket-spz') {
    return _writeBucketGlbTaskOutput(task);
  }
  if (task.kind === 'simplify-bucket-spz') {
    return _writeSimplifiedBucketGlbTaskOutput(task);
  }
  throw new ConversionError(`Unknown worker task kind: ${task.kind}`);
}

if (!isMainThread && parentPort) {
  parentPort.on('message', async (msg) => {
    if (!msg || msg.type !== 'worker-task') {
      parentPort.postMessage({ error: 'Unknown worker message type.' });
      return;
    }
    try {
      parentPort.postMessage({ result: await runWorkerTask(msg.task) });
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
      fs.mkdirSync(outDir, { recursive: true });
      writeGraphdecoLikePly(samplePlyPath, cloud);
      const buildArgs = { ...args };
      buildArgs.clean = false;
      await convertPartitionedPlyTo3DTiles(samplePlyPath, outDir, buildArgs);
      console.log(`[ok] self-test completed: ${outDir}`);
      console.log(`[ok] sample input PLY: ${samplePlyPath}`);
      if (args.openInspector) {
        await openTilesetInspector(outDir);
      }
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
    const result = await convertPartitionedPlyTo3DTiles(
      inPath,
      outDir,
      args,
    );
    console.log(
      `[info] parsed PLY | splats=${result.splatCount} | sh_degree=${result.shDegree}`,
    );
    console.log(`[ok] output completed: ${outDir}`);
    if (args.openInspector) {
      await openTilesetInspector(outDir);
    }
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
  main,
  convertPlyTo3DTiles,
};
