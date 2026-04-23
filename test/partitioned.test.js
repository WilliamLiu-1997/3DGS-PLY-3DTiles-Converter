const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');

const {
  convert,
  parseCommonGaussianPly,
  makeSelfTestCloud,
  writeGraphdecoLikePly,
} = require('../src');
const { GltfBuilder } = require('../src/gltf');

function rmrf(targetPath) {
  fs.rmSync(targetPath, { recursive: true, force: true });
}

function approxEqual(lhs, rhs, epsilon = 1e-5) {
  return Math.abs(lhs - rhs) <= epsilon;
}

function writeAsciiGraphdecoLikePly(filePath, cloud) {
  const lines = [
    'ply',
    'format ascii 1.0',
    `element vertex ${cloud.length}`,
    'property float x',
    'property float y',
    'property float z',
    'property float nx',
    'property float ny',
    'property float nz',
    'property float f_dc_0',
    'property float f_dc_1',
    'property float f_dc_2',
    'property float opacity',
    'property float scale_0',
    'property float scale_1',
    'property float scale_2',
    'property float rot_0',
    'property float rot_1',
    'property float rot_2',
    'property float rot_3',
    'end_header',
  ];

  for (let i = 0; i < cloud.length; i++) {
    const base3 = i * 3;
    const base4 = i * 4;
    const coeffBase = i * 3;
    const opacity = Math.min(1.0 - 1e-7, Math.max(1e-7, cloud.opacity[i]));
    const opacityLogit = Math.log(opacity / (1.0 - opacity));
    lines.push(
      [
        cloud.positions[base3 + 0].toPrecision(9),
        cloud.positions[base3 + 1].toPrecision(9),
        cloud.positions[base3 + 2].toPrecision(9),
        '0',
        '0',
        '0',
        cloud.shCoeffs[coeffBase + 0].toPrecision(9),
        cloud.shCoeffs[coeffBase + 1].toPrecision(9),
        cloud.shCoeffs[coeffBase + 2].toPrecision(9),
        opacityLogit.toPrecision(9),
        cloud.scaleLog[base3 + 0].toPrecision(9),
        cloud.scaleLog[base3 + 1].toPrecision(9),
        cloud.scaleLog[base3 + 2].toPrecision(9),
        cloud.quatsXYZW[base4 + 3].toPrecision(9),
        cloud.quatsXYZW[base4 + 0].toPrecision(9),
        cloud.quatsXYZW[base4 + 1].toPrecision(9),
        cloud.quatsXYZW[base4 + 2].toPrecision(9),
      ].join(' '),
    );
  }

  fs.writeFileSync(filePath, `${lines.join('\n')}\n`, 'utf8');
}

async function main() {
  const tempRoot = fs.mkdtempSync(
    path.join(os.tmpdir(), '3dgs-ply-3dtiles-converter-'),
  );

  try {
    const cloud = makeSelfTestCloud(8192);
    const binaryPath = path.join(tempRoot, 'scene-binary.ply');
    const asciiPath = path.join(tempRoot, 'scene-ascii.ply');
    writeGraphdecoLikePly(binaryPath, cloud);
    writeAsciiGraphdecoLikePly(asciiPath, cloud);

    const parsedBinary = await parseCommonGaussianPly(
      binaryPath,
      'graphdeco',
      'srgb_rec709_display',
      false,
    );
    const parsedAscii = await parseCommonGaussianPly(
      asciiPath,
      'graphdeco',
      'srgb_rec709_display',
      false,
    );

    assert.strictEqual(parsedBinary.length, cloud.length);
    assert.strictEqual(parsedAscii.length, cloud.length);
    assert.strictEqual(parsedBinary.shDegree, cloud.shDegree);
    assert.strictEqual(parsedAscii.shDegree, cloud.shDegree);
    assert.ok(approxEqual(parsedBinary.positions[0], cloud.positions[0]));
    assert.ok(approxEqual(parsedBinary.scaleLog[1], cloud.scaleLog[1]));
    assert.ok(approxEqual(parsedBinary.opacity[2], cloud.opacity[2]));
    assert.ok(approxEqual(parsedAscii.positions[0], cloud.positions[0], 1e-4));
    assert.ok(approxEqual(parsedAscii.scaleLog[1], cloud.scaleLog[1], 1e-4));
    assert.ok(approxEqual(parsedAscii.opacity[2], cloud.opacity[2], 1e-4));

    const partitionedOut = path.join(tempRoot, 'out-partitioned');
    await convert(binaryPath, partitionedOut, {
      plyBuildMode: 'partitioned',
      maxDepth: 4,
      leafLimit: 256,
      buildConcurrency: 1,
      contentWorkers: 0,
      clean: true,
    });
    const partitionedSummary = JSON.parse(
      fs.readFileSync(
        path.join(partitionedOut, 'build_summary.json'),
        'utf8',
      ),
    );
    assert.strictEqual(partitionedSummary.ply_build_mode, 'partitioned');
    assert.strictEqual(partitionedSummary.build_concurrency, 1);
    assert.strictEqual(
      partitionedSummary.partitioned_handoff_encoding,
      'canonical32',
    );
    assert.strictEqual(partitionedSummary.checkpoint_reused, false);
    assert.ok(
      !fs.existsSync(path.join(partitionedOut, '.tmp-ply-partitions')),
      'partitioned temp workspace should be removed after success',
    );

    const entireOut = path.join(tempRoot, 'out-entire');
    await convert(binaryPath, entireOut, {
      plyBuildMode: 'entire',
      maxDepth: 4,
      leafLimit: 256,
      buildConcurrency: 1,
      contentWorkers: 0,
      clean: true,
    });
    const entireSummary = JSON.parse(
      fs.readFileSync(path.join(entireOut, 'build_summary.json'), 'utf8'),
    );
    assert.strictEqual(entireSummary.ply_build_mode, 'entire');
    assert.strictEqual(entireSummary.build_concurrency, 1);
    assert.ok(entireSummary.node_count > 1);

    const resumeOut = path.join(tempRoot, 'out-resume');
    const originalWrite = GltfBuilder.prototype.writeSpzStreamGlb;
    let injectedFailure = true;
    GltfBuilder.prototype.writeSpzStreamGlb = function patchedWrite(...args) {
      if (injectedFailure) {
        injectedFailure = false;
        throw new Error('test injected failure after partition');
      }
      return originalWrite.apply(this, args);
    };

    let resumeFailed = false;
    try {
      await convert(binaryPath, resumeOut, {
        plyBuildMode: 'partitioned',
        maxDepth: 4,
        leafLimit: 256,
        buildConcurrency: 1,
        contentWorkers: 0,
        clean: true,
      });
    } catch (err) {
      resumeFailed = true;
      assert.match(
        String(err && err.message ? err.message : err),
        /test injected failure after partition/,
      );
    } finally {
      GltfBuilder.prototype.writeSpzStreamGlb = originalWrite;
    }
    assert.ok(resumeFailed, 'expected injected conversion failure');
    assert.ok(
      fs.existsSync(
        path.join(resumeOut, '.tmp-ply-partitions', 'pipeline-state.json'),
      ),
      'checkpoint state should be preserved on failure',
    );

    await convert(binaryPath, resumeOut, {
      plyBuildMode: 'partitioned',
      maxDepth: 4,
      leafLimit: 256,
      buildConcurrency: 1,
      contentWorkers: 0,
    });
    const resumeSummary = JSON.parse(
      fs.readFileSync(path.join(resumeOut, 'build_summary.json'), 'utf8'),
    );
    assert.strictEqual(resumeSummary.checkpoint_reused, true);
    assert.strictEqual(resumeSummary.build_concurrency, 1);
    assert.ok(
      !fs.existsSync(path.join(resumeOut, '.tmp-ply-partitions')),
      'checkpoint workspace should be removed after resumed success',
    );

    console.log('ok');
  } finally {
    rmrf(tempRoot);
  }
}

main().catch((err) => {
  console.error(err && err.stack ? err.stack : String(err));
  process.exit(1);
});
