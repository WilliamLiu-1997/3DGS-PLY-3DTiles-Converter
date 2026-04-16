<div align="center">

# 3DGS-PLY-3DTiles-Converter

**Convert Gaussian Splatting PLY files into 3D Tiles with SPZ-compressed GLB content.**

[![npm version](https://img.shields.io/npm/v/3dgs-ply-3dtiles-converter)](https://www.npmjs.com/package/3dgs-ply-3dtiles-converter)
[![CI](https://github.com/WilliamLiu-1997/3DGS-PLY-3DTiles-Converter/actions/workflows/ci.yml/badge.svg)](https://github.com/WilliamLiu-1997/3DGS-PLY-3DTiles-Converter/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

<img src="https://raw.githubusercontent.com/WilliamLiu-1997/3DGS-PLY-3DTiles-Converter/main/3DGS-PLY-3DTiles-Converter.png" alt="3DGS-PLY-3DTiles-Converter" width="960" />

</div>

This package targets **Node.js** and exposes both:

- A CLI entry (`3dgs-ply-3dtiles-converter`)
- A library API (`convert`, `convertCloud`, and lower-level helpers)

## Install

```bash
npm install 3dgs-ply-3dtiles-converter
```

Requires Node.js 14.14 or newer.

## Run from CLI

```bash
3dgs-ply-3dtiles-converter [options] <input.ply> <output_dir>
3dgs-ply-3dtiles-converter --self-test <output_dir>
```

```bash
npx 3dgs-ply-3dtiles-converter [options] <input.ply> <output_dir>
```

From a cloned repository:

```bash
node ./bin/3dgs-ply-3dtiles-converter.js [options] <input.ply> <output_dir>
node ./bin/3dgs-ply-3dtiles-converter.js --self-test <output_dir>
```

Output is written under:

- `tileset.json` (main tileset root)
- `build_summary.json` (conversion metadata, including `source`)
- `tiles/{level}/{x}/{y}/{z}.glb` (tile content)
- `subtrees/{level}/{x}/{y}/{z}.subtree` (when `--tiling-mode implicit`)

## API usage

```js
const {
  convert,
  convertCloud,
  parseCommonGaussianPly,
  makeConversionArgs,
} = require('3dgs-ply-3dtiles-converter');

(async () => {
  const result = await convert('data/scene.ply', './out/tileset', {
    maxDepth: 5,
    leafLimit: 10000,
    spzSh1Bits: 8,
    spzShRestBits: 8,
    contentWorkers: 4,
  });
  console.log(result.outputDir, result.splatCount);
})();
```

```js
const {
  parseCommonGaussianPly,
  convertCloud,
} = require('3dgs-ply-3dtiles-converter');

(async () => {
  const cloud = parseCommonGaussianPly(
    'data/scene.ply',
    'graphdeco',
    'srgb_rec709_display',
    false,
  );
  const result = await convertCloud(cloud, './out/tileset', {
    tilingMode: 'implicit',
    subtreeLevels: 2,
  });
  console.log(result.outputDir, result.splatCount);
})();
```

## API return values

- `convert(...)` returns
  - `inputPath`: absolute input path
  - `outputDir`: absolute output path
  - `splatCount`: point count
  - `shDegree`: inferred SH degree
  - `args`: normalized and validated parameters
- `convertCloud(...)` returns the same object except `inputPath` is omitted.

## Parameters (CLI + API)

The library API accepts the same option names as CLI flags, with camelCase names.

Examples:

- CLI `--max-depth 5` equals API `{ maxDepth: 5 }`
- CLI `--spz-sh1-bits 6` equals API `{ spzSh1Bits: 6 }`

### Common positional args

| Type     | CLI             | API          | Description                                                                            |
| -------- | --------------- | ------------ | -------------------------------------------------------------------------------------- |
| required | `input.ply`     | `inputPath`  | Path to input PLY. Required unless `--self-test` or API helper `convertCloud` is used. |
| required | `output_dir`    | `outputDir`  | Output directory path. Required unless `--help` is used.                               |
| optional | `--help` / `-h` | `help: true` | Print help text and exit.                                                              |

### Conversion options

| Option                  | Type    | CLI flag                    | API field              | Default               | Valid range                                 | Notes                                                       |
| ----------------------- | ------- | --------------------------- | ---------------------- | --------------------- | ------------------------------------------- | ----------------------------------------------------------- |
| Input convention        | string  | `--input-convention`        | `inputConvention`      | `graphdeco`           | `graphdeco`, `khr_native`                   | Controls PLY quaternion interpretation and opacity mapping. |
| Linear scale input      | boolean | `--linear-scale-input`      | `linearScaleInput`     | `false`               | `true`/`false`                              | If enabled, converts scale values as `ln(max(v, 1e-8))`.    |
| Color space             | string  | `--color-space`             | `colorSpace`           | `srgb_rec709_display` | `lin_rec709_display`, `srgb_rec709_display` | Emitted in tileset extension metadata.                      |
| Max depth               | integer | `--max-depth`               | `maxDepth`             | `5`                   | `>= 0`                                      | Controls octree depth.                                      |
| Leaf limit              | integer | `--leaf-limit`              | `leafLimit`            | `10000`               | `>= 1`                                      | Max splat count per leaf tile before split stops.           |
| Tiling mode             | string  | `--tiling-mode`             | `tilingMode`           | `explicit`            | `explicit`, `implicit`                      | Controls whether subtree files are generated.               |
| Subtree levels          | integer | `--subtree-levels`          | `subtreeLevels`        | `2`                   | `>= 1`                                      | Grouping depth for implicit subtree files.                  |
| Min geometric error     | number  | `--min-geometric-error`     | `minGeometricError`    | `null`                | any finite number                           | Minimum geometric error for the deepest emitted level.      |
| SH1 bits                | integer | `--spz-sh1-bits`            | `spzSh1Bits`           | `8`                   | `1..8`                                      | SPZ quantization bits for DC SH coefficients.               |
| SH rest bits            | integer | `--spz-sh-rest-bits`        | `spzShRestBits`        | `8`                   | `1..8`                                      | SPZ quantization bits for higher SH coefficients.           |
| Source up-axis          | string  | `--source-up-axis`          | `sourceUpAxis`         | `z`                   | `z`, `y`                                    | Source-to-3D-tiles up-axis conversion.                      |
| Sampling rate per level | number  | `--sampling-rate-per-level` | `samplingRatePerLevel` | `0.5`                 | `(0,1]`                                     | LOD sampling ratio between levels.                          |
| Content workers         | integer | `--content-workers`         | `contentWorkers`       | `4`                   | `>= 0`                                      | Parallel SPZ/GLB workers. `0` disables worker pool.         |
| Self-test               | boolean | `--self-test`               | `selfTest`             | `false`               | `true`/`false`                              | Generates synthetic cloud and writes sample PLY.            |
| Self-test count         | integer | `--self-test-count`         | `selfTestCount`        | `6000`                | integer                                     | Number of synthetic splats.                                 |
| Clean output            | boolean | `--clean`                   | `clean`                | `false`               | `true`/`false`                              | Removes existing output directory before self-test.         |

## Utility exports

- `parseCommonGaussianPly(filePath, inputConvention, colorSpace, linearScaleInput)`  
  Parse a PLY into `GaussianCloud`.
- `GaussianCloud`  
  In-memory point-cloud class (`positions`, `scaleLog`, `quatsXYZW`, `opacity`, `shCoeffs`, `color0`).
- `makeConversionArgs(inputPath, outputDir, options, { requireInput })`  
  Build and validate normalized arguments without starting conversion.
- `parseArgs(argv)`  
  Parse CLI args array directly.
- `usage()`  
  Return CLI usage string.

## Entry files

- `src/index.js` - main package export (`module.exports`)
- `src/cli.js` - CLI runner
- `src/args.js` - CLI/API argument parsing, normalization, validation
- `src/parser.js` - cloud model, PLY parse/load, self-test data helpers
- `src/codec.js` - SPZ stream encoding and worker payload helpers
- `src/gltf.js` - GLB/gltf assembly for Gaussian Splatting content
- `src/builder.js` - octree/subtree construction and tileset writing
- `src/convert-core.js` - CLI/options, worker entry, top-level conversion wiring
- `bin/3dgs-ply-3dtiles-converter.js` - npm binary entry

## Error handling

Conversion failures throw `ConversionError`.  
Examples:

- Invalid option values
- Missing input/output paths
- Invalid PLY fields
- Missing required PLY properties
