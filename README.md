<div align="center">

# 3DGS-PLY-3DTiles-Converter

**Convert Gaussian Splatting PLY files into 3D Tiles with SPZ-compressed GLB content.**

[![npm version](https://img.shields.io/npm/v/3dgs-ply-3dtiles-converter)](https://www.npmjs.com/package/3dgs-ply-3dtiles-converter)
[![CI](https://github.com/WilliamLiu-1997/3DGS-PLY-3DTiles-Converter/actions/workflows/ci.yml/badge.svg)](https://github.com/WilliamLiu-1997/3DGS-PLY-3DTiles-Converter/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

<img src="https://raw.githubusercontent.com/WilliamLiu-1997/3DGS-PLY-3DTiles-Converter/main/3DGS-PLY-3DTiles-Converter.png" alt="3DGS-PLY-3DTiles-Converter" width="960" />

</div>

Node.js CLI and library for converting GraphDECO or KHR-native Gaussian Splatting PLY files into explicit 3D Tiles. The converter writes SPZ-compressed GLB tile content, uses a temp-file-backed pipeline for large inputs, and supports optional geospatial placement through a root transform or WGS84 coordinate.

## Install

```bash
npm install 3dgs-ply-3dtiles-converter
```

Requires Node.js 14.14 or newer.

## CLI

```bash
3dgs-ply-3dtiles-converter [options] <input.ply> <output_dir>
```

Example:

```bash
3dgs-ply-3dtiles-converter scene.ply out_tiles
```

You can also run it without installing globally:

```bash
npx 3dgs-ply-3dtiles-converter scene.ply out_tiles
```

From a cloned repository:

```bash
node ./bin/3dgs-ply-3dtiles-converter.js scene.ply out_tiles
```

Self-test:

```bash
3dgs-ply-3dtiles-converter --self-test out_self_test --no-open-inspector
```

By default, conversion removes the existing `output_dir` before rebuilding and opens the generated tileset in `3dtiles-inspector` after success. Use `--continue` to resume from a preserved failed workspace, and use `--no-open-inspector` for batch or CI runs.

## Output

Generated output includes:

- `tileset.json` - compact explicit 3D Tiles tileset.
- `build_summary.json` - compact conversion metadata, timings, memory plan, checkpoint state, tiling metadata, and placement fields.
- `tiles/{level}/{x}/{y}/{z}.glb` - SPZ-compressed tile content.

Generated `tileset.json` files declare top-level `3DTILES_content_gltf` extension metadata so CesiumJS can detect `KHR_gaussian_splatting` and `KHR_gaussian_splatting_compression_spz_2` content.

## API

```js
const { convert } = require('3dgs-ply-3dtiles-converter');

(async () => {
  const result = await convert('data/scene.ply', './out/tileset', {
    memoryBudget: 4,
    maxDepth: 8,
    leafLimit: 100,
    openInspector: false,
  });

  console.log(result.outputDir, result.splatCount);
})();
```

`convert(inputPath, outputDir, options)` returns:

| Field        | Description                                                                                                               |
| ------------ | ------------------------------------------------------------------------------------------------------------------------- |
| `inputPath`  | Absolute input PLY path.                                                                                                  |
| `outputDir`  | Absolute output directory.                                                                                                |
| `splatCount` | Parsed splat count.                                                                                                       |
| `shDegree`   | Inferred spherical-harmonics degree.                                                                                      |
| `args`       | Normalized conversion arguments. If `coordinate` is provided, `args.transform` contains the generated ENU root transform. |

The library API accepts the same option names as the CLI, using camelCase fields. For example, `--memory-budget 4` maps to `{ memoryBudget: 4 }`.

## Options

| Area | CLI | API | Default | Notes |
| ---- | --- | --- | ------- | ----- |
| Input convention | `--input-convention <value>` | `inputConvention` | `graphdeco` | Use `graphdeco` or `khr_native`; controls quaternion interpretation and opacity mapping. |
| Linear scale input | `--linear-scale-input` | `linearScaleInput` | `false` | Converts linear scale values to log scale. |
| Color space | `--color-space <value>` | `colorSpace` | `srgb_rec709_display` | Use `lin_rec709_display` or `srgb_rec709_display`; written to tileset extension metadata. |
| Tiling depth | `--max-depth <int>` | `maxDepth` | `8` | Maximum logical k-d tree LOD depth. |
| Leaf size | `--leaf-limit <int>` | `leafLimit` | `100` | Max splats per leaf before splitting stops. |
| Geometric error floor | `--min-geometric-error <number>` | `minGeometricError` | `null` | Minimum geometric error for the deepest emitted level. |
| SPZ quantization | `--spz-sh1-bits <1..8>` and `--spz-sh-rest-bits <1..8>` | `spzSh1Bits`, `spzShRestBits` | `8`, `8` | SH coefficient quantization bits. |
| SPZ compression | `--spz-compression-level <0..9>` | `spzCompressionLevel` | `8` | gzip compression level for SPZ payloads. |
| Placement matrix | `--transform <json_matrix4>` | `transform` | `null` | Writes `tileset.root.transform` directly. |
| Placement coordinate | `--coordinate <json_[lat,long,height]>` | `coordinate` | `null` | Generates an ENU root transform from WGS84 degrees/meters. |
| LOD sampling | `--sampling-rate-per-level <0..1]` | `samplingRatePerLevel` | `0.5` | Sampling ratio between LOD levels. |
| Sampling mode | `--sample-mode <value>` | `sampleMode` | `merge` | Use `sample` or `merge`; `sample` keeps representatives and `merge` merges assigned splats. |
| Memory budget | `--memory-budget <gb>` | `memoryBudget` | `2` | Sizes scan buffers, bucket buffers, simplify scratch space, write concurrency, and workers. |
| Bounds mode | `--obb` or `--aabb` | `orientedBoundingBoxes` | `true` | Emits root-PCA OBB bounds by default; `--aabb` uses axis-aligned bounds and split planes. |
| Inspector | `--open-inspector` or `--no-open-inspector` | `openInspector` | `true` | Opens the generated tileset in `3dtiles-inspector` after success. |
| Self-test count | `--self-test-count <int>` | `selfTestCount` | `1000000` | Number of synthetic splats when using `--self-test`. |
| Output cleanup | `--clean` or `--continue` | `clean` | `true` | `--continue` preserves the output directory and resumes a failed checkpoint. |

Use `--help` to print the CLI usage text.

## Tiling and Performance

The converter always writes explicit 3D Tiles. It builds a visual-cost-balanced k-d tree, uses root-PCA oriented bounding boxes by default, and falls back to AABB behavior when `--aabb` is set.

Large PLY files are processed through a temp-file-backed pipeline. The pipeline streams PLY records into leaf and handoff buckets, builds parent LODs bottom-up, and derives internal concurrency from `memoryBudget`. Successful conversions remove the temp workspace. Failed conversions preserve it so a later run with `--continue` can reuse checkpoints.

## Global Placement

When the tileset needs geospatial placement, use one placement option:

- `--coordinate "[lat,long,height]"` or `{ coordinate: [lat, long, height] }` anchors the tileset origin at a WGS84 coordinate and generates a standard ENU transform.
- `--transform "[...16 numbers...]"` or `{ transform: [...] }` writes `tileset.root.transform` directly.

`transform` is interpreted in final 3D Tiles tile coordinates, not raw glTF Y-up node space. The old `--source-up-axis` / `sourceUpAxis` option is not supported.

Examples:

```bash
3dgs-ply-3dtiles-converter scene.ply out_tiles --coordinate "[31.2304,121.4737,30]" --no-open-inspector
```

```js
await convert('scene.ply', './out_tiles', {
  coordinate: [31.2304, 121.4737, 30],
  openInspector: false,
});
```

## Repository Layout

- `src/index.js` - package export.
- `src/cli.js` - CLI runner.
- `src/args.js` - CLI/API argument parsing and validation.
- `src/parser.js` - PLY streaming helpers and self-test data.
- `src/codec.js` - SPZ encoding and worker payload helpers.
- `src/gltf.js` - GLB assembly.
- `src/builder.js` - shared simplify planning, progress, and worker-pool utilities.
- `src/partitioned-ply.js` - temp-file-backed conversion pipeline.
- `src/convert-core.js` - top-level conversion wiring and worker entry.
- `bin/3dgs-ply-3dtiles-converter.js` - npm binary entry.

## Errors

Conversion failures throw `ConversionError`. Common causes include invalid option values, missing input/output paths, unsupported PLY fields, or missing required Gaussian PLY properties.
