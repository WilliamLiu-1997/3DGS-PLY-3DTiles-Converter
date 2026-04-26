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

## Source Coordinates

The converter inspects PLY header comments for explicit coordinate metadata. PLYs with projected/geospatial comments such as `epsg` or `offsetx/offsety/offsetz` are treated as source Z-up data. PLYs without usable coordinate-system metadata keep the default GraphDECO/COLMAP camera-style basis: +Y down and +Z forward.

The resolved source coordinate system is written to `build_summary.json` as `source_coordinate_system`, with source and reason fields for auditability.

## API

```js
const { convert } = require('3dgs-ply-3dtiles-converter');

(async () => {
  const result = await convert('data/scene.ply', './out/tileset', {
    memoryBudget: 4,
    maxDepth: 8,
    tileRefinement: 2,
    leafLimit: 1000,
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

| Area                  | CLI                                                     | API                           | Default               | Notes                                                                                       |
| --------------------- | ------------------------------------------------------- | ----------------------------- | --------------------- | ------------------------------------------------------------------------------------------- |
| Input convention      | `--input-convention <value>`                            | `inputConvention`             | `graphdeco`           | Use `graphdeco` or `khr_native`; controls quaternion interpretation and opacity mapping.    |
| Linear scale input    | `--linear-scale-input`                                  | `linearScaleInput`            | `false`               | Converts linear scale values to log scale.                                                  |
| Color space           | `--color-space <value>`                                 | `colorSpace`                  | `srgb_rec709_display` | Use `lin_rec709_display` or `srgb_rec709_display`; written to tileset extension metadata.   |
| Tiling depth          | `--max-depth <int>`                                     | `maxDepth`                    | `8`                   | Maximum tree LOD depth.                                                                     |
| Root tile refinement  | `--tile-refinement <int>`                               | `tileRefinement`              | `2`                   | Higher values produce more, smaller tiles.                                                  |
| Leaf size             | `--leaf-limit <int>`                                    | `leafLimit`                   | `1000`                | Target splat-count limit for leaf tiles.                                                    |
| Split midpoint bias   | `--split-midpoint-penalty <number>`                     | `splitMidpointPenalty`        | `0.5`                 | Higher values prefer split planes closer to the projection midpoint.                        |
| Split count balance   | `--split-count-balance-penalty <number>`                | `splitCountBalancePenalty`    | `0.1`                 | Higher values prefer more even splat counts across child tiles.                             |
| Geometric error floor | `--min-geometric-error <number>`                        | `minGeometricError`           | `null`                | Minimum geometric error for the deepest emitted level.                                      |
| SPZ quantization      | `--spz-sh1-bits <1..8>` and `--spz-sh-rest-bits <1..8>` | `spzSh1Bits`, `spzShRestBits` | `8`, `8`              | SH coefficient quantization bits.                                                           |
| SPZ compression       | `--spz-compression-level <0..9>`                        | `spzCompressionLevel`         | `8`                   | gzip compression level for SPZ payloads.                                                    |
| Placement matrix      | `--transform <json_matrix4>`                            | `transform`                   | `null`                | Writes `tileset.root.transform` directly.                                                   |
| Placement coordinate  | `--coordinate <json_[lat,long,height]>`                 | `coordinate`                  | `null`                | Generates an ENU root transform from WGS84 degrees/meters.                                  |
| LOD sampling          | `--sampling-rate-per-level <0..1]`                      | `samplingRatePerLevel`        | `0.5`                 | Sampling ratio between LOD levels.                                                          |
| Sampling mode         | `--sample-mode <value>`                                 | `sampleMode`                  | `merge`               | Use `sample` or `merge`; `sample` keeps representatives and `merge` merges assigned splats. |
| Memory budget         | `--memory-budget <gb>`                                  | `memoryBudget`                | `2`                   | Sizes scan buffers, bucket buffers, simplify scratch space, write concurrency, and workers. |
| Bounds mode           | `--obb` or `--aabb`                                     | `orientedBoundingBoxes`       | `false`               | Emits AABB bounds and split planes by default; `--obb` enables root-PCA oriented bounds.    |
| Inspector             | `--open-inspector` or `--no-open-inspector`             | `openInspector`               | `true`                | Opens the generated tileset in `3dtiles-inspector` after success.                           |
| Self-test count       | `--self-test-count <int>`                               | `selfTestCount`               | `1000000`             | Number of synthetic splats when using `--self-test`.                                        |
| Output cleanup        | `--clean` or `--continue`                               | `clean`                       | `true`                | `--continue` preserves the output directory and resumes a failed checkpoint.                |

Use `--help` to print the CLI usage text.

## Tiling and Performance

The converter always writes explicit 3D Tiles. It builds a volume-aware k-d tree with AABB bounds and split planes by default: each split divides candidate axes into 256 equal segments, tests each internal boundary, and chooses the lowest score from normalized child tile volume sum plus configurable midpoint-distance and splat-count balance penalties. When the longest axis is less than 2x the second-longest axis, the second axis is tested in the same pass, but its score is multiplied by `sqrt(longest / second)` so the primary axis keeps a proportional preference. Long, thin non-root tiles use the same scoring once as a virtual split. After each k-d split, current-LOD leaf tiles whose volume is more than 3x the median volume at the same logical depth get one extra virtual split at that same logical LOD. Use `--obb` to enable root-PCA oriented bounding boxes and root-basis split planes.

Use `--tile-refinement 2` when the first tile level should be finer: the root performs two initial k-d split rounds and emits up to four direct child tiles while keeping those children at logical depth 1. Higher integer values continue the same pattern.

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
