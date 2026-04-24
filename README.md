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
- A library API (`convert`)

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

- `tileset.json` (main tileset root, written as compact single-line JSON)
- `build_summary.json` (conversion metadata, including `source`, `root_transform`, `root_coordinate`, and `root_transform_source` when global placement is used, written as compact single-line JSON)
- `tiles/{level}/{x}/{y}/{z}.glb` (tile content)
- `subtrees/{level}/{x}/{y}/{z}.subtree` (when `--tiling-mode implicit`)

Generated `tileset.json` files declare the top-level `3DTILES_content_gltf` tileset extension metadata that CesiumJS uses to detect `KHR_gaussian_splatting` and `KHR_gaussian_splatting_compression_spz_2` glTF tile content.

Large PLY conversion through `convert(...)` now uses a temp-file-backed pipeline. That path writes canonical leaf/handoff buckets, builds parent LODs from handoff data, uses exact streaming simplify so internal nodes do not need a full in-memory SH payload up front, processes each level with bounded concurrency, and automatically resumes from its temp workspace if the same output directory is rerun without `--clean`.

## API usage

```js
const { convert } = require('3dgs-ply-3dtiles-converter');

(async () => {
  const result = await convert('data/scene.ply', './out/tileset', {
    maxDepth: 5,
    leafLimit: 5000,
    spzSh1Bits: 8,
    spzShRestBits: 8,
    buildConcurrency: 4,
    contentWorkers: 4,
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
  - `args`: normalized and validated parameters; when `coordinate` is provided, `args.transform` contains the generated ENU root transform

## Parameters (CLI + API)

The library API accepts the same option names as CLI flags, with camelCase names.

Examples:

- CLI `--max-depth 5` equals API `{ maxDepth: 5 }`
- CLI `--spz-sh1-bits 6` equals API `{ spzSh1Bits: 6 }`
- CLI `--sample-mode merge` equals API `{ sampleMode: 'merge' }`
- CLI `--coordinate "[31.2304,121.4737,30]"` equals API `{ coordinate: [31.2304, 121.4737, 30] }`

### Common positional args

| Type     | CLI             | API          | Description                                               |
| -------- | --------------- | ------------ | --------------------------------------------------------- |
| required | `input.ply`     | `inputPath`  | Path to input PLY. Required unless `--self-test` is used. |
| required | `output_dir`    | `outputDir`  | Output directory path. Required unless `--help` is used.  |
| optional | `--help` / `-h` | `help: true` | Print help text and exit.                                 |

### Conversion options

| Option                  | Type    | CLI flag                    | API field              | Default               | Valid range                                 | Notes                                                                                                                                                                                                                                                             |
| ----------------------- | ------- | --------------------------- | ---------------------- | --------------------- | ------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Input convention        | string  | `--input-convention`        | `inputConvention`      | `graphdeco`           | `graphdeco`, `khr_native`                   | Controls PLY quaternion interpretation and opacity mapping.                                                                                                                                                                                                       |
| Linear scale input      | boolean | `--linear-scale-input`      | `linearScaleInput`     | `false`               | `true`/`false`                              | If enabled, converts scale values as `ln(max(v, 1e-8))`.                                                                                                                                                                                                          |
| Color space             | string  | `--color-space`             | `colorSpace`           | `srgb_rec709_display` | `lin_rec709_display`, `srgb_rec709_display` | Emitted in tileset extension metadata.                                                                                                                                                                                                                            |
| Max depth               | integer | `--max-depth`               | `maxDepth`             | `5`                   | `>= 0`                                      | Controls octree depth.                                                                                                                                                                                                                                            |
| Leaf limit              | integer | `--leaf-limit`              | `leafLimit`            | `5000`                | `>= 1`                                      | Max splat count per leaf tile before split stops.                                                                                                                                                                                                                 |
| Tiling mode             | string  | `--tiling-mode`             | `tilingMode`           | `explicit`            | `explicit`, `implicit`                      | Controls whether subtree files are generated.                                                                                                                                                                                                                     |
| Subtree levels          | integer | `--subtree-levels`          | `subtreeLevels`        | `2`                   | `>= 1`                                      | Grouping depth for implicit subtree files.                                                                                                                                                                                                                        |
| Min geometric error     | number  | `--min-geometric-error`     | `minGeometricError`    | `null`                | any finite number                           | Minimum geometric error for the deepest emitted level.                                                                                                                                                                                                            |
| SH1 bits                | integer | `--spz-sh1-bits`            | `spzSh1Bits`           | `8`                   | `1..8`                                      | SPZ quantization bits for DC SH coefficients.                                                                                                                                                                                                                     |
| SH rest bits            | integer | `--spz-sh-rest-bits`        | `spzShRestBits`        | `8`                   | `1..8`                                      | SPZ quantization bits for higher SH coefficients.                                                                                                                                                                                                                 |
| Root transform          | matrix4 | `--transform`               | `transform`            | `null`                | 4x4 JSON matrix or 16 numbers               | Writes `tileset.root.transform` directly. Nested `[[...]]` matrices are read as row-major and converted to 3D Tiles column-major storage.                                                                                                                         |
| Root coordinate         | vec3    | `--coordinate`              | `coordinate`           | `null`                | `[lat, long, height]`                       | Generates `tileset.root.transform` from WGS84 degrees/meters as a standard ENU frame in 3D Tiles tile coordinates. The API also accepts object forms such as `{ lat, lon, height }` and `{ latitude, longitude, altitude }`. Mutually exclusive with `transform`. |
| Sampling rate per level | number  | `--sampling-rate-per-level` | `samplingRatePerLevel` | `0.5`                 | `(0,1]`                                     | LOD sampling ratio between levels.                                                                                                                                                                                                                                |
| Sampling mode           | string  | `--sample-mode`             | `sampleMode`           | `merge`               | `sample`, `merge`                           | `sample` keeps representative splats; `merge` merges assigned splats into the target count. Voxel representative picks are always coarse-biased.                                                                                                                  |
| Build concurrency       | integer | `--build-concurrency`       | `buildConcurrency`     | `4`                   | `>= 1`                                      | Parallel node builds per level. Lower values reduce peak memory.                                                                                                                                                                                                  |
| Content workers         | integer | `--content-workers`         | `contentWorkers`       | `4`                   | `>= 0`                                      | Parallel SPZ/GLB workers. `0` disables worker pool.                                                                                                                                                                                                               |
| Open inspector          | boolean | `--open-inspector`          | `openInspector`        | `false`               | `true`/`false`                              | Opens the generated `tileset.json` in `3dtiles-inspector` after conversion completes. The local inspector server keeps running until stopped.                                                                                                                     |
| Self-test               | boolean | `--self-test`               | `selfTest`             | `false`               | `true`/`false`                              | Generates synthetic cloud and writes sample PLY.                                                                                                                                                                                                                  |
| Self-test count         | integer | `--self-test-count`         | `selfTestCount`        | `1000000`             | integer                                     | Number of synthetic splats.                                                                                                                                                                                                                                       |
| Clean output            | boolean | `--clean`                   | `clean`                | `false`               | `true`/`false`                              | Removes existing output directory before self-test.                                                                                                                                                                                                               |

## Global placement

Use one of the following options when the generated tileset should be geolocated:

- `transform`: Directly provide the final `tileset.root.transform`.
- `coordinate`: Provide `[lat, long, height]` in WGS84 degrees/meters and let the converter generate an ENU transform automatically.

`transform` is interpreted in final 3D Tiles tile coordinates, not raw glTF Y-up node space. The converter now always applies its built-in source normalization path internally, so `--source-up-axis` / `sourceUpAxis` is no longer supported.

Tile bounding volumes are emitted in the same 3D Tiles tile frame as content and root transforms.

`coordinate` anchors the tileset's local origin at the provided geodetic position. If you need to place another local point at that position, provide a custom `transform` instead. In the API, `coordinate` also accepts object forms such as `{ lat, lon, height }`.

Examples:

```bash
3dgs-ply-3dtiles-converter --coordinate "[31.2304,121.4737,30]" scene.ply out_tiles
```

```js
await convert('scene.ply', './out_tiles', {
  coordinate: [31.2304, 121.4737, 30],
});
```

## Pipeline notes

- The converter uses a single temp-file-backed build pipeline and is designed for multi-GB inputs.
- If a build fails, rerunning the same command against the same `outputDir` resumes from the preserved temp workspace. Use `--clean` to discard the checkpoint and rebuild from scratch.

## Entry files

- `src/index.js` - main package export (`module.exports`)
- `src/cli.js` - CLI runner
- `src/args.js` - CLI/API argument parsing, normalization, validation
- `src/parser.js` - cloud model, PLY streaming helpers, self-test data helpers
- `src/codec.js` - SPZ stream encoding and worker payload helpers
- `src/gltf.js` - GLB/gltf assembly for Gaussian Splatting content
- `src/builder.js` - shared simplify planning, subtree binary helpers, progress, and worker-pool utilities
- `src/convert-core.js` - CLI/options, worker entry, top-level conversion wiring
- `bin/3dgs-ply-3dtiles-converter.js` - npm binary entry

## Error handling

Conversion failures throw `ConversionError`.  
Examples:

- Invalid option values
- Missing input/output paths
- Invalid PLY fields
- Missing required PLY properties
