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
- `build_summary.json` (conversion metadata, timing diagnostics, `memory_budget_plan`, `peak_rss_bytes`, and placement fields such as `root_transform`, `root_coordinate`, and `root_transform_source`, written as compact single-line JSON)
- `tiles/{level}/{x}/{y}/{z}.glb` (tile content)

Generated `tileset.json` files declare the top-level `3DTILES_content_gltf` tileset extension metadata that CesiumJS uses to detect `KHR_gaussian_splatting` and `KHR_gaussian_splatting_compression_spz_2` glTF tile content.

Tiling is explicit and uses a count-balanced k-d tree. Each logical split chooses one axis and a histogram median split plane for the current bucket, so leaf buckets stay globally balanced by splat count while dense regions naturally become smaller. Non-root tiles whose tight bounding box exceeds a `2:1` longest-to-width ratio are removed from the emitted tileset and replaced by equal-length virtual segments along their longest axis. These virtual segment splits can add deeper physical `tiles/{level}/{x}/{y}/{z}.glb` paths without increasing logical LOD depth or consuming `maxDepth`.

Large PLY conversion through `convert(...)` now uses a temp-file-backed pipeline. That path writes canonical leaf/handoff buckets, builds parent LODs from handoff data, uses exact streaming simplify so internal nodes do not need a full in-memory SH payload up front, and processes each level with memory-budgeted concurrency. Successful conversions remove the temp workspace; failed conversions preserve it so the same output directory can resume when rerun without `--clean`.

## API usage

```js
const { convert } = require('3dgs-ply-3dtiles-converter');

(async () => {
  const result = await convert('data/scene.ply', './out/tileset', {
    maxDepth: 8,
    leafLimit: 100,
    spzSh1Bits: 8,
    spzShRestBits: 8,
    spzCompressionLevel: 8,
    memoryBudget: 2,
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

- CLI `--max-depth 8` equals API `{ maxDepth: 8 }`
- CLI `--spz-sh1-bits 6` equals API `{ spzSh1Bits: 6 }`
- CLI `--spz-compression-level 6` equals API `{ spzCompressionLevel: 6 }`
- CLI `--sample-mode merge` equals API `{ sampleMode: 'merge' }`
- CLI `--memory-budget 4` equals API `{ memoryBudget: 4 }`
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
| Max depth               | integer | `--max-depth`               | `maxDepth`             | `8`                   | `>= 0`                                      | Maximum logical k-d tree LOD depth. Virtual long-tile segment paths may be physically deeper.                                                                                                                                                                      |
| Leaf limit              | integer | `--leaf-limit`              | `leafLimit`            | `100`                 | `>= 1`                                      | Max splat count per leaf tile before split stops.                                                                                                                                                                                                                 |
| Min geometric error     | number  | `--min-geometric-error`     | `minGeometricError`    | `null`                | any finite number                           | Minimum geometric error for the deepest emitted level.                                                                                                                                                                                                            |
| SH1 bits                | integer | `--spz-sh1-bits`            | `spzSh1Bits`           | `8`                   | `1..8`                                      | SPZ quantization bits for DC SH coefficients.                                                                                                                                                                                                                     |
| SH rest bits            | integer | `--spz-sh-rest-bits`        | `spzShRestBits`        | `8`                   | `1..8`                                      | SPZ quantization bits for higher SH coefficients.                                                                                                                                                                                                                 |
| SPZ compression level   | integer | `--spz-compression-level`   | `spzCompressionLevel`  | `8`                   | `0..9`                                      | gzip compression `level` used for SPZ payloads. Does not expose or change gzip `memLevel`.                                                                                                                                                                        |
| Root transform          | matrix4 | `--transform`               | `transform`            | `null`                | 4x4 JSON matrix or 16 numbers               | Writes `tileset.root.transform` directly. Nested `[[...]]` matrices are read as row-major and converted to 3D Tiles column-major storage.                                                                                                                         |
| Root coordinate         | vec3    | `--coordinate`              | `coordinate`           | `null`                | `[lat, long, height]`                       | Generates `tileset.root.transform` from WGS84 degrees/meters as a standard ENU frame in 3D Tiles tile coordinates. The API also accepts object forms such as `{ lat, lon, height }` and `{ latitude, longitude, altitude }`. Mutually exclusive with `transform`. |
| Sampling rate per level | number  | `--sampling-rate-per-level` | `samplingRatePerLevel` | `0.5`                 | `(0,1]`                                     | LOD sampling ratio between levels.                                                                                                                                                                                                                                |
| Sampling mode           | string  | `--sample-mode`             | `sampleMode`           | `merge`               | `sample`, `merge`                           | `sample` keeps representative splats; `merge` merges assigned splats into the target count. Voxel representative picks are always coarse-biased.                                                                                                                  |
| Memory budget           | number  | `--memory-budget`           | `memoryBudget`         | `2`                   | `> 0` GB                                    | Shared memory budget, in GB, used to size scan/bucket buffers, partition arenas, simplify scratch buffers, partition write concurrency, bottom-up build concurrency, and SPZ/GLB worker count.                                                                       |
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
- `src/builder.js` - shared simplify planning, progress, and worker-pool utilities
- `src/convert-core.js` - CLI/options, worker entry, top-level conversion wiring
- `bin/3dgs-ply-3dtiles-converter.js` - npm binary entry

## Error handling

Conversion failures throw `ConversionError`.  
Examples:

- Invalid option values
- Missing input/output paths
- Invalid PLY fields
- Missing required PLY properties
