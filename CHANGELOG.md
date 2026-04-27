# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog and the project follows Semantic Versioning.

## [Unreleased]

### Changed

- Changed k-d split-plane selection to test 64 equal longest-axis boundaries and score each split by normalized child tile volume sum plus midpoint-distance penalty; when the longest/second-longest axis ratio is below 1.5, the second axis is tested too.
- Changed long-tile virtual splitting to use the same 64-segment volume/midpoint score, with a single virtual split per long-tile branch instead of equal-length segment buckets.
- Added a virtual volume-rebalance split after k-d splits for current-LOD leaf tiles whose volume is more than 3x the median volume at the same logical depth.
- Changed the default estimated geometric-error multiplier from `2.5` to `2`.
- Updated the `3dtiles-inspector` dependency range to `^0.1.7`.

## [0.3.3] - 2026-04-25

### Changed

- Changed the default `tileRefinement` / `--tile-refinement` value from `1` to `2`, producing more, smaller first-level tiles by default.

## [0.3.2] - 2026-04-25

### Changed

- Updated the `3dtiles-inspector` dependency range to `^0.1.4`.

## [0.3.1] - 2026-04-25

### Changed

- Changed the default bounds mode to AABB. Use `--obb` or `orientedBoundingBoxes: true` to enable root-PCA oriented bounds.

## [0.3.0] - 2026-04-25

### Added

- Added `tileRefinement` / `--tile-refinement` to perform extra root-child k-d split rounds without increasing logical LOD depth. Higher values produce more, smaller tiles.

## [0.2.0] - 2026-04-25

### Added

- Added a temp-file-backed large-PLY conversion pipeline for binary and ASCII input. The pipeline streams input into canonical leaf and handoff buckets, builds parent LODs from handoff data, removes successful temp workspaces, and can resume a preserved failed workspace with `--continue`.
- Added `memoryBudget` / `--memory-budget` to size scan buffers, partition arenas, simplify scratch space, partition write concurrency, bottom-up build concurrency, and SPZ/GLB worker count from one GB-based budget.
- Added `spzCompressionLevel` / `--spz-compression-level` to control the gzip compression level used for SPZ payloads.
- Added root-PCA oriented bounding boxes by default, plus `orientedBoundingBoxes` and `--obb` / `--aabb` controls to switch emitted 3D Tiles `box` bounds and k-d split planes between OBB and AABB modes.
- Added optional local inspection through `3dtiles-inspector`, controlled by `openInspector` and `--open-inspector` / `--no-open-inspector`.
- Added build diagnostics to `build_summary.json`, including handoff encoding, memory budget plan, derived concurrency, checkpoint reuse state, timings, peak RSS, k-d tiling metadata, OBB mode, virtual node count, and SPZ compression level.
- Added automatic source coordinate-system detection from PLY header comments, including projected/geospatial metadata such as `epsg` and `offset*`, with the resolved source frame recorded in `build_summary.json`.

### Changed

- Replaced the in-memory octree build path with a single explicit, visual-cost-balanced k-d tree pipeline that uses root PCA axes by default and keeps leaf buckets balanced by weighted splat count.
- Replaced long, thin non-root k-d tiles with equal-length virtual segment paths so emitted intermediate tiles avoid extreme aspect ratios while logical LOD depth remains bounded by `maxDepth`.
- Changed defaults: `maxDepth` is now `8`, `leafLimit` is now `1000`, SPZ gzip compression level is now `8`, `clean` is now `true`, `selfTestCount` is now `1000000`, and inspector launch is enabled by default.
- Changed resume semantics so the default conversion rebuilds from a clean output directory; use `--continue` or `clean: false` to reuse a preserved checkpoint.
- Updated voxel simplification so retained splat targets drive voxel grouping directly and representative selection stays coarse-biased across `sample` and `merge` modes.
- Improved large-file conversion throughput by staging position data when it fits the memory budget, prefetching binary PLY chunks, compacting partition writes, limiting active leaf file handles, batching GLB writes, streaming unsimplified bucket content directly to SPZ/GLB output, and running exact bucket simplification and content packing in the derived worker pool.
- Changed progress reporting to concise spinner-style status lines with throttled phase detail logs.

### Removed

- Removed implicit tiling output and the `tilingMode` / `--tiling-mode` and `subtreeLevels` / `--subtree-levels` options.
- Removed `buildConcurrency` / `--build-concurrency` and `contentWorkers` / `--content-workers`; conversion now derives internal concurrency from `memoryBudget`.
- Reduced the published package API to the supported `convert` entry point only, removing package-root helper exports such as `convertPlyTo3DTiles`, `convertCloud`, `parseCommonGaussianPly`, `makeConversionArgs`, and `run`, plus the package `./cli` export.

## [0.1.6] - 2026-04-19

### Added

- Added `transform` and `coordinate` placement options to both the CLI and API. `transform` writes `tileset.root.transform` directly, while `coordinate` accepts `[lat, long, height]` in WGS84 degrees/meters and generates an ENU root transform automatically.
- Added `root_transform`, `root_coordinate`, and `root_transform_source` placement metadata to generated `build_summary.json` files.

### Changed

- Removed `sourceUpAxis` / `--source-up-axis` and standardized conversion on the built-in y-axis/camera-style normalization path.
- Split voxel simplification's grouping budget from its final retained splat budget so `targetCount` continues to control emitted splat counts while a separate `voxelTargetCount` controls initial voxel occupancy. The default voxel budget now biases toward fewer occupied voxels so a fixed output budget can place multiple representatives in the same voxel more often.
- Updated voxel-based geometric error estimation and worker-thread simplification tasks to use the same `voxelTargetCount` handling as the main-thread simplification path.

### Fixed

- Rotated emitted tile bounding volumes into the same 3D Tiles z-up frame as content and root transforms so geographic placement and custom `tileset.root.transform` values align with the generated GLB content.

## [0.1.5] - 2026-04-18

### Added

- Added `sampleMode` with CLI flag `--sample-mode` and API field `sampleMode` for `sample` and `merge` modes. `sample` preserves the previous representative-splat downsampling behavior, while `merge` merges assigned splats to the target count and preferentially merges detail splats before coarse splats.

### Changed

- Changed the default sampling mode from `sample` to `merge` for both CLI and API conversions.

## [0.1.4] - 2026-04-18

### Fixed

- Added the top-level `3DTILES_content_gltf` tileset extension metadata to generated explicit and implicit `tileset.json` files so CesiumJS can detect `KHR_gaussian_splatting` and `KHR_gaussian_splatting_compression_spz_2` content correctly.
- Switched generated `tileset.json` and `build_summary.json` output to compact single-line JSON to reduce file size.

## [0.1.3] - 2026-04-16

### Fixed

- Tightened the positive radius bias used for voxel detail representative selection so downsampling keeps detail picks closer to the voxel's weighted center instead of over-favoring very small splats.

## [0.1.2] - 2026-04-16

### Changed

- Updated `build_summary.json` to track the actual emitted LOD depth with `effective_max_depth`, removed the redundant `root_geometric_error_base` field, and limit per-depth sampling and geometric-error tables to the deepest emitted level.
- When `minGeometricError` is configured, the deepest emitted tile level now receives that value even if the build stops earlier than the configured `maxDepth`.
- Reduced the default `leafLimit` from `25000` to `10000` to encourage smaller leaf tiles by default.

## [0.1.1] - 2026-04-16

### Added

- GitHub Actions CI and npm publish workflows.
- GitHub issue templates, pull request template, and release note configuration.
- README badges and documented tag-driven release flow.
- README cover image for the GitHub and npm package landing pages.

### Changed

- README hero layout and top-of-page presentation.
- npm package metadata for the published GitHub repository.

## [0.1.0] - 2026-04-16

### Added

- Initial public npm release for converting Gaussian Splatting PLY files into 3D Tiles.
- CLI and library APIs for explicit and implicit tiling workflows.
- SPZ-compressed GLB content generation, worker-based packing, and self-test generation helpers.
