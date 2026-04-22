# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog and the project follows Semantic Versioning.

## [Unreleased]

### Added

- Added CLI-only `--open-viewer`, which starts a localhost viewer session after conversion, opens the browser automatically, and lets users save updated root transforms back into `tileset.json` and `build_summary.json`. Viewer assets are written to an OS temp directory and removed on exit.
- Added a dedicated `3dtiles-viewer <tiles_dir>` bin (and the equivalent `3dgs-ply-3dtiles-converter --viewer-dir <tiles_dir>` flag) for skipping conversion and opening the interactive viewer directly on an existing 3D Tiles output directory.
- Added vendored viewer assets under `src/viewer/`, including the browser-side runtime and the shared `cameraController.js` copied from the 3DGS plugin examples.
- Added viewer `Lat` / `Lon` / `Height` controls for moving the camera to a specific WGS84 coordinate and for applying the same ENU root placement as `--coordinate` before saving.
- Added viewer `Set Position`, which lets users click the globe and move the tileset to that location with the same ENU placement logic as `--coordinate`.

### Changed

- The library API now rejects `openViewer` / `open_viewer` / `--open-viewer` so browser-launching behavior remains CLI-only.
- Viewer assets are no longer written into the tiles output directory; they live in an OS temp directory routed by the local server and are removed on clean shutdown. This prevents the viewer from touching user files outside the served tiles.
- The viewer server rejects cross-origin POSTs to `/__viewer/*` by checking the `Origin` header against the server's own 127.0.0.1 origin.

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
