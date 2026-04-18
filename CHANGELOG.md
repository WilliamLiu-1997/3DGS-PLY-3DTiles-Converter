# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog and the project follows Semantic Versioning.

## [Unreleased]

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
