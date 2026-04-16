# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog and the project follows Semantic Versioning.

## [Unreleased]

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
