# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog and the project follows Semantic Versioning.

## [Unreleased]

### Changed

- Restructured the branch into the standalone `3dtiles-viewer` package surface.
- Removed legacy non-viewer CLI and library exports from the published package.
- Renamed the temporary asset directory prefix from `3dgs-ply-3dtiles-viewer-*` to `3dtiles-viewer-*`.
- Replaced CDN-based viewer module loading with a locally bundled viewer app plus locally served DRACO / Basis decoder assets built from npm-installed packages.
- Moved generated viewer bundle and decoder artifacts out of `src/` and into `dist/viewer-assets/` so source code no longer carries copied dependency assets.
- Changed the viewer entry input from a tiles directory to the root tileset JSON path while still serving sibling assets from that file's parent directory.

## [0.1.6] - 2026-04-19

### Added

- Added the standalone `3dtiles-viewer <tiles_dir>` CLI for opening an existing 3D Tiles directory in a local interactive viewer.
- Added viewer `Lat` / `Lon` / `Height` controls for moving the camera to a specific WGS84 coordinate.
- Added `Move Tiles`, `Set Position`, and `Save` workflows for editing and persisting the tileset root transform.

### Changed

- Viewer assets are written into an OS temp directory served by the local server instead of being copied into the tiles directory.
- `Save` updates `tileset.json` and also synchronizes `build_summary.json` when that file is present.

### Fixed

- The viewer server rejects cross-origin POSTs to `/__viewer/*` by checking the `Origin` header against the server's own localhost origin.
