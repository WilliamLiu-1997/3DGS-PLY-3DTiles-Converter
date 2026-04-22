# 3dtiles-viewer

`3dtiles-viewer` is a Node.js package for opening an existing 3D Tiles root JSON in a local interactive viewer, adjusting the root transform, and saving the result back to disk.

Requires Node.js 14.14 or newer.

## Install

```bash
npm install 3dtiles-viewer
```

## CLI

```bash
3dtiles-viewer <tileset_json>
```

```bash
npx 3dtiles-viewer <tileset_json>
```

From a cloned repository:

```bash
node ./bin/3dtiles-viewer.js <tileset_json>
```

The CLI starts a localhost HTTP server, copies the viewer assets into a temporary directory, opens the default browser, and keeps running until you stop it with `Ctrl+C`.

`<tileset_json>` must point to the root tileset JSON file, for example `out_tiles/tiles.json`.

## Node API

```js
const {
  runViewer,
} = require('3dtiles-viewer');

(async () => {
  await runViewer('./out_tiles/tileset.json');
})();
```

If you want explicit pre-validation or need to manage the session lifecycle yourself:

```js
const {
  resolveAndValidateTilesetPath,
  startViewerSession,
} = require('3dtiles-viewer');

(async () => {
  const tilesetPath = resolveAndValidateTilesetPath(
    './out_tiles/tileset.json',
  );
  const session = await startViewerSession(tilesetPath, {
    openBrowser: false,
    handleSignals: false,
  });

  console.log(session.url);

  // Later:
  await session.close();
})();
```

## Viewer behavior

- `Translate`, `Rotate`, and `Reset` control the current root transform edit.
- `Lat`, `Lon`, and `Height` move the camera to a WGS84 position.
- `Move Tiles` relocates the tileset origin to the specified WGS84 coordinate with an ENU-aligned root transform.
- `Set Position` lets you click the globe, terrain, or loaded tiles and move the tileset there.
- `Terrain` toggles Cesium World Terrain while keeping the satellite imagery overlay.
- `Geometric Error` scales the viewer's overall LOD target from `1/16x` to `16x`.
- `Save` writes the updated transform back to the root tileset JSON you opened, including custom names such as `tiles.json`.
- If `build_summary.json` exists, `Save` also synchronizes `root_transform`, clears `root_coordinate`, sets `root_transform_source` to `transform`, and records `viewer_geometric_error_scale`.

## Package surface

- `src/index.js` exports the public Node API.
- `src/cli.js` implements the standalone CLI.
- `src/viewer/session.js` manages the local server, temp assets, browser launch, and save handling.
- `src/viewer/app.js` contains the browser runtime source.
- `dist/viewer-assets/viewer/` contains the generated browser bundle and local decoder assets built by `npm run build:viewer`.
- `src/viewer/cameraController.js` contains the vendored camera controller used by the runtime.

## Error handling

Viewer failures throw `ViewerError` from the Node API and print a concise error message in the CLI.
