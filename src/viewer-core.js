const { resolveAndValidateTilesetPath } = require('./tileset-path');
const { startViewerSession } = require('./viewer/session');

async function runViewer(rawPath, options = {}) {
  const tilesetPath = resolveAndValidateTilesetPath(rawPath);
  const session = await startViewerSession(tilesetPath, options);
  console.log(`[ok] viewer ready: ${session.url}`);
  console.log('[info] press Ctrl+C to stop the local viewer server.');
  await session.waitUntilClosed();
  return session;
}

module.exports = {
  resolveAndValidateTilesetPath,
  runViewer,
};
