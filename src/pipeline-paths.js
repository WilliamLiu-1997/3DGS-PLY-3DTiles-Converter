const path = require('path');

function canonicalNodePath(baseDir, subdir, node) {
  return path.join(
    baseDir,
    subdir,
    String(node.level),
    String(node.x),
    `${node.y}_${node.z}.bin`,
  );
}

function contentRelPath(level, x, y, z) {
  return `tiles/${level}/${x}/${y}_${z}.glb`;
}

module.exports = {
  canonicalNodePath,
  contentRelPath,
};
