const DEFAULT_SOURCE_COORDINATE_SYSTEM = 'camera_y_down_z_forward';
const SOURCE_COORDINATE_SYSTEM_Z_UP = 'z_up';
const SOURCE_COORDINATE_SYSTEM_GLTF_Y_UP = 'gltf_y_up';

const SOURCE_COORDINATE_SYSTEMS = {
  [DEFAULT_SOURCE_COORDINATE_SYSTEM]: {
    sourceToGltfYUp: [
      [1.0, 0.0, 0.0],
      [0.0, -1.0, 0.0],
      [0.0, 0.0, -1.0],
    ],
    sourceToTileZUp: [
      [1.0, 0.0, 0.0],
      [0.0, 0.0, 1.0],
      [0.0, -1.0, 0.0],
    ],
  },
  [SOURCE_COORDINATE_SYSTEM_Z_UP]: {
    sourceToGltfYUp: [
      [1.0, 0.0, 0.0],
      [0.0, 0.0, 1.0],
      [0.0, -1.0, 0.0],
    ],
    sourceToTileZUp: [
      [1.0, 0.0, 0.0],
      [0.0, 1.0, 0.0],
      [0.0, 0.0, 1.0],
    ],
  },
  [SOURCE_COORDINATE_SYSTEM_GLTF_Y_UP]: {
    sourceToGltfYUp: [
      [1.0, 0.0, 0.0],
      [0.0, 1.0, 0.0],
      [0.0, 0.0, 1.0],
    ],
    sourceToTileZUp: [
      [1.0, 0.0, 0.0],
      [0.0, 0.0, -1.0],
      [0.0, 1.0, 0.0],
    ],
  },
};

function sourceCoordinateSystemInfo(sourceCoordinateSystem) {
  return (
    SOURCE_COORDINATE_SYSTEMS[sourceCoordinateSystem] ||
    SOURCE_COORDINATE_SYSTEMS[DEFAULT_SOURCE_COORDINATE_SYSTEM]
  );
}

function normalizeCommentText(comments) {
  if (!Array.isArray(comments) || comments.length === 0) {
    return '';
  }
  return comments
    .map((comment) => String(comment || '').trim().toLowerCase())
    .filter(Boolean)
    .join('\n');
}

function hasExplicitZUpComment(text) {
  return (
    /\bz[-_\s]?up\b/.test(text) ||
    /\bup[-_\s]?axis\s*[:=]?\s*z\b/.test(text) ||
    /\baxis[-_\s]?up\s*[:=]?\s*z\b/.test(text) ||
    /\bcoordinate[-_\s]?system\s*[:=]?.*\bz\b.*\bup\b/.test(text)
  );
}

function hasExplicitGltfYUpComment(text) {
  return (
    /\bgltf[-_\s]?y[-_\s]?up\b/.test(text) ||
    /\bsource[-_\s]?coordinate[-_\s]?system\s*[:=]?\s*gltf\b/.test(text)
  );
}

function hasExplicitCameraComment(text) {
  return (
    /\bcamera[-_\s]?style\b/.test(text) ||
    /\bcamera[-_\s]?coordinates?\b/.test(text) ||
    /\bcolmap\b/.test(text) ||
    /\by[-_\s]?down\b/.test(text) ||
    /\bz[-_\s]?forward\b/.test(text)
  );
}

function hasProjectedCoordinateMetadata(text) {
  return (
    /\bepsg\b/.test(text) ||
    /\bproj(?:4|ection)?\b/.test(text) ||
    /\bcrs\b/.test(text) ||
    /\butm\b/.test(text) ||
    /\boffsetx\b/.test(text) ||
    /\boffsety\b/.test(text) ||
    /\boffsetz\b/.test(text)
  );
}

function detectSourceCoordinateSystemFromPlyHeader(header) {
  const text = normalizeCommentText(header && header.comments);
  if (!text) {
    return {
      sourceCoordinateSystem: DEFAULT_SOURCE_COORDINATE_SYSTEM,
      source: 'default',
      reason: 'PLY header has no coordinate-system comments.',
    };
  }

  if (hasExplicitCameraComment(text)) {
    return {
      sourceCoordinateSystem: DEFAULT_SOURCE_COORDINATE_SYSTEM,
      source: 'ply_comment',
      reason: 'PLY comments describe camera-style coordinates.',
    };
  }

  if (hasExplicitGltfYUpComment(text)) {
    return {
      sourceCoordinateSystem: SOURCE_COORDINATE_SYSTEM_GLTF_Y_UP,
      source: 'ply_comment',
      reason: 'PLY comments describe glTF Y-up coordinates.',
    };
  }

  if (hasExplicitZUpComment(text)) {
    return {
      sourceCoordinateSystem: SOURCE_COORDINATE_SYSTEM_Z_UP,
      source: 'ply_comment',
      reason: 'PLY comments describe Z-up coordinates.',
    };
  }

  if (hasProjectedCoordinateMetadata(text)) {
    return {
      sourceCoordinateSystem: SOURCE_COORDINATE_SYSTEM_Z_UP,
      source: 'ply_comment_projected_crs',
      reason: 'PLY comments include projected/geospatial coordinate metadata.',
    };
  }

  return {
    sourceCoordinateSystem: DEFAULT_SOURCE_COORDINATE_SYSTEM,
    source: 'default',
    reason: 'PLY comments do not identify the source coordinate system.',
  };
}

module.exports = {
  DEFAULT_SOURCE_COORDINATE_SYSTEM,
  SOURCE_COORDINATE_SYSTEM_GLTF_Y_UP,
  SOURCE_COORDINATE_SYSTEM_Z_UP,
  detectSourceCoordinateSystemFromPlyHeader,
  sourceCoordinateSystemInfo,
};
