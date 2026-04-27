const fs = require('fs');

const { ConversionError, Bounds, TileNode, ensure } = require('./parser');
const { serializeBoundsState, deserializeBoundsState } = require('./pipeline-state');

const ROUTE_MODE_KD = 'kd';
const KD_CHILD_SLOT_COUNT = 2;
const ADAPTIVE_SPLIT_EQUAL_SEGMENTS = 256;
const ADAPTIVE_SPLIT_VOLUME_SCORE_WEIGHT = 1.0;
const DEFAULT_ADAPTIVE_SPLIT_MIDPOINT_PENALTY_WEIGHT = 0.5;
const DEFAULT_ADAPTIVE_SPLIT_COUNT_BALANCE_PENALTY_WEIGHT = 0.0;
const KD_TREE_AXIS_COMPETITION_ASPECT_RATIO = 2.0;
const LONG_TILE_VIRTUAL_KD_SEGMENT_COUNT = 2;
const ADAPTIVE_MAX_LONG_WIDTH_RATIO = 4.0;
const ADAPTIVE_MAX_CHILD_VOLUME_RATIO = 3.0;
const ADAPTIVE_SPLIT_EPSILON = 1e-9;
const TILING_TREE_PROGRESS_ROW_INTERVAL = 65536;
const POSITION_ROW_FLOAT_COUNT = 4;
const POSITION_ROW_BYTE_SIZE =
  POSITION_ROW_FLOAT_COUNT * Float32Array.BYTES_PER_ELEMENT;
const IS_LITTLE_ENDIAN = (() => {
  const probe = new Uint8Array(new Uint16Array([0x0102]).buffer);
  return probe[0] === 0x02;
})();

function makeNodeKey(level, x, y, z) {
  return `${level}/${x}/${y}/${z}`;
}

function dotDirection(direction, x, y, z) {
  return direction[0] * x + direction[1] * y + direction[2] * z;
}

function pointPlaneSlot(splitDirection, splitOffset, x, y, z) {
  return dotDirection(splitDirection, x, y, z) >= splitOffset ? 1 : 0;
}

function pointKdSlotForNode(node, x, y, z) {
  ensure(
    node.splitDirection && Number.isFinite(node.splitOffset),
    `Missing k-d split plane for node ${node.key}.`,
  );
  return pointPlaneSlot(node.splitDirection, node.splitOffset, x, y, z);
}

function coordinateForAxis(axis, x, y, z) {
  return axis === 1 ? y : axis === 2 ? z : x;
}

function cloneBounds(bounds) {
  if (!bounds) {
    return null;
  }
  return new Bounds(bounds.minimum.slice(), bounds.maximum.slice());
}

function normalizeBoxArray(box) {
  if (!Array.isArray(box) || box.length !== 12) {
    return null;
  }
  const out = box.map((value) => Number(value));
  return out.every((value) => Number.isFinite(value)) ? out : null;
}

function normalizeOrientedBox(box) {
  const normalized = normalizeBoxArray(box);
  return normalized && normalized.length === 12 ? normalized : null;
}

function normalizeVector3(values) {
  if (!Array.isArray(values) && !ArrayBuffer.isView(values)) {
    return null;
  }
  if (values.length !== 3) {
    return null;
  }
  const out = Array.from(values, Number);
  const length = Math.sqrt(out[0] * out[0] + out[1] * out[1] + out[2] * out[2]);
  if (!Number.isFinite(length) || length <= ADAPTIVE_SPLIT_EPSILON) {
    return null;
  }
  out[0] /= length;
  out[1] /= length;
  out[2] /= length;
  return out;
}

function normalizeSplitDirection(splitDirection) {
  return normalizeVector3(splitDirection);
}

function normalizeSplitOffset(splitOffset) {
  return Number.isFinite(splitOffset) ? Number(splitOffset) : null;
}

function makeChildSlotArray() {
  return new Array(KD_CHILD_SLOT_COUNT).fill(null);
}

function nodeBuildState(node) {
  if (!node._build) {
    node._build = {};
  }
  return node._build;
}

function existingNodeBuildState(node) {
  return node ? node._build || null : null;
}

function pruneNodeBuildState(node) {
  const build = existingNodeBuildState(node);
  if (build && Object.keys(build).length === 0) {
    delete node._build;
  }
}

function clearNodeBuildStateField(node, field) {
  const build = existingNodeBuildState(node);
  if (!build) {
    return;
  }
  delete build[field];
  pruneNodeBuildState(node);
}

function clearStatsNodeBuildStateField(statsList, field) {
  for (const stats of statsList) {
    clearNodeBuildStateField(stats.node, field);
  }
}

function makePartitionTreeNode({
  level,
  depth = level,
  x,
  y,
  z,
  bounds,
  orientedBox = null,
  count,
  leaf = true,
  splitDirection = null,
  splitOffset = null,
  childSlot = null,
  virtual = false,
  contentTargetOverride = null,
}) {
  return {
    key: makeNodeKey(level, x, y, z),
    level,
    depth,
    x,
    y,
    z,
    childSlot,
    count,
    bounds,
    orientedBox: normalizeOrientedBox(orientedBox),
    leaf,
    virtual: !!virtual,
    splitDirection: normalizeSplitDirection(splitDirection),
    splitOffset: normalizeSplitOffset(splitOffset),
    contentTargetOverride,
    children: [],
    childrenBySlot: makeChildSlotArray(),
    occupiedChildCount: 0,
    bucketPath: null,
    contentUri: null,
    handoffPath: null,
    bucketRowCount: null,
    handoffRowCount: null,
    handoffConsumed: false,
    ownError: null,
    buildState: 'pending',
  };
}

function serializeNodeMeta(node) {
  const meta = {
    key: node.key,
    level: node.level,
    x: node.x,
    y: node.y,
    z: node.z,
    count: node.count,
    bounds: serializeBoundsState(node.bounds),
  };
  if (node.orientedBox) {
    meta.orientedBox = node.orientedBox.slice();
  }
  if (node.depth !== node.level) {
    meta.depth = node.depth;
  }
  if (Number.isInteger(node.childSlot)) {
    meta.childSlot = node.childSlot;
  }
  if (node.leaf === false) {
    meta.leaf = false;
  }
  if (node.virtual) {
    meta.virtual = true;
  }
  if (node.splitDirection) {
    meta.splitDirection = node.splitDirection.slice();
  }
  if (Number.isFinite(node.splitOffset)) {
    meta.splitOffset = node.splitOffset;
  }
  if (
    Number.isFinite(node.contentTargetOverride) &&
    node.contentTargetOverride > 0
  ) {
    meta.contentTargetOverride = node.contentTargetOverride;
  }
  if (
    Number.isInteger(node.occupiedChildCount) &&
    node.occupiedChildCount > 0
  ) {
    meta.occupiedChildCount = node.occupiedChildCount;
  }
  if (node.bucketPath) {
    meta.bucketPath = node.bucketPath;
  }
  if (node.contentUri) {
    meta.contentUri = node.contentUri;
  }
  if (node.handoffPath) {
    meta.handoffPath = node.handoffPath;
  }
  if (Number.isFinite(node.bucketRowCount) && node.bucketRowCount >= 0) {
    meta.bucketRowCount = node.bucketRowCount;
  }
  if (Number.isFinite(node.handoffRowCount) && node.handoffRowCount >= 0) {
    meta.handoffRowCount = node.handoffRowCount;
  }
  if (node.handoffConsumed) {
    meta.handoffConsumed = true;
  }
  if (Number.isFinite(node.ownError) || node.ownError === 0) {
    meta.ownError = node.ownError;
  }
  if (node.buildState && node.buildState !== 'pending') {
    meta.buildState = node.buildState;
  }
  if (node.children.length > 0) {
    meta.children = node.children.map((child) => serializeNodeMeta(child));
  }
  return meta;
}

function deserializeNodeMeta(data) {
  const node = {
    key: data.key,
    level: data.level,
    depth: Number.isInteger(data.depth) ? data.depth : data.level,
    x: data.x,
    y: data.y,
    z: data.z,
    childSlot: Number.isInteger(data.childSlot) ? data.childSlot : null,
    count: data.count,
    bounds: deserializeBoundsState(data.bounds),
    orientedBox: normalizeOrientedBox(data.orientedBox),
    leaf: data.leaf !== false,
    virtual: !!data.virtual,
    splitDirection: normalizeSplitDirection(data.splitDirection),
    splitOffset: normalizeSplitOffset(data.splitOffset),
    contentTargetOverride:
      Number.isFinite(data.contentTargetOverride) &&
      data.contentTargetOverride > 0
        ? data.contentTargetOverride
        : null,
    children: [],
    childrenBySlot: makeChildSlotArray(),
    occupiedChildCount: Number.isInteger(data.occupiedChildCount)
      ? data.occupiedChildCount
      : 0,
    bucketPath: data.bucketPath ?? null,
    contentUri: data.contentUri ?? null,
    handoffPath: data.handoffPath ?? null,
    bucketRowCount:
      Number.isFinite(data.bucketRowCount) && data.bucketRowCount >= 0
        ? data.bucketRowCount
        : null,
    handoffRowCount:
      Number.isFinite(data.handoffRowCount) && data.handoffRowCount >= 0
        ? data.handoffRowCount
        : null,
    handoffConsumed: !!data.handoffConsumed,
    ownError:
      Number.isFinite(data.ownError) || data.ownError === 0
        ? data.ownError
        : null,
    buildState: data.buildState ?? 'pending',
  };
  if (Array.isArray(data.children)) {
    for (const childData of data.children) {
      const child = deserializeNodeMeta(childData);
      node.children.push(child);
      const slot =
        Number.isInteger(child.childSlot) && child.childSlot >= 0
          ? child.childSlot
          : node.children.length - 1;
      node.childrenBySlot[slot] = child;
    }
  }
  if (!Number.isInteger(data.occupiedChildCount) && node.children.length > 0) {
    node.occupiedChildCount = node.children.length;
  }
  return node;
}

function collectTreeStats(node) {
  const nodes = [];
  const leaves = [];
  const virtualNodes = [];
  let maxLevel = 0;
  let maxDepth = 0;
  const levels = [];
  const visit = (current) => {
    nodes.push(current);
    if (current.virtual) {
      virtualNodes.push(current);
    }
    if (!levels[current.level]) {
      levels[current.level] = [];
    }
    levels[current.level].push(current);
    if (current.leaf) {
      leaves.push(current);
    }
    if (current.level > maxLevel) {
      maxLevel = current.level;
    }
    if (current.depth > maxDepth) {
      maxDepth = current.depth;
    }
    for (const child of current.children) {
      visit(child);
    }
  };
  visit(node);
  return { nodes, leaves, virtualNodes, maxLevel, maxDepth, levels };
}

function resolveLeafNodeForPoint(root, x, y, z) {
  let node = root;
  while (!node.leaf) {
    const slot = pointKdSlotForNode(node, x, y, z);
    const child = node.childrenBySlot[slot];
    ensure(
      !!child,
      `Failed to resolve leaf bucket for point at node ${node.key} slot=${slot}.`,
    );
    node = child;
  }
  return node;
}

function resetNodeArtifacts(node) {
  node.contentUri = null;
  node.handoffPath = null;
  node.handoffRowCount = null;
  node.handoffConsumed = false;
  node.ownError = null;
  node.buildState = 'pending';
  if (node.leaf) {
    node.bucketPath = null;
    node.bucketRowCount = null;
  }
  for (const child of node.children) {
    resetNodeArtifacts(child);
  }
}

function normalizeSplitWeight(weight) {
  return Number.isFinite(weight) && weight > 0.0 ? weight : 1.0;
}

async function readExactFromHandle(handle, buffer, length, position, message) {
  let offset = 0;
  while (offset < length) {
    const { bytesRead } = await handle.read(
      buffer,
      offset,
      length - offset,
      position + offset,
    );
    if (bytesRead === 0) {
      throw new ConversionError(message);
    }
    offset += bytesRead;
  }
}

function normalizedTileRefinement(value) {
  return Math.max(1, Math.floor(value || 1));
}

function markRootTileRefinementStart(root, tileRefinement) {
  if (normalizedTileRefinement(tileRefinement) > 1) {
    nodeBuildState(root).rootTileRefinementDepth = 0;
  }
}

function rootTileRefinementDepth(node) {
  const build = existingNodeBuildState(node);
  return build && Number.isInteger(build.rootTileRefinementDepth)
    ? build.rootTileRefinementDepth
    : null;
}

function canSplitRootTileRefinement(node, maxDepth, tileRefinement) {
  const refinement = normalizedTileRefinement(tileRefinement);
  const depth = rootTileRefinementDepth(node);
  return (
    maxDepth > 0 &&
    refinement > 1 &&
    depth != null &&
    depth > 0 &&
    depth < refinement &&
    existingNodeBuildState(node)?.rootTileRefinementExhausted !== true &&
    node.count > 1
  );
}

function markRootTileRefinementExhausted(node) {
  if (node && rootTileRefinementDepth(node) != null) {
    nodeBuildState(node).rootTileRefinementExhausted = true;
  }
}

function splitActionKeepsLogicalDepth(action) {
  return (
    action &&
    (splitActionIsLongTile(action) ||
      splitActionIsVolumeRebalance(action) ||
      action.rootTileRefinement)
  );
}

function splitActionMakesVirtualNode(action) {
  return splitActionKeepsLogicalDepth(action);
}

function splitActionIsLongTile(action) {
  return !!(action && action.longTileSplit);
}

function splitActionIsVolumeRebalance(action) {
  return !!(action && action.volumeRebalanceSplit);
}

function markSplitActionExhausted(node, action) {
  if (splitActionIsVolumeRebalance(action)) {
    clearVolumeRebalanceSplitRequest(node);
  } else if (splitActionIsLongTile(action)) {
    node.aspectSplitExhausted = true;
  } else {
    node.splitExhausted = true;
    markRootTileRefinementExhausted(node);
  }
}

function noteAffectedVolumeRebalanceDepths(depths, node, action) {
  if (!depths || !node) {
    return;
  }
  const parentDepth = Number.isInteger(node.depth) ? node.depth : node.level;
  depths.add(parentDepth);
  depths.add(
    splitActionKeepsLogicalDepth(action) ? parentDepth : parentDepth + 1,
  );
}

function assignRootTileRefinementChildState(parent, child, action, refinement) {
  if (!action || action.kind !== ROUTE_MODE_KD) {
    return;
  }
  const parentDepth = rootTileRefinementDepth(parent);
  const tileRefinement = normalizedTileRefinement(refinement);
  if (parentDepth == null || parentDepth >= tileRefinement) {
    return;
  }
  const childBuild = nodeBuildState(child);
  if (action.rootTileRefinement) {
    childBuild.rootTileRefinementDepth = parentDepth + 1;
  } else if (splitActionKeepsLogicalDepth(action)) {
    childBuild.rootTileRefinementDepth = parentDepth;
  } else {
    childBuild.rootTileRefinementDepth = parentDepth + 1;
  }
}

function makeAdaptiveSplitCandidateOptions(options = {}) {
  return {
    tileRefinement: normalizedTileRefinement(options.tileRefinement),
  };
}

function normalizeSplitPenaltyWeight(value, fallback) {
  return Number.isFinite(value) && value >= 0.0 ? value : fallback;
}

function makeAdaptiveSplitPenaltyOptions(options = {}) {
  return {
    splitMidpointPenaltyWeight: normalizeSplitPenaltyWeight(
      options.splitMidpointPenalty ?? options.splitMidpointPenaltyWeight,
      DEFAULT_ADAPTIVE_SPLIT_MIDPOINT_PENALTY_WEIGHT,
    ),
    splitCountBalancePenaltyWeight: normalizeSplitPenaltyWeight(
      options.splitCountBalancePenalty ??
        options.splitCountBalancePenaltyWeight,
      DEFAULT_ADAPTIVE_SPLIT_COUNT_BALANCE_PENALTY_WEIGHT,
    ),
  };
}

function collectAdaptiveSplitCandidates(
  node,
  maxDepth,
  leafLimit,
  out,
  options = {},
) {
  const candidateOptions = makeAdaptiveSplitCandidateOptions(options);
  if (node.leaf) {
    const canSplitByRootRefinement = canSplitRootTileRefinement(
      node,
      maxDepth,
      candidateOptions.tileRefinement,
    );
    const canSplitByCount =
      !node.splitExhausted && node.depth < maxDepth && node.count > leafLimit;
    const canSplitByAspect =
      node.level > 0 &&
      !node.aspectSplitExhausted &&
      boundsLongWidthAspect(node.bounds) > ADAPTIVE_MAX_LONG_WIDTH_RATIO;
    const build = existingNodeBuildState(node);
    const canSplitByVolumeRebalance =
      build?.forceVolumeRebalanceSplit === true &&
      build.volumeRebalanceSplitExhausted !== true;
    if (
      node.count > 1 &&
      (canSplitByVolumeRebalance ||
        canSplitByRootRefinement ||
        canSplitByAspect ||
        canSplitByCount)
    ) {
      out.push(node);
    }
    return;
  }
  for (const child of node.children) {
    collectAdaptiveSplitCandidates(
      child,
      maxDepth,
      leafLimit,
      out,
      candidateOptions,
    );
  }
}

function makeAdaptiveSplitStats(node, basisAxes = null, options = {}) {
  const normalizedBasisAxes = basisAxes ? normalizeBasisAxes(basisAxes) : null;
  const penaltyOptions = makeAdaptiveSplitPenaltyOptions(options);
  return {
    node,
    count: 0,
    minimum: [Infinity, Infinity, Infinity],
    maximum: [-Infinity, -Infinity, -Infinity],
    splitDirection: null,
    splitOffset: null,
    projectionCandidateStats: null,
    basisAxes: normalizedBasisAxes,
    basisProjectionMinimum: normalizedBasisAxes
      ? [Infinity, Infinity, Infinity]
      : null,
    basisProjectionMaximum: normalizedBasisAxes
      ? [-Infinity, -Infinity, -Infinity]
      : null,
    action: null,
    childStats: null,
    positionIndexStart: null,
    positionIndexEnd: null,
    splitMidpointPenaltyWeight: penaltyOptions.splitMidpointPenaltyWeight,
    splitCountBalancePenaltyWeight:
      penaltyOptions.splitCountBalancePenaltyWeight,
  };
}

function boundsFromMinMax(minimum, maximum, fallbackBounds) {
  if (
    minimum.every((value) => Number.isFinite(value)) &&
    maximum.every((value) => Number.isFinite(value))
  ) {
    return new Bounds(minimum.slice(), maximum.slice());
  }
  return cloneBounds(fallbackBounds);
}

function longWidthAspectFromExtents(extents) {
  const ranked = extents
    .map((extent, axis) => ({
      axis,
      extent: Number.isFinite(extent) ? Math.max(0.0, extent) : 0.0,
    }))
    .sort((a, b) => b.extent - a.extent);
  const longest = ranked[0];
  const width = ranked[1];
  const epsilon = Math.max(
    ADAPTIVE_SPLIT_EPSILON,
    longest.extent * ADAPTIVE_SPLIT_EPSILON,
  );
  return {
    aspect:
      longest.extent > epsilon
        ? longest.extent / Math.max(width.extent, epsilon)
        : 1.0,
    longestAxis: longest.axis,
    widthAxis: width.axis,
    longestExtent: longest.extent,
    widthExtent: width.extent,
  };
}

function boundsLongWidthAspect(bounds) {
  return longWidthAspectFromExtents(bounds.extents()).aspect;
}

function volumeFromExtentValues(extentX, extentY, extentZ) {
  let maxExtent = 0.0;
  if (Number.isFinite(extentX) && extentX > maxExtent) {
    maxExtent = extentX;
  }
  if (Number.isFinite(extentY) && extentY > maxExtent) {
    maxExtent = extentY;
  }
  if (Number.isFinite(extentZ) && extentZ > maxExtent) {
    maxExtent = extentZ;
  }
  const epsilon = Math.max(
    ADAPTIVE_SPLIT_EPSILON,
    maxExtent * ADAPTIVE_SPLIT_EPSILON,
  );
  // Keep planar or linear leaves in same-depth scale comparisons instead of
  // letting one degenerate axis collapse their regularized volume to zero.
  const finiteX = Number.isFinite(extentX) && extentX > 0.0 ? extentX : 0.0;
  const finiteY = Number.isFinite(extentY) && extentY > 0.0 ? extentY : 0.0;
  const finiteZ = Number.isFinite(extentZ) && extentZ > 0.0 ? extentZ : 0.0;
  return (
    Math.max(finiteX, epsilon) *
    Math.max(finiteY, epsilon) *
    Math.max(finiteZ, epsilon)
  );
}

function volumeFromExtents(extents) {
  return volumeFromExtentValues(extents[0], extents[1], extents[2]);
}

function boundsVolume(bounds) {
  if (!bounds) {
    return 0.0;
  }
  return volumeFromExtents(bounds.extents());
}

function volumeFromMinMax(minimum, maximum) {
  if (
    !minimum ||
    !maximum ||
    !minimum.every((value) => Number.isFinite(value)) ||
    !maximum.every((value) => Number.isFinite(value))
  ) {
    return Infinity;
  }
  return volumeFromExtentValues(
    maximum[0] - minimum[0],
    maximum[1] - minimum[1],
    maximum[2] - minimum[2],
  );
}

function medianSorted(values) {
  const count = values.length;
  if (count === 0) {
    return null;
  }
  const midpoint = Math.floor(count / 2);
  if (count % 2 === 1) {
    return values[midpoint];
  }
  return (values[midpoint - 1] + values[midpoint]) * 0.5;
}

function clearVolumeRebalanceSplitRequest(node, exhausted = true) {
  if (!node) {
    return;
  }
  const build = nodeBuildState(node);
  delete build.forceVolumeRebalanceSplit;
  if (exhausted) {
    build.volumeRebalanceSplitExhausted = true;
  }
  pruneNodeBuildState(node);
}

function collectCurrentLodTileVolumeEntries(
  node,
  entriesByDepth,
  targetDepths = null,
  maxTargetDepth = Infinity,
) {
  const nodeDepth = Number.isInteger(node.depth) ? node.depth : node.level;
  if (targetDepths && nodeDepth > maxTargetDepth) {
    return;
  }
  if (node.leaf && !node.virtual) {
    const volume = boundsVolume(node.bounds);
    if (Number.isFinite(volume) && volume > 0.0) {
      if (targetDepths && !targetDepths.has(nodeDepth)) {
        return;
      }
      let entries = entriesByDepth.get(nodeDepth);
      if (!entries) {
        entries = [];
        entriesByDepth.set(nodeDepth, entries);
      }
      entries.push({ node, volume });
    }
    return;
  }

  for (const child of node.children) {
    collectCurrentLodTileVolumeEntries(
      child,
      entriesByDepth,
      targetDepths,
      maxTargetDepth,
    );
  }
}

function markCurrentLodTilesForVolumeRebalance(root, targetDepths = null) {
  const normalizedTargetDepths =
    targetDepths && targetDepths.size > 0 ? targetDepths : null;
  const maxTargetDepth = normalizedTargetDepths
    ? Math.max(...normalizedTargetDepths)
    : Infinity;
  const entriesByDepth = new Map();
  collectCurrentLodTileVolumeEntries(
    root,
    entriesByDepth,
    normalizedTargetDepths,
    maxTargetDepth,
  );

  for (const entries of entriesByDepth.values()) {
    if (entries.length < 2) {
      for (const entry of entries) {
        const build = existingNodeBuildState(entry.node);
        if (build) {
          delete build.forceVolumeRebalanceSplit;
          pruneNodeBuildState(entry.node);
        }
      }
      continue;
    }

    const volumes = entries.map((entry) => entry.volume).sort((a, b) => a - b);
    const median = medianSorted(volumes);
    if (!Number.isFinite(median) || median <= 0.0) {
      for (const entry of entries) {
        const build = existingNodeBuildState(entry.node);
        if (build) {
          delete build.forceVolumeRebalanceSplit;
          pruneNodeBuildState(entry.node);
        }
      }
      continue;
    }

    for (const entry of entries) {
      const node = entry.node;
      const volumeRatio = entry.volume / median;
      const build = existingNodeBuildState(node);
      if (
        volumeRatio > ADAPTIVE_MAX_CHILD_VOLUME_RATIO &&
        node.count > 1 &&
        build?.volumeRebalanceSplitExhausted !== true
      ) {
        nodeBuildState(node).forceVolumeRebalanceSplit = true;
      } else if (build?.forceVolumeRebalanceSplit === true) {
        delete build.forceVolumeRebalanceSplit;
        pruneNodeBuildState(node);
      }
    }
  }
}

function updatePositionStatsBounds(stats, x, y, z) {
  const minimum = stats.minimum;
  const maximum = stats.maximum;

  if (x < minimum[0]) minimum[0] = x;
  if (x > maximum[0]) maximum[0] = x;

  if (y < minimum[1]) minimum[1] = y;
  if (y > maximum[1]) maximum[1] = y;

  if (z < minimum[2]) minimum[2] = z;
  if (z > maximum[2]) maximum[2] = z;
}

function updatePositionMomentStats(stats, x, y, z) {
  const nextCount = stats.count + 1;
  const mean = stats.mean;
  const dx = x - mean[0];
  const dy = y - mean[1];
  const dz = z - mean[2];
  const invCount = 1.0 / nextCount;

  mean[0] += dx * invCount;
  mean[1] += dy * invCount;
  mean[2] += dz * invCount;

  const dx2 = x - mean[0];
  const dy2 = y - mean[1];
  const dz2 = z - mean[2];
  const m2 = stats.covarianceM2;
  m2[0] += dx * dx2;
  m2[1] += dx * dy2;
  m2[2] += dx * dz2;
  m2[3] += dy * dy2;
  m2[4] += dy * dz2;
  m2[5] += dz * dz2;

  stats.count = nextCount;
  updatePositionStatsBounds(stats, x, y, z);
}

function updateWeightedPositionMomentStats(stats, x, y, z, weight = 1.0) {
  const splitWeight = normalizeSplitWeight(weight);
  const totalWeight =
    Number.isFinite(stats.totalWeight) && stats.totalWeight > 0.0
      ? stats.totalWeight
      : 0.0;
  const nextTotalWeight = totalWeight + splitWeight;
  if (!Number.isFinite(nextTotalWeight) || nextTotalWeight <= 0.0) {
    updatePositionMomentStats(stats, x, y, z);
    return;
  }

  const mean = stats.mean;
  const dx = x - mean[0];
  const dy = y - mean[1];
  const dz = z - mean[2];
  const weightRatio = splitWeight / nextTotalWeight;

  mean[0] += dx * weightRatio;
  mean[1] += dy * weightRatio;
  mean[2] += dz * weightRatio;

  const dx2 = x - mean[0];
  const dy2 = y - mean[1];
  const dz2 = z - mean[2];
  const m2 = stats.covarianceM2;
  m2[0] += splitWeight * dx * dx2;
  m2[1] += splitWeight * dx * dy2;
  m2[2] += splitWeight * dx * dz2;
  m2[3] += splitWeight * dy * dy2;
  m2[4] += splitWeight * dy * dz2;
  m2[5] += splitWeight * dz * dz2;

  stats.count += 1;
  stats.totalWeight = nextTotalWeight;
  updatePositionStatsBounds(stats, x, y, z);
}

function updateAdaptiveSplitStats(stats, x, y, z) {
  stats.count += 1;
  updatePositionStatsBounds(stats, x, y, z);
  if (stats.basisAxes) {
    for (let axis = 0; axis < 3; axis++) {
      const projection = dotDirection(stats.basisAxes[axis], x, y, z);
      if (projection < stats.basisProjectionMinimum[axis]) {
        stats.basisProjectionMinimum[axis] = projection;
      }
      if (projection > stats.basisProjectionMaximum[axis]) {
        stats.basisProjectionMaximum[axis] = projection;
      }
    }
  }
}

function eigenDecompositionSymmetric3x3(matrix) {
  const a = matrix.slice();
  const vectors = [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0];
  const index = (row, col) => row * 3 + col;

  for (let sweep = 0; sweep < 16; sweep++) {
    let p = 0;
    let q = 1;
    let largest = Math.abs(a[index(0, 1)]);
    const xz = Math.abs(a[index(0, 2)]);
    if (xz > largest) {
      p = 0;
      q = 2;
      largest = xz;
    }
    const yz = Math.abs(a[index(1, 2)]);
    if (yz > largest) {
      p = 1;
      q = 2;
      largest = yz;
    }
    if (largest <= ADAPTIVE_SPLIT_EPSILON) {
      break;
    }

    const app = a[index(p, p)];
    const aqq = a[index(q, q)];
    const apq = a[index(p, q)];
    if (!Number.isFinite(apq) || Math.abs(apq) <= ADAPTIVE_SPLIT_EPSILON) {
      break;
    }

    const tau = (aqq - app) / (2.0 * apq);
    const t =
      tau >= 0.0
        ? 1.0 / (tau + Math.sqrt(1.0 + tau * tau))
        : -1.0 / (-tau + Math.sqrt(1.0 + tau * tau));
    const c = 1.0 / Math.sqrt(1.0 + t * t);
    const s = t * c;

    for (let k = 0; k < 3; k++) {
      if (k === p || k === q) {
        continue;
      }
      const akp = a[index(k, p)];
      const akq = a[index(k, q)];
      const newKp = c * akp - s * akq;
      const newKq = s * akp + c * akq;
      a[index(k, p)] = newKp;
      a[index(p, k)] = newKp;
      a[index(k, q)] = newKq;
      a[index(q, k)] = newKq;
    }

    a[index(p, p)] = app - t * apq;
    a[index(q, q)] = aqq + t * apq;
    a[index(p, q)] = 0.0;
    a[index(q, p)] = 0.0;

    for (let k = 0; k < 3; k++) {
      const vkp = vectors[index(k, p)];
      const vkq = vectors[index(k, q)];
      vectors[index(k, p)] = c * vkp - s * vkq;
      vectors[index(k, q)] = s * vkp + c * vkq;
    }
  }

  const entries = [0, 1, 2]
    .map((axis) => ({
      value: a[index(axis, axis)],
      vector: [
        vectors[index(0, axis)],
        vectors[index(1, axis)],
        vectors[index(2, axis)],
      ],
    }))
    .sort((lhs, rhs) => rhs.value - lhs.value);

  return {
    values: entries.map((entry) => entry.value),
    vectors: entries.map((entry) => entry.vector),
  };
}

function projectionRangeForBounds(bounds, direction) {
  const min = bounds.minimum;
  const max = bounds.maximum;
  let projectionMin = Infinity;
  let projectionMax = -Infinity;
  for (let ix = 0; ix < 2; ix++) {
    const x = ix === 0 ? min[0] : max[0];
    for (let iy = 0; iy < 2; iy++) {
      const y = iy === 0 ? min[1] : max[1];
      for (let iz = 0; iz < 2; iz++) {
        const z = iz === 0 ? min[2] : max[2];
        const projection = dotDirection(direction, x, y, z);
        if (projection < projectionMin) projectionMin = projection;
        if (projection > projectionMax) projectionMax = projection;
      }
    }
  }
  return { minimum: projectionMin, maximum: projectionMax };
}

function defaultBasisAxes() {
  return [
    [1.0, 0.0, 0.0],
    [0.0, 1.0, 0.0],
    [0.0, 0.0, 1.0],
  ];
}

function normalizeBasisAxes(axes) {
  if (!Array.isArray(axes) || axes.length !== 3) {
    return defaultBasisAxes();
  }
  const normalized = axes.map((axis) => normalizeVector3(axis));
  return normalized.every((axis) => !!axis) ? normalized : defaultBasisAxes();
}

function projectedAxisInfosForBasis(bounds, basisAxes) {
  const axes = normalizeBasisAxes(basisAxes);
  return axes
    .map((direction, axis) => {
      const range = projectionRangeForBounds(bounds, direction);
      const extent = range.maximum - range.minimum;
      return {
        axis,
        direction,
        range,
        extent: Number.isFinite(extent) ? Math.max(0.0, extent) : 0.0,
      };
    })
    .sort((a, b) => b.extent - a.extent);
}

function projectedAxisInfosFromStats(stats, bounds, basisAxes) {
  if (
    stats &&
    stats.basisAxes &&
    stats.basisProjectionMinimum &&
    stats.basisProjectionMaximum
  ) {
    return stats.basisAxes
      .map((direction, axis) => {
        const minimum = stats.basisProjectionMinimum[axis];
        const maximum = stats.basisProjectionMaximum[axis];
        const extent = maximum - minimum;
        return {
          axis,
          direction,
          range: { minimum, maximum },
          extent: Number.isFinite(extent) ? Math.max(0.0, extent) : 0.0,
        };
      })
      .sort((a, b) => b.extent - a.extent);
  }
  return projectedAxisInfosForBasis(bounds, basisAxes);
}

function projectedAspectInfoFromAxisInfos(ranked) {
  if (!ranked || ranked.length === 0) {
    return {
      aspect: 1.0,
      axis: 0,
      direction: [1.0, 0.0, 0.0],
      range: { minimum: 0.0, maximum: 0.0 },
      longestExtent: 0.0,
      widthExtent: 0.0,
    };
  }
  const longest = ranked[0];
  const width = ranked[1] || longest;
  const epsilon = Math.max(
    ADAPTIVE_SPLIT_EPSILON,
    longest.extent * ADAPTIVE_SPLIT_EPSILON,
  );
  return {
    aspect:
      longest.extent > epsilon
        ? longest.extent / Math.max(width.extent, epsilon)
        : 1.0,
    axis: longest.axis,
    direction: longest.direction,
    range: longest.range,
    longestExtent: longest.extent,
    widthExtent: width.extent,
  };
}

function projectedAspectInfoForBasis(bounds, basisAxes) {
  return projectedAspectInfoFromAxisInfos(
    projectedAxisInfosForBasis(bounds, basisAxes),
  );
}

function projectedAspectInfoFromStats(stats, bounds, basisAxes) {
  return projectedAspectInfoFromAxisInfos(
    projectedAxisInfosFromStats(stats, bounds, basisAxes),
  );
}

function cross3(lhs, rhs) {
  return [
    lhs[1] * rhs[2] - lhs[2] * rhs[1],
    lhs[2] * rhs[0] - lhs[0] * rhs[2],
    lhs[0] * rhs[1] - lhs[1] * rhs[0],
  ];
}

function dot3(lhs, rhs) {
  return lhs[0] * rhs[0] + lhs[1] * rhs[1] + lhs[2] * rhs[2];
}

function subtractProjection(vector, axis) {
  const scale = dot3(vector, axis);
  return [
    vector[0] - axis[0] * scale,
    vector[1] - axis[1] * scale,
    vector[2] - axis[2] * scale,
  ];
}

function fallbackAxisLeastAlignedWith(axis) {
  const absX = Math.abs(axis[0]);
  const absY = Math.abs(axis[1]);
  const absZ = Math.abs(axis[2]);
  if (absX <= absY && absX <= absZ) {
    return [1.0, 0.0, 0.0];
  }
  if (absY <= absX && absY <= absZ) {
    return [0.0, 1.0, 0.0];
  }
  return [0.0, 0.0, 1.0];
}

function stableOrientedAxis(axis) {
  const normalized = normalizeVector3(axis);
  if (!normalized) {
    return null;
  }
  let strongestAxis = 0;
  let strongestAbs = Math.abs(normalized[0]);
  for (let i = 1; i < 3; i++) {
    const absValue = Math.abs(normalized[i]);
    if (absValue > strongestAbs) {
      strongestAxis = i;
      strongestAbs = absValue;
    }
  }
  if (normalized[strongestAxis] < 0.0) {
    normalized[0] = -normalized[0];
    normalized[1] = -normalized[1];
    normalized[2] = -normalized[2];
  }
  return normalized;
}

function orthonormalBasisFromMomentStats(stats) {
  const m2 = stats.covarianceM2 || [];
  const maxAbs = Math.max(
    Math.abs(m2[0] || 0.0),
    Math.abs(m2[1] || 0.0),
    Math.abs(m2[2] || 0.0),
    Math.abs(m2[3] || 0.0),
    Math.abs(m2[4] || 0.0),
    Math.abs(m2[5] || 0.0),
  );
  if (!Number.isFinite(maxAbs) || maxAbs <= ADAPTIVE_SPLIT_EPSILON) {
    return [
      [1.0, 0.0, 0.0],
      [0.0, 1.0, 0.0],
      [0.0, 0.0, 1.0],
    ];
  }

  const decomposition = eigenDecompositionSymmetric3x3([
    m2[0],
    m2[1],
    m2[2],
    m2[1],
    m2[3],
    m2[4],
    m2[2],
    m2[4],
    m2[5],
  ]);
  const axis0 = stableOrientedAxis(decomposition.vectors[0]) || [1.0, 0.0, 0.0];
  let axis1 = stableOrientedAxis(
    subtractProjection(decomposition.vectors[1], axis0),
  );
  if (!axis1) {
    axis1 = stableOrientedAxis(
      subtractProjection(fallbackAxisLeastAlignedWith(axis0), axis0),
    );
  }
  if (!axis1) {
    axis1 = [0.0, 1.0, 0.0];
  }
  let axis2 = normalizeVector3(cross3(axis0, axis1));
  if (!axis2) {
    axis2 = stableOrientedAxis(
      subtractProjection(fallbackAxisLeastAlignedWith(axis1), axis1),
    );
  }
  axis2 = axis2 || [0.0, 0.0, 1.0];
  return [axis0, axis1, axis2];
}

function makeOrientedBoxStats(node) {
  return {
    node,
    count: 0,
    totalWeight: 0.0,
    mean: [0.0, 0.0, 0.0],
    covarianceM2: [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    minimum: [Infinity, Infinity, Infinity],
    maximum: [-Infinity, -Infinity, -Infinity],
    axes: null,
    projectionMinimum: [Infinity, Infinity, Infinity],
    projectionMaximum: [-Infinity, -Infinity, -Infinity],
  };
}

function cloneBasisAxes(axes) {
  return axes.map((axis) => axis.slice());
}

function prepareOrientedBoxStats(stats, basisAxes = null) {
  stats.axes = basisAxes
    ? cloneBasisAxes(basisAxes)
    : orthonormalBasisFromMomentStats(stats);
  stats.projectionMinimum = [Infinity, Infinity, Infinity];
  stats.projectionMaximum = [-Infinity, -Infinity, -Infinity];
}

function updateOrientedBoxProjectionStats(stats, x, y, z) {
  const axes = stats.axes;
  if (!axes) {
    return;
  }
  for (let axis = 0; axis < 3; axis++) {
    const projection = dotDirection(axes[axis], x, y, z);
    if (projection < stats.projectionMinimum[axis]) {
      stats.projectionMinimum[axis] = projection;
    }
    if (projection > stats.projectionMaximum[axis]) {
      stats.projectionMaximum[axis] = projection;
    }
  }
}

function updateOrientedBoxStatsWithBasis(stats, x, y, z) {
  stats.count += 1;
  updatePositionStatsBounds(stats, x, y, z);
  updateOrientedBoxProjectionStats(stats, x, y, z);
}

function orientedBoxFromProjectionStats(stats, fallbackBounds) {
  if (!stats || !stats.axes || stats.count <= 0) {
    return fallbackBounds ? fallbackBounds.toBoxArray() : null;
  }
  const center = [0.0, 0.0, 0.0];
  const halfAxes = [];
  for (let axis = 0; axis < 3; axis++) {
    const min = stats.projectionMinimum[axis];
    const max = stats.projectionMaximum[axis];
    if (!Number.isFinite(min) || !Number.isFinite(max)) {
      return fallbackBounds ? fallbackBounds.toBoxArray() : null;
    }
    const midpoint = (min + max) * 0.5;
    const halfExtent = Math.max((max - min) * 0.5, 1e-6);
    const direction = stats.axes[axis];
    center[0] += direction[0] * midpoint;
    center[1] += direction[1] * midpoint;
    center[2] += direction[2] * midpoint;
    halfAxes.push([
      direction[0] * halfExtent,
      direction[1] * halfExtent,
      direction[2] * halfExtent,
    ]);
  }
  return [
    center[0],
    center[1],
    center[2],
    halfAxes[0][0],
    halfAxes[0][1],
    halfAxes[0][2],
    halfAxes[1][0],
    halfAxes[1][1],
    halfAxes[1][2],
    halfAxes[2][0],
    halfAxes[2][1],
    halfAxes[2][2],
  ];
}

function kdProjectionCandidateFromAxisInfo(axisInfo) {
  if (!axisInfo || !axisInfo.direction || !axisInfo.range) {
    return null;
  }
  const projectionMin = axisInfo.range.minimum;
  const projectionMax = axisInfo.range.maximum;
  const projectionExtent = projectionMax - projectionMin;
  const epsilon = Math.max(
    ADAPTIVE_SPLIT_EPSILON,
    Math.abs(projectionExtent) * ADAPTIVE_SPLIT_EPSILON,
  );
  if (
    !Number.isFinite(projectionExtent) ||
    projectionExtent <= epsilon ||
    !Number.isFinite(projectionMin) ||
    !Number.isFinite(projectionMax)
  ) {
    return null;
  }
  return {
    basisAxis: axisInfo.axis,
    splitDirection: axisInfo.direction.slice(),
    coordinateAxis: Number.isInteger(axisInfo.coordinateAxis)
      ? axisInfo.coordinateAxis
      : null,
    projectionMin,
    projectionMax,
    projectionExtent,
    axisScoreMultiplier: 1.0,
  };
}

function kdProjectionCandidatesFromAxisInfos(axisInfos) {
  const primary = kdProjectionCandidateFromAxisInfo(axisInfos && axisInfos[0]);
  if (!primary) {
    return [];
  }

  const candidates = [primary];
  const secondaryInfo = axisInfos[1];
  const secondary = kdProjectionCandidateFromAxisInfo(secondaryInfo);
  if (secondary) {
    const ratio =
      secondary.projectionExtent > ADAPTIVE_SPLIT_EPSILON
        ? primary.projectionExtent / secondary.projectionExtent
        : Infinity;
    if (
      Number.isFinite(ratio) &&
      ratio < KD_TREE_AXIS_COMPETITION_ASPECT_RATIO
    ) {
      primary.axisScoreMultiplier = 1.0;
      secondary.axisScoreMultiplier = Math.max(1.0, Math.sqrt(ratio));
      candidates.push(secondary);
    }
  }
  return candidates;
}

function makeKdSplitAction(stats, tightBounds, basisAxes = null) {
  const axisInfos = projectedAxisInfosFromStats(stats, tightBounds, basisAxes);
  if (!basisAxes && !stats.basisAxes) {
    for (const axisInfo of axisInfos) {
      axisInfo.coordinateAxis = axisInfo.axis;
    }
  }
  const candidates = kdProjectionCandidatesFromAxisInfos(axisInfos);
  const primary = candidates[0];
  if (!primary) {
    return null;
  }
  return {
    kind: ROUTE_MODE_KD,
    basisAxis: primary.basisAxis,
    splitDirection: primary.splitDirection.slice(),
    splitOffset: null,
    projectionMin: primary.projectionMin,
    projectionMax: primary.projectionMax,
    projectionCandidates: candidates,
  };
}

function resetAdaptiveProjectionStats(stats) {
  const action = stats.action;
  ensure(
    action && action.kind === ROUTE_MODE_KD,
    `Missing k-d projection action for node ${stats.node.key}.`,
  );
  const segmentCount = ADAPTIVE_SPLIT_EQUAL_SEGMENTS;
  const parentVolume = boundsVolume(stats.node.bounds);
  const candidates =
    action.projectionCandidates && action.projectionCandidates.length > 0
      ? action.projectionCandidates
      : [
          {
            basisAxis: action.basisAxis,
            splitDirection: action.splitDirection,
            coordinateAxis: action.coordinateAxis,
            projectionMin: action.projectionMin,
            projectionMax: action.projectionMax,
            axisScoreMultiplier: 1.0,
          },
        ];
  stats.projectionCandidateStats = candidates.map((candidate) => ({
    basisAxis: candidate.basisAxis,
    splitDirection: candidate.splitDirection.slice(),
    coordinateAxis: Number.isInteger(candidate.coordinateAxis)
      ? candidate.coordinateAxis
      : null,
    projectionMin: candidate.projectionMin,
    projectionMax: candidate.projectionMax,
    axisScoreMultiplier:
      Number.isFinite(candidate.axisScoreMultiplier) &&
      candidate.axisScoreMultiplier > 0.0
        ? candidate.axisScoreMultiplier
        : 1.0,
    parentVolume,
    splitMidpointPenaltyWeight: stats.splitMidpointPenaltyWeight,
    splitCountBalancePenaltyWeight: stats.splitCountBalancePenaltyWeight,
    projectionBins: makeProjectionVolumeBins(segmentCount),
    projectionTotalCount: 0,
  }));
}

function projectionBinIndex(projection, min, extent, binCount) {
  if (
    !Number.isFinite(projection) ||
    !Number.isFinite(min) ||
    !Number.isFinite(extent) ||
    extent <= 0.0 ||
    !Number.isInteger(binCount) ||
    binCount <= 0
  ) {
    return 0;
  }
  let bin = Math.floor(((projection - min) / extent) * binCount);
  return Math.max(0, Math.min(binCount - 1, bin));
}

function makeProjectionVolumeBin() {
  return {
    count: 0,
    minimum: [Infinity, Infinity, Infinity],
    maximum: [-Infinity, -Infinity, -Infinity],
  };
}

function makeProjectionVolumeBins(binCount) {
  const count = new Float64Array(binCount);
  const minimum = new Float64Array(binCount * 3);
  const maximum = new Float64Array(binCount * 3);
  minimum.fill(Infinity);
  maximum.fill(-Infinity);
  return {
    binCount,
    count,
    minimum,
    maximum,
  };
}

function updateProjectionVolumeBin(bins, index, x, y, z) {
  bins.count[index] += 1;
  const base = index * 3;
  const minimum = bins.minimum;
  const maximum = bins.maximum;
  if (x < minimum[base + 0]) minimum[base + 0] = x;
  if (y < minimum[base + 1]) minimum[base + 1] = y;
  if (z < minimum[base + 2]) minimum[base + 2] = z;
  if (x > maximum[base + 0]) maximum[base + 0] = x;
  if (y > maximum[base + 1]) maximum[base + 1] = y;
  if (z > maximum[base + 2]) maximum[base + 2] = z;
}

function addVolumeBinToAccumulator(accumulator, bins, index) {
  const binCount = bins.count[index];
  if (binCount <= 0) {
    return;
  }
  accumulator.count += binCount;
  const base = index * 3;
  const accMin = accumulator.minimum;
  const accMax = accumulator.maximum;
  const binMin = bins.minimum;
  const binMax = bins.maximum;
  const minX = binMin[base + 0];
  const minY = binMin[base + 1];
  const minZ = binMin[base + 2];
  const maxX = binMax[base + 0];
  const maxY = binMax[base + 1];
  const maxZ = binMax[base + 2];
  if (minX < accMin[0]) accMin[0] = minX;
  if (minY < accMin[1]) accMin[1] = minY;
  if (minZ < accMin[2]) accMin[2] = minZ;
  if (maxX > accMax[0]) accMax[0] = maxX;
  if (maxY > accMax[1]) accMax[1] = maxY;
  if (maxZ > accMax[2]) accMax[2] = maxZ;
}

function buildProjectionVolumeSuffixAccumulators(bins) {
  const binCount = bins.binCount;
  const count = new Float64Array(binCount + 1);
  const minimum = new Float64Array((binCount + 1) * 3);
  const maximum = new Float64Array((binCount + 1) * 3);
  let suffixCount = 0;
  let minX = Infinity;
  let minY = Infinity;
  let minZ = Infinity;
  let maxX = -Infinity;
  let maxY = -Infinity;
  let maxZ = -Infinity;
  const store = (index) => {
    const base = index * 3;
    count[index] = suffixCount;
    minimum[base + 0] = minX;
    minimum[base + 1] = minY;
    minimum[base + 2] = minZ;
    maximum[base + 0] = maxX;
    maximum[base + 1] = maxY;
    maximum[base + 2] = maxZ;
  };
  store(binCount);
  for (let index = binCount - 1; index >= 0; index--) {
    const currentCount = bins.count[index];
    if (currentCount > 0) {
      suffixCount += currentCount;
      const base = index * 3;
      const binMin = bins.minimum;
      const binMax = bins.maximum;
      if (binMin[base + 0] < minX) minX = binMin[base + 0];
      if (binMin[base + 1] < minY) minY = binMin[base + 1];
      if (binMin[base + 2] < minZ) minZ = binMin[base + 2];
      if (binMax[base + 0] > maxX) maxX = binMax[base + 0];
      if (binMax[base + 1] > maxY) maxY = binMax[base + 1];
      if (binMax[base + 2] > maxZ) maxZ = binMax[base + 2];
    }
    store(index);
  }
  return { count, minimum, maximum };
}

function suffixAccumulatorVolume(suffixes, index) {
  const base = index * 3;
  return volumeFromExtentValues(
    suffixes.maximum[base + 0] - suffixes.minimum[base + 0],
    suffixes.maximum[base + 1] - suffixes.minimum[base + 1],
    suffixes.maximum[base + 2] - suffixes.minimum[base + 2],
  );
}

function childEntryFromValues(slot, count, minimum, maximum) {
  return {
    slot,
    count,
    minimum: minimum.slice(),
    maximum: maximum.slice(),
  };
}

function splitProjectionForCandidate(candidate, x, y, z) {
  const axis = candidate.coordinateAxis;
  if (axis === 0) return x;
  if (axis === 1) return y;
  if (axis === 2) return z;
  return dotDirection(candidate.splitDirection, x, y, z);
}

function addProjectionVolumePoint(candidate, projection, x, y, z) {
  const bins = candidate.projectionBins;
  const min = candidate.projectionMin;
  const max = candidate.projectionMax;
  const extent = max - min;
  const binCount = bins ? bins.binCount : 0;
  if (!bins || binCount <= 0) {
    return false;
  }
  if (!Number.isFinite(projection)) {
    return false;
  }
  if (!Number.isFinite(extent) || extent <= 0.0) {
    return false;
  }
  const bin = projectionBinIndex(projection, min, extent, binCount);
  updateProjectionVolumeBin(bins, bin, x, y, z);
  return true;
}

function updateAdaptiveProjectionStats(stats, x, y, z) {
  const candidates = stats.projectionCandidateStats;
  if (!candidates || candidates.length === 0) {
    return;
  }
  for (const candidate of candidates) {
    const projection = splitProjectionForCandidate(candidate, x, y, z);
    if (addProjectionVolumePoint(candidate, projection, x, y, z)) {
      candidate.projectionTotalCount += 1;
    }
  }
}

function normalizedProjectionMidpointPenalty(split, min, max) {
  if (
    !Number.isFinite(split) ||
    !Number.isFinite(min) ||
    !Number.isFinite(max)
  ) {
    return Infinity;
  }
  const extent = max - min;
  if (!Number.isFinite(extent) || extent <= ADAPTIVE_SPLIT_EPSILON) {
    return Infinity;
  }
  const halfExtent = extent * 0.5;
  const midpoint = min + halfExtent;
  return Math.min(1.0, Math.max(0.0, Math.abs(split - midpoint) / halfExtent));
}

function normalizedProjectionVolumeScore(volumeSum, parentVolume) {
  if (!Number.isFinite(volumeSum)) {
    return Infinity;
  }
  if (
    !Number.isFinite(parentVolume) ||
    parentVolume <= ADAPTIVE_SPLIT_EPSILON
  ) {
    return volumeSum;
  }
  return Math.max(0.0, volumeSum / parentVolume);
}

function projectionSplitObjectiveScore(
  volumeScore,
  midpointPenalty,
  countBalance,
  midpointPenaltyWeight,
  countBalancePenaltyWeight,
) {
  if (
    !Number.isFinite(volumeScore) ||
    !Number.isFinite(midpointPenalty) ||
    !Number.isFinite(countBalance)
  ) {
    return Infinity;
  }
  const midpointWeight = normalizeSplitPenaltyWeight(
    midpointPenaltyWeight,
    DEFAULT_ADAPTIVE_SPLIT_MIDPOINT_PENALTY_WEIGHT,
  );
  const countWeight = normalizeSplitPenaltyWeight(
    countBalancePenaltyWeight,
    DEFAULT_ADAPTIVE_SPLIT_COUNT_BALANCE_PENALTY_WEIGHT,
  );
  return (
    volumeScore * ADAPTIVE_SPLIT_VOLUME_SCORE_WEIGHT +
    midpointPenalty * midpointWeight +
    countBalance * countWeight
  );
}

function chooseProjectionSplitForCandidate(candidate) {
  const bins = candidate.projectionBins;
  if (!bins) {
    return null;
  }
  const min = candidate.projectionMin;
  const max = candidate.projectionMax;
  const extent = max - min;
  const binCount = bins.binCount;
  const epsilon = Math.max(
    ADAPTIVE_SPLIT_EPSILON,
    Math.abs(extent) * ADAPTIVE_SPLIT_EPSILON,
  );
  if (
    !Number.isFinite(extent) ||
    extent <= epsilon ||
    binCount <= 1 ||
    !Number.isInteger(candidate.projectionTotalCount) ||
    candidate.projectionTotalCount <= 1
  ) {
    return null;
  }

  const suffixes = buildProjectionVolumeSuffixAccumulators(bins);
  const lower = makeProjectionVolumeBin();
  let bestBoundary = -1;
  let bestSplitOffset = null;
  let bestScore = Infinity;
  let bestVolumeSum = Infinity;
  let bestMaxChildVolume = Infinity;
  let bestCountBalance = Infinity;
  let bestMidpointPenalty = Infinity;
  let bestLowerCount = 0;
  let bestUpperCount = 0;
  let bestLowerMinimum = null;
  let bestLowerMaximum = null;
  let bestUpperMinimum = null;
  let bestUpperMaximum = null;
  for (let boundary = 1; boundary < binCount; boundary++) {
    addVolumeBinToAccumulator(lower, bins, boundary - 1);
    const upperCount = suffixes.count[boundary];
    if (lower.count <= 0 || upperCount <= 0) {
      continue;
    }
    const split = min + (extent * boundary) / binCount;
    if (split <= min + epsilon || split >= max - epsilon) {
      continue;
    }
    const lowerVolume = volumeFromMinMax(lower.minimum, lower.maximum);
    const upperVolume = suffixAccumulatorVolume(suffixes, boundary);
    const volumeSum = lowerVolume + upperVolume;
    if (!Number.isFinite(volumeSum)) {
      continue;
    }
    const midpointPenalty = normalizedProjectionMidpointPenalty(
      split,
      min,
      max,
    );
    const volumeScore = normalizedProjectionVolumeScore(
      volumeSum,
      candidate.parentVolume,
    );
    const maxChildVolume = Math.max(lowerVolume, upperVolume);
    const countBalance =
      Math.abs(lower.count - upperCount) / candidate.projectionTotalCount;
    const score = projectionSplitObjectiveScore(
      volumeScore,
      midpointPenalty,
      countBalance,
      candidate.splitMidpointPenaltyWeight,
      candidate.splitCountBalancePenaltyWeight,
    );
    if (!Number.isFinite(score)) {
      continue;
    }
    if (
      projectionSplitMetricsAreBetter(
        score,
        volumeSum,
        maxChildVolume,
        countBalance,
        midpointPenalty,
        bestScore,
        bestVolumeSum,
        bestMaxChildVolume,
        bestCountBalance,
        bestMidpointPenalty,
      )
    ) {
      const upperBase = boundary * 3;
      bestBoundary = boundary;
      bestSplitOffset = split;
      bestScore = score;
      bestVolumeSum = volumeSum;
      bestMaxChildVolume = maxChildVolume;
      bestCountBalance = countBalance;
      bestMidpointPenalty = midpointPenalty;
      bestLowerCount = lower.count;
      bestUpperCount = upperCount;
      bestLowerMinimum = lower.minimum.slice();
      bestLowerMaximum = lower.maximum.slice();
      bestUpperMinimum = [
        suffixes.minimum[upperBase + 0],
        suffixes.minimum[upperBase + 1],
        suffixes.minimum[upperBase + 2],
      ];
      bestUpperMaximum = [
        suffixes.maximum[upperBase + 0],
        suffixes.maximum[upperBase + 1],
        suffixes.maximum[upperBase + 2],
      ];
    }
  }
  if (bestBoundary <= 0) {
    return null;
  }
  return {
    candidate,
    splitOffset: bestSplitOffset,
    boundary: bestBoundary,
    score: bestScore,
    adjustedScore:
      bestScore *
      (Number.isFinite(candidate.axisScoreMultiplier) &&
      candidate.axisScoreMultiplier > 0.0
        ? candidate.axisScoreMultiplier
        : 1.0),
    volumeSum: bestVolumeSum,
    maxChildVolume: bestMaxChildVolume,
    countBalance: bestCountBalance,
    midpointPenalty: bestMidpointPenalty,
    childEntries: [
      childEntryFromValues(
        0,
        bestLowerCount,
        bestLowerMinimum,
        bestLowerMaximum,
      ),
      childEntryFromValues(
        1,
        bestUpperCount,
        bestUpperMinimum,
        bestUpperMaximum,
      ),
    ],
  };
}

function pairwiseComparisonEpsilon(lhs, rhs) {
  const lhsScale = Number.isFinite(lhs) ? Math.abs(lhs) : 0.0;
  const rhsScale = Number.isFinite(rhs) ? Math.abs(rhs) : 0.0;
  const scale = Math.max(1.0, lhsScale, rhsScale);
  return ADAPTIVE_SPLIT_EPSILON * scale;
}

function valueIsLess(lhs, rhs) {
  return lhs < rhs - pairwiseComparisonEpsilon(lhs, rhs);
}

function valueIsGreater(lhs, rhs) {
  return lhs > rhs + pairwiseComparisonEpsilon(lhs, rhs);
}

function compareProjectionSplitMetricValues(
  score,
  volumeSum,
  maxChildVolume,
  countBalance,
  midpointPenalty,
  bestScore,
  bestVolumeSum,
  bestMaxChildVolume,
  bestCountBalance,
  bestMidpointPenalty,
) {
  if (valueIsLess(score, bestScore)) {
    return -1;
  }
  if (valueIsGreater(score, bestScore)) {
    return 1;
  }
  if (valueIsLess(volumeSum, bestVolumeSum)) {
    return -1;
  }
  if (valueIsGreater(volumeSum, bestVolumeSum)) {
    return 1;
  }
  if (valueIsLess(maxChildVolume, bestMaxChildVolume)) {
    return -1;
  }
  if (valueIsGreater(maxChildVolume, bestMaxChildVolume)) {
    return 1;
  }
  if (valueIsLess(countBalance, bestCountBalance)) {
    return -1;
  }
  if (valueIsGreater(countBalance, bestCountBalance)) {
    return 1;
  }
  if (valueIsLess(midpointPenalty, bestMidpointPenalty)) {
    return -1;
  }
  if (valueIsGreater(midpointPenalty, bestMidpointPenalty)) {
    return 1;
  }
  return 0;
}

function projectionSplitChoiceIsBetter(choice, bestChoice) {
  if (!choice) {
    return false;
  }
  if (!bestChoice) {
    return true;
  }
  const metricComparison = compareProjectionSplitMetricValues(
    Number.isFinite(choice.adjustedScore) ? choice.adjustedScore : choice.score,
    choice.volumeSum,
    choice.maxChildVolume,
    choice.countBalance,
    choice.midpointPenalty,
    Number.isFinite(bestChoice.adjustedScore)
      ? bestChoice.adjustedScore
      : bestChoice.score,
    bestChoice.volumeSum,
    bestChoice.maxChildVolume,
    bestChoice.countBalance,
    bestChoice.midpointPenalty,
  );
  if (metricComparison !== 0) {
    return metricComparison < 0;
  }
  const choiceExtent =
    choice.candidate.projectionMax - choice.candidate.projectionMin;
  const bestExtent =
    bestChoice.candidate.projectionMax - bestChoice.candidate.projectionMin;
  if (valueIsGreater(choiceExtent, bestExtent)) {
    return true;
  }
  if (valueIsLess(choiceExtent, bestExtent)) {
    return false;
  }
  return (
    (choice.candidate.basisAxis || 0) < (bestChoice.candidate.basisAxis || 0)
  );
}

function projectionSplitMetricsAreBetter(
  score,
  volumeSum,
  maxChildVolume,
  countBalance,
  midpointPenalty,
  bestScore,
  bestVolumeSum,
  bestMaxChildVolume,
  bestCountBalance,
  bestMidpointPenalty,
) {
  if (!Number.isFinite(bestScore)) {
    return true;
  }
  return (
    compareProjectionSplitMetricValues(
      score,
      volumeSum,
      maxChildVolume,
      countBalance,
      midpointPenalty,
      bestScore,
      bestVolumeSum,
      bestMaxChildVolume,
      bestCountBalance,
      bestMidpointPenalty,
    ) < 0
  );
}

function chooseProjectionSplit(stats) {
  const action = stats.action;
  const candidates = stats.projectionCandidateStats;
  if (!action || !candidates || candidates.length === 0) {
    return null;
  }

  let bestChoice = null;
  for (const candidate of candidates) {
    const choice = chooseProjectionSplitForCandidate(candidate);
    if (projectionSplitChoiceIsBetter(choice, bestChoice)) {
      bestChoice = choice;
    }
  }
  return bestChoice;
}

function finalizeKdSplitAction(stats) {
  const action = stats.action;
  if (!action || action.kind !== ROUTE_MODE_KD) {
    return true;
  }
  const choice = chooseProjectionSplit(stats);
  const splitOffset = choice ? choice.splitOffset : null;
  if (!Number.isFinite(splitOffset)) {
    return false;
  }
  const candidate = choice.candidate;
  action.basisAxis = candidate.basisAxis;
  action.splitDirection = candidate.splitDirection.slice();
  action.coordinateAxis = Number.isInteger(candidate.coordinateAxis)
    ? candidate.coordinateAxis
    : null;
  action.projectionMin = candidate.projectionMin;
  action.projectionMax = candidate.projectionMax;
  action.splitOffset = splitOffset;
  action.childEntries = choice.childEntries;
  stats.splitDirection = action.splitDirection.slice();
  stats.splitOffset = splitOffset;
  return true;
}

function axisDirection(axis) {
  return axis === 1
    ? [0.0, 1.0, 0.0]
    : axis === 2
      ? [0.0, 0.0, 1.0]
      : [1.0, 0.0, 0.0];
}

function makeLongTileSplitAction(
  node,
  tightBounds,
  basisAxes = null,
  projectedAspectInfo = null,
) {
  if (node.level <= 0 || node.aspectSplitExhausted) {
    return null;
  }

  const aspectInfo =
    projectedAspectInfo ||
    (basisAxes
      ? projectedAspectInfoForBasis(tightBounds, basisAxes)
      : longWidthAspectFromExtents(tightBounds.extents()));
  if (aspectInfo.aspect <= ADAPTIVE_MAX_LONG_WIDTH_RATIO) {
    return null;
  }

  const axis = basisAxes ? aspectInfo.axis : aspectInfo.longestAxis;
  const splitDirection = basisAxes ? aspectInfo.direction : axisDirection(axis);
  const projectionMin = basisAxes
    ? aspectInfo.range.minimum
    : tightBounds.minimum[axis];
  const projectionMax = basisAxes
    ? aspectInfo.range.maximum
    : tightBounds.maximum[axis];
  const projectionExtent = projectionMax - projectionMin;
  const epsilon = Math.max(
    ADAPTIVE_SPLIT_EPSILON,
    Math.abs(projectionExtent) * ADAPTIVE_SPLIT_EPSILON,
  );
  if (
    !splitDirection ||
    !Number.isFinite(projectionExtent) ||
    projectionExtent <= epsilon ||
    aspectInfo.widthExtent <= epsilon
  ) {
    return null;
  }

  return {
    kind: ROUTE_MODE_KD,
    basisAxis: axis,
    longTileSplit: true,
    splitDirection,
    splitOffset: null,
    projectionMin,
    projectionMax,
  };
}

function chooseAdaptiveSplitAction(
  stats,
  tightBounds,
  maxDepth,
  leafLimit,
  basisAxes = null,
  options = {},
) {
  const node = stats.node;
  const tileRefinement = normalizedTileRefinement(options.tileRefinement);
  const build = existingNodeBuildState(node);
  if (
    build?.forceVolumeRebalanceSplit === true &&
    build.volumeRebalanceSplitExhausted !== true
  ) {
    const action = makeKdSplitAction(stats, tightBounds, basisAxes);
    if (action) {
      action.volumeRebalanceSplit = true;
      return action;
    }
    return {
      kind: null,
      aspectExhausted: false,
      splitExhausted: false,
      volumeRebalanceExhausted: true,
    };
  }

  if (canSplitRootTileRefinement(node, maxDepth, tileRefinement)) {
    const action = makeKdSplitAction(stats, tightBounds, basisAxes);
    if (action) {
      action.rootTileRefinement = true;
      return action;
    }
    return {
      kind: null,
      aspectExhausted: false,
      splitExhausted: false,
      rootTileRefinementExhausted: true,
    };
  }

  const projectedAspectInfo = basisAxes
    ? projectedAspectInfoFromStats(stats, tightBounds, basisAxes)
    : null;
  const aspectCandidate =
    node.level > 0 &&
    !node.aspectSplitExhausted &&
    (projectedAspectInfo
      ? projectedAspectInfo.aspect
      : boundsLongWidthAspect(tightBounds)) > ADAPTIVE_MAX_LONG_WIDTH_RATIO;
  if (aspectCandidate) {
    const action = makeLongTileSplitAction(
      node,
      tightBounds,
      basisAxes,
      projectedAspectInfo,
    );
    if (action) {
      return action;
    }
  }

  if (!node.splitExhausted && node.depth < maxDepth && node.count > leafLimit) {
    const action = makeKdSplitAction(stats, tightBounds, basisAxes);
    if (action) {
      return action;
    }
    return {
      kind: null,
      aspectExhausted: aspectCandidate,
      splitExhausted: true,
    };
  }

  return {
    kind: null,
    aspectExhausted: aspectCandidate,
    splitExhausted: false,
  };
}

function clearNodeSplitRouting(node) {
  node.splitDirection = null;
  node.splitOffset = null;
}

function applyAdaptiveSplitActionToNode(node, action) {
  ensure(
    action && action.kind === ROUTE_MODE_KD,
    `Unsupported adaptive split action for node ${node.key}.`,
  );
  node.splitDirection = action.splitDirection.slice();
  node.splitOffset = action.splitOffset;
}

function pointKdSlotForAction(action, x, y, z) {
  ensure(
    action && action.splitDirection && Number.isFinite(action.splitOffset),
    'Missing k-d split plane for action.',
  );
  return pointPlaneSlot(action.splitDirection, action.splitOffset, x, y, z);
}

function childSlotForAdaptiveAction(stats, x, y, z) {
  return pointKdSlotForAction(stats.action, x, y, z);
}

function makeAdaptiveChildStatsSlots() {
  return new Array(KD_CHILD_SLOT_COUNT).fill(null);
}

function adaptiveChildStatsHasEntries(childStats) {
  return !!(childStats && childStats.some((entry) => !!entry));
}

function updateAdaptiveChildStats(stats, slot, x, y, z) {
  if (!stats.childStats) {
    stats.childStats = makeAdaptiveChildStatsSlots();
  }
  let entry = stats.childStats[slot];
  if (!entry) {
    entry = {
      slot,
      count: 0,
      minimum: [Infinity, Infinity, Infinity],
      maximum: [-Infinity, -Infinity, -Infinity],
    };
    stats.childStats[slot] = entry;
  }
  entry.count += 1;
  const minimum = entry.minimum;
  const maximum = entry.maximum;
  if (x < minimum[0]) minimum[0] = x;
  if (y < minimum[1]) minimum[1] = y;
  if (z < minimum[2]) minimum[2] = z;
  if (x > maximum[0]) maximum[0] = x;
  if (y > maximum[1]) maximum[1] = y;
  if (z > maximum[2]) maximum[2] = z;
}

function sortedAdaptiveChildStats(stats) {
  if (!stats.childStats) {
    return [];
  }
  return stats.childStats.filter((entry) => !!entry);
}

function childStatsSlotsFromSplitAction(action) {
  const entries = action && action.childEntries;
  if (!Array.isArray(entries) || entries.length === 0) {
    return null;
  }
  const out = makeAdaptiveChildStatsSlots();
  for (const entry of entries) {
    if (
      !entry ||
      entry.count <= 0 ||
      !Number.isInteger(entry.slot) ||
      entry.slot < 0 ||
      entry.slot >= KD_CHILD_SLOT_COUNT
    ) {
      continue;
    }
    out[entry.slot] = {
      slot: entry.slot,
      count: entry.count,
      minimum: entry.minimum.slice(),
      maximum: entry.maximum.slice(),
    };
  }
  return adaptiveChildStatsHasEntries(out) ? out : null;
}

function makeAdaptiveChildEntry(slot, start = null, end = null) {
  return {
    slot,
    count: 0,
    minimum: [Infinity, Infinity, Infinity],
    maximum: [-Infinity, -Infinity, -Infinity],
    start,
    end,
  };
}

function updateAdaptiveChildEntry(entry, x, y, z) {
  entry.count += 1;
  if (x < entry.minimum[0]) entry.minimum[0] = x;
  if (y < entry.minimum[1]) entry.minimum[1] = y;
  if (z < entry.minimum[2]) entry.minimum[2] = z;
  if (x > entry.maximum[0]) entry.maximum[0] = x;
  if (y > entry.maximum[1]) entry.maximum[1] = y;
  if (z > entry.maximum[2]) entry.maximum[2] = z;
}

function makeAdaptiveRootNode(rootBounds, vertexCount) {
  return makePartitionTreeNode({
    level: 0,
    depth: 0,
    x: 0,
    y: 0,
    z: 0,
    bounds: cloneBounds(rootBounds),
    count: vertexCount,
    leaf: true,
  });
}

function makePositionIndexArray(vertexCount) {
  const indices = new Uint32Array(vertexCount);
  for (let rowIndex = 0; rowIndex < vertexCount; rowIndex++) {
    indices[rowIndex] = rowIndex;
  }
  return indices;
}

function setNodePositionIndexRange(node, start, end) {
  const build = nodeBuildState(node);
  build.positionIndexStart = start;
  build.positionIndexEnd = end;
}

function nodePositionIndexRange(node) {
  const build = existingNodeBuildState(node);
  return build &&
    Number.isInteger(build.positionIndexStart) &&
    Number.isInteger(build.positionIndexEnd)
    ? build
    : null;
}

function clearNodePositionIndexRanges(node) {
  const build = existingNodeBuildState(node);
  if (build) {
    delete build.positionIndexStart;
    delete build.positionIndexEnd;
    pruneNodeBuildState(node);
  }
  for (const child of node.children) {
    clearNodePositionIndexRanges(child);
  }
}

function makeTilingProgressState(progress, vertexCount, maxDepth) {
  if (!progress) {
    return null;
  }
  const rows = Math.max(1, Math.floor(vertexCount || 1));
  const depthRounds = Math.max(1, Math.floor(maxDepth || 1));
  const total = rows * depthRounds * 2;
  const state = {
    progress,
    total,
    estimatedTotal: total,
    current: 0,
    message: 'building k-d tree',
    estimateExpanded: false,
    estimateExpansionLogged: false,
    virtualSegmentActions: 0,
    virtualSegmentCount: 0,
  };
  if (typeof progress.reset === 'function') {
    progress.reset(total, formatTilingProgressMessage(state));
  } else {
    progress.current = 0;
    progress.setTotal(total);
    progress.update(0, formatTilingProgressMessage(state));
  }
  return state;
}

function formatProgressInteger(value) {
  return Math.max(0, Math.floor(value || 0)).toLocaleString('en-US');
}

function formatTilingProgressMessage(state, message = null) {
  const base = message || (state ? state.message : '') || 'building k-d tree';
  if (!state || !state.estimateExpanded) {
    return base;
  }
  const overEstimateRows = Math.max(0, state.current - state.estimatedTotal);
  if (state.virtualSegmentActions > 0) {
    return (
      `${base} | extra virtual long-tile work ` +
      `splits=${state.virtualSegmentActions} ` +
      `segments=${state.virtualSegmentCount} ` +
      `over=${formatProgressInteger(overEstimateRows)} rows`
    );
  }
  return `${base} | work estimate expanded`;
}

function noteTilingVirtualSegmentAction(progressState, action) {
  if (!progressState || !splitActionIsLongTile(action)) {
    return;
  }
  progressState.virtualSegmentActions += 1;
  progressState.virtualSegmentCount += Math.max(
    0,
    LONG_TILE_VIRTUAL_KD_SEGMENT_COUNT,
  );
}

function maybeExpandTilingProgressTotal(state) {
  if (!state || state.current < state.total) {
    return;
  }
  const increment = Math.max(
    TILING_TREE_PROGRESS_ROW_INTERVAL,
    Math.floor(state.total * 0.1),
  );
  state.estimateExpanded = true;
  state.total = state.current + increment;
  state.progress.setTotal(state.total);
  if (
    !state.estimateExpansionLogged &&
    typeof state.progress.logDetail === 'function'
  ) {
    state.progress.logDetail('work estimate expanded');
    state.estimateExpansionLogged = true;
  }
}

function startTilingProgressPhase(progressState, total, message) {
  if (!progressState) {
    return null;
  }
  if (message) {
    progressState.message = message;
    progressState.progress.update(
      progressState.current,
      formatTilingProgressMessage(progressState, message),
    );
  }
  return {
    overall: progressState,
    total: Math.max(0, Math.floor(total || 0)),
    current: 0,
    nextUpdate: TILING_TREE_PROGRESS_ROW_INTERVAL,
  };
}

function advanceTilingProgress(state, rows, { force = false } = {}) {
  if (!state || rows < 0) {
    return;
  }
  if (rows > 0) {
    state.current += rows;
    state.overall.current += rows;
  }
  if (!force && state.current < state.nextUpdate) {
    return;
  }
  maybeExpandTilingProgressTotal(state.overall);
  const visibleCurrent =
    state.overall.total > 0
      ? Math.min(state.overall.current, state.overall.total)
      : state.overall.current;
  state.overall.progress.update(
    visibleCurrent,
    formatTilingProgressMessage(state.overall),
  );
  state.nextUpdate =
    state.current + Math.max(1, TILING_TREE_PROGRESS_ROW_INTERVAL);
}

function updateAdaptiveSplitStatsForPositionIndexRange(
  stats,
  positions,
  indices,
  start,
  end,
  progressState = null,
) {
  let pendingProgressRows = 0;
  for (let offset = start; offset < end; offset++) {
    const rowIndex = indices[offset];
    const base = rowIndex * 3;
    updateAdaptiveSplitStats(
      stats,
      positions[base + 0],
      positions[base + 1],
      positions[base + 2],
    );
    pendingProgressRows += 1;
    if (pendingProgressRows >= TILING_TREE_PROGRESS_ROW_INTERVAL) {
      advanceTilingProgress(progressState, pendingProgressRows);
      pendingProgressRows = 0;
    }
  }
  if (pendingProgressRows > 0) {
    advanceTilingProgress(progressState, pendingProgressRows);
  }
}

function updateAdaptiveProjectionStatsForPositionIndexRange(
  stats,
  positions,
  indices,
  start,
  end,
  progressState = null,
) {
  let pendingProgressRows = 0;
  for (let offset = start; offset < end; offset++) {
    const rowIndex = indices[offset];
    const base = rowIndex * 3;
    updateAdaptiveProjectionStats(
      stats,
      positions[base + 0],
      positions[base + 1],
      positions[base + 2],
    );
    pendingProgressRows += 1;
    if (pendingProgressRows >= TILING_TREE_PROGRESS_ROW_INTERVAL) {
      advanceTilingProgress(progressState, pendingProgressRows);
      pendingProgressRows = 0;
    }
  }
  if (pendingProgressRows > 0) {
    advanceTilingProgress(progressState, pendingProgressRows);
  }
}

function partitionKdPositionIndexRange(
  stats,
  positions,
  indices,
  start,
  end,
  progressState = null,
) {
  const action = stats.action;
  ensure(
    action && action.splitDirection && Number.isFinite(action.splitOffset),
    `Missing k-d split plane for node ${stats.node.key}.`,
  );
  const split = action.splitOffset;
  ensure(
    Number.isFinite(split),
    `Missing k-d split offset for node ${stats.node.key}.`,
  );
  const coordinateAxis =
    Number.isInteger(action.coordinateAxis) ? action.coordinateAxis : null;

  const lower = makeAdaptiveChildEntry(0, start, null);
  const upper = makeAdaptiveChildEntry(1, null, end);
  let left = start;
  let right = end - 1;
  let pendingProgressRows = 0;

  while (left <= right) {
    pendingProgressRows += 1;
    if (pendingProgressRows >= TILING_TREE_PROGRESS_ROW_INTERVAL) {
      advanceTilingProgress(progressState, pendingProgressRows);
      pendingProgressRows = 0;
    }

    const rowIndex = indices[left];
    const base = rowIndex * 3;
    const x = positions[base + 0];
    const y = positions[base + 1];
    const z = positions[base + 2];
    const coordinate =
      coordinateAxis != null
        ? coordinateForAxis(coordinateAxis, x, y, z)
        : dotDirection(action.splitDirection, x, y, z);

    if (coordinate >= split) {
      updateAdaptiveChildEntry(upper, x, y, z);
      const swap = indices[right];
      indices[right] = rowIndex;
      indices[left] = swap;
      right -= 1;
    } else {
      updateAdaptiveChildEntry(lower, x, y, z);
      left += 1;
    }
  }

  lower.end = left;
  upper.start = left;
  ensure(
    lower.count + upper.count === end - start,
    `K-d child partition row mismatch for node ${stats.node.key}.`,
  );
  ensure(
    lower.end - lower.start === lower.count &&
      upper.end - upper.start === upper.count,
    `K-d child partition range mismatch for node ${stats.node.key}.`,
  );

  if (pendingProgressRows > 0) {
    advanceTilingProgress(progressState, pendingProgressRows);
  }

  const occupied = [];
  if (lower.count > 0) {
    occupied.push(lower);
  }
  if (upper.count > 0) {
    occupied.push(upper);
  }
  return occupied;
}

async function forEachStagedPosition(source, callback) {
  if (source.positions) {
    const positions = source.positions;
    const weights = source.weights || null;
    for (let rowIndex = 0; rowIndex < source.vertexCount; rowIndex++) {
      const base = rowIndex * 3;
      callback(
        positions[base + 0],
        positions[base + 1],
        positions[base + 2],
        weights ? weights[rowIndex] : 1.0,
      );
    }
    return;
  }

  const targetChunkBytes =
    Number.isFinite(source.chunkBytes) && source.chunkBytes > 0
      ? Math.floor(source.chunkBytes)
      : 8 * 1024 * 1024;
  const rowsPerChunk = Math.max(
    1,
    Math.floor(targetChunkBytes / POSITION_ROW_BYTE_SIZE),
  );
  const chunk = Buffer.allocUnsafe(rowsPerChunk * POSITION_ROW_BYTE_SIZE);
  const floatView =
    IS_LITTLE_ENDIAN && (chunk.byteOffset & 3) === 0
      ? new Float32Array(
          chunk.buffer,
          chunk.byteOffset,
          rowsPerChunk * POSITION_ROW_FLOAT_COUNT,
        )
      : null;
  const handle = await fs.promises.open(source.positionsPath, 'r');
  try {
    let fileOffset = 0;
    for (
      let rowBase = 0;
      rowBase < source.vertexCount;
      rowBase += rowsPerChunk
    ) {
      const rowCount = Math.min(rowsPerChunk, source.vertexCount - rowBase);
      const byteCount = rowCount * POSITION_ROW_BYTE_SIZE;
      await readExactFromHandle(
        handle,
        chunk,
        byteCount,
        fileOffset,
        `Staged position file ended early: ${source.positionsPath}`,
      );
      fileOffset += byteCount;

      for (let i = 0; i < rowCount; i++) {
        let x;
        let y;
        let z;
        let weight;
        if (floatView) {
          const base = i * POSITION_ROW_FLOAT_COUNT;
          x = floatView[base + 0];
          y = floatView[base + 1];
          z = floatView[base + 2];
          weight = floatView[base + 3];
        } else {
          const base = i * POSITION_ROW_BYTE_SIZE;
          x = chunk.readFloatLE(base + 0);
          y = chunk.readFloatLE(base + 4);
          z = chunk.readFloatLE(base + 8);
          weight = chunk.readFloatLE(base + 12);
        }
        callback(x, y, z, weight);
      }
    }
  } finally {
    await handle.close();
  }
}

function forEachNodeOnPointPath(root, x, y, z, callback) {
  let node = root;
  while (node) {
    callback(node);
    if (node.leaf) {
      return;
    }

    const slot = pointKdSlotForNode(node, x, y, z);
    const child = node.childrenBySlot[slot];
    ensure(
      !!child,
      `Failed to resolve OBB stats path for point at node ${node.key} slot=${slot}.`,
    );
    node = child;
  }
}

async function computeRootBasisAxesFromPositions(source) {
  const stats = makeOrientedBoxStats(null);
  await forEachStagedPosition(source, (x, y, z, weight) => {
    updateWeightedPositionMomentStats(stats, x, y, z, weight);
  });
  return orthonormalBasisFromMomentStats(stats);
}

async function computeNodeOrientedBoxesFromPositions(
  root,
  source,
  rootBasisAxes,
) {
  ensure(!!rootBasisAxes, 'Missing root basis axes for OBB computation.');
  const statsByNode = new Map();
  await forEachStagedPosition(source, (x, y, z) => {
    forEachNodeOnPointPath(root, x, y, z, (node) => {
      let stats = statsByNode.get(node);
      if (!stats) {
        stats = makeOrientedBoxStats(node);
        prepareOrientedBoxStats(stats, rootBasisAxes);
        statsByNode.set(node, stats);
      }
      updateOrientedBoxStatsWithBasis(stats, x, y, z);
    });
  });

  for (const stats of statsByNode.values()) {
    const node = stats.node;
    node.count = stats.count;
    node.bounds = boundsFromMinMax(stats.minimum, stats.maximum, node.bounds);
    node.orientedBox = normalizeOrientedBox(
      orientedBoxFromProjectionStats(stats, node.bounds),
    );
  }
}

async function buildAdaptiveNodeTreeFromPositions(
  source,
  rootBounds,
  maxDepth,
  leafLimit,
  options = {},
) {
  const orientedBoundingBoxes = options.orientedBoundingBoxes === true;
  const rootBasisAxes = orientedBoundingBoxes
    ? options.rootBasisAxes || (await computeRootBasisAxesFromPositions(source))
    : null;
  const buildOptions = {
    ...options,
    splitBasisAxes: rootBasisAxes,
  };
  const root = source.positions
    ? await buildAdaptiveNodeTreeFromMemoryPositions(
        source,
        rootBounds,
        maxDepth,
        leafLimit,
        buildOptions,
      )
    : await buildAdaptiveNodeTreeFromStreamingPositions(
        source,
        rootBounds,
        maxDepth,
        leafLimit,
        buildOptions,
      );
  if (orientedBoundingBoxes) {
    await computeNodeOrientedBoxesFromPositions(root, source, rootBasisAxes);
  }
  return root;
}

async function buildAdaptiveNodeTreeFromMemoryPositions(
  source,
  rootBounds,
  maxDepth,
  leafLimit,
  options = {},
) {
  let nextPhysicalCoordinate = 1;
  const allocateChildCoordinates = (parent) => ({
    level: parent.level + 1,
    x: nextPhysicalCoordinate++,
    y: 0,
    z: 0,
  });

  const root = makeAdaptiveRootNode(rootBounds, source.vertexCount);
  const tileRefinement = normalizedTileRefinement(options.tileRefinement);
  markRootTileRefinementStart(root, tileRefinement);

  if (maxDepth <= 0 || source.vertexCount <= 1) {
    return root;
  }

  const positions = source.positions;
  ensure(
    positions && positions.length >= source.vertexCount * 3,
    'In-memory staged positions are missing or truncated.',
  );
  const indices = makePositionIndexArray(source.vertexCount);
  const progress = makeTilingProgressState(
    options.progress || null,
    source.vertexCount,
    maxDepth,
  );
  const splitBasisAxes = options.splitBasisAxes || null;
  const splitPenaltyOptions = makeAdaptiveSplitPenaltyOptions(options);
  setNodePositionIndexRange(root, 0, source.vertexCount);

  try {
    while (true) {
      const candidates = [];
      collectAdaptiveSplitCandidates(root, maxDepth, leafLimit, candidates, {
        tileRefinement,
      });
      if (candidates.length === 0) {
        break;
      }

      const candidateRanges = [];
      let candidateRowTotal = 0;
      for (const node of candidates) {
        const range = nodePositionIndexRange(node);
        const start = range ? range.positionIndexStart : null;
        const end = range ? range.positionIndexEnd : null;
        ensure(
          Number.isInteger(start) && Number.isInteger(end) && end >= start,
          `Missing in-memory position index range for node ${node.key}.`,
        );
        candidateRanges.push({ node, start, end });
        candidateRowTotal += end - start;
      }
      const splitProgress = startTilingProgressPhase(
        progress,
        candidateRowTotal,
        `building k-d tree | split candidates=${candidates.length}`,
      );
      const statsList = [];
      for (const range of candidateRanges) {
        const node = range.node;
        const stats = makeAdaptiveSplitStats(
          node,
          splitBasisAxes,
          splitPenaltyOptions,
        );
        stats.positionIndexStart = range.start;
        stats.positionIndexEnd = range.end;
        updateAdaptiveSplitStatsForPositionIndexRange(
          stats,
          positions,
          indices,
          range.start,
          range.end,
          splitProgress,
        );
        statsList.push(stats);
      }
      advanceTilingProgress(splitProgress, 0, { force: true });

      const actionStats = [];
      const kdActionStats = [];
      for (const stats of statsList) {
        stats.node.count = stats.count;
        const tightBounds = boundsFromMinMax(
          stats.minimum,
          stats.maximum,
          stats.node.bounds,
        );
        const action = chooseAdaptiveSplitAction(
          stats,
          tightBounds,
          maxDepth,
          leafLimit,
          splitBasisAxes,
          { tileRefinement },
        );
        if (!action.kind) {
          stats.node.bounds = tightBounds;
          if (action.aspectExhausted) {
            stats.node.aspectSplitExhausted = true;
          }
          if (action.splitExhausted) {
            stats.node.splitExhausted = true;
          }
          if (action.rootTileRefinementExhausted) {
            markRootTileRefinementExhausted(stats.node);
          }
          if (action.volumeRebalanceExhausted) {
            clearVolumeRebalanceSplitRequest(stats.node);
          }
          continue;
        }
        stats.node.bounds = tightBounds;
        stats.action = action;
        actionStats.push(stats);
        if (action.kind === ROUTE_MODE_KD) {
          resetAdaptiveProjectionStats(stats);
          kdActionStats.push(stats);
        }
      }

      if (kdActionStats.length > 0) {
        let projectionRowTotal = 0;
        for (const stats of kdActionStats) {
          projectionRowTotal +=
            stats.positionIndexEnd - stats.positionIndexStart;
        }
        const projectionProgress = startTilingProgressPhase(
          progress,
          projectionRowTotal,
          `building k-d tree | volume split candidates=${kdActionStats.length}`,
        );
        for (const stats of kdActionStats) {
          updateAdaptiveProjectionStatsForPositionIndexRange(
            stats,
            positions,
            indices,
            stats.positionIndexStart,
            stats.positionIndexEnd,
            projectionProgress,
          );
        }
        advanceTilingProgress(projectionProgress, 0, { force: true });
      }

      const splittableStats = [];
      for (const stats of actionStats) {
        if (
          stats.action.kind === ROUTE_MODE_KD &&
          !finalizeKdSplitAction(stats)
        ) {
          markSplitActionExhausted(stats.node, stats.action);
          continue;
        }
        if (splitActionIsVolumeRebalance(stats.action)) {
          clearVolumeRebalanceSplitRequest(stats.node);
        }
        applyAdaptiveSplitActionToNode(stats.node, stats.action);
        splittableStats.push(stats);
      }

      if (splittableStats.length === 0) {
        break;
      }

      let bucketProgressRowTotal = 0;
      for (const stats of splittableStats) {
        const rowCount = stats.positionIndexEnd - stats.positionIndexStart;
        noteTilingVirtualSegmentAction(progress, stats.action);
        bucketProgressRowTotal += rowCount;
      }
      const bucketProgress = startTilingProgressPhase(
        progress,
        bucketProgressRowTotal,
        `building k-d tree | bucket splits=${splittableStats.length}`,
      );
      let splitNodeCount = 0;
      const affectedVolumeDepths = new Set();
      for (const stats of splittableStats) {
        const node = stats.node;
        const start = stats.positionIndexStart;
        const end = stats.positionIndexEnd;
        const occupied = partitionKdPositionIndexRange(
          stats,
          positions,
          indices,
          start,
          end,
          bucketProgress,
        );

        if (occupied.length <= 1) {
          clearNodeSplitRouting(node);
          node.leaf = true;
          markSplitActionExhausted(node, stats.action);
          continue;
        }

        node.leaf = false;
        node.virtual = splitActionMakesVirtualNode(stats.action);
        node.children = [];
        node.childrenBySlot = makeChildSlotArray();
        node.occupiedChildCount = occupied.length;
        for (const entry of occupied) {
          const slot = entry.slot;
          const coords = allocateChildCoordinates(node);
          const child = makePartitionTreeNode({
            level: coords.level,
            depth: splitActionKeepsLogicalDepth(stats.action)
              ? node.depth
              : node.depth + 1,
            x: coords.x,
            y: coords.y,
            z: coords.z,
            childSlot: slot,
            bounds: boundsFromMinMax(entry.minimum, entry.maximum, node.bounds),
            count: entry.count,
            leaf: true,
          });
          assignRootTileRefinementChildState(
            node,
            child,
            stats.action,
            tileRefinement,
          );
          if (
            node.aspectSplitExhausted ||
            splitActionIsLongTile(stats.action)
          ) {
            child.aspectSplitExhausted = true;
          }
          setNodePositionIndexRange(child, entry.start, entry.end);
          node.children.push(child);
          node.childrenBySlot[slot] = child;
        }
        splitNodeCount += 1;
        noteAffectedVolumeRebalanceDepths(
          affectedVolumeDepths,
          node,
          stats.action,
        );
      }

      if (splitNodeCount > 0) {
        markCurrentLodTilesForVolumeRebalance(root, affectedVolumeDepths);
      }
      advanceTilingProgress(bucketProgress, 0, { force: true });
      if (splitNodeCount === 0) {
        break;
      }
    }
  } finally {
    clearNodePositionIndexRanges(root);
  }

  return root;
}

async function buildAdaptiveNodeTreeFromStreamingPositions(
  source,
  rootBounds,
  maxDepth,
  leafLimit,
  options = {},
) {
  let nextPhysicalCoordinate = 1;
  const allocateChildCoordinates = (parent) => ({
    level: parent.level + 1,
    x: nextPhysicalCoordinate++,
    y: 0,
    z: 0,
  });

  const root = makeAdaptiveRootNode(rootBounds, source.vertexCount);
  const splitBasisAxes = options.splitBasisAxes || null;
  const splitPenaltyOptions = makeAdaptiveSplitPenaltyOptions(options);
  const tileRefinement = normalizedTileRefinement(options.tileRefinement);
  markRootTileRefinementStart(root, tileRefinement);

  if (maxDepth <= 0 || source.vertexCount <= 1) {
    return root;
  }

  while (true) {
    const candidates = [];
    collectAdaptiveSplitCandidates(root, maxDepth, leafLimit, candidates, {
      tileRefinement,
    });
    if (candidates.length === 0) {
      break;
    }

    const statsList = [];
    for (const node of candidates) {
      const stats = makeAdaptiveSplitStats(
        node,
        splitBasisAxes,
        splitPenaltyOptions,
      );
      statsList.push(stats);
      nodeBuildState(node).activeSplitStats = stats;
    }

    try {
      await forEachStagedPosition(source, (x, y, z) => {
        const leaf = resolveLeafNodeForPoint(root, x, y, z);
        const build = leaf._build;
        const stats = build ? build.activeSplitStats : null;
        if (stats) {
          updateAdaptiveSplitStats(stats, x, y, z);
        }
      });
    } finally {
      clearStatsNodeBuildStateField(statsList, 'activeSplitStats');
    }

    const actionStats = [];
    const kdActionStats = [];
    for (const stats of statsList) {
      stats.node.count = stats.count;
      const tightBounds = boundsFromMinMax(
        stats.minimum,
        stats.maximum,
        stats.node.bounds,
      );
      const action = chooseAdaptiveSplitAction(
        stats,
        tightBounds,
        maxDepth,
        leafLimit,
        splitBasisAxes,
        { tileRefinement },
      );
      if (!action.kind) {
        stats.node.bounds = tightBounds;
        if (action.aspectExhausted) {
          stats.node.aspectSplitExhausted = true;
        }
        if (action.splitExhausted) {
          stats.node.splitExhausted = true;
        }
        if (action.rootTileRefinementExhausted) {
          markRootTileRefinementExhausted(stats.node);
        }
        if (action.volumeRebalanceExhausted) {
          clearVolumeRebalanceSplitRequest(stats.node);
        }
        continue;
      }
      stats.node.bounds = tightBounds;
      stats.action = action;
      if (action.kind === ROUTE_MODE_KD) {
        resetAdaptiveProjectionStats(stats);
        kdActionStats.push(stats);
      }
      actionStats.push(stats);
    }

    if (kdActionStats.length > 0) {
      for (const stats of kdActionStats) {
        nodeBuildState(stats.node).activeProjectionStats = stats;
      }
      try {
        await forEachStagedPosition(source, (x, y, z) => {
          const leaf = resolveLeafNodeForPoint(root, x, y, z);
          const build = leaf._build;
          const stats = build ? build.activeProjectionStats : null;
          if (stats) {
            updateAdaptiveProjectionStats(stats, x, y, z);
          }
        });
      } finally {
        clearStatsNodeBuildStateField(kdActionStats, 'activeProjectionStats');
      }
    }

    const splittableStats = [];
    let needsChildStatsScan = false;
    for (const stats of actionStats) {
      if (
        stats.action.kind === ROUTE_MODE_KD &&
        !finalizeKdSplitAction(stats)
      ) {
        markSplitActionExhausted(stats.node, stats.action);
        continue;
      }
      if (splitActionIsVolumeRebalance(stats.action)) {
        clearVolumeRebalanceSplitRequest(stats.node);
      }
      applyAdaptiveSplitActionToNode(stats.node, stats.action);
      stats.childStats = childStatsSlotsFromSplitAction(stats.action);
      if (!stats.childStats) {
        stats.childStats = makeAdaptiveChildStatsSlots();
        needsChildStatsScan = true;
      }
      splittableStats.push(stats);
    }

    if (splittableStats.length === 0) {
      break;
    }

    if (needsChildStatsScan) {
      const childScanStats = [];
      for (const stats of splittableStats) {
        if (!adaptiveChildStatsHasEntries(stats.childStats)) {
          childScanStats.push(stats);
          nodeBuildState(stats.node).activeChildStats = stats;
        }
      }

      try {
        await forEachStagedPosition(source, (x, y, z) => {
          const leaf = resolveLeafNodeForPoint(root, x, y, z);
          const build = leaf._build;
          const stats = build ? build.activeChildStats : null;
          if (!stats) {
            return;
          }
          const slot = childSlotForAdaptiveAction(stats, x, y, z);
          updateAdaptiveChildStats(stats, slot, x, y, z);
        });
      } finally {
        clearStatsNodeBuildStateField(childScanStats, 'activeChildStats');
      }
    }

    let splitNodeCount = 0;
    const affectedVolumeDepths = new Set();
    for (const stats of splittableStats) {
      const node = stats.node;
      const occupied = sortedAdaptiveChildStats(stats);

      if (occupied.length <= 1) {
        clearNodeSplitRouting(node);
        node.leaf = true;
        markSplitActionExhausted(node, stats.action);
        continue;
      }

      node.leaf = false;
      node.virtual = splitActionMakesVirtualNode(stats.action);
      node.children = [];
      node.childrenBySlot = makeChildSlotArray();
      node.occupiedChildCount = occupied.length;
      for (const entry of occupied) {
        const slot = entry.slot;
        const coords = allocateChildCoordinates(node);
        const child = makePartitionTreeNode({
          level: coords.level,
          depth: splitActionKeepsLogicalDepth(stats.action)
            ? node.depth
            : node.depth + 1,
          x: coords.x,
          y: coords.y,
          z: coords.z,
          childSlot: slot,
          bounds: boundsFromMinMax(entry.minimum, entry.maximum, node.bounds),
          count: entry.count,
          leaf: true,
        });
        assignRootTileRefinementChildState(
          node,
          child,
          stats.action,
          tileRefinement,
        );
        if (node.aspectSplitExhausted || splitActionIsLongTile(stats.action)) {
          child.aspectSplitExhausted = true;
        }
        node.children.push(child);
        node.childrenBySlot[slot] = child;
      }
      splitNodeCount += 1;
      noteAffectedVolumeRebalanceDepths(
        affectedVolumeDepths,
        node,
        stats.action,
      );
    }

    if (splitNodeCount > 0) {
      markCurrentLodTilesForVolumeRebalance(root, affectedVolumeDepths);
    }
    if (splitNodeCount === 0) {
      break;
    }
  }

  return root;
}

module.exports = {
  makeNodeKey,
  pointKdSlotForNode,
  coordinateForAxis,
  cloneBounds,
  normalizeBoxArray,
  normalizeOrientedBox,
  normalizeVector3,
  normalizeSplitDirection,
  normalizeSplitOffset,
  makeChildSlotArray,
  nodeBuildState,
  existingNodeBuildState,
  pruneNodeBuildState,
  makePartitionTreeNode,
  serializeNodeMeta,
  deserializeNodeMeta,
  collectTreeStats,
  resolveLeafNodeForPoint,
  resetNodeArtifacts,
  normalizeSplitWeight,
  buildAdaptiveNodeTreeFromPositions,
  makeOrientedBoxStats,
  updateWeightedPositionMomentStats,
  orthonormalBasisFromMomentStats,
  boundsFromMinMax,
  splitActionKeepsLogicalDepth,
  splitActionMakesVirtualNode,
  splitActionIsLongTile,
  splitActionIsVolumeRebalance,
};
