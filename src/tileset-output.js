const { TileNode, ensure } = require('./parser');
const { DEFAULT_SOURCE_COORDINATE_SYSTEM, sourceCoordinateSystemInfo } = require('./coordinates');
const { SPZ_STREAM_VERSION } = require('./codec');
const { SOURCE_REPOSITORY, samplingDivisorForDepth, geometricErrorScaleForDepth, rootGeometricErrorFromMinLevel } = require('./builder');
const { makeMemoryBudgetPlan, serializeMemoryBudgetPlan } = require('./memory-plan');
const { HANDOFF_BUCKET_ENCODING } = require('./bucket-io');

const TILING_STRATEGY_KD_TREE = 'kd_tree';
const ESTIMATED_GEOMETRIC_ERROR_MULTIPLIER = 2.5;
const GLTF_TILESET_CONTENT_EXTENSION = '3DTILES_content_gltf';
const GAUSSIAN_SPLATTING_GLTF_EXTENSIONS = [
  'KHR_gaussian_splatting',
  'KHR_gaussian_splatting_compression_spz_2',
];

function makeTilesetAsset() {
  return {
    version: '1.1',
  };
}

function applyTilesetGltfContentExtensions(tileset) {
  tileset.extensions = {
    ...(tileset.extensions || {}),
    [GLTF_TILESET_CONTENT_EXTENSION]: {
      extensionsRequired: [...GAUSSIAN_SPLATTING_GLTF_EXTENSIONS],
      extensionsUsed: [...GAUSSIAN_SPLATTING_GLTF_EXTENSIONS],
    },
  };

  const extensionsUsed = Array.isArray(tileset.extensionsUsed)
    ? tileset.extensionsUsed.filter((name) => !!name)
    : [];
  if (!extensionsUsed.includes(GLTF_TILESET_CONTENT_EXTENSION)) {
    extensionsUsed.push(GLTF_TILESET_CONTENT_EXTENSION);
  }
  tileset.extensionsUsed = extensionsUsed;
  return tileset;
}

function useOrientedBoundingBoxes(args) {
  return !!(args && args.orientedBoundingBoxes === true);
}

function applyRootTransform(root, transform) {
  if (!transform) {
    return root;
  }
  root.transform = transform.slice();
  return root;
}

function applyContentBoxTransform(box, sourceCoordinateSystem = null) {
  if (!Array.isArray(box) || box.length !== 12) {
    return box;
  }

  const sourceToTileZUp = sourceCoordinateSystemInfo(
    sourceCoordinateSystem,
  ).sourceToTileZUp;
  const out = box.slice();
  for (const base of [0, 3, 6, 9]) {
    const x = out[base];
    const y = out[base + 1];
    const z = out[base + 2];
    out[base] =
      sourceToTileZUp[0][0] * x +
      sourceToTileZUp[0][1] * y +
      sourceToTileZUp[0][2] * z;
    out[base + 1] =
      sourceToTileZUp[1][0] * x +
      sourceToTileZUp[1][1] * y +
      sourceToTileZUp[1][2] * z;
    out[base + 2] =
      sourceToTileZUp[2][0] * x +
      sourceToTileZUp[2][1] * y +
      sourceToTileZUp[2][2] * z;
  }
  return out;
}

function fallbackRootGeometricError(bounds, splatCount) {
  const ex = bounds.extents();
  const diag = Math.sqrt(ex[0] * ex[0] + ex[1] * ex[1] + ex[2] * ex[2]);
  if (splatCount <= 1) {
    return Math.max(diag * 1e-6, 1e-6);
  }
  return Math.max(diag * 0.125, diag * 1e-6, 1e-6);
}

function resolveRootGeometricError(rootNode, rootBounds, params, lodMaxDepth) {
  if (params.minGeometricError != null && params.minGeometricError > 0.0) {
    return {
      value: rootGeometricErrorFromMinLevel(
        params.minGeometricError,
        lodMaxDepth,
        params.samplingRatePerLevel,
      ),
      source: 'configured_min_geometric_error',
    };
  }

  if (Number.isFinite(rootNode.ownError) && rootNode.ownError > 0.0) {
    const ex = rootBounds.extents();
    const diag = Math.sqrt(ex[0] * ex[0] + ex[1] * ex[1] + ex[2] * ex[2]);
    return {
      value:
        Math.max(rootNode.ownError, diag * 1e-6, 1e-6) *
        ESTIMATED_GEOMETRIC_ERROR_MULTIPLIER,
      source: 'estimated_root_simplify_scaled',
    };
  }

  return {
    value:
      fallbackRootGeometricError(rootBounds, rootNode.count) *
      ESTIMATED_GEOMETRIC_ERROR_MULTIPLIER,
    source: 'estimated_root_fallback_scaled',
  };
}

function buildTileNodeTree(
  node,
  rootGeometricError,
  lodMaxDepth,
  samplingRatePerLevel,
  nodesByKey,
) {
  ensure(
    !node.virtual,
    `Virtual node ${node.key} cannot be emitted as a tile.`,
  );
  const error =
    rootGeometricError *
    geometricErrorScaleForDepth(node.depth, lodMaxDepth, samplingRatePerLevel);
  const children = buildEmittedTileChildren(
    node,
    rootGeometricError,
    lodMaxDepth,
    samplingRatePerLevel,
    nodesByKey,
  );
  const tileNode = new TileNode(
    node.level,
    node.x,
    node.y,
    node.z,
    node.bounds,
    error,
    node.contentUri,
    children,
  );
  tileNode.orientedBox = node.orientedBox ? node.orientedBox.slice() : null;
  nodesByKey.set(tileNode.key(), tileNode);
  return tileNode;
}

function buildEmittedTileChildren(
  node,
  rootGeometricError,
  lodMaxDepth,
  samplingRatePerLevel,
  nodesByKey,
) {
  const children = [];
  const directRealChildren = node.children.filter((child) => !child.virtual);
  const virtualChildren = node.children.filter((child) => child.virtual);
  for (const child of directRealChildren) {
    children.push(
      buildTileNodeTree(
        child,
        rootGeometricError,
        lodMaxDepth,
        samplingRatePerLevel,
        nodesByKey,
      ),
    );
  }
  for (const child of virtualChildren) {
    children.push(
      ...buildEmittedTileChildren(
        child,
        rootGeometricError,
        lodMaxDepth,
        samplingRatePerLevel,
        nodesByKey,
      ),
    );
  }
  return children;
}

function tileToJson(node, sourceCoordinateSystem = null) {
  const box = node.orientedBox || node.bounds.toBoxArray();
  const obj = {
    boundingVolume: {
      box: applyContentBoxTransform(box, sourceCoordinateSystem),
    },
    geometricError: node.error,
    refine: 'REPLACE',
    content: { uri: node.contentUri },
  };
  if (node.children.length > 0) {
    obj.children = node.children.map((child) =>
      tileToJson(child, sourceCoordinateSystem),
    );
  }
  return obj;
}

function makeBuildSummary(
  args,
  header,
  layout,
  rootNode,
  rootGeometricError,
  rootGeometricErrorSource,
  nodeCount,
  maxDepth,
  availableLevels,
  physicalMaxLevel,
  physicalLevels,
  partitionNodeCount,
  virtualNodeCount,
  checkpointInfo,
  memoryPlan,
  timingsMs = {},
  peakRssBytes = null,
) {
  const samplingDivisorsByDepth = {};
  const samplingRatesByDepth = {};
  const geometricErrorScaleByDepth = {};
  const geometricErrorByDepth = {};
  const effectiveMaxDepth = maxDepth;
  const resolvedMemoryPlan = memoryPlan || makeMemoryBudgetPlan(args);

  for (let depth = 0; depth <= effectiveMaxDepth; depth++) {
    const geometricScale = geometricErrorScaleForDepth(
      depth,
      effectiveMaxDepth,
      args.samplingRatePerLevel,
    );
    samplingDivisorsByDepth[String(depth)] = samplingDivisorForDepth(
      depth,
      effectiveMaxDepth,
      args.samplingRatePerLevel,
    );
    samplingRatesByDepth[String(depth)] =
      args.samplingRatePerLevel ** Math.max(0, effectiveMaxDepth - depth);
    geometricErrorScaleByDepth[String(depth)] = geometricScale;
    geometricErrorByDepth[String(depth)] = rootGeometricError * geometricScale;
  }

  const peakRss =
    Number.isFinite(peakRssBytes) && peakRssBytes > 0
      ? Math.floor(peakRssBytes)
      : null;

  return {
    input_splats: header.vertexCount,
    sh_degree: layout.degree,
    handoff_encoding: HANDOFF_BUCKET_ENCODING,
    max_depth: args.maxDepth,
    tile_refinement: args.tileRefinement,
    leaf_limit: args.leafLimit,
    color_space: args.colorSpace,
    memory_budget_gb: args.memoryBudget,
    sampling_rate_per_level: args.samplingRatePerLevel,
    tiling_strategy: TILING_STRATEGY_KD_TREE,
    oriented_bounding_boxes: useOrientedBoundingBoxes(args),
    content_codec: 'spz_stream',
    spz_version: SPZ_STREAM_VERSION,
    spz_sh1_bits: args.spzSh1Bits,
    spz_sh_rest_bits: args.spzShRestBits,
    spz_compression_level: args.spzCompressionLevel,
    source_coordinate_system:
      args.resolvedSourceCoordinateSystem || DEFAULT_SOURCE_COORDINATE_SYSTEM,
    source_coordinate_system_source:
      args.resolvedSourceCoordinateSystemSource || 'default',
    source_coordinate_system_reason:
      args.resolvedSourceCoordinateSystemReason || null,
    root_transform: args.transform ? args.transform.slice() : null,
    root_coordinate: args.coordinate ? args.coordinate.slice() : null,
    root_transform_source: args.coordinate
      ? 'coordinate'
      : args.transform
        ? 'transform'
        : null,
    sample_mode: args.sampleMode,
    configured_min_geometric_error:
      args.minGeometricError != null && args.minGeometricError > 0.0
        ? args.minGeometricError
        : null,
    diagnostics: {
      checkpoint: {
        reused: !!checkpointInfo.reused,
        stage: checkpointInfo.stage,
      },
      memory_budget_plan: serializeMemoryBudgetPlan(resolvedMemoryPlan),
      concurrency: {
        build: resolvedMemoryPlan.buildConcurrency,
        content_workers: resolvedMemoryPlan.contentWorkers,
        partition_write: resolvedMemoryPlan.partitionWriteConcurrency,
      },
      timings_ms: timingsMs,
      peak_rss_bytes: peakRss,
      tree: {
        node_count: nodeCount,
        partition_node_count: partitionNodeCount,
        virtual_node_count: virtualNodeCount,
        available_levels: availableLevels,
        effective_max_depth: effectiveMaxDepth,
        physical_levels: physicalLevels,
        physical_max_level: physicalMaxLevel,
      },
      geometric_error: {
        source: rootGeometricErrorSource,
        root: rootNode.error,
        min:
          rootGeometricError *
          geometricErrorScaleForDepth(
            effectiveMaxDepth,
            effectiveMaxDepth,
            args.samplingRatePerLevel,
          ),
        scale_by_depth: geometricErrorScaleByDepth,
        by_depth: geometricErrorByDepth,
      },
      sampling: {
        rates_by_depth: samplingRatesByDepth,
        divisors_by_depth: samplingDivisorsByDepth,
      },
    },
    source: SOURCE_REPOSITORY,
  };
}

module.exports = {
  makeTilesetAsset,
  applyTilesetGltfContentExtensions,
  useOrientedBoundingBoxes,
  applyRootTransform,
  resolveRootGeometricError,
  buildTileNodeTree,
  tileToJson,
  makeBuildSummary,
};
