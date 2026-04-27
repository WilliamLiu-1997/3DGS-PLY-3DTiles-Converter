const fs = require('fs');
const path = require('path');

const {
  ensure,
  _readPlyHeaderFromHandle,
  _buildGaussianPlyLayout,
} = require('./parser');
const {
  DEFAULT_SOURCE_COORDINATE_SYSTEM,
  detectSourceCoordinateSystemFromPlyHeader,
} = require('./coordinates');
const {
  ConsoleProgressBar,
  SpzContentWorkerPool,
} = require('./builder');
const { makeMemoryBudgetPlan } = require('./memory-plan');
const {
  deserializeBoundsState,
  enqueuePipelineStateSave,
  fingerprintsMatch,
  makeEmptyPipelineState,
  makePipelineFingerprint,
  pathExists,
  readPipelineState,
  removeFileIfExists,
  serializeBoundsState,
} = require('./pipeline-state');
const {
  collectTreeStats,
  deserializeNodeMeta,
  resetNodeArtifacts,
  serializeNodeMeta,
  buildAdaptiveNodeTreeFromPositions,
} = require('./adaptive-tiler');
const {
  partitionLeafBuckets,
  scanGlobalBoundsAndStagePositionsInMemory,
  scanGlobalBoundsAndWritePositions,
} = require('./scan-stage');
const {
  assignVirtualSegmentContentTargets,
  buildPartitionedBottomUp,
} = require('./build-stage');
const {
  makeTilesetAsset,
  applyTilesetGltfContentExtensions,
  useOrientedBoundingBoxes,
  applyRootTransform,
  resolveRootGeometricError,
  buildTileNodeTree,
  tileToJson,
  makeBuildSummary,
} = require('./tileset-output');
const {
  writeBucketGlbTaskOutput,
  writeSimplifiedBucketGlbTaskOutput,
} = require('./tile-content');

const DEFAULT_WORKER_SCRIPT = path.join(__dirname, 'convert-core.js');
const TEMP_WORKSPACE_NAME = '.tmp-ply-partitions';
const PIPELINE_STATE_FILE = 'pipeline-state.json';
const PIPELINE_STATE_VERSION = 21;
const PIPELINE_STAGE_BUCKETED = 'bucketed';
const LEAF_BUCKET_DIR = 'leaf';
const HANDOFF_BUCKET_DIR = 'handoff';
const POSITION_TMP_FILE = 'positions.tmp';
const PIPELINE_STATE_SAVE_INTERVAL_MS = 5000;
const PIPELINE_STATE_SAVE_NODE_INTERVAL = 512;
const AUTO_MAX_DEPTH_TARGET_SPLATS = 160000;
const AUTO_TILE_REFINEMENT_LEAF_LIMIT_MULTIPLIER = 5;

function calculateAutoMaxDepth(splatCount, samplingRatePerLevel) {
  if (
    !Number.isFinite(splatCount) ||
    splatCount <= AUTO_MAX_DEPTH_TARGET_SPLATS
  ) {
    return 0;
  }
  if (!Number.isFinite(samplingRatePerLevel) || samplingRatePerLevel >= 1.0) {
    return 0;
  }
  const rawDepth =
    Math.log(AUTO_MAX_DEPTH_TARGET_SPLATS / splatCount) /
    Math.log(samplingRatePerLevel);
  if (!Number.isFinite(rawDepth) || rawDepth <= 0.0) {
    return 0;
  }
  return Math.ceil(rawDepth);
}

function resolveMaxDepthFromHeader(args, header) {
  if (args.maxDepth != null) {
    args.maxDepthSource = 'explicit';
    return;
  }
  args.maxDepth = calculateAutoMaxDepth(
    header.vertexCount,
    args.samplingRatePerLevel,
  );
  args.maxDepthSource = 'auto';
  args.autoMaxDepthTargetSplats = AUTO_MAX_DEPTH_TARGET_SPLATS;
}

function estimateRootRefinementTileCount(tileRefinement, maxDepth) {
  if (!Number.isFinite(maxDepth) || maxDepth <= 0) {
    return 1;
  }
  const refinement = Math.max(1, Math.floor(tileRefinement || 1));
  let tileCount = 2;
  for (let pass = 1; pass < refinement; pass++) {
    tileCount += Math.floor(tileCount / 2);
  }
  return tileCount;
}

function calculateAutoTileRefinement(
  splatCount,
  leafLimit,
  maxDepth,
  samplingRatePerLevel,
) {
  const depth = Math.max(0, Math.floor(maxDepth || 0));
  const depthTileFactor =
    depth <= 0 || !Number.isFinite(samplingRatePerLevel)
      ? 1
      : (1 / samplingRatePerLevel) ** depth;
  const denominator =
    leafLimit * AUTO_TILE_REFINEMENT_LEAF_LIMIT_MULTIPLIER * depthTileFactor;
  if (depth <= 0) {
    return {
      tileRefinement: 1,
      targetRootTiles:
        Number.isFinite(splatCount) &&
        Number.isFinite(denominator) &&
        denominator > 0.0
          ? splatCount / denominator
          : 1,
      estimatedRootTiles: 1,
    };
  }
  if (
    !Number.isFinite(splatCount) ||
    !Number.isFinite(denominator) ||
    denominator <= 0.0 ||
    splatCount <= denominator
  ) {
    return {
      tileRefinement: 1,
      targetRootTiles: 1,
      estimatedRootTiles: estimateRootRefinementTileCount(1, depth),
    };
  }

  const targetRootTiles = splatCount / denominator;
  let tileRefinement = 1;
  let estimatedRootTiles = estimateRootRefinementTileCount(
    tileRefinement,
    depth,
  );
  while (estimatedRootTiles < targetRootTiles) {
    tileRefinement += 1;
    estimatedRootTiles = estimateRootRefinementTileCount(tileRefinement, depth);
  }
  return { tileRefinement, targetRootTiles, estimatedRootTiles };
}

function resolveTileRefinementFromHeader(args, header) {
  if (args.tileRefinement != null) {
    args.tileRefinementSource = 'explicit';
    return;
  }
  const resolved = calculateAutoTileRefinement(
    header.vertexCount,
    args.leafLimit,
    args.maxDepth,
    args.samplingRatePerLevel,
  );
  args.tileRefinement = resolved.tileRefinement;
  args.tileRefinementSource = 'auto';
  args.autoTileRefinementLeafLimitMultiplier =
    AUTO_TILE_REFINEMENT_LEAF_LIMIT_MULTIPLIER;
  args.autoTileRefinementTargetRootTiles = resolved.targetRootTiles;
  args.autoTileRefinementEstimatedRootTiles = resolved.estimatedRootTiles;
}

function formatTilingParameterNumber(value, fractionDigits = 3) {
  return Number.isFinite(value) ? value.toFixed(fractionDigits) : String(value);
}

function logResolvedTilingParameters(args, header) {
  const maxDepthParts = [
    `maxDepth=${args.maxDepth}`,
    `source=${args.maxDepthSource || 'explicit'}`,
  ];
  if (args.maxDepthSource === 'auto') {
    maxDepthParts.push(`targetSplats=${args.autoMaxDepthTargetSplats}`);
    maxDepthParts.push(`inputSplats=${header.vertexCount}`);
    maxDepthParts.push(`samplingRatePerLevel=${args.samplingRatePerLevel}`);
  }

  const tileRefinementParts = [
    `tileRefinement=${args.tileRefinement}`,
    `source=${args.tileRefinementSource || 'explicit'}`,
  ];
  if (args.tileRefinementSource === 'auto') {
    tileRefinementParts.push(
      `targetRootTiles=${formatTilingParameterNumber(
        args.autoTileRefinementTargetRootTiles,
      )}`,
    );
    tileRefinementParts.push(
      `estimatedRootTiles=${args.autoTileRefinementEstimatedRootTiles}`,
    );
    tileRefinementParts.push(`leafLimit=${args.leafLimit}`);
    tileRefinementParts.push(`samplingRatePerLevel=${args.samplingRatePerLevel}`);
    tileRefinementParts.push(
      `leafLimitMultiplier=${args.autoTileRefinementLeafLimitMultiplier}`,
    );
  }

  console.log(
    `[info] resolved tiling parameters | ${maxDepthParts.join(' ')} | ${tileRefinementParts.join(' ')}`,
  );
}

function currentPeakRssBytes() {
  if (typeof process.resourceUsage === 'function') {
    const usage = process.resourceUsage();
    if (usage && Number.isFinite(usage.maxRSS) && usage.maxRSS > 0) {
      return usage.maxRSS * 1024;
    }
  }
  return process.memoryUsage().rss;
}

function elapsedMsSince(startMs) {
  return Math.max(0, Date.now() - startMs);
}

function splitBasisLabelForArgs(args) {
  return useOrientedBoundingBoxes(args) ? 'root-basis' : 'AABB-axis';
}

async function ensureReusablePartitionState(rootNode) {
  const { leaves } = collectTreeStats(rootNode);
  for (const leaf of leaves) {
    if (
      !leaf.bucketPath ||
      !Number.isInteger(leaf.bucketRowCount) ||
      leaf.bucketRowCount <= 0 ||
      !(await pathExists(leaf.bucketPath))
    ) {
      return false;
    }
  }
  return true;
}

async function convertPartitionedPlyTo3DTiles(inputPath, outputDir, args) {
  const outputDirAbs = path.resolve(outputDir);
  const tempDir = path.join(outputDirAbs, TEMP_WORKSPACE_NAME);
  const timingsMs = {};
  const conversionStartedAt = Date.now();
  let memoryPlan = makeMemoryBudgetPlan(args);
  const buildConcurrency = memoryPlan.buildConcurrency;
  const contentWorkers = memoryPlan.contentWorkers;
  const partitionWriteConcurrency = memoryPlan.partitionWriteConcurrency;
  const params = {
    outputDir: outputDirAbs,
    colorSpace: args.colorSpace,
    samplingRatePerLevel: args.samplingRatePerLevel,
    sampleMode: args.sampleMode,
    spzSh1Bits: args.spzSh1Bits,
    spzShRestBits: args.spzShRestBits,
    spzCompressionLevel: args.spzCompressionLevel,
    minGeometricError: args.minGeometricError,
    bucketChunkBytes: memoryPlan.bucketChunkBytes,
    sourceCoordinateSystem: DEFAULT_SOURCE_COORDINATE_SYSTEM,
    contentWorkerPool:
      contentWorkers > 0
        ? new SpzContentWorkerPool(contentWorkers, DEFAULT_WORKER_SCRIPT)
        : null,
  };

  if (fs.existsSync(outputDirAbs) && args.clean) {
    const cleanStartedAt = Date.now();
    console.log(`[info] cleaning output directory: ${outputDirAbs}`);
    await fs.promises.rm(outputDirAbs, { recursive: true, force: true });
    timingsMs.clean_output_ms = elapsedMsSince(cleanStartedAt);
    console.log(
      `[info] cleaned output directory | ms=${timingsMs.clean_output_ms}`,
    );
  }
  await fs.promises.mkdir(outputDirAbs, { recursive: true });
  if (fs.existsSync(path.join(outputDirAbs, 'viewer.html'))) {
    await removeFileIfExists(path.join(outputDirAbs, 'viewer.html'));
  }
  await fs.promises.rm(path.join(outputDirAbs, 'subtrees'), {
    recursive: true,
    force: true,
  });

  const inputStat = await fs.promises.stat(inputPath);
  const handle = await fs.promises.open(inputPath, 'r');
  let checkpointInfo = { reused: false, stage: null };
  let pipelineState = null;
  let success = false;
  try {
    console.log(`[info] scanning PLY header: ${inputPath}`);
    const headerStartedAt = Date.now();
    const header = await _readPlyHeaderFromHandle(handle, inputPath);
    const sourceCoordinateSystemDetection =
      detectSourceCoordinateSystemFromPlyHeader(header);
    args.resolvedSourceCoordinateSystem =
      sourceCoordinateSystemDetection.sourceCoordinateSystem;
    args.resolvedSourceCoordinateSystemSource =
      sourceCoordinateSystemDetection.source;
    args.resolvedSourceCoordinateSystemReason =
      sourceCoordinateSystemDetection.reason;
    params.sourceCoordinateSystem =
      sourceCoordinateSystemDetection.sourceCoordinateSystem;
    console.log(
      `[info] source coordinate system: ${sourceCoordinateSystemDetection.sourceCoordinateSystem} | source=${sourceCoordinateSystemDetection.source}`,
    );

    const layout = _buildGaussianPlyLayout(
      header.vertexProps,
      inputPath,
      args.inputConvention,
      args.linearScaleInput,
    );
    resolveMaxDepthFromHeader(args, header);
    resolveTileRefinementFromHeader(args, header);
    logResolvedTilingParameters(args, header);
    timingsMs.header_ms = elapsedMsSince(headerStartedAt);
    const fingerprint = makePipelineFingerprint(
      inputPath,
      inputStat,
      args,
      sourceCoordinateSystemDetection.sourceCoordinateSystem,
      DEFAULT_SOURCE_COORDINATE_SYSTEM,
    );

    if (!args.clean) {
      pipelineState = await readPipelineState(
        tempDir,
        PIPELINE_STATE_FILE,
        PIPELINE_STAGE_BUCKETED,
      );
      if (
        pipelineState &&
        pipelineState.version === PIPELINE_STATE_VERSION &&
        fingerprintsMatch(pipelineState.fingerprint, fingerprint)
      ) {
        checkpointInfo = {
          reused: true,
          stage: pipelineState.stage || 'init',
        };
        console.log(
          `[info] reusing checkpoint | stage=${checkpointInfo.stage}`,
        );
      } else {
        pipelineState = null;
      }
    }

    if (!pipelineState) {
      await fs.promises.rm(tempDir, { recursive: true, force: true });
      await fs.promises.mkdir(tempDir, { recursive: true });
      pipelineState = makeEmptyPipelineState(
        PIPELINE_STATE_VERSION,
        fingerprint,
      );
    } else {
      await fs.promises.mkdir(tempDir, { recursive: true });
    }

    memoryPlan = makeMemoryBudgetPlan(args, { header, layout });
    params.bucketChunkBytes = memoryPlan.bucketChunkBytes;

    let rootBounds = null;
    let rootNodeMeta = null;
    if (
      pipelineState.rootBounds &&
      pipelineState.rootNode &&
      pipelineState.layout &&
      pipelineState.layout.coeffCount === layout.coeffCount &&
      pipelineState.layout.degree === layout.degree
    ) {
      rootBounds = deserializeBoundsState(pipelineState.rootBounds);
      rootNodeMeta = deserializeNodeMeta(pipelineState.rootNode);
    }

    if (!rootBounds || !rootNodeMeta) {
      const positionsPath = path.join(tempDir, POSITION_TMP_FILE);
      try {
        console.log(
          `[info] scan 1/4 | vertices=${header.vertexCount} | sh_degree=${layout.degree} | staging positions`,
        );
        const scanStartedAt = Date.now();
        const scanProgress = new ConsoleProgressBar(
          'scan 1/4',
          header.vertexCount,
        );
        scanProgress.update(0, 'staging positions');
        if (memoryPlan.inMemoryPositions) {
          const staged = await scanGlobalBoundsAndStagePositionsInMemory(
            handle,
            inputPath,
            header,
            layout,
            {
              chunkBytes: memoryPlan.scanChunkBytes,
              includeWeights: useOrientedBoundingBoxes(args),
              progress: scanProgress,
            },
          );
          rootBounds = staged.bounds;
          timingsMs.scan_positions_ms = elapsedMsSince(scanStartedAt);
          scanProgress.done(`ms=${timingsMs.scan_positions_ms}`);

          console.log(
            `[info] scan 2/4 | building ${splitBasisLabelForArgs(args)} volume-minimizing k-d tiling tree from memory`,
          );
          const tilingStartedAt = Date.now();
          const tilingProgress = new ConsoleProgressBar(
            'tiling',
            header.vertexCount,
          );
          rootNodeMeta = await buildAdaptiveNodeTreeFromPositions(
            {
              positions: staged.positions,
              weights: staged.weights,
              vertexCount: header.vertexCount,
            },
            rootBounds,
            args.maxDepth,
            args.leafLimit,
            {
              progress: tilingProgress,
              orientedBoundingBoxes: useOrientedBoundingBoxes(args),
              rootBasisAxes: staged.rootBasisAxes,
              tileRefinement: args.tileRefinement,
              splitMidpointPenalty: args.splitMidpointPenalty,
              splitCountBalancePenalty: args.splitCountBalancePenalty,
            },
          );
          timingsMs.build_tiling_tree_ms = elapsedMsSince(tilingStartedAt);
          tilingProgress.done(`ms=${timingsMs.build_tiling_tree_ms}`);
          staged.positions = null;
          staged.weights = null;
        } else {
          const staged = await scanGlobalBoundsAndWritePositions(
            handle,
            inputPath,
            header,
            layout,
            positionsPath,
            {
              chunkBytes: memoryPlan.scanChunkBytes,
              includeWeights: useOrientedBoundingBoxes(args),
              positionTmpBufferBytes: memoryPlan.positionTmpBufferBytes,
              progress: scanProgress,
            },
          );
          rootBounds = staged.bounds;
          timingsMs.scan_positions_ms = elapsedMsSince(scanStartedAt);
          scanProgress.done(`ms=${timingsMs.scan_positions_ms}`);

          console.log(
            `[info] scan 2/4 | building ${splitBasisLabelForArgs(args)} volume-minimizing k-d tiling tree from positions`,
          );
          const tilingStartedAt = Date.now();
          rootNodeMeta = await buildAdaptiveNodeTreeFromPositions(
            {
              positionsPath,
              vertexCount: header.vertexCount,
              chunkBytes: memoryPlan.scanChunkBytes,
            },
            rootBounds,
            args.maxDepth,
            args.leafLimit,
            {
              orientedBoundingBoxes: useOrientedBoundingBoxes(args),
              rootBasisAxes: staged.rootBasisAxes,
              tileRefinement: args.tileRefinement,
              splitMidpointPenalty: args.splitMidpointPenalty,
              splitCountBalancePenalty: args.splitCountBalancePenalty,
            },
          );
          timingsMs.build_tiling_tree_ms = elapsedMsSince(tilingStartedAt);
        }
      } finally {
        await removeFileIfExists(positionsPath);
      }
      ensure(!!rootNodeMeta, 'Failed to build k-d tiling tree.');
      pipelineState.rootBounds = serializeBoundsState(rootBounds);
      pipelineState.rootNode = serializeNodeMeta(rootNodeMeta);
      pipelineState.layout = {
        degree: layout.degree,
        coeffCount: layout.coeffCount,
      };
      pipelineState.stage = 'scanned';
      pipelineState.updatedAt = new Date().toISOString();
      await fs.promises.writeFile(
        path.join(tempDir, PIPELINE_STATE_FILE),
        JSON.stringify(pipelineState),
        'utf8',
      );
    }

    const treeStats = collectTreeStats(rootNodeMeta);
    memoryPlan = makeMemoryBudgetPlan(args, { header, layout });
    params.bucketChunkBytes = memoryPlan.bucketChunkBytes;
    const lodMaxDepth = Math.max(
      0,
      Math.min(args.maxDepth, treeStats.maxDepth),
    );
    assignVirtualSegmentContentTargets(
      rootNodeMeta,
      lodMaxDepth,
      args.samplingRatePerLevel,
    );

    const canReuseLeafBuckets =
      pipelineState.stage !== 'init' &&
      (await ensureReusablePartitionState(rootNodeMeta));
    if (!canReuseLeafBuckets) {
      resetNodeArtifacts(rootNodeMeta);
      await fs.promises.rm(path.join(tempDir, LEAF_BUCKET_DIR), {
        recursive: true,
        force: true,
      });
      await fs.promises.rm(path.join(tempDir, HANDOFF_BUCKET_DIR), {
        recursive: true,
        force: true,
      });
      console.log(
        `[info] scan 3/4 | partitioning ${treeStats.leaves.length} leaf buckets`,
      );
      const partitionProgress = new ConsoleProgressBar(
        'partition',
        header.vertexCount,
      );
      const partitionStartedAt = Date.now();
      const partitionResult = await partitionLeafBuckets(
        handle,
        inputPath,
        header,
        layout,
        rootNodeMeta,
        tempDir,
        {
          progress: partitionProgress,
          memoryPlan,
          writeConcurrency: partitionWriteConcurrency,
        },
      );
      timingsMs.partition_ms = elapsedMsSince(partitionStartedAt);
      partitionProgress.done(`rows=${partitionResult.rowCount}`);
      pipelineState.rootNode = serializeNodeMeta(rootNodeMeta);
      pipelineState.stage = PIPELINE_STAGE_BUCKETED;
      pipelineState.updatedAt = new Date().toISOString();
      console.log(
        `[info] scan 4/4 | writing ${treeStats.leaves.length} leaf buckets`,
      );
      await fs.promises.writeFile(
        path.join(tempDir, PIPELINE_STATE_FILE),
        JSON.stringify(pipelineState),
        'utf8',
      );
    } else if (checkpointInfo.reused) {
      console.log('[info] scan 4/4 | reusing existing leaf buckets');
    }

    await handle.close();

    const stateCtx = {
      tempDir,
      pipelineState,
      rootBounds,
      rootNode: rootNodeMeta,
      layout,
      savePromise: Promise.resolve(),
      serializeNodeMeta,
      pipelineStateFile: PIPELINE_STATE_FILE,
      pipelineStateVersion: PIPELINE_STATE_VERSION,
      pipelineStateSaveIntervalMs: PIPELINE_STATE_SAVE_INTERVAL_MS,
      pipelineStateSaveNodeInterval: PIPELINE_STATE_SAVE_NODE_INTERVAL,
    };

    const ctx = {
      layout,
      lodMaxDepth,
      params,
      tempDir,
      nodeConcurrency: buildConcurrency,
      leafNodeConcurrency: buildConcurrency,
      buildMemoryBudgetBytes: memoryPlan.usableBudgetBytes,
      bucketChunkBytes: memoryPlan.bucketChunkBytes,
      simplifyScratchBytes: memoryPlan.simplifyScratchBytes,
      bucketEntryCacheBytes: memoryPlan.bucketEntryCacheBytes,
      pipelineState,
      rootBounds,
      rootNode: rootNodeMeta,
      savePromise: Promise.resolve(),
      serializeNodeMeta,
      pipelineStateFile: PIPELINE_STATE_FILE,
      pipelineStateVersion: PIPELINE_STATE_VERSION,
      pipelineStateSaveIntervalMs: PIPELINE_STATE_SAVE_INTERVAL_MS,
      pipelineStateSaveNodeInterval: PIPELINE_STATE_SAVE_NODE_INTERVAL,
      lastPipelineStateSaveAt: Date.now(),
      nodesSincePipelineStateSave: 0,
      pendingHandoffCleanup: new Set(),
      buildProgress: new ConsoleProgressBar('build', treeStats.nodes.length),
    };

    enqueuePipelineStateSave(stateCtx, pipelineState.stage);
    ctx.savePromise = stateCtx.savePromise;
    ctx.pipelineState = stateCtx.pipelineState;

    console.log('[info] building tiles bottom-up');
    const buildStartedAt = Date.now();
    await buildPartitionedBottomUp(rootNodeMeta, ctx);
    timingsMs.build_tiles_ms = elapsedMsSince(buildStartedAt);
    ctx.buildProgress.done(
      `nodes=${treeStats.nodes.length} levels=${treeStats.maxDepth + 1} logical/${treeStats.maxLevel + 1} physical`,
    );
    await ctx.savePromise;

    const { value: rootGeometricError, source: rootGeometricErrorSource } =
      resolveRootGeometricError(rootNodeMeta, rootBounds, params, lodMaxDepth);
    const nodesByKey = new Map();
    const rootTileNode = buildTileNodeTree(
      rootNodeMeta,
      rootGeometricError,
      lodMaxDepth,
      args.samplingRatePerLevel,
      nodesByKey,
    );

    const availableLevels = treeStats.maxDepth + 1;
    const physicalLevels = treeStats.maxLevel + 1;
    const tileset = applyTilesetGltfContentExtensions({
      asset: makeTilesetAsset(),
      geometricError: rootTileNode.error,
      root: applyRootTransform(
        tileToJson(rootTileNode, args.resolvedSourceCoordinateSystem),
        args.transform,
      ),
    });

    const writeMetadataStartedAt = Date.now();
    await fs.promises.writeFile(
      path.join(outputDirAbs, 'tileset.json'),
      JSON.stringify(tileset),
      'utf8',
    );
    timingsMs.write_tileset_ms = elapsedMsSince(writeMetadataStartedAt);
    await fs.promises.writeFile(
      path.join(outputDirAbs, 'build_summary.json'),
      JSON.stringify(
        makeBuildSummary(
          args,
          header,
          layout,
          rootTileNode,
          rootGeometricError,
          rootGeometricErrorSource,
          nodesByKey.size,
          treeStats.maxDepth,
          availableLevels,
          treeStats.maxLevel,
          physicalLevels,
          treeStats.nodes.length,
          treeStats.virtualNodes.length,
          checkpointInfo,
          memoryPlan,
          {
            ...timingsMs,
            total_ms: elapsedMsSince(conversionStartedAt),
          },
          currentPeakRssBytes(),
        ),
      ),
      'utf8',
    );

    console.log(
      `[info] nodes=${nodesByKey.size} | levels=${availableLevels} logical/${physicalLevels} physical | splats=${header.vertexCount}`,
    );
    success = true;
    return {
      splatCount: header.vertexCount,
      shDegree: layout.degree,
      nodeCount: nodesByKey.size,
      levels: availableLevels,
      physicalLevels,
    };
  } finally {
    try {
      await handle.close();
    } catch {}
    if (params.contentWorkerPool) {
      await params.contentWorkerPool.close();
    }
    if (success) {
      try {
        await fs.promises.rm(tempDir, { recursive: true, force: true });
      } catch {}
    } else {
      console.error('[info] checkpoint preserved in output temp workspace');
    }
  }
}

module.exports = {
  convertPartitionedPlyTo3DTiles,
  _writeBucketGlbTaskOutput: writeBucketGlbTaskOutput,
  _writeSimplifiedBucketGlbTaskOutput: writeSimplifiedBucketGlbTaskOutput,
};
