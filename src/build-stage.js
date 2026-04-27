const path = require('path');

const { ensure } = require('./parser');
const {
  normalizeSplatTargetCount,
  constrainTargetSplatCount,
  samplingDivisorForDepth,
} = require('./builder');
const {
  enqueuePipelineStateSave,
  pathExists,
  removeFileIfExists,
} = require('./pipeline-state');
const { canonicalNodePath } = require('./pipeline-paths');
const {
  HANDOFF_BUCKET_ENCODING,
  materializeLinkedHandoffFile,
  bucketRowByteSize,
  leafBucketSpec,
  handoffBucketSpec,
  flattenBucketSpecs,
  collectBucketEntries,
  materializeCanonicalEntriesFile,
} = require('./bucket-io');
const {
  writeBucketContentFile,
  writeSimplifiedBucketContentFile,
  spzBytesPerBucketRow,
  safeNodeBoundsTranslation,
} = require('./tile-content');
const {
  collectTreeStats,
  nodeBuildState,
  existingNodeBuildState,
} = require('./adaptive-tiler');
const { runWithConcurrency, runWithConcurrencyBudget } = require('./concurrency');

const HANDOFF_BUCKET_DIR = 'handoff';
const BUILD_MIN_TASK_MEMORY_BYTES = 32 * 1024 * 1024;
const PIPELINE_STAGE_BUCKETED = 'bucketed';

function targetSplatCountForDepth(
  depth,
  lodMaxDepth,
  samplingRatePerLevel,
  splatCount,
) {
  const divisor = samplingDivisorForDepth(
    depth,
    lodMaxDepth,
    samplingRatePerLevel,
  );
  return Math.max(1, Math.min(splatCount, Math.ceil(splatCount / divisor)));
}

function resolveNodeContentTarget(node, ctx, inputSplatCount) {
  if (
    Number.isFinite(node.contentTargetOverride) &&
    node.contentTargetOverride > 0
  ) {
    return normalizeSplatTargetCount(
      node.contentTargetOverride,
      inputSplatCount,
    );
  }
  const targetRaw = targetSplatCountForDepth(
    node.depth,
    ctx.lodMaxDepth,
    ctx.params.samplingRatePerLevel,
    node.count,
  );
  return constrainTargetSplatCount(
    targetRaw,
    inputSplatCount,
    node.occupiedChildCount,
  );
}

function resolveVirtualNodeContentTarget(
  node,
  lodMaxDepth,
  samplingRatePerLevel,
) {
  const targetRaw =
    Number.isFinite(node.contentTargetOverride) &&
    node.contentTargetOverride > 0
      ? node.contentTargetOverride
      : targetSplatCountForDepth(
          node.depth,
          lodMaxDepth,
          samplingRatePerLevel,
          node.count,
        );
  return constrainTargetSplatCount(targetRaw, node.count, node.children.length);
}

function minimumContentTargetForVirtualBudget(node) {
  const build = existingNodeBuildState(node);
  if (
    build &&
    Number.isInteger(build.virtualBudgetMinimum) &&
    build.virtualBudgetMinimum >= 0
  ) {
    return build.virtualBudgetMinimum;
  }
  return computeVirtualBudgetMinimums(node);
}

function computeVirtualBudgetMinimums(node) {
  let minimum;
  if (!node.virtual) {
    minimum = node.count > 0 ? 1 : 0;
  } else {
    minimum = node.children.reduce(
      (sum, child) => sum + computeVirtualBudgetMinimums(child),
      0,
    );
  }
  nodeBuildState(node).virtualBudgetMinimum = minimum;
  return minimum;
}

function childSlotSortKey(child) {
  return Number.isInteger(child.childSlot) ? child.childSlot : 0;
}

function compareContentAllocationPriority(a, b) {
  if (b.remainder !== a.remainder) return b.remainder - a.remainder;
  if (b.child.count !== a.child.count) return b.child.count - a.child.count;
  return childSlotSortKey(a.child) - childSlotSortKey(b.child);
}

function distributeRemainingByPriority(allocations, remaining) {
  const active = allocations
    .filter((entry) => entry.capacity > 0)
    .sort(compareContentAllocationPriority);
  let activeCount = active.length;
  while (remaining > 0 && activeCount > 0) {
    if (remaining < activeCount) {
      for (let i = 0; i < remaining; i++) {
        active[i].allocation += 1;
        active[i].capacity -= 1;
      }
      return 0;
    }

    let minCapacity = Infinity;
    for (let i = 0; i < activeCount; i++) {
      if (active[i].capacity < minCapacity) {
        minCapacity = active[i].capacity;
      }
    }
    const rounds = Math.min(minCapacity, Math.floor(remaining / activeCount));
    if (rounds <= 0) {
      break;
    }
    for (let i = 0; i < activeCount; i++) {
      active[i].allocation += rounds;
      active[i].capacity -= rounds;
    }
    remaining -= rounds * activeCount;

    let writeIndex = 0;
    for (let i = 0; i < activeCount; i++) {
      if (active[i].capacity > 0) {
        active[writeIndex++] = active[i];
      }
    }
    activeCount = writeIndex;
  }
  return remaining;
}

function allocateContentTargetsByChildCount(node, totalTarget) {
  const children = node.children.filter((child) => child.count > 0);
  if (children.length === 0) {
    return;
  }

  const minTotal = children.reduce(
    (sum, child) => sum + minimumContentTargetForVirtualBudget(child),
    0,
  );
  const total = Math.max(
    minTotal,
    Math.min(node.count, Math.floor(totalTarget)),
  );
  const allocations = children.map((child) => {
    const minimum = minimumContentTargetForVirtualBudget(child);
    return {
      child,
      allocation: minimum,
      capacity: Math.max(0, child.count - minimum),
      remainder: 0.0,
    };
  });
  let remaining =
    total - allocations.reduce((sum, entry) => sum + entry.allocation, 0);
  if (remaining > 0) {
    const denominator = Math.max(1, node.count);
    for (const entry of allocations) {
      const exact = (remaining * entry.child.count) / denominator;
      const extra = Math.min(entry.capacity, Math.floor(exact));
      entry.allocation += extra;
      entry.capacity -= extra;
      entry.remainder = exact - extra;
    }
    remaining =
      total - allocations.reduce((sum, entry) => sum + entry.allocation, 0);
  }

  remaining = distributeRemainingByPriority(allocations, remaining);

  for (const entry of allocations) {
    entry.child.contentTargetOverride = entry.allocation;
  }
}

function assignVirtualSegmentContentTargetsRecursive(
  node,
  lodMaxDepth,
  samplingRatePerLevel,
) {
  if (node.virtual) {
    const target = resolveVirtualNodeContentTarget(
      node,
      lodMaxDepth,
      samplingRatePerLevel,
    );
    allocateContentTargetsByChildCount(node, target);
  }
  for (const child of node.children) {
    assignVirtualSegmentContentTargetsRecursive(
      child,
      lodMaxDepth,
      samplingRatePerLevel,
    );
  }
}

function assignVirtualSegmentContentTargets(
  node,
  lodMaxDepth,
  samplingRatePerLevel,
) {
  computeVirtualBudgetMinimums(node);
  assignVirtualSegmentContentTargetsRecursive(
    node,
    lodMaxDepth,
    samplingRatePerLevel,
  );
}

async function isPartitionedNodeComplete(node, ctx) {
  if (node.virtual) {
    if (node.buildState !== 'completed') {
      return false;
    }
    if (node.depth > 0 && !node.handoffConsumed) {
      return hasExistingActiveHandoffSources(node);
    }
    return true;
  }
  if (node.buildState !== 'completed' || !node.contentUri) {
    return false;
  }
  const contentPath = path.join(ctx.params.outputDir, node.contentUri);
  if (!(await pathExists(contentPath))) {
    return false;
  }
  if (node.depth > 0 && !node.handoffConsumed) {
    if (!node.handoffPath) {
      return false;
    }
    return pathExists(node.handoffPath);
  }
  return true;
}

async function hasExistingActiveHandoffSources(node) {
  let spec;
  try {
    spec = handoffBucketSpec(node);
  } catch {
    return false;
  }
  const sources = flattenBucketSpecs([spec]).filter(
    (source) => source && source.filePath,
  );
  if (sources.length === 0) {
    return false;
  }
  for (const source of sources) {
    if (!(await pathExists(source.filePath))) {
      return false;
    }
  }
  return true;
}

function consumeHandoffSources(node, ctx) {
  if (!node || node.handoffConsumed) {
    return;
  }
  if (node.handoffPath) {
    ctx.pendingHandoffCleanup.add(node.handoffPath);
    node.handoffPath = null;
    node.handoffRowCount = null;
    node.handoffConsumed = true;
    return;
  }
  if (node.virtual) {
    for (const child of node.children) {
      consumeHandoffSources(child, ctx);
    }
  }
  node.handoffPath = null;
  node.handoffRowCount = null;
  node.handoffConsumed = true;
}

async function processPartitionedLeafNode(node, ctx) {
  ensure(!!node.bucketPath, `Missing leaf bucket path for node ${node.key}.`);
  const { entries, totalRows } = await collectBucketEntries(
    [leafBucketSpec(node)],
    ctx.layout.coeffCount,
  );
  node.bucketRowCount = totalRows;
  let handoffPromise = Promise.resolve();
  if (node.depth > 0) {
    node.handoffPath = canonicalNodePath(ctx.tempDir, HANDOFF_BUCKET_DIR, node);
    handoffPromise = materializeLinkedHandoffFile(
      node.bucketPath,
      node.handoffPath,
    );
    node.handoffRowCount = totalRows;
    node.handoffConsumed = false;
  } else {
    node.handoffPath = null;
    node.handoffRowCount = null;
    node.handoffConsumed = true;
  }

  node.ownError = 0.0;
  const contentPromise = writeBucketContentFile(
    ctx.params,
    entries,
    ctx.layout.coeffCount,
    totalRows,
    ctx.layout.degree,
    node.level,
    node.x,
    node.y,
    node.z,
    safeNodeBoundsTranslation(node),
  ).then((contentUri) => {
    node.contentUri = contentUri;
  });
  await Promise.all([handoffPromise, contentPromise]);
  node.buildState = 'completed';
  enqueuePipelineStateSave(ctx, PIPELINE_STAGE_BUCKETED);
}

async function processPartitionedVirtualInternalNode(node, ctx) {
  const inputSpecs = node.children.map((child) => handoffBucketSpec(child));
  const { entries, totalRows } = await collectBucketEntries(
    inputSpecs,
    ctx.layout.coeffCount,
  );

  ensure(
    entries.length > 0 && totalRows > 0,
    `Virtual node ${node.key} has no active handoff sources.`,
  );
  node.handoffPath = null;
  node.handoffRowCount = totalRows;
  node.handoffConsumed = node.depth <= 0;

  node.ownError = 0.0;
  node.contentUri = null;
  node.buildState = 'completed';
  enqueuePipelineStateSave(ctx, PIPELINE_STAGE_BUCKETED);
}

async function processPartitionedInternalNode(node, ctx) {
  if (node.virtual) {
    await processPartitionedVirtualInternalNode(node, ctx);
    return;
  }

  const inputSpecs = node.children.map((child) => handoffBucketSpec(child));
  const { entries, totalRows } = await collectBucketEntries(
    inputSpecs,
    ctx.layout.coeffCount,
  );
  const contentTarget = resolveNodeContentTarget(node, ctx, totalRows);
  if (totalRows <= contentTarget) {
    let handoffPromise = Promise.resolve();
    if (node.depth > 0) {
      node.handoffPath = canonicalNodePath(
        ctx.tempDir,
        HANDOFF_BUCKET_DIR,
        node,
      );
      handoffPromise = materializeCanonicalEntriesFile(
        entries,
        node.handoffPath,
        ctx.layout.coeffCount,
        { bucketChunkBytes: ctx.bucketChunkBytes },
      );
      node.handoffRowCount = totalRows;
      node.handoffConsumed = false;
    } else {
      node.handoffPath = null;
      node.handoffRowCount = null;
      node.handoffConsumed = true;
    }

    node.ownError = 0.0;
    const contentPromise = writeBucketContentFile(
      ctx.params,
      entries,
      ctx.layout.coeffCount,
      totalRows,
      ctx.layout.degree,
      node.level,
      node.x,
      node.y,
      node.z,
      safeNodeBoundsTranslation(node),
    ).then((contentUri) => {
      node.contentUri = contentUri;
    });
    await Promise.all([handoffPromise, contentPromise]);
  } else {
    const handoffPath =
      node.depth > 0
        ? canonicalNodePath(ctx.tempDir, HANDOFF_BUCKET_DIR, node)
        : null;
    const result = await writeSimplifiedBucketContentFile(
      ctx.params,
      entries,
      ctx.layout.coeffCount,
      totalRows,
      contentTarget,
      node.bounds,
      node.level,
      node.x,
      node.y,
      node.z,
      handoffPath,
      {
        bucketChunkBytes: ctx.bucketChunkBytes,
        simplifyScratchBytes: ctx.simplifyScratchBytes,
        bucketEntryCacheBytes: ctx.bucketEntryCacheBytes,
        errorBufferBytes: ctx.simplifyScratchBytes,
        translation: safeNodeBoundsTranslation(node),
      },
    );
    if (node.depth > 0) {
      node.handoffPath = handoffPath;
      node.handoffRowCount = result.handoffRowCount;
      node.handoffConsumed = false;
    } else {
      node.handoffPath = null;
      node.handoffRowCount = null;
      node.handoffConsumed = true;
    }

    node.ownError = result.ownError;
    node.contentUri = result.contentUri;
  }
  node.buildState = 'completed';
  for (const child of node.children) {
    consumeHandoffSources(child, ctx);
  }
  enqueuePipelineStateSave(ctx, PIPELINE_STAGE_BUCKETED);
}

function estimateBuildNodeInputRows(node) {
  if (node.leaf) {
    const bucketRows =
      Number.isInteger(node.bucketRowCount) && node.bucketRowCount >= 0
        ? node.bucketRowCount
        : null;
    return Math.max(1, bucketRows ?? node.count ?? 1);
  }
  let total = 0;
  for (const child of node.children) {
    total += estimateHandoffSourceRows(child);
  }
  return Math.max(1, total > 0 ? total : (node.count ?? 1));
}

function estimateHandoffSourceRows(node) {
  if (!node || node.handoffConsumed) {
    return 0;
  }
  if (Number.isInteger(node.handoffRowCount) && node.handoffRowCount >= 0) {
    return node.handoffRowCount;
  }
  if (node.virtual && !node.handoffPath) {
    return node.children.reduce(
      (sum, child) => sum + estimateHandoffSourceRows(child),
      0,
    );
  }
  return Number.isInteger(node.count) && node.count > 0 ? node.count : 0;
}

function estimateBuildNodeMemoryBytes(node, ctx) {
  const inputRows = estimateBuildNodeInputRows(node);
  const coeffCount = ctx.layout.coeffCount;
  const rowByteSize = bucketRowByteSize(HANDOFF_BUCKET_ENCODING, coeffCount);
  const spzRowBytes = spzBytesPerBucketRow(coeffCount);
  const base = BUILD_MIN_TASK_MEMORY_BYTES;

  if (node.leaf) {
    return base + inputRows * (spzRowBytes * 2 + rowByteSize * 0.25);
  }

  if (node.virtual) {
    return base + inputRows * rowByteSize;
  }

  const targetRows = resolveNodeContentTarget(node, ctx, inputRows);
  if (inputRows <= targetRows) {
    return base + inputRows * (spzRowBytes * 2 + rowByteSize);
  }

  return (
    base +
    inputRows * (rowByteSize + 96) +
    targetRows * (rowByteSize + spzRowBytes * 2)
  );
}

async function processBuildNodes(nodes, concurrency, ctx) {
  await runWithConcurrencyBudget(
    nodes,
    concurrency,
    ctx.buildMemoryBudgetBytes,
    (node) => estimateBuildNodeMemoryBytes(node, ctx),
    async (node) => {
      if (await isPartitionedNodeComplete(node, ctx)) {
        tickBuildProgress(ctx, node, 'checkpoint');
        return;
      }
      if (node.leaf) {
        await processPartitionedLeafNode(node, ctx);
      } else {
        await processPartitionedInternalNode(node, ctx);
      }
      tickBuildProgress(ctx, node);
    },
  );
}

function tickBuildProgress(ctx, node, status) {
  if (!ctx || !ctx.buildProgress || !node) {
    return;
  }
  const kind = node.virtual ? 'virtual' : node.leaf ? 'leaf' : 'internal';
  const suffix = status ? ` ${status}` : '';
  ctx.buildProgress.tick(`level=${node.level} ${kind}${suffix}`);
}

async function buildPartitionedBottomUp(rootNode, ctx) {
  const treeStats = collectTreeStats(rootNode);
  const cleanupTasks = [];
  for (let level = treeStats.maxLevel; level >= 0; level--) {
    const levelNodes = treeStats.levels[level] || [];
    ctx.pendingHandoffCleanup = new Set();
    const leafNodes = [];
    const internalNodes = [];
    for (const node of levelNodes) {
      if (node.leaf) {
        leafNodes.push(node);
      } else {
        internalNodes.push(node);
      }
    }
    await processBuildNodes(leafNodes, ctx.leafNodeConcurrency, ctx);
    await processBuildNodes(internalNodes, ctx.nodeConcurrency, ctx);
    if (levelNodes.length > 0) {
      await enqueuePipelineStateSave(ctx, PIPELINE_STAGE_BUCKETED, {
        force: true,
      });
      const handoffPaths = Array.from(ctx.pendingHandoffCleanup);
      if (handoffPaths.length > 0) {
        cleanupTasks.push(
          runWithConcurrency(
            handoffPaths,
            ctx.nodeConcurrency,
            async (filePath) => {
              await removeFileIfExists(filePath);
            },
          ).then(
            () => null,
            (err) => err,
          ),
        );
      }
    }
  }
  const cleanupErrors = (await Promise.all(cleanupTasks)).filter(Boolean);
  if (cleanupErrors.length > 0) {
    throw cleanupErrors[0];
  }
  await enqueuePipelineStateSave(ctx, 'built', { force: true });
}

module.exports = {
  assignVirtualSegmentContentTargets,
  resolveNodeContentTarget,
  buildPartitionedBottomUp,
};
