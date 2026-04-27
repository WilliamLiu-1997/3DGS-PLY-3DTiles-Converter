async function runWithConcurrency(items, limit, onItem) {
  if (!items || items.length === 0) {
    return;
  }
  const concurrency = Math.max(
    1,
    Math.min(items.length, Math.floor(limit || 1)),
  );
  let cursor = 0;
  const workers = Array.from({ length: concurrency }, async () => {
    while (true) {
      const index = cursor++;
      if (index >= items.length) {
        return;
      }
      await onItem(items[index], index);
    }
  });
  await Promise.all(workers);
}

async function runWithConcurrencyBudget(
  items,
  limit,
  budgetBytes,
  estimateBytes,
  onItem,
) {
  if (!items || items.length === 0) {
    return;
  }

  const concurrency = Math.max(
    1,
    Math.min(items.length, Math.floor(limit || 1)),
  );
  const budget = Math.max(1, Math.floor(budgetBytes || 1));
  const pending = items.map((item, index) => ({
    item,
    index,
    estimated: Math.max(1, Math.floor(estimateBytes ? estimateBytes(item) : 1)),
  }));
  let activeCount = 0;
  let activeBytes = 0;

  await new Promise((resolve, reject) => {
    let settled = false;
    const findLaunchIndex = () => {
      if (pending.length === 0 || activeCount >= concurrency) {
        return -1;
      }
      if (activeCount === 0) {
        return 0;
      }

      const remaining = Math.max(0, budget - activeBytes);
      let bestIndex = -1;
      let bestBytes = 0;
      for (let i = 0; i < pending.length; i++) {
        const estimated = pending[i].estimated;
        if (estimated > remaining) {
          continue;
        }
        if (bestIndex < 0 || estimated > bestBytes) {
          bestIndex = i;
          bestBytes = estimated;
        }
      }
      return bestIndex;
    };

    const maybeLaunch = () => {
      if (settled) {
        return;
      }

      while (pending.length > 0 && activeCount < concurrency) {
        const launchIndex = findLaunchIndex();
        if (launchIndex < 0) {
          break;
        }

        const { item, index, estimated } = pending.splice(launchIndex, 1)[0];
        activeCount += 1;
        activeBytes += estimated;

        Promise.resolve()
          .then(() => onItem(item, index))
          .then(
            () => {
              activeCount -= 1;
              activeBytes -= estimated;
              if (pending.length === 0 && activeCount === 0) {
                settled = true;
                resolve();
                return;
              }
              maybeLaunch();
            },
            (err) => {
              settled = true;
              reject(err);
            },
          );
      }

      if (pending.length === 0 && activeCount === 0) {
        settled = true;
        resolve();
      }
    };

    maybeLaunch();
  });
}

module.exports = {
  runWithConcurrency,
  runWithConcurrencyBudget,
};
