const { ConversionError } = require('./parser');

function usage() {
  return [
    'Usage: 3dgs-ply-3dtiles-converter [options] <input.ply> <output_dir>',
    '       3dgs-ply-3dtiles-converter --self-test <output_dir>',
    '',
    'Options:',
    '  --input-convention <graphdeco|khr_native>',
    '  --linear-scale-input',
    '  --color-space <lin_rec709_display|srgb_rec709_display>',
    '  --max-depth <int>',
    '  --leaf-limit <int>',
    '  --tiling-mode <explicit|implicit>',
    '  --subtree-levels <int>',
    '  --min-geometric-error <number>',
    '  --spz-sh1-bits <1..8>',
    '  --spz-sh-rest-bits <1..8>',
    '  --source-up-axis <z|y>',
    '  --sampling-rate-per-level <0..1]',
    '  --content-workers <0+>',
    '  --self-test',
    '  --self-test-count <int>',
    '  --clean',
    '  --help',
  ].join('\n');
}

const DEFAULT_CONVERSION_ARGS = {
  input: null,
  output: null,
  inputConvention: 'graphdeco',
  linearScaleInput: false,
  colorSpace: 'srgb_rec709_display',
  maxDepth: 4,
  leafLimit: 10000,
  tilingMode: 'explicit',
  subtreeLevels: 2,
  minGeometricError: null,
  spzSh1Bits: 8,
  spzShRestBits: 8,
  sourceUpAxis: 'z',
  samplingRatePerLevel: 0.5,
  contentWorkers: 4,
  clean: false,
  selfTest: false,
  selfTestCount: 6000,
  help: false,
};

function firstDefined(...values) {
  for (const value of values) {
    if (value !== undefined) {
      return value;
    }
  }
  return undefined;
}

function normalizeToInt(value, name) {
  if (value === undefined || value === null) {
    return value;
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    return Math.trunc(value);
  }
  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isFinite(parsed)) {
    throw new ConversionError(`Invalid integer for ${name}: ${value}`);
  }
  return parsed;
}

function normalizeToFloat(value, name) {
  if (value === undefined || value === null) {
    return value;
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  const parsed = Number.parseFloat(String(value));
  if (!Number.isFinite(parsed)) {
    throw new ConversionError(`Invalid number for ${name}: ${value}`);
  }
  return parsed;
}

function assertChoice(value, choices, flagName) {
  if (!choices.includes(value)) {
    throw new ConversionError(
      `Invalid ${flagName}: ${value}. Allowed: ${choices.join(', ')}`,
    );
  }
}

function validateConversionArgs(args, { requireInput = false } = {}) {
  if (args.maxDepth < 0) {
    throw new ConversionError('--max-depth must be >= 0');
  }
  if (args.leafLimit < 1) {
    throw new ConversionError('--leaf-limit must be >= 1');
  }
  if (args.subtreeLevels < 1) {
    throw new ConversionError('--subtree-levels must be >= 1');
  }
  if (args.spzSh1Bits < 1 || args.spzSh1Bits > 8) {
    throw new ConversionError('--spz-sh1-bits must be in [1, 8]');
  }
  if (args.spzShRestBits < 1 || args.spzShRestBits > 8) {
    throw new ConversionError('--spz-sh-rest-bits must be in [1, 8]');
  }
  if (args.samplingRatePerLevel <= 0.0 || args.samplingRatePerLevel > 1.0) {
    throw new ConversionError('--sampling-rate-per-level must be in (0, 1]');
  }
  if (args.contentWorkers < 0) {
    throw new ConversionError('--content-workers must be >= 0');
  }

  assertChoice(
    args.inputConvention,
    ['graphdeco', 'khr_native'],
    '--input-convention',
  );
  assertChoice(
    args.colorSpace,
    ['lin_rec709_display', 'srgb_rec709_display'],
    '--color-space',
  );
  assertChoice(args.tilingMode, ['explicit', 'implicit'], '--tiling-mode');
  assertChoice(args.sourceUpAxis, ['z', 'y'], '--source-up-axis');

  if (requireInput && !args.input) {
    throw new ConversionError('Please provide input PLY path.');
  }
  if (!args.help && !args.output) {
    throw new ConversionError('Missing output directory.');
  }
}

function parseArgs(argv) {
  const args = { ...DEFAULT_CONVERSION_ARGS };
  const positionals = [];

  for (let i = 0; i < argv.length; i++) {
    const token = argv[i];
    const requireValue = (name) => {
      const value = argv[i + 1];
      if (value == null || value.startsWith('--')) {
        throw new ConversionError(`Missing value for ${name}.`);
      }
      i += 1;
      return value;
    };

    if (token === '--help' || token === '-h') {
      args.help = true;
      continue;
    }
    if (token === '--input-convention') {
      args.inputConvention = requireValue(token);
      continue;
    }
    if (token === '--linear-scale-input') {
      args.linearScaleInput = true;
      continue;
    }
    if (token === '--color-space') {
      args.colorSpace = requireValue(token);
      continue;
    }
    if (token === '--max-depth') {
      const raw = requireValue(token);
      const value = Number.parseInt(raw, 10);
      if (!Number.isInteger(value)) {
        throw new ConversionError(`Invalid integer for --max-depth: ${raw}`);
      }
      args.maxDepth = value;
      continue;
    }
    if (token === '--leaf-limit') {
      const raw = requireValue(token);
      const value = Number.parseInt(raw, 10);
      if (!Number.isInteger(value)) {
        throw new ConversionError(`Invalid integer for --leaf-limit: ${raw}`);
      }
      args.leafLimit = value;
      continue;
    }
    if (token === '--tiling-mode') {
      args.tilingMode = requireValue(token);
      continue;
    }
    if (token === '--subtree-levels') {
      const raw = requireValue(token);
      const value = Number.parseInt(raw, 10);
      if (!Number.isInteger(value)) {
        throw new ConversionError(
          `Invalid integer for --subtree-levels: ${raw}`,
        );
      }
      args.subtreeLevels = value;
      continue;
    }
    if (token === '--min-geometric-error') {
      const raw = requireValue(token);
      const value = Number.parseFloat(raw);
      if (!Number.isFinite(value)) {
        throw new ConversionError(
          `Invalid number for --min-geometric-error: ${raw}`,
        );
      }
      args.minGeometricError = value;
      continue;
    }
    if (token === '--spz-sh1-bits') {
      const raw = requireValue(token);
      const value = Number.parseInt(raw, 10);
      if (!Number.isInteger(value)) {
        throw new ConversionError(`Invalid integer for --spz-sh1-bits: ${raw}`);
      }
      args.spzSh1Bits = value;
      continue;
    }
    if (token === '--spz-sh-rest-bits') {
      const raw = requireValue(token);
      const value = Number.parseInt(raw, 10);
      if (!Number.isInteger(value)) {
        throw new ConversionError(
          `Invalid integer for --spz-sh-rest-bits: ${raw}`,
        );
      }
      args.spzShRestBits = value;
      continue;
    }
    if (token === '--source-up-axis') {
      args.sourceUpAxis = requireValue(token);
      continue;
    }
    if (token === '--sampling-rate-per-level') {
      const raw = requireValue(token);
      const value = Number.parseFloat(raw);
      if (!Number.isFinite(value)) {
        throw new ConversionError(
          `Invalid number for --sampling-rate-per-level: ${raw}`,
        );
      }
      args.samplingRatePerLevel = value;
      continue;
    }
    if (token === '--content-workers') {
      const raw = requireValue(token);
      const value = Number.parseInt(raw, 10);
      if (!Number.isInteger(value)) {
        throw new ConversionError(
          `Invalid integer for --content-workers: ${raw}`,
        );
      }
      args.contentWorkers = value;
      continue;
    }
    if (token === '--self-test') {
      args.selfTest = true;
      continue;
    }
    if (token === '--self-test-count') {
      const raw = requireValue(token);
      const value = Number.parseInt(raw, 10);
      if (!Number.isInteger(value)) {
        throw new ConversionError(
          `Invalid integer for --self-test-count: ${raw}`,
        );
      }
      args.selfTestCount = value;
      continue;
    }
    if (token === '--clean') {
      args.clean = true;
      continue;
    }
    if (token.startsWith('--')) {
      throw new ConversionError(`Unknown option ${token}`);
    }
    positionals.push(token);
  }

  if (args.selfTest) {
    if (positionals.length > 0) {
      args.output = positionals[0];
    }
    if (positionals.length > 1) {
      throw new ConversionError(
        `Unexpected positional argument: ${positionals[1]}`,
      );
    }
  } else {
    if (positionals.length > 0) {
      args.input = positionals[0];
    }
    if (positionals.length > 1) {
      args.output = positionals[1];
    }
    if (positionals.length > 2) {
      throw new ConversionError(
        `Unexpected positional argument: ${positionals[2]}`,
      );
    }
  }

  validateConversionArgs(args);
  return args;
}

function makeConversionArgs(
  input,
  output,
  options = {},
  { requireInput = false } = {},
) {
  const merged = {
    ...DEFAULT_CONVERSION_ARGS,
    ...options,
    input: firstDefined(
      input,
      options.input,
      options.inputPath,
      options.input_file,
    ),
    output: firstDefined(
      output,
      options.output,
      options.outputDir,
      options.output_dir,
    ),
    inputConvention: firstDefined(
      options.inputConvention,
      options['input-convention'],
      options.input_convention,
      DEFAULT_CONVERSION_ARGS.inputConvention,
    ),
    linearScaleInput: firstDefined(
      options.linearScaleInput,
      options['linear-scale-input'],
      options.linear_scale_input,
      DEFAULT_CONVERSION_ARGS.linearScaleInput,
    ),
    colorSpace: firstDefined(
      options.colorSpace,
      options['color-space'],
      options.color_space,
      DEFAULT_CONVERSION_ARGS.colorSpace,
    ),
    maxDepth: normalizeToInt(
      firstDefined(
        options.maxDepth,
        options['max-depth'],
        options.max_depth,
        DEFAULT_CONVERSION_ARGS.maxDepth,
      ),
      '--max-depth',
    ),
    leafLimit: normalizeToInt(
      firstDefined(
        options.leafLimit,
        options['leaf-limit'],
        options.leaf_limit,
        DEFAULT_CONVERSION_ARGS.leafLimit,
      ),
      '--leaf-limit',
    ),
    tilingMode: firstDefined(
      options.tilingMode,
      options['tiling-mode'],
      options.tiling_mode,
      DEFAULT_CONVERSION_ARGS.tilingMode,
    ),
    subtreeLevels: normalizeToInt(
      firstDefined(
        options.subtreeLevels,
        options['subtree-levels'],
        options.subtree_levels,
        DEFAULT_CONVERSION_ARGS.subtreeLevels,
      ),
      '--subtree-levels',
    ),
    minGeometricError: firstDefined(
      options.minGeometricError,
      options['min-geometric-error'],
      options.min_geometric_error,
      DEFAULT_CONVERSION_ARGS.minGeometricError,
    ),
    spzSh1Bits: normalizeToInt(
      firstDefined(
        options.spzSh1Bits,
        options['spz-sh1-bits'],
        options.spz_sh1_bits,
        DEFAULT_CONVERSION_ARGS.spzSh1Bits,
      ),
      '--spz-sh1-bits',
    ),
    spzShRestBits: normalizeToInt(
      firstDefined(
        options.spzShRestBits,
        options['spz-sh-rest-bits'],
        options.spz_sh_rest_bits,
        DEFAULT_CONVERSION_ARGS.spzShRestBits,
      ),
      '--spz-sh-rest-bits',
    ),
    sourceUpAxis: firstDefined(
      options.sourceUpAxis,
      options['source-up-axis'],
      options.source_up_axis,
      DEFAULT_CONVERSION_ARGS.sourceUpAxis,
    ),
    samplingRatePerLevel: normalizeToFloat(
      firstDefined(
        options.samplingRatePerLevel,
        options['sampling-rate-per-level'],
        options.sampling_rate_per_level,
        DEFAULT_CONVERSION_ARGS.samplingRatePerLevel,
      ),
      '--sampling-rate-per-level',
    ),
    contentWorkers: normalizeToInt(
      firstDefined(
        options.contentWorkers,
        options['content-workers'],
        options.content_workers,
        DEFAULT_CONVERSION_ARGS.contentWorkers,
      ),
      '--content-workers',
    ),
    clean: firstDefined(options.clean, DEFAULT_CONVERSION_ARGS.clean),
    selfTest: firstDefined(
      options.selfTest,
      options['self-test'],
      DEFAULT_CONVERSION_ARGS.selfTest,
    ),
    selfTestCount: normalizeToInt(
      firstDefined(
        options.selfTestCount,
        options['self-test-count'],
        options.self_test_count,
        DEFAULT_CONVERSION_ARGS.selfTestCount,
      ),
      '--self-test-count',
    ),
    help: firstDefined(
      options.help,
      options['help'],
      DEFAULT_CONVERSION_ARGS.help,
    ),
  };

  validateConversionArgs(merged, { requireInput });
  return merged;
}

module.exports = {
  DEFAULT_CONVERSION_ARGS,
  usage,
  parseArgs,
  makeConversionArgs,
  normalizeToInt,
  normalizeToFloat,
  validateConversionArgs,
};
