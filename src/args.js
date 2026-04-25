const { ConversionError } = require('./parser');

const WGS84_SEMI_MAJOR_AXIS = 6378137.0;
const WGS84_FLATTENING = 1.0 / 298.257223563;
const WGS84_ECCENTRICITY_SQUARED = WGS84_FLATTENING * (2.0 - WGS84_FLATTENING);

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
    '  --tile-refinement <int>',
    '  --leaf-limit <int>',
    '  --min-geometric-error <number>',
    '  --spz-sh1-bits <1..8>',
    '  --spz-sh-rest-bits <1..8>',
    '  --spz-compression-level <0..9>',
    '  --transform <json_matrix4>',
    '  --coordinate <json_[lat,long,height]>',
    '  --sampling-rate-per-level <0..1]',
    '  --sample-mode <sample|merge>',
    '  --memory-budget <gb>',
    '  --obb / --aabb',
    '  --open-inspector / --no-open-inspector',
    '  --self-test',
    '  --self-test-count <int>',
    '  --clean / --continue',
    '  --help',
  ].join('\n');
}

const DEFAULT_CONVERSION_ARGS = {
  input: null,
  output: null,
  inputConvention: 'graphdeco',
  linearScaleInput: false,
  colorSpace: 'srgb_rec709_display',
  maxDepth: 8,
  tileRefinement: 1,
  leafLimit: 100,
  minGeometricError: null,
  spzSh1Bits: 8,
  spzShRestBits: 8,
  spzCompressionLevel: 8,
  transform: null,
  coordinate: null,
  samplingRatePerLevel: 0.5,
  sampleMode: 'merge',
  memoryBudget: 2,
  orientedBoundingBoxes: false,
  openInspector: true,
  clean: true,
  selfTest: false,
  selfTestCount: 1000000,
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

function normalizeToStrictInt(value, name) {
  if (value === undefined || value === null) {
    return value;
  }
  const parsed =
    typeof value === 'number' && Number.isFinite(value)
      ? value
      : Number(String(value));
  if (!Number.isInteger(parsed)) {
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

function parseJsonValue(value, name) {
  if (value === undefined || value === null || typeof value !== 'string') {
    return value;
  }
  try {
    return JSON.parse(value);
  } catch (err) {
    throw new ConversionError(`Invalid JSON for ${name}: ${value}`);
  }
}

function normalizeMatrix4(value, name) {
  if (value === undefined || value === null) {
    return null;
  }

  const parsed = parseJsonValue(value, name);
  if (!Array.isArray(parsed)) {
    throw new ConversionError(
      `${name} must be a 4x4 JSON matrix or a flat 16-number array.`,
    );
  }

  if (
    parsed.length === 4 &&
    parsed.every((row) => Array.isArray(row) && row.length === 4)
  ) {
    const rows = parsed.map((row, rowIndex) =>
      row.map((entry, columnIndex) =>
        normalizeToFloat(entry, `${name}[${rowIndex}][${columnIndex}]`),
      ),
    );
    return [
      rows[0][0],
      rows[1][0],
      rows[2][0],
      rows[3][0],
      rows[0][1],
      rows[1][1],
      rows[2][1],
      rows[3][1],
      rows[0][2],
      rows[1][2],
      rows[2][2],
      rows[3][2],
      rows[0][3],
      rows[1][3],
      rows[2][3],
      rows[3][3],
    ];
  }

  if (parsed.length !== 16 || parsed.some((entry) => Array.isArray(entry))) {
    throw new ConversionError(
      `${name} must be a 4x4 JSON matrix or a flat 16-number array.`,
    );
  }

  return parsed.map((entry, index) =>
    normalizeToFloat(entry, `${name}[${index}]`),
  );
}

function normalizeCoordinate(value, name) {
  if (value === undefined || value === null) {
    return null;
  }

  const parsed = parseJsonValue(value, name);
  let triplet = null;

  if (Array.isArray(parsed)) {
    if (parsed.length !== 3 || parsed.some((entry) => Array.isArray(entry))) {
      throw new ConversionError(
        `${name} must be [lat, long, height] in degrees/meters.`,
      );
    }
    triplet = parsed;
  } else if (typeof parsed === 'object') {
    if (!parsed) {
      throw new ConversionError(
        `${name} must be [lat, long, height] in degrees/meters.`,
      );
    }
    const lat = firstDefined(parsed.lat, parsed.latitude);
    const lon = firstDefined(
      parsed.long,
      parsed.lng,
      parsed.lon,
      parsed.longitude,
    );
    const height = firstDefined(parsed.height, parsed.alt, parsed.altitude);
    if (lat === undefined || lon === undefined || height === undefined) {
      throw new ConversionError(
        `${name} object must include lat/latitude, long/lon/lng/longitude, and height.`,
      );
    }
    triplet = [lat, lon, height];
  } else {
    throw new ConversionError(
      `${name} must be [lat, long, height] in degrees/meters.`,
    );
  }

  const lat = normalizeToFloat(triplet[0], `${name}[0]`);
  const lon = normalizeToFloat(triplet[1], `${name}[1]`);
  const height = normalizeToFloat(triplet[2], `${name}[2]`);

  if (lat < -90.0 || lat > 90.0) {
    throw new ConversionError(`${name}[0] latitude must be in [-90, 90].`);
  }
  if (lon < -180.0 || lon > 180.0) {
    throw new ConversionError(`${name}[1] longitude must be in [-180, 180].`);
  }

  return [lat, lon, height];
}

function degreesToRadians(degrees) {
  return (degrees * Math.PI) / 180.0;
}

function makeCoordinateTransform(coordinate) {
  if (!coordinate) {
    return null;
  }

  const latRad = degreesToRadians(coordinate[0]);
  const lonRad = degreesToRadians(coordinate[1]);
  const height = coordinate[2];

  const sinLat = Math.sin(latRad);
  const cosLat = Math.cos(latRad);
  const sinLon = Math.sin(lonRad);
  const cosLon = Math.cos(lonRad);

  const primeVerticalRadius =
    WGS84_SEMI_MAJOR_AXIS /
    Math.sqrt(1.0 - WGS84_ECCENTRICITY_SQUARED * sinLat * sinLat);

  const tx = (primeVerticalRadius + height) * cosLat * cosLon;
  const ty = (primeVerticalRadius + height) * cosLat * sinLon;
  const tz =
    (primeVerticalRadius * (1.0 - WGS84_ECCENTRICITY_SQUARED) + height) *
    sinLat;

  const east = [-sinLon, cosLon, 0.0];
  const north = [-sinLat * cosLon, -sinLat * sinLon, cosLat];
  const up = [cosLat * cosLon, cosLat * sinLon, sinLat];

  // 3D Tiles applies transforms in this order:
  // 1. glTF node hierarchy
  // 2. glTF y-up to z-up runtime transform
  // 3. tile.transform
  //
  // Therefore the root tileset transform has to be expressed in the 3D Tiles
  // z-up tile frame, not raw glTF y-up space. The emitted content is normalized
  // to the converter's single supported source-up-axis mode before
  // tile.transform is applied.
  return [
    east[0],
    east[1],
    east[2],
    0.0,
    north[0],
    north[1],
    north[2],
    0.0,
    up[0],
    up[1],
    up[2],
    0.0,
    tx,
    ty,
    tz,
    1.0,
  ];
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
  if (!Number.isInteger(args.tileRefinement) || args.tileRefinement < 1) {
    throw new ConversionError('--tile-refinement must be an integer >= 1');
  }
  if (args.leafLimit < 1) {
    throw new ConversionError('--leaf-limit must be >= 1');
  }
  if (args.spzSh1Bits < 1 || args.spzSh1Bits > 8) {
    throw new ConversionError('--spz-sh1-bits must be in [1, 8]');
  }
  if (args.spzShRestBits < 1 || args.spzShRestBits > 8) {
    throw new ConversionError('--spz-sh-rest-bits must be in [1, 8]');
  }
  if (
    !Number.isInteger(args.spzCompressionLevel) ||
    args.spzCompressionLevel < 0 ||
    args.spzCompressionLevel > 9
  ) {
    throw new ConversionError('--spz-compression-level must be in [0, 9]');
  }
  if (args.samplingRatePerLevel <= 0.0 || args.samplingRatePerLevel > 1.0) {
    throw new ConversionError('--sampling-rate-per-level must be in (0, 1]');
  }
  if (!Number.isFinite(args.memoryBudget) || args.memoryBudget <= 0.0) {
    throw new ConversionError('--memory-budget must be > 0 GB');
  }
  if (typeof args.orientedBoundingBoxes !== 'boolean') {
    throw new ConversionError('--obb / --aabb must normalize to boolean');
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
  assertChoice(args.sampleMode, ['sample', 'merge'], '--sample-mode');
  if (args.transform !== null) {
    if (!Array.isArray(args.transform) || args.transform.length !== 16) {
      throw new ConversionError(
        '--transform must normalize to a 16-number matrix.',
      );
    }
  }
  if (args.coordinate !== null) {
    if (!Array.isArray(args.coordinate) || args.coordinate.length !== 3) {
      throw new ConversionError(
        '--coordinate must normalize to [lat, long, height].',
      );
    }
  }

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
    if (token === '--tile-refinement') {
      args.tileRefinement = normalizeToStrictInt(
        requireValue(token),
        token,
      );
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
    if (token === '--spz-compression-level') {
      const raw = requireValue(token);
      const value = Number.parseInt(raw, 10);
      if (!Number.isInteger(value)) {
        throw new ConversionError(
          `Invalid integer for --spz-compression-level: ${raw}`,
        );
      }
      args.spzCompressionLevel = value;
      continue;
    }
    if (token === '--transform') {
      args.transform = requireValue(token);
      continue;
    }
    if (token === '--coordinate') {
      args.coordinate = requireValue(token);
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
    if (token === '--sample-mode') {
      args.sampleMode = requireValue(token);
      continue;
    }
    if (token === '--memory-budget') {
      const raw = requireValue(token);
      const value = Number.parseFloat(raw);
      if (!Number.isFinite(value)) {
        throw new ConversionError(`Invalid number for --memory-budget: ${raw}`);
      }
      args.memoryBudget = value;
      continue;
    }
    if (token === '--obb') {
      args.orientedBoundingBoxes = true;
      continue;
    }
    if (token === '--aabb') {
      args.orientedBoundingBoxes = false;
      continue;
    }
    if (token === '--open-inspector') {
      args.openInspector = true;
      continue;
    }
    if (token === '--no-open-inspector') {
      args.openInspector = false;
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
    if (token === '--continue') {
      args.clean = false;
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

  if (args.transform != null && args.coordinate != null) {
    throw new ConversionError(
      'Please provide either --transform or --coordinate, not both.',
    );
  }
  args.coordinate = normalizeCoordinate(args.coordinate, '--coordinate');
  args.transform =
    args.coordinate != null
      ? makeCoordinateTransform(args.coordinate)
      : normalizeMatrix4(args.transform, '--transform');
  validateConversionArgs(args);
  return args;
}

function makeConversionArgs(
  input,
  output,
  options = {},
  { requireInput = false } = {},
) {
  const rawTransform = firstDefined(
    options.transform,
    options['transform'],
    options.rootTransform,
    options.root_transform,
  );
  const rawCoordinate = firstDefined(
    options.coordinate,
    options['coordinate'],
    options.rootCoordinate,
    options.root_coordinate,
  );
  if (rawTransform != null && rawCoordinate != null) {
    throw new ConversionError(
      'Please provide either transform or coordinate, not both.',
    );
  }

  const noOpenInspector = firstDefined(
    options.noOpenInspector,
    options['no-open-inspector'],
    options.no_open_inspector,
  );
  const continueRequested = firstDefined(
    options['continue'],
    options.continueBuild,
    options.continue_build,
  );

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
    tileRefinement: normalizeToStrictInt(
      firstDefined(
        options.tileRefinement,
        options['tile-refinement'],
        options.tile_refinement,
        DEFAULT_CONVERSION_ARGS.tileRefinement,
      ),
      '--tile-refinement',
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
    spzCompressionLevel: normalizeToInt(
      firstDefined(
        options.spzCompressionLevel,
        options['spz-compression-level'],
        options.spz_compression_level,
        DEFAULT_CONVERSION_ARGS.spzCompressionLevel,
      ),
      '--spz-compression-level',
    ),
    coordinate: normalizeCoordinate(rawCoordinate, 'coordinate'),
    transform: null,
    samplingRatePerLevel: normalizeToFloat(
      firstDefined(
        options.samplingRatePerLevel,
        options['sampling-rate-per-level'],
        options.sampling_rate_per_level,
        DEFAULT_CONVERSION_ARGS.samplingRatePerLevel,
      ),
      '--sampling-rate-per-level',
    ),
    sampleMode: firstDefined(
      options.sampleMode,
      options['sample-mode'],
      options.sample_mode,
      DEFAULT_CONVERSION_ARGS.sampleMode,
    ),
    memoryBudget: normalizeToFloat(
      firstDefined(
        options.memoryBudget,
        options['memory-budget'],
        options.memory_budget,
        options.memoryBudgetGb,
        options.memory_budget_gb,
        DEFAULT_CONVERSION_ARGS.memoryBudget,
      ),
      '--memory-budget',
    ),
    orientedBoundingBoxes: firstDefined(
      options.orientedBoundingBoxes,
      options['oriented-bounding-boxes'],
      options.oriented_bounding_boxes,
      options.obb,
      options.useObb,
      options.use_obb,
      DEFAULT_CONVERSION_ARGS.orientedBoundingBoxes,
    ),
    openInspector:
      noOpenInspector === true
        ? false
        : firstDefined(
            options.openInspector,
            options['open-inspector'],
            options.open_inspector,
            DEFAULT_CONVERSION_ARGS.openInspector,
          ),
    clean:
      continueRequested === true
        ? false
        : firstDefined(options.clean, DEFAULT_CONVERSION_ARGS.clean),
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
  delete merged.sourceUpAxis;
  delete merged['source-up-axis'];
  delete merged.source_up_axis;
  delete merged.sourceCoordinateSystem;
  delete merged.source_coordinate_system;
  delete merged['source-coordinate-system'];
  delete merged.resolvedSourceCoordinateSystem;
  delete merged.resolved_source_coordinate_system;
  delete merged.buildConcurrency;
  delete merged['build-concurrency'];
  delete merged.build_concurrency;
  delete merged.contentWorkers;
  delete merged['content-workers'];
  delete merged.content_workers;

  merged.transform =
    merged.coordinate != null
      ? makeCoordinateTransform(merged.coordinate)
      : normalizeMatrix4(rawTransform, 'transform');
  validateConversionArgs(merged, { requireInput });
  return merged;
}

module.exports = {
  DEFAULT_CONVERSION_ARGS,
  usage,
  parseArgs,
  makeConversionArgs,
  normalizeMatrix4,
  normalizeCoordinate,
  makeCoordinateTransform,
  normalizeToInt,
  normalizeToStrictInt,
  normalizeToFloat,
  validateConversionArgs,
};
