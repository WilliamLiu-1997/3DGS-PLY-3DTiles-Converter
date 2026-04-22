const { ConversionError } = require('./parser');
const {
  main,
  runViewer,
  resolveAndValidateViewerDir,
} = require('./convert-core');
const { usage, parseArgs } = require('./args');

async function run(argv = process.argv.slice(2)) {
  return main(argv);
}

function viewerUsage() {
  return [
    'Usage: 3dtiles-viewer [options] <tiles_dir>',
    '       3dtiles-viewer --viewer-dir <tiles_dir>',
    '',
    'Options:',
    '  --viewer-dir <tiles_dir>',
    '  --help',
  ].join('\n');
}

function parseViewerCliArgs(argv) {
  let viewerDir = null;
  let help = false;
  const positionals = [];

  for (let i = 0; i < argv.length; i++) {
    const token = argv[i];
    if (token === '--help' || token === '-h') {
      help = true;
      continue;
    }
    if (token === '--viewer-dir') {
      const next = argv[i + 1];
      if (next === undefined) {
        throw new ConversionError('--viewer-dir requires a value.');
      }
      viewerDir = next;
      i++;
      continue;
    }
    if (token.startsWith('--')) {
      throw new ConversionError(`Unknown option ${token}`);
    }
    positionals.push(token);
  }

  if (help) {
    return { help: true, viewerDir: null };
  }

  if (viewerDir == null) {
    if (positionals.length === 0) {
      throw new ConversionError(
        'Missing <tiles_dir>. Provide a positional path or use --viewer-dir.',
      );
    }
    viewerDir = positionals[0];
    positionals.shift();
  }

  if (positionals.length > 0) {
    throw new ConversionError(
      `Unexpected positional argument: ${positionals[0]}`,
    );
  }

  return { help: false, viewerDir };
}

async function runViewerCli(argv = process.argv.slice(2)) {
  try {
    const args = parseViewerCliArgs(argv);
    if (args.help) {
      console.log(viewerUsage());
      return 0;
    }
    const dir = resolveAndValidateViewerDir(args.viewerDir);
    await runViewer(dir);
    return 0;
  } catch (err) {
    if (err instanceof ConversionError) {
      console.error(`Viewer failed: ${err.message}`);
    } else if (err != null) {
      console.error(err.message || String(err));
    }
    return 2;
  }
}

module.exports = {
  run,
  runViewerCli,
  usage,
  viewerUsage,
  parseArgs,
  parseViewerCliArgs,
};

if (require.main === module) {
  run(process.argv.slice(2)).then((code) => process.exit(code));
}
