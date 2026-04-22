const { ViewerError } = require('./errors');
const { runViewer } = require('./viewer-core');

function usage() {
  return [
    'Usage: 3dtiles-viewer [options] <tileset_json>',
    '',
    'Options:',
    '  --help',
  ].join('\n');
}

function parseArgs(argv) {
  let help = false;
  const positionals = [];

  for (let i = 0; i < argv.length; i++) {
    const token = argv[i];
    if (token === '--help' || token === '-h') {
      help = true;
      continue;
    }
    if (token.startsWith('--')) {
      throw new ViewerError(`Unknown option ${token}`);
    }
    positionals.push(token);
  }

  if (help) {
    return { help: true, tilesetPath: null };
  }

  if (positionals.length === 0) {
    throw new ViewerError('Missing <tileset_json>.');
  }

  if (positionals.length > 1) {
    throw new ViewerError(`Unexpected positional argument: ${positionals[1]}`);
  }

  return { help: false, tilesetPath: positionals[0] };
}

async function run(argv = process.argv.slice(2)) {
  try {
    const args = parseArgs(argv);
    if (args.help) {
      console.log(usage());
      return 0;
    }
    await runViewer(args.tilesetPath);
    return 0;
  } catch (err) {
    if (err instanceof ViewerError) {
      console.error(`Viewer failed: ${err.message}`);
    } else if (err != null) {
      console.error(err.message || String(err));
    }
    return 2;
  }
}

module.exports = {
  run,
  usage,
  parseArgs,
};

if (require.main === module) {
  run(process.argv.slice(2))
    .then((code) => process.exit(code))
    .catch((err) => {
      console.error(err && err.message ? err.message : String(err));
      process.exit(2);
    });
}
