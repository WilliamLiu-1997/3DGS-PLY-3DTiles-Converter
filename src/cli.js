const { main } = require('./convert-core');
const { usage, parseArgs } = require('./args');

async function run(argv = process.argv.slice(2)) {
  return main(argv);
}

module.exports = {
  run,
  usage,
  parseArgs,
};

if (require.main === module) {
  run(process.argv.slice(2)).then((code) => process.exit(code));
}
