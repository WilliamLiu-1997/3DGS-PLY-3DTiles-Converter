const { main } = require('./convert-core');

async function run(argv = process.argv.slice(2)) {
  return main(argv);
}

module.exports = {
  run,
};

if (require.main === module) {
  run(process.argv.slice(2)).then((code) => process.exit(code));
}
