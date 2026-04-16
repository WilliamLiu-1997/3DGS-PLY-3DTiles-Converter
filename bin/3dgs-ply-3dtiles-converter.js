#!/usr/bin/env node

const { run } = require('../src/cli');

run(process.argv.slice(2))
  .then((code) => process.exit(code))
  .catch((err) => {
    console.error(err && err.message ? err.message : String(err));
    process.exit(2);
  });
