#!/usr/bin/env node

const { runViewerCli } = require('../src/cli');

runViewerCli(process.argv.slice(2))
  .then((code) => process.exit(code))
  .catch((err) => {
    console.error(err && err.message ? err.message : String(err));
    process.exit(2);
  });
