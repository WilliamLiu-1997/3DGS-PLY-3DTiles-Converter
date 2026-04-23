const core = require('./convert-core');

module.exports = {
  ...core,

  // Library-friendly aliases
  convert: core.convertPlyTo3DTiles,
  run: core.main,
};
