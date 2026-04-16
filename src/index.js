const core = require('./convert-core');

module.exports = {
  ...core,

  // Library-friendly aliases
  convert: core.convertPlyTo3DTiles,
  convertCloud: core.convertCloudTo3DTiles,
  run: core.main,
};
