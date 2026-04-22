const fs = require('fs');
const path = require('path');

const esbuild = require('esbuild');

const ROOT_DIR = path.resolve(__dirname, '..');
const VIEWER_DIR = path.join(ROOT_DIR, 'src', 'viewer');
const OUTPUT_ROOT_DIR = path.join(ROOT_DIR, 'dist', 'viewer-assets');
const OUTPUT_VIEWER_DIR = path.join(OUTPUT_ROOT_DIR, 'viewer');
const OUTPUT_APP_PATH = path.join(OUTPUT_VIEWER_DIR, 'app.js');
const OUTPUT_VENDOR_DIR = path.join(OUTPUT_VIEWER_DIR, 'vendor');

function copyFiles(sourceDir, targetDir, fileNames) {
  fs.mkdirSync(targetDir, { recursive: true });
  for (const fileName of fileNames) {
    fs.copyFileSync(path.join(sourceDir, fileName), path.join(targetDir, fileName));
  }
}

function resolvePackageDir(packageName) {
  let currentDir = path.dirname(require.resolve(packageName));
  while (true) {
    const packageJsonPath = path.join(currentDir, 'package.json');
    if (fs.existsSync(packageJsonPath)) {
      return currentDir;
    }
    const parentDir = path.dirname(currentDir);
    if (parentDir === currentDir) {
      throw new Error(`Could not resolve package directory for ${packageName}.`);
    }
    currentDir = parentDir;
  }
}

async function main() {
  fs.rmSync(OUTPUT_ROOT_DIR, { recursive: true, force: true });
  fs.mkdirSync(OUTPUT_VIEWER_DIR, { recursive: true });

  await esbuild.build({
    entryPoints: [path.join(VIEWER_DIR, 'app.js')],
    bundle: true,
    format: 'esm',
    legalComments: 'none',
    outfile: OUTPUT_APP_PATH,
    platform: 'browser',
    target: ['es2020'],
  });

  const threeDir = resolvePackageDir('three');
  copyFiles(
    path.join(threeDir, 'examples', 'jsm', 'libs', 'draco', 'gltf'),
    path.join(
      OUTPUT_VENDOR_DIR,
      'three',
      'examples',
      'jsm',
      'libs',
      'draco',
      'gltf',
    ),
    ['draco_decoder.js', 'draco_decoder.wasm', 'draco_wasm_wrapper.js'],
  );
  copyFiles(
    path.join(threeDir, 'examples', 'jsm', 'libs', 'basis'),
    path.join(
      OUTPUT_VENDOR_DIR,
      'three',
      'examples',
      'jsm',
      'libs',
      'basis',
    ),
    ['basis_transcoder.js', 'basis_transcoder.wasm'],
  );
}

main().catch((err) => {
  console.error(err && err.stack ? err.stack : String(err));
  process.exit(1);
});
