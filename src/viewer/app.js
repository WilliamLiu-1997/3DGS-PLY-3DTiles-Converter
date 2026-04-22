import {
  AmbientLight,
  Color,
  MathUtils,
  Group,
  Matrix4,
  PerspectiveCamera,
  Quaternion,
  Raycaster,
  Scene,
  Sphere,
  Vector2,
  Vector3,
  WebGLRenderer,
} from 'three';
import { TilesRenderer } from '3d-tiles-renderer';
import {
  GLTFExtensionsPlugin,
  ImplicitTilingPlugin,
  TileCompressionPlugin,
  TilesFadePlugin,
  UnloadTilesPlugin,
  XYZTilesPlugin,
} from '3d-tiles-renderer/plugins';
import {
  CesiumIonAuthPlugin,
  ImageOverlayPlugin,
  QuantizedMeshPlugin,
  XYZTilesOverlay,
} from '3d-tiles-renderer/three/plugins';
import { GaussianSplatPlugin } from '3d-tiles-rendererjs-3dgs-plugin';
import { Ion } from 'cesium';
import { DRACOLoader } from 'three/addons/loaders/DRACOLoader.js';
import { KTX2Loader } from 'three/addons/loaders/KTX2Loader.js';
import { MeshoptDecoder } from 'three/addons/libs/meshopt_decoder.module.js';
import { TransformControls } from 'three/addons/controls/TransformControls.js';
import { CameraController } from './cameraController.js';

const SAVE_URL = new URL('../__viewer/save-transform', import.meta.url).href;
const SHUTDOWN_URL = new URL('../__viewer/shutdown', import.meta.url).href;
const VIEWER_CONFIG =
  globalThis.__TILES_VIEWER_CONFIG__ &&
  typeof globalThis.__TILES_VIEWER_CONFIG__ === 'object'
    ? globalThis.__TILES_VIEWER_CONFIG__
    : {};
const ROOT_TILESET_LABEL =
  typeof VIEWER_CONFIG.tilesetLabel === 'string' &&
  VIEWER_CONFIG.tilesetLabel.length > 0
    ? VIEWER_CONFIG.tilesetLabel
    : 'tileset.json';
const TILESET_URL = normalizeLocalResourceUrl(
  VIEWER_CONFIG.tilesetUrl || new URL('../tileset.json', import.meta.url).href,
);
const THREE_EXAMPLES_BASE_URL = new URL(
  './vendor/three/examples/jsm/',
  import.meta.url,
).href;
const DRACO_DECODER_PATH = `${THREE_EXAMPLES_BASE_URL}libs/draco/gltf/`;
const BASIS_TRANSCODER_PATH = `${THREE_EXAMPLES_BASE_URL}libs/basis/`;
const SATELLITE_IMAGERY = {
  url: 'https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
  levels: 18,
};
const CESIUM_ION_TERRAIN = {
  apiToken: Ion.defaultAccessToken,
  assetId: 1,
};
const CAMERA_CENTER_MODE_DISTANCE = 3000000;
const CAMERA_CENTER_MODE_DISTANCE_SQ = CAMERA_CENTER_MODE_DISTANCE ** 2;
const MOVE_TO_TILES_HEADING = 0;
const MOVE_TO_TILES_PITCH = MathUtils.degToRad(-30);
const MOVE_TO_TILES_ROLL = 0;
const MOVE_TO_COORDINATE_RADIUS = 10;

const statusEl = document.getElementById('status');
const toolbarEl = document.getElementById('toolbar');
const toolbarDockEl = toolbarEl.parentElement;
const toolbarToggleButton = document.getElementById('toolbar-toggle');
const translateButton = document.getElementById('translate');
const rotateButton = document.getElementById('rotate');
const moveToTilesButton = document.getElementById('move-to-tiles');
const terrainButton = document.getElementById('terrain');
const latitudeInput = document.getElementById('latitude');
const longitudeInput = document.getElementById('longitude');
const heightInput = document.getElementById('height');
const moveCameraToCoordinateButton = document.getElementById(
  'move-camera-to-coordinate',
);
const moveTilesToCoordinateButton = document.getElementById(
  'move-tiles-to-coordinate',
);
const geometricErrorScaleInput = document.getElementById(
  'geometric-error-scale',
);
const geometricErrorValueEl = document.getElementById('geometric-error-value');
const setPositionButton = document.getElementById('set-position');
const resetButton = document.getElementById('reset');
const saveButton = document.getElementById('save');
const GEOMETRIC_ERROR_SCALE_MIN_EXPONENT = -4;
const GEOMETRIC_ERROR_SCALE_MAX_EXPONENT = 4;
const GEOMETRIC_ERROR_SCALE_STEP = 0.1;
const DEFAULT_ERROR_TARGET = 6;
const DEFAULT_TERRAIN_ERROR_TARGET = 2;

function normalizeLocalResourceUrl(value) {
  if (typeof value !== 'string' || value.length === 0) {
    return value;
  }

  if (value.startsWith('//')) {
    return `/${value.replace(/^\/+/, '')}`;
  }

  if (value.startsWith('/')) {
    return value.replace(/\/{2,}/g, '/');
  }

  if (/^[a-z][a-z\d+.-]*:/i.test(value)) {
    try {
      const parsed = new URL(value);
      if (parsed.origin === window.location.origin) {
        parsed.pathname = parsed.pathname.replace(/\/{2,}/g, '/');
        return parsed.toString();
      }
    } catch (err) {
      return value;
    }
  }

  return value;
}

function forceOpaqueMaterial(material) {
  if (!material) {
    return;
  }
  if (Array.isArray(material)) {
    material.forEach(forceOpaqueMaterial);
    return;
  }
  material.transparent = false;
}

function forceOpaqueScene(root) {
  root.traverse((child) => {
    if (child.material) {
      forceOpaqueMaterial(child.material);
    }
  });
}

function setStatus(message, isError = false) {
  statusEl.textContent = message;
  statusEl.classList.toggle('error', !!isError);
}

let shutdownRequested = false;

function requestViewerShutdown() {
  if (shutdownRequested) {
    return;
  }
  shutdownRequested = true;

  let sent = false;
  try {
    if (navigator.sendBeacon) {
      sent = navigator.sendBeacon(SHUTDOWN_URL, '');
    }
  } catch (err) {
    sent = false;
  }

  if (!sent) {
    fetch(SHUTDOWN_URL, {
      method: 'POST',
      body: '',
      keepalive: true,
    }).catch(() => {});
  }
}

function getFiniteMatrix4Array(value, name = 'matrix') {
  if (!Array.isArray(value) || value.length !== 16) {
    throw new Error(`${name} must be a 16-number matrix.`);
  }

  return value.map((entry, index) => {
    const number = Number(entry);
    if (!Number.isFinite(number)) {
      throw new Error(`${name}[${index}] must be a finite number.`);
    }
    return number;
  });
}

function parseCoordinateInputs() {
  const latitude = Number(latitudeInput.value);
  const longitude = Number(longitudeInput.value);
  const height = Number(heightInput.value);

  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
    setStatus('Latitude and longitude must be valid numbers.', true);
    return null;
  }

  if (!Number.isFinite(height)) {
    setStatus('Height must be a valid number.', true);
    return null;
  }

  if (latitude < -90 || latitude > 90) {
    setStatus('Latitude must be in [-90, 90].', true);
    return null;
  }

  if (longitude < -180 || longitude > 180) {
    setStatus('Longitude must be in [-180, 180].', true);
    return null;
  }

  return {
    height,
    latitude,
    longitude,
  };
}

function updateModeButtons(mode) {
  translateButton.classList.toggle('active', mode === 'translate');
  rotateButton.classList.toggle('active', mode === 'rotate');
}

function composeMatrix(target, matrix) {
  matrix.decompose(target.position, target.quaternion, target.scale);
  target.updateMatrix();
  target.updateMatrixWorld(true);
}

function formatCoordinateInputValue(value, digits) {
  return Number.isFinite(value) ? value.toFixed(digits) : '';
}

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

function exponentToGeometricErrorScale(exponent) {
  return 2 ** exponent;
}

function formatGeometricErrorScale(value) {
  if (value < 0.1) {
    return value.toFixed(3);
  }

  return value.toFixed(2);
}

function setRaycasterFromCamera(raycaster, coords, camera) {
  const { origin, direction } = raycaster.ray;
  const nearZ = camera.reversedDepth ? 1 : -1;
  const farZ = camera.reversedDepth ? 0 : 1;

  origin.set(coords.x, coords.y, nearZ).unproject(camera);
  direction.set(coords.x, coords.y, farZ).unproject(camera).sub(origin);
  raycaster.near = 0;
  raycaster.far = direction.length();
  raycaster.camera = camera;
  direction.normalize();
}

function mouseToCoords(clientX, clientY, element, target) {
  const rect = element.getBoundingClientRect();
  target.x = ((clientX - rect.left) / rect.width) * 2 - 1;
  target.y = -((clientY - rect.top) / rect.height) * 2 + 1;
}

const renderer = new WebGLRenderer({
  antialias: false,
  alpha: true,
  premultipliedAlpha: true,
  reversedDepthBuffer: true,
});
renderer.setPixelRatio(window.devicePixelRatio);
renderer.setSize(window.innerWidth, window.innerHeight);
document.getElementById('app').appendChild(renderer.domElement);

const dracoLoader = new DRACOLoader();
dracoLoader.setDecoderPath(DRACO_DECODER_PATH);

const ktx2Loader = new KTX2Loader();
ktx2Loader.setTranscoderPath(BASIS_TRANSCODER_PATH);
ktx2Loader.detectSupport(renderer);

const scene = new Scene();
scene.background = new Color(0xffffff);

const terrainLight = new AmbientLight(0xffffff, Math.PI);
terrainLight.visible = false;
scene.add(terrainLight);

const camera = new PerspectiveCamera(
  60,
  window.innerWidth / window.innerHeight,
  1,
  2e7,
);
camera.position.set(0, 0, 1.75e7);
camera.updateMatrixWorld(true);

const contentGroup = new Group();
scene.add(contentGroup);

const globeGroup = new Group();
contentGroup.add(globeGroup);

const editableGroup = new Group();
contentGroup.add(editableGroup);
const transformHandle = new Group();
scene.add(transformHandle);

const cameraController = new CameraController(renderer, contentGroup, camera);
let globeTiles = null;
let terrainEnabled = true;

function configureGlobeTiles(next) {
  next.registerPlugin(new TilesFadePlugin());
  next.registerPlugin(new TileCompressionPlugin());
  next.registerPlugin(new UnloadTilesPlugin());
  next.preprocessURL = normalizeLocalResourceUrl;
  next.setCamera(camera);
  next.setResolutionFromRenderer(camera, renderer);
  next.addEventListener('load-model', ({ scene: modelScene }) => {
    forceOpaqueScene(modelScene);
  });
  return next;
}

function createImageryGlobeTiles() {
  const next = new TilesRenderer();
  next.downloadQueue.maxJobs = 8;
  next.parseQueue.maxJobs = 2;
  next.registerPlugin(
    new XYZTilesPlugin({
      shape: 'ellipsoid',
      center: true,
      levels: SATELLITE_IMAGERY.levels,
      url: SATELLITE_IMAGERY.url,
    }),
  );
  configureGlobeTiles(next);
  next.errorTarget = DEFAULT_ERROR_TARGET;
  return next;
}

function createTerrainGlobeTiles() {
  const next = new TilesRenderer();
  next.downloadQueue.maxJobs = 8;
  next.parseQueue.maxJobs = 2;
  next.registerPlugin(
    new CesiumIonAuthPlugin({
      apiToken: CESIUM_ION_TERRAIN.apiToken,
      assetId: String(CESIUM_ION_TERRAIN.assetId),
      autoRefreshToken: true,
      assetTypeHandler: (type, tilesRenderer) => {
        if (type === 'TERRAIN') {
          tilesRenderer.registerPlugin(new QuantizedMeshPlugin({}));
        }
      },
    }),
  );
  next.registerPlugin(
    new ImageOverlayPlugin({
      renderer,
      overlays: [
        new XYZTilesOverlay({
          url: SATELLITE_IMAGERY.url,
          levels: SATELLITE_IMAGERY.levels,
          tileDimension: 256,
          projection: 'EPSG:3857',
          color: 0xffffff,
          opacity: 1,
        }),
      ],
    }),
  );
  configureGlobeTiles(next);
  next.errorTarget = DEFAULT_TERRAIN_ERROR_TARGET;
  return next;
}

const transformControls = new TransformControls(camera, renderer.domElement);
const transformControlsHelper =
  typeof transformControls.getHelper === 'function'
    ? transformControls.getHelper()
    : null;
transformControls.setMode('translate');
transformControls.setSpace('local');
transformControls.size = 0.95;
transformControls.addEventListener('dragging-changed', ({ value }) => {
  cameraController.enabled = !value;
});
transformControls.addEventListener('objectChange', () => {
  if (syncingTransformHandle) {
    return;
  }

  transformHandle.updateMatrix();
  transformHandle.updateMatrixWorld(true);
  applyEditableGroupMatrixFromRootTransform(transformHandle.matrix);
  syncCoordinateInputsFromTilesTransform();
});
if (transformControlsHelper) {
  scene.add(transformControlsHelper);
}

const sphere = new Sphere();
const worldRight = new Vector3(1, 0, 0);
const centerNorth = new Vector3(0, 1, 0);
const cartographicTarget = {
  height: 0,
  lat: 0,
  lon: 0,
};
const moveToTilesBasis = new Matrix4();
const moveToTilesPosition = new Vector3();
const moveToTilesEast = new Vector3();
const moveToTilesNorth = new Vector3();
const moveToTilesUp = new Vector3();
const moveToTilesForward = new Vector3();
const moveToTilesRight = new Vector3();
const moveToTilesBackward = new Vector3();
const moveToTilesQuaternion = new Quaternion();
const coordinateWorldPosition = new Vector3();
const coordinateTransformMatrix = new Matrix4();
const coordinateEditMatrix = new Matrix4();
const currentRootTransformMatrix = new Matrix4();
const savedRootInverseMatrix = new Matrix4();
const pointerCoords = new Vector2();
const pickRaycaster = new Raycaster();
const pickTargets = [];
let tiles = null;
let toolbarVisible = true;
let activeTransformMode = null;
let geometricErrorScaleExponent = 0;
let geometricErrorScale = 1;
let lastSavedGeometricErrorScale = 1;
let lastSavedMatrix = new Matrix4();
const savedRootMatrix = new Matrix4();
let savedRootMatrixPromise = Promise.resolve();
let savedRootMatrixLoadError = null;
let pendingSetPosition = false;
let syncingTransformHandle = false;
let tilesTransformDirty = false;

function getActiveEllipsoid() {
  return tiles?.ellipsoid || globeTiles?.ellipsoid || null;
}

function updateTilesetErrorTarget() {
  if (!tiles) {
    return;
  }

  tiles.errorTarget = DEFAULT_ERROR_TARGET / geometricErrorScale;
}

function updateGeometricErrorScaleDisplay() {
  geometricErrorValueEl.textContent = `x${formatGeometricErrorScale(
    geometricErrorScale,
  )}`;
}

function setGeometricErrorScaleExponent(exponent) {
  geometricErrorScaleExponent = clamp(
    Number(exponent),
    GEOMETRIC_ERROR_SCALE_MIN_EXPONENT,
    GEOMETRIC_ERROR_SCALE_MAX_EXPONENT,
  );
  geometricErrorScale = exponentToGeometricErrorScale(
    geometricErrorScaleExponent,
  );
  geometricErrorScaleInput.value = geometricErrorScaleExponent.toFixed(1);
  updateGeometricErrorScaleDisplay();
  updateTilesetErrorTarget();
}

function syncTerrainButton() {
  terrainButton.classList.toggle('active', terrainEnabled);
  terrainLight.visible = terrainEnabled;
}

function syncToolbarVisibility() {
  toolbarDockEl.classList.toggle('expanded', toolbarVisible);
  toolbarDockEl.classList.toggle('collapsed', !toolbarVisible);
  toolbarEl.classList.toggle('hidden', !toolbarVisible);
  toolbarToggleButton.textContent = toolbarVisible
    ? 'Hide Panel'
    : 'Show Panel';
  toolbarToggleButton.setAttribute('aria-expanded', String(toolbarVisible));
}

function toggleToolbarVisibility() {
  toolbarVisible = !toolbarVisible;
  syncToolbarVisibility();
}

function setTerrainEnabled(enabled) {
  const next = enabled ? createTerrainGlobeTiles() : createImageryGlobeTiles();

  if (globeTiles) {
    globeGroup.remove(globeTiles.group);
    globeTiles.dispose();
  }

  terrainEnabled = enabled;
  globeTiles = next;
  globeGroup.add(next.group);
  cameraController.setEllipsoid(getActiveEllipsoid());
  syncTerrainButton();
}

function syncTransformControlsState() {
  const controlsVisible = activeTransformMode !== null && !pendingSetPosition;
  if (controlsVisible) {
    if (transformControls.object !== transformHandle) {
      transformControls.attach(transformHandle);
    }
  } else if (transformControls.object) {
    transformControls.detach();
  }
  transformControls.enabled = controlsVisible;
  if (transformControlsHelper) {
    transformControlsHelper.visible = controlsVisible;
    transformControlsHelper.updateMatrixWorld(true);
  }
}

function setTransformMode(mode) {
  activeTransformMode = mode;
  if (mode !== null) {
    transformControls.setMode(mode);
  }
  updateModeButtons(mode);
  syncTransformControlsState();
}

function toggleTransformMode(mode) {
  setTransformMode(activeTransformMode === mode ? null : mode);
}

geometricErrorScaleInput.min = String(GEOMETRIC_ERROR_SCALE_MIN_EXPONENT);
geometricErrorScaleInput.max = String(GEOMETRIC_ERROR_SCALE_MAX_EXPONENT);
geometricErrorScaleInput.step = String(GEOMETRIC_ERROR_SCALE_STEP);
setGeometricErrorScaleExponent(geometricErrorScaleExponent);
setTerrainEnabled(terrainEnabled);
setTransformMode(activeTransformMode);
syncToolbarVisibility();

function applySavedMatrix(matrix) {
  composeMatrix(editableGroup, matrix);
  invalidateTilesetTransforms();
  syncTransformHandleFromTilesTransform();
  syncCoordinateInputsFromTilesTransform();
}

function getCurrentMatrix() {
  editableGroup.updateMatrix();
  editableGroup.updateMatrixWorld(true);
  return editableGroup.matrix.clone();
}

function getCurrentRootTransform(target) {
  editableGroup.updateMatrix();
  editableGroup.updateMatrixWorld(true);
  return target
    .copy(editableGroup.matrix)
    .multiply(savedRootInverseMatrix.copy(lastSavedMatrix).invert())
    .multiply(savedRootMatrix);
}

function updateTilesRendererGroupMatrices(tilesRenderer) {
  const group = tilesRenderer?.group;
  if (!group) {
    return;
  }

  group.updateMatrixWorld(true);

  if (
    group.matrixWorldInverse &&
    typeof group.matrixWorldInverse.copy === 'function'
  ) {
    group.matrixWorldInverse.copy(group.matrixWorld).invert();
  }
}

function refreshLoadedTileSceneMatrices(tilesRenderer) {
  if (
    !tilesRenderer ||
    typeof tilesRenderer.forEachLoadedModel !== 'function'
  ) {
    return;
  }

  tilesRenderer.forEachLoadedModel((loadedScene) => {
    if (typeof loadedScene.updateWorldMatrix === 'function') {
      loadedScene.updateWorldMatrix(false, true);
    } else {
      loadedScene.updateMatrixWorld(true);
    }
  });
}

function invalidateTilesetTransforms() {
  tilesTransformDirty = true;
  editableGroup.updateMatrixWorld(true);
  updateTilesRendererGroupMatrices(tiles);
  refreshLoadedTileSceneMatrices(tiles);
  transformControlsHelper?.updateMatrixWorld(true);
}

function applyEditableGroupMatrixFromRootTransform(rootTransform) {
  coordinateEditMatrix
    .copy(rootTransform)
    .multiply(savedRootInverseMatrix.copy(savedRootMatrix).invert())
    .multiply(lastSavedMatrix);
  composeMatrix(editableGroup, coordinateEditMatrix);
  invalidateTilesetTransforms();
}

function syncTransformHandleFromTilesTransform() {
  syncingTransformHandle = true;
  try {
    composeMatrix(
      transformHandle,
      getCurrentRootTransform(currentRootTransformMatrix),
    );
    transformHandle.updateMatrixWorld(true);
    transformControlsHelper?.updateMatrixWorld(true);
  } finally {
    syncingTransformHandle = false;
  }
}

function resetEditableGroup() {
  editableGroup.position.set(0, 0, 0);
  editableGroup.quaternion.identity();
  editableGroup.scale.set(1, 1, 1);
  editableGroup.updateMatrix();
  editableGroup.updateMatrixWorld(true);
  lastSavedMatrix.identity();
  transformHandle.position.set(0, 0, 0);
  transformHandle.quaternion.identity();
  transformHandle.scale.set(1, 1, 1);
  transformHandle.updateMatrix();
  transformHandle.updateMatrixWorld(true);
  syncTransformControlsState();
  tilesTransformDirty = true;
}

function getTilesetWorldBoundingSphere() {
  if (!tiles || !tiles.getBoundingSphere(sphere)) {
    return false;
  }

  editableGroup.updateMatrixWorld(true);
  sphere.center.applyMatrix4(editableGroup.matrixWorld);
  sphere.radius *= editableGroup.matrixWorld.getMaxScaleOnAxis();
  return true;
}

function getCameraDistanceForBoundingSphere(radius) {
  const verticalHalfFov = MathUtils.degToRad(camera.fov) * 0.5;
  const horizontalHalfFov = Math.atan(
    Math.tan(verticalHalfFov) * camera.aspect,
  );
  const limitingHalfFov = Math.max(
    Math.min(verticalHalfFov, horizontalHalfFov),
    1e-3,
  );

  return Math.max(radius / Math.sin(limitingHalfFov), 1);
}

function isCenterModePosition(position) {
  return position.lengthSq() <= CAMERA_CENTER_MODE_DISTANCE_SQ;
}

function getLocalFrame(referencePoint) {
  const ellipsoid = getActiveEllipsoid();
  ellipsoid.getPositionToCartographic(referencePoint, cartographicTarget);
  ellipsoid.getEastNorthUpFrame(
    cartographicTarget.lat,
    cartographicTarget.lon,
    cartographicTarget.height,
    moveToTilesBasis,
  );
  moveToTilesEast.setFromMatrixColumn(moveToTilesBasis, 0).normalize();
  moveToTilesNorth.setFromMatrixColumn(moveToTilesBasis, 1).normalize();
  moveToTilesUp.setFromMatrixColumn(moveToTilesBasis, 2).normalize();
}

function getCoordinateWorldPosition(latitude, longitude, height, target) {
  const ellipsoid = getActiveEllipsoid();
  return ellipsoid.getCartographicToPosition(
    MathUtils.degToRad(latitude),
    MathUtils.degToRad(longitude),
    height,
    target,
  );
}

function getCoordinateTransform(latitude, longitude, height, target) {
  const ellipsoid = getActiveEllipsoid();
  return ellipsoid.getEastNorthUpFrame(
    MathUtils.degToRad(latitude),
    MathUtils.degToRad(longitude),
    height,
    target,
  );
}

async function refreshSavedRootMatrix(url) {
  const response = await fetch(url, { cache: 'no-store' });
  if (!response.ok) {
    throw new Error(
      `Failed to load ${ROOT_TILESET_LABEL} metadata for coordinate placement (${response.status}).`,
    );
  }

  const payload = await response.json();
  savedRootMatrix.identity();

  const rootTransform = payload?.root?.transform;
  if (rootTransform != null) {
    savedRootMatrix.fromArray(
      getFiniteMatrix4Array(rootTransform, 'tileset.root.transform'),
    );
  }

  return savedRootMatrix;
}

function syncCoordinateInputsFromTilesTransform() {
  if (savedRootMatrixLoadError) {
    return;
  }

  const ellipsoid = getActiveEllipsoid();
  getCurrentRootTransform(currentRootTransformMatrix);
  coordinateWorldPosition.setFromMatrixPosition(currentRootTransformMatrix);
  ellipsoid.getPositionToCartographic(
    coordinateWorldPosition,
    cartographicTarget,
  );

  latitudeInput.value = formatCoordinateInputValue(
    MathUtils.radToDeg(cartographicTarget.lat),
    8,
  );
  longitudeInput.value = formatCoordinateInputValue(
    MathUtils.radToDeg(cartographicTarget.lon),
    8,
  );
  heightInput.value = formatCoordinateInputValue(cartographicTarget.height, 3);
}

function updateCoordinateInputs(latitude, longitude, height) {
  latitudeInput.value = formatCoordinateInputValue(latitude, 8);
  longitudeInput.value = formatCoordinateInputValue(longitude, 8);
  heightInput.value = formatCoordinateInputValue(height, 3);
}

function raycastPickWorldPosition(target) {
  pickTargets.length = 0;

  if (tiles?.group) {
    pickTargets.push(tiles.group);
  }

  if (globeTiles?.group) {
    pickTargets.push(globeTiles.group);
  }

  if (pickTargets.length === 0) {
    return false;
  }

  for (const root of pickTargets) {
    root.updateMatrixWorld(true);
  }

  const [hit] = pickRaycaster.intersectObjects(pickTargets, true);
  if (!hit) {
    return false;
  }

  target.copy(hit.point);
  return true;
}

function setSetPositionMode(active) {
  pendingSetPosition = active;
  setPositionButton.classList.toggle('active', active);
  cameraController.enabled = !active;
  syncTransformControlsState();
}

function cancelSetPositionMode() {
  if (!pendingSetPosition) {
    return;
  }

  setSetPositionMode(false);
}

async function applyTilesPlacementFromCoordinate(latitude, longitude, height) {
  await savedRootMatrixPromise;
  if (savedRootMatrixLoadError) {
    throw savedRootMatrixLoadError;
  }

  getCoordinateTransform(
    latitude,
    longitude,
    height,
    coordinateTransformMatrix,
  );
  coordinateEditMatrix
    .copy(coordinateTransformMatrix)
    .multiply(savedRootInverseMatrix.copy(savedRootMatrix).invert())
    .multiply(lastSavedMatrix);
  composeMatrix(editableGroup, coordinateEditMatrix);
  invalidateTilesetTransforms();
  syncTransformHandleFromTilesTransform();
  syncCoordinateInputsFromTilesTransform();
}

function pickCoordinateFromPointerEvent(event) {
  const ellipsoid = getActiveEllipsoid();
  if (!ellipsoid) {
    return null;
  }

  mouseToCoords(
    event.clientX,
    event.clientY,
    renderer.domElement,
    pointerCoords,
  );
  setRaycasterFromCamera(pickRaycaster, pointerCoords, camera);

  if (
    !raycastPickWorldPosition(coordinateWorldPosition) &&
    !ellipsoid.intersectRay(pickRaycaster.ray, coordinateWorldPosition)
  ) {
    return null;
  }

  ellipsoid.getPositionToCartographic(
    coordinateWorldPosition,
    cartographicTarget,
  );
  return {
    height: cartographicTarget.height,
    latitude: MathUtils.radToDeg(cartographicTarget.lat),
    longitude: MathUtils.radToDeg(cartographicTarget.lon),
  };
}

async function handleSetPositionPointerDown(event) {
  if (!pendingSetPosition) {
    return;
  }

  if (event.pointerType === 'mouse' && event.button !== 0) {
    return;
  }

  event.preventDefault();
  event.stopPropagation();

  const coordinate = pickCoordinateFromPointerEvent(event);
  if (!coordinate) {
    setStatus(
      'No globe, terrain, or tiles hit under cursor. Click the globe, terrain, or tiles to place the tileset root.',
      true,
    );
    return;
  }

  updateCoordinateInputs(
    coordinate.latitude,
    coordinate.longitude,
    coordinate.height,
  );

  try {
    await applyTilesPlacementFromCoordinate(
      coordinate.latitude,
      coordinate.longitude,
      coordinate.height,
    );
    setStatus(
      'Moved tileset root to the clicked position using ENU orientation. Click Save to persist.',
    );
    setSetPositionMode(false);
  } catch (err) {
    setStatus(err && err.message ? err.message : String(err), true);
  }
}

function getCenterModeHeadingPitchRollForward(heading, pitch) {
  const cosPitch = Math.cos(pitch);
  const sinPitch = Math.sin(pitch);
  const cosHeading = Math.cos(heading);
  const sinHeading = Math.sin(heading);

  moveToTilesForward.set(
    sinHeading * cosPitch,
    cosHeading * cosPitch,
    sinPitch,
  );

  return moveToTilesForward.normalize();
}

function getHeadingPitchRollForward(referencePoint, heading, pitch) {
  if (isCenterModePosition(referencePoint)) {
    return getCenterModeHeadingPitchRollForward(heading, pitch);
  }

  if (referencePoint.lengthSq() < 1e-6) {
    return moveToTilesForward.set(0, 0, -1);
  }

  getLocalFrame(referencePoint);

  const cosPitch = Math.cos(pitch);
  const sinPitch = Math.sin(pitch);
  const cosHeading = Math.cos(heading);
  const sinHeading = Math.sin(heading);

  moveToTilesForward
    .copy(moveToTilesNorth)
    .multiplyScalar(cosHeading * cosPitch)
    .addScaledVector(moveToTilesEast, sinHeading * cosPitch)
    .addScaledVector(moveToTilesUp, sinPitch)
    .normalize();

  return moveToTilesForward;
}

function getCenterModeHeadingPitchRollBasis(heading, pitch, roll) {
  getCenterModeHeadingPitchRollForward(heading, pitch);

  moveToTilesRight
    .copy(worldRight)
    .multiplyScalar(Math.cos(heading))
    .addScaledVector(centerNorth, -Math.sin(heading))
    .normalize();
  moveToTilesUp.crossVectors(moveToTilesRight, moveToTilesForward).normalize();

  if (roll !== 0) {
    moveToTilesRight.applyAxisAngle(moveToTilesForward, roll).normalize();
    moveToTilesUp.applyAxisAngle(moveToTilesForward, roll).normalize();
  }

  moveToTilesBackward.copy(moveToTilesForward).negate();
}

function getHeadingPitchRollQuaternion(referencePoint, heading, pitch, roll) {
  if (isCenterModePosition(referencePoint)) {
    getCenterModeHeadingPitchRollBasis(heading, pitch, roll);
  } else if (referencePoint.lengthSq() < 1e-6) {
    moveToTilesQuaternion.identity();
    return moveToTilesQuaternion;
  } else {
    getHeadingPitchRollForward(referencePoint, heading, pitch);
    moveToTilesRight
      .copy(moveToTilesEast)
      .multiplyScalar(Math.cos(heading))
      .addScaledVector(moveToTilesNorth, -Math.sin(heading))
      .normalize();
    moveToTilesUp
      .crossVectors(moveToTilesRight, moveToTilesForward)
      .normalize();

    if (roll !== 0) {
      moveToTilesRight.applyAxisAngle(moveToTilesForward, roll).normalize();
      moveToTilesUp.applyAxisAngle(moveToTilesForward, roll).normalize();
    }

    moveToTilesBackward.copy(moveToTilesForward).negate();
  }

  moveToTilesBasis.makeBasis(
    moveToTilesRight,
    moveToTilesUp,
    moveToTilesBackward,
  );
  return moveToTilesQuaternion.setFromRotationMatrix(moveToTilesBasis);
}

function getBoundingSphereFlyToPosition(target, range, options) {
  const { heading, pitch } = options;
  if (heading === undefined && pitch === undefined) {
    const direction =
      target.lengthSq() > 1e-6
        ? moveToTilesPosition.copy(target).normalize()
        : camera.position.lengthSq() > 1e-6
          ? moveToTilesPosition.copy(camera.position).normalize()
          : moveToTilesPosition.set(0, -1, 0);
    return direction.multiplyScalar(range).add(target);
  }

  const resolvedHeading = heading ?? 0;
  const resolvedPitch = pitch ?? -Math.PI / 2;
  const centerForward = getCenterModeHeadingPitchRollForward(
    resolvedHeading,
    resolvedPitch,
  );
  const centerPosition = moveToTilesPosition
    .copy(target)
    .addScaledVector(centerForward, -range);
  if (isCenterModePosition(centerPosition)) {
    return centerPosition;
  }

  const forward = getHeadingPitchRollForward(
    target,
    resolvedHeading,
    resolvedPitch,
  );
  return moveToTilesPosition.copy(target).addScaledVector(forward, -range);
}

function getFlyToPoseFromBoundingSphere(target, radius, options) {
  const safeRadius = Math.max(radius, 1);
  let offsetDistance = safeRadius;

  if (camera instanceof PerspectiveCamera) {
    const verticalFov = MathUtils.degToRad(camera.fov);
    const horizontalFov =
      2 * Math.atan(Math.tan(verticalFov / 2) * camera.aspect);
    const minHalfFov = Math.max(0.1, Math.min(verticalFov, horizontalFov) / 2);
    offsetDistance = safeRadius / Math.sin(minHalfFov) + safeRadius * 0.75;
  } else {
    offsetDistance = getCameraDistanceForBoundingSphere(safeRadius);
  }

  const position = getBoundingSphereFlyToPosition(
    target,
    offsetDistance,
    options,
  );
  const quaternion = getHeadingPitchRollQuaternion(
    isCenterModePosition(position) ? position : target,
    options.heading ?? 0,
    options.pitch ?? -Math.PI / 2,
    options.roll ?? 0,
  );

  return {
    position,
    quaternion,
  };
}

function frameTileset() {
  if (!getTilesetWorldBoundingSphere()) {
    return false;
  }

  const pose = getFlyToPoseFromBoundingSphere(sphere.center, sphere.radius, {
    heading: MOVE_TO_TILES_HEADING,
    pitch: MOVE_TO_TILES_PITCH,
    roll: MOVE_TO_TILES_ROLL,
  });
  camera.position.copy(pose.position);
  camera.quaternion.copy(pose.quaternion);
  camera.updateMatrixWorld(true);
  cameraController.setCamera(camera);
  return true;
}

function moveCameraToTiles() {
  cancelSetPositionMode();
  if (frameTileset()) {
    setStatus('Moved camera to the tileset.');
  } else {
    setStatus('Tileset is not ready to frame yet.', true);
  }
}

function toggleSetPositionMode() {
  if (pendingSetPosition) {
    setSetPositionMode(false);
    setStatus('Set Position cancelled.');
    return;
  }

  setSetPositionMode(true);
  setStatus('Click the globe, terrain, or tiles to place the tileset root.');
}

function moveCameraToCoordinate() {
  cancelSetPositionMode();
  const coordinate = parseCoordinateInputs();
  if (!coordinate) {
    return;
  }

  getCoordinateWorldPosition(
    coordinate.latitude,
    coordinate.longitude,
    coordinate.height,
    coordinateWorldPosition,
  );
  const pose = getFlyToPoseFromBoundingSphere(
    coordinateWorldPosition,
    MOVE_TO_COORDINATE_RADIUS,
    {
      heading: MOVE_TO_TILES_HEADING,
      pitch: MOVE_TO_TILES_PITCH,
      roll: MOVE_TO_TILES_ROLL,
    },
  );
  camera.position.copy(pose.position);
  camera.quaternion.copy(pose.quaternion);
  camera.updateMatrixWorld(true);
  cameraController.setCamera(camera);
  setStatus('Moved camera to the specified coordinate.');
}

async function moveTilesToCoordinate() {
  cancelSetPositionMode();
  const coordinate = parseCoordinateInputs();
  if (!coordinate) {
    return;
  }

  try {
    await applyTilesPlacementFromCoordinate(
      coordinate.latitude,
      coordinate.longitude,
      coordinate.height,
    );
    setStatus(
      'Moved tileset root to the specified coordinate using ENU orientation. Click Save to persist.',
    );
  } catch (err) {
    setStatus(err && err.message ? err.message : String(err), true);
  }
}

function resetToSaved() {
  cancelSetPositionMode();
  applySavedMatrix(lastSavedMatrix);
  setStatus('Reset to the last saved transform.');
}

function loadTileset(url) {
  if (tiles) {
    editableGroup.remove(tiles.group);
    tiles.dispose();
    tiles = null;
  }

  resetEditableGroup();
  lastSavedGeometricErrorScale = 1;
  savedRootMatrix.identity();
  savedRootMatrixLoadError = null;
  savedRootMatrixPromise = refreshSavedRootMatrix(url).then(
    () => {
      savedRootMatrixLoadError = null;
      syncTransformHandleFromTilesTransform();
      syncCoordinateInputsFromTilesTransform();
    },
    (err) => {
      savedRootMatrixLoadError = err;
      savedRootMatrix.identity();
      syncTransformHandleFromTilesTransform();
      syncCoordinateInputsFromTilesTransform();
    },
  );

  const next = new TilesRenderer(url);
  next.downloadQueue.maxJobs = 8;
  next.parseQueue.maxJobs = 4;
  next.registerPlugin(new TilesFadePlugin());
  next.registerPlugin(new TileCompressionPlugin());
  next.registerPlugin(new UnloadTilesPlugin());
  next.registerPlugin(new ImplicitTilingPlugin());
  next.registerPlugin(new GaussianSplatPlugin({ renderer, scene }));
  next.registerPlugin(
    new GLTFExtensionsPlugin({
      metadata: true,
      rtc: true,
      dracoLoader,
      ktxLoader: ktx2Loader,
      meshoptDecoder: MeshoptDecoder,
      autoDispose: false,
    }),
  );
  next.preprocessURL = normalizeLocalResourceUrl;
  next.setCamera(camera);
  next.setResolutionFromRenderer(camera, renderer);
  tiles = next;
  updateTilesetErrorTarget();
  next.addEventListener('load-model', ({ scene: modelScene }) => {
    forceOpaqueScene(modelScene);
    tilesTransformDirty = true;
  });
  next.addEventListener('tile-visibility-change', () => {
    tilesTransformDirty = true;
  });

  const lruCache = next.lruCache;
  lruCache.minSize = 256;
  lruCache.maxSize = 4096;
  lruCache.minBytesSize = 0.2 * 2 ** 30;
  lruCache.maxBytesSize = 2 * 2 ** 30;
  lruCache.unloadPercent = 0.1;

  editableGroup.add(next.group);

  let framed = false;
  const tryFrame = () => {
    if (framed) {
      return;
    }
    if (frameTileset()) {
      framed = true;
      setStatus('Tileset ready.');
    }
  };

  next.addEventListener('load-tile-set', tryFrame);
  next.addEventListener('load-tileset', tryFrame);
}

async function saveTransform() {
  cancelSetPositionMode();
  saveButton.disabled = true;
  setStatus('Saving transform...');

  const currentMatrix = getCurrentMatrix();
  const incrementalMatrix = currentMatrix
    .clone()
    .multiply(lastSavedMatrix.clone().invert());
  const incrementalGeometricErrorScale =
    geometricErrorScale / lastSavedGeometricErrorScale;

  try {
    const response = await fetch(SAVE_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        geometricErrorScale: incrementalGeometricErrorScale,
        transform: incrementalMatrix.toArray(),
      }),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(payload.error || 'Save failed.');
    }
    if (payload && payload.transform != null) {
      savedRootMatrix.fromArray(
        getFiniteMatrix4Array(payload.transform, 'transform'),
      );
      savedRootMatrixLoadError = null;
      savedRootMatrixPromise = Promise.resolve(savedRootMatrix);
    } else {
      savedRootMatrixPromise = refreshSavedRootMatrix(TILESET_URL).then(
        () => {
          savedRootMatrixLoadError = null;
        },
        (err) => {
          savedRootMatrixLoadError = err;
          savedRootMatrix.identity();
        },
      );
      await savedRootMatrixPromise;
      if (savedRootMatrixLoadError) {
        throw savedRootMatrixLoadError;
      }
    }
    lastSavedGeometricErrorScale = geometricErrorScale;
    lastSavedMatrix.copy(currentMatrix);
    syncTransformHandleFromTilesTransform();
    syncCoordinateInputsFromTilesTransform();
    setStatus(
      `Saved transform and geometric-error scale x${formatGeometricErrorScale(
        geometricErrorScale,
      )} to ${ROOT_TILESET_LABEL} and build_summary.json.`,
    );
  } catch (err) {
    setStatus(err && err.message ? err.message : String(err), true);
  } finally {
    saveButton.disabled = false;
  }
}

translateButton.addEventListener('click', () => {
  cancelSetPositionMode();
  toggleTransformMode('translate');
  setStatus(
    activeTransformMode === 'translate'
      ? 'Translate mode enabled.'
      : 'Translate mode disabled.',
  );
});
rotateButton.addEventListener('click', () => {
  cancelSetPositionMode();
  toggleTransformMode('rotate');
  setStatus(
    activeTransformMode === 'rotate'
      ? 'Rotate mode enabled.'
      : 'Rotate mode disabled.',
  );
});
toolbarToggleButton.addEventListener('click', toggleToolbarVisibility);
terrainButton.addEventListener('click', () => {
  setTerrainEnabled(!terrainEnabled);
  setStatus(
    terrainEnabled
      ? 'Terrain enabled with Cesium World Terrain.'
      : 'Terrain disabled. Using ellipsoid imagery globe.',
  );
});
geometricErrorScaleInput.addEventListener('input', () => {
  setGeometricErrorScaleExponent(geometricErrorScaleInput.value);
});
geometricErrorScaleInput.addEventListener('change', () => {
  setStatus(
    `Geometric-error scale set to x${formatGeometricErrorScale(
      geometricErrorScale,
    )}.`,
  );
});
moveToTilesButton.addEventListener('click', moveCameraToTiles);
moveCameraToCoordinateButton.addEventListener('click', moveCameraToCoordinate);
moveTilesToCoordinateButton.addEventListener('click', moveTilesToCoordinate);
setPositionButton.addEventListener('click', toggleSetPositionMode);
resetButton.addEventListener('click', resetToSaved);
saveButton.addEventListener('click', saveTransform);
renderer.domElement.addEventListener(
  'pointerdown',
  handleSetPositionPointerDown,
);

window.addEventListener('resize', () => {
  renderer.setSize(window.innerWidth, window.innerHeight);
  camera.aspect = window.innerWidth / window.innerHeight;
  camera.updateProjectionMatrix();
  tiles?.setResolutionFromRenderer(camera, renderer);
  globeTiles?.setResolutionFromRenderer(camera, renderer);
});

window.addEventListener('pagehide', requestViewerShutdown);
window.addEventListener('beforeunload', () => {
  requestViewerShutdown();
  cameraController.dispose();
  dracoLoader.dispose();
  ktx2Loader.dispose();
});

loadTileset(TILESET_URL);

function frame() {
  cameraController.update();
  if (tilesTransformDirty) {
    editableGroup.updateMatrixWorld(true);
    updateTilesRendererGroupMatrices(tiles);
    refreshLoadedTileSceneMatrices(tiles);
    tilesTransformDirty = false;
  }
  globeTiles?.update();
  tiles?.update();
  renderer.render(scene, camera);
  requestAnimationFrame(frame);
}

frame();
