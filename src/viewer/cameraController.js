import {
  EventDispatcher,
  Matrix4,
  Plane,
  Quaternion,
  Ray,
  Raycaster,
  Vector2,
  Vector3,
} from 'three';

const createUninitializedCallback = (name) => () => {
  console.warn(`${name} was called before initialization.`);
};
const CAMERA_CENTER_MODE_DISTANCE = 3000000;
const CAMERA_CENTER_MODE_DISTANCE_SQ = CAMERA_CENTER_MODE_DISTANCE ** 2;

class PointerTracker {
  buttons;
  pointerType;
  pointerOrder;
  previousPositions;
  pointerPositions;
  startPositions;
  hoverPosition;
  hoverSet;
  constructor() {
    this.buttons = 0;
    this.pointerType = null;
    this.pointerOrder = [];
    this.previousPositions = {};
    this.pointerPositions = {};
    this.startPositions = {};
    this.hoverPosition = new Vector2();
    this.hoverSet = false;
  }
  reset() {
    this.buttons = 0;
    this.pointerType = null;
    this.pointerOrder = [];
    this.previousPositions = {};
    this.pointerPositions = {};
    this.startPositions = {};
    this.hoverPosition = new Vector2();
    this.hoverSet = false;
  }
  // The pointers can be set multiple times per frame so track whether the pointer has
  // been set this frame or not so we don't overwrite the previous position and lose information
  // about pointer movement
  updateFrame() {
    const { previousPositions, pointerPositions } = this;
    for (const id in pointerPositions) {
      previousPositions[id].copy(pointerPositions[id]);
    }
  }
  setHoverEvent(e) {
    // @ts-expect-error
    if (e.pointerType === 'mouse' || e.type === 'wheel') {
      this.getAdjustedPointer(e, this.hoverPosition);
      this.hoverSet = true;
    }
  }
  getLatestPoint(target) {
    if (this.pointerType !== null) {
      this.getCenterPoint(target);
      return target;
    } else if (this.hoverSet) {
      target.copy(this.hoverPosition);
      return target;
    } else {
      return null;
    }
  }
  // get the pointer position in the coordinate system of the target element
  getAdjustedPointer(e, target) {
    const domRef = e.target;
    const rect = domRef.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;
    target.set(x, y);
  }
  addPointer(e) {
    // @ts-expect-error
    const id = e.pointerId;
    const position = new Vector2();
    this.getAdjustedPointer(e, position);
    if (this.pointerOrder.indexOf(id) === -1) {
      this.pointerOrder.push(id);
    }
    this.pointerPositions[id] = position;
    this.previousPositions[id] = position.clone();
    this.startPositions[id] = position.clone();
    if (this.getPointerCount() === 1) {
      // @ts-expect-error
      this.pointerType = e.pointerType;
      this.buttons = e.buttons;
    }
  }
  updatePointer(e) {
    // @ts-expect-error
    const id = e.pointerId;
    if (!(id in this.pointerPositions)) {
      return false;
    }
    this.getAdjustedPointer(e, this.pointerPositions[id]);
    return true;
  }
  deletePointer(e) {
    const id = e.pointerId;
    const pointerOrder = this.pointerOrder;
    pointerOrder.splice(pointerOrder.indexOf(id), 1);
    delete this.pointerPositions[id];
    delete this.previousPositions[id];
    delete this.startPositions[id];
    if (this.getPointerCount() === 0) {
      this.buttons = 0;
      this.pointerType = null;
    }
  }
  getPointerCount() {
    return this.pointerOrder.length;
  }
  getCenterPoint(target, pointerPositions = this.pointerPositions) {
    const pointerOrder = this.pointerOrder;
    if (this.getPointerCount() === 1 || this.getPointerType() === 'mouse') {
      const id = pointerOrder[0];
      target.copy(pointerPositions[id]);
      return target;
    } else if (this.getPointerCount() === 2) {
      const id0 = this.pointerOrder[0];
      const id1 = this.pointerOrder[1];
      const p0 = pointerPositions[id0];
      const p1 = pointerPositions[id1];
      target.addVectors(p0, p1).multiplyScalar(0.5);
      return target;
    } else if (this.getPointerCount() > 2) {
      target.set(0, 0);
      for (let i = 0; i < pointerOrder.length; i++) {
        const id = pointerOrder[i];
        target.add(pointerPositions[id]);
      }
      target.divideScalar(pointerOrder.length);
      return target;
    }
    return null;
  }
  getPreviousCenterPoint(target) {
    return this.getCenterPoint(target, this.previousPositions);
  }
  getStartCenterPoint(target) {
    return this.getCenterPoint(target, this.startPositions);
  }
  getMoveDistance() {
    this.getCenterPoint(_vec);
    this.getPreviousCenterPoint(_vec2);
    return _vec.sub(_vec2).length();
  }
  getTouchPointerDistance(pointerPositions = this.pointerPositions) {
    if (this.getPointerCount() <= 1 || this.getPointerType() === 'mouse') {
      return 0;
    }
    const { pointerOrder } = this;
    const id0 = pointerOrder[0];
    const id1 = pointerOrder[1];
    const p0 = pointerPositions[id0];
    const p1 = pointerPositions[id1];
    return p0.distanceTo(p1);
  }
  getPreviousTouchPointerDistance() {
    return this.getTouchPointerDistance(this.previousPositions);
  }
  getStartTouchPointerDistance() {
    return this.getTouchPointerDistance(this.startPositions);
  }
  getPointerType() {
    return this.pointerType;
  }
  isPointerTouch() {
    return this.getPointerType() === 'touch';
  }
  getPointerButtons() {
    return this.buttons;
  }
  isLeftClicked() {
    return Boolean(this.buttons & 1);
  }
  isRightClicked() {
    return Boolean(this.buttons & 2);
  }
  isMiddleClicked() {
    return Boolean(this.buttons & 4);
  }
}
const _matrix = new Matrix4();
// custom version of set raycaster from camera that relies on the underlying matrices
// so the ray origin is position at the camera near clip.
function setRaycasterFromCamera(raycaster, coords, camera) {
  const ray = raycaster instanceof Ray ? raycaster : raycaster.ray;
  const { origin, direction } = ray;
  // With reversed depth the NDC z range is [1, 0] (near→1, far→0)
  // instead of the standard [-1, 1] (near→-1, far→1).
  const nearZ = camera.reversedDepth ? 1 : -1;
  const farZ = camera.reversedDepth ? 0 : 1;
  // get the origin and direction of the frustum ray
  origin.set(coords.x, coords.y, nearZ).unproject(camera);
  direction.set(coords.x, coords.y, farZ).unproject(camera).sub(origin);
  // @ts-expect-error
  if (!raycaster.isRay) {
    // compute the far value based on the distance from point on the near
    // plane and point on the far plane. Then normalize the direction.
    // @ts-expect-error
    raycaster.near = 0;
    // @ts-expect-error
    raycaster.far = direction.length();
    // @ts-expect-error
    raycaster.camera = camera;
  }
  // normalize the ray direction
  direction.normalize();
}
function mouseToCoords(clientX, clientY, element, target) {
  const rect = element.getBoundingClientRect();
  target.x = ((clientX - rect.left) / rect.width) * 2 - 1;
  target.y = -((clientY - rect.top) / rect.height) * 2 + 1;
}
function makeRotateAroundPoint(point, quat, target) {
  target.makeTranslation(-point.x, -point.y, -point.z);
  _matrix.makeRotationFromQuaternion(quat);
  target.premultiply(_matrix);
  _matrix.makeTranslation(point.x, point.y, point.z);
  target.premultiply(_matrix);
  return target;
}
const NONE = 0;
const DRAG = 1;
const ROTATE = 2;
const IDLE = 3;
const START_EVENT = { type: 'start' };
const UPDATE_EVENT = { type: 'update' };
const FINISH_EVENT = { type: 'finish' };
const THRESHOLD = 1e-3;
const MAX = 1e8;
const VIRTUAL_HIT_DISTANCE = 50;
const ZOOM_OUT_TRANSITION_COS_THRESHOLD = Math.cos((105 * Math.PI) / 180);
const ZOOM_OUT_TRANSITION_COS_MAX_THRESHOLD = Math.cos((95 * Math.PI) / 180);
const _pointer = new Vector2();
const _pointer1 = new Vector2();
const _pointer2 = new Vector2();
const _pivotPoint = new Vector3();
const _up = new Vector3(0, 1, 0);
const _right = new Vector3(1, 0, 0);
const _forward = new Vector3(0, 0, -1);
const _worldZ = new Vector3(0, 0, 1);
const _vec = new Vector3(1, 1, 1);
const _vec1 = new Vector3();
const _vec2 = new Vector3();
const _vec3 = new Vector3();
const _vec4 = new Vector3();
const _vec5 = new Vector3();
const _vec6 = new Vector3();
const _axis = new Vector3();
const _localUp = new Vector3();
const _localRight = new Vector3();
const _localForward = new Vector3();
const _rotMatrix = new Matrix4();
const _invMatrix = new Matrix4();
const _quaternion = new Quaternion();
const _plane = new Plane();
const _ray = new Ray();
const _zoomOutMetrics = { distanceScale: 1, transitionWeight: 0 };
class CameraController extends EventDispatcher {
  enableDamping;
  dampingFactor;
  state;
  zooming;
  touchZooming;
  minDistance;
  minZoomLimit;
  #pointerTracker;
  #domElement;
  #camera;
  #scene;
  #raycaster;
  #zoomDelta;
  #zoomInertia;
  #rotateInertia;
  #dragInertia;
  #dragAnchorPoint;
  #dragStartPosition;
  #dragStartQuaternion;
  #dragPlaneNormal;
  #inertiaValue;
  #enabled;
  #ellipsoid;
  #ellipsoidMaxRadius;
  #lastTime;
  #hit;
  #contextMenuEvent = createUninitializedCallback(
    'CameraController.#contextMenuEvent',
  );
  #pointerDownEvent = createUninitializedCallback(
    'CameraController.#pointerDownEvent',
  );
  #pointerMoveEvent = createUninitializedCallback(
    'CameraController.#pointerMoveEvent',
  );
  #pointerUpEvent = createUninitializedCallback(
    'CameraController.#pointerUpEvent',
  );
  #wheelEvent = createUninitializedCallback('CameraController.#wheelEvent');
  #pointerEnterEvent = createUninitializedCallback(
    'CameraController.#pointerEnterEvent',
  );
  #zoomTimeout;
  constructor(renderer, scene, camera, options = {}) {
    super();
    if (typeof options !== 'object' || options === null) {
      options = {};
    }
    this.#scene = scene;
    this.#camera = camera;
    this.#domElement = options.domElement ?? renderer.domElement;
    this.enableDamping = options.enableDamping ?? true;
    this.dampingFactor = 0.8;
    this.minDistance = 0.5;
    this.minZoomLimit = false;
    this.state = NONE;
    this.zooming = false;
    this.touchZooming = false;
    this.#pointerTracker = new PointerTracker();
    this.#raycaster = new Raycaster();
    this.#raycaster.params.Points.threshold = 0.1;
    this.#zoomDelta = 0;
    this.#zoomInertia = 0;
    this.#rotateInertia = new Vector2();
    this.#dragInertia = new Vector3();
    this.#dragAnchorPoint = new Vector3();
    this.#dragStartPosition = new Vector3();
    this.#dragStartQuaternion = new Quaternion();
    this.#dragPlaneNormal = new Vector3();
    this.#inertiaValue = 0;
    this.#enabled = false;
    this.#ellipsoid = null;
    this.#ellipsoidMaxRadius = 0;
    this.#lastTime = 0;
    this.#hit = null;
    this.#zoomTimeout = null;
    this.init();
  }
  get enabled() {
    return this.#enabled;
  }
  set enabled(v) {
    if (v !== this.enabled) {
      this.#enabled = v;
      this.#resetState();
      this.#pointerTracker.reset();
      if (!this.enabled) {
        this.#dragInertia.set(0, 0, 0);
        this.#rotateInertia.set(0, 0);
        this.#zoomInertia = 0;
        this.#inertiaValue = 0;
      }
      this.dispatchEvent(UPDATE_EVENT);
      this.dispatchEvent(FINISH_EVENT);
    }
  }
  get camera() {
    return this.#camera;
  }
  #setState(state = this.state) {
    if (this.state === state) {
      return;
    }
    this.state = state;
  }
  #setZooming(zooming, touchZooming = false) {
    this.zooming = zooming;
    this.touchZooming = touchZooming;
  }
  #resetState() {
    this.state = NONE;
    this.zooming = false;
    this.touchZooming = false;
    this.#inertiaValue = 0;
    this.#rotateInertia.set(0, 0);
    this.#dragInertia.set(0, 0, 0);
    this.#dragAnchorPoint.set(0, 0, 0);
    this.#dragStartPosition.set(0, 0, 0);
    this.#dragStartQuaternion.identity();
    this.#dragPlaneNormal.set(0, 0, 0);
    this.#zoomInertia = 0;
    this.#hit = null;
  }
  setCamera(camera) {
    this.#camera = camera;
    this.#resetState();
    this.dispatchEvent(UPDATE_EVENT);
    this.dispatchEvent(FINISH_EVENT);
  }
  setEllipsoid(ellipsoid) {
    this.#ellipsoid = ellipsoid;
    const r = ellipsoid.radius;
    this.#ellipsoidMaxRadius = Math.max(r.x, r.y, r.z);
  }
  init() {
    this.#domElement.style.touchAction = 'none';
    this.#contextMenuEvent = this.#contextMenu.bind(this);
    this.#pointerDownEvent = this.#pointerDown.bind(this);
    this.#pointerMoveEvent = this.#pointerMove.bind(this);
    this.#pointerUpEvent = this.#pointerUp.bind(this);
    this.#wheelEvent = this.#wheel.bind(this);
    this.#pointerEnterEvent = this.#pointerEnter.bind(this);
    this.#bindEvents();
    this.#enabled = true;
  }
  update(time = performance.now()) {
    const deltaTime = time - this.#lastTime;
    if (!this.#enabled || !this.#camera || deltaTime === 0) {
      return;
    }
    this.#lastTime = time;
    if (this.state === NONE && !this.zooming) {
      return;
    }
    const factor =
      (deltaTime * (1 - this.dampingFactor)) /
      (50 +
        50 * (1 - this.dampingFactor) +
        Math.max(0.001, (1 - this.#inertiaValue) ** 3) * 50);
    this.#inertiaValue -= factor;
    this.#inertiaValue = Math.max(this.#inertiaValue, 0);
    if (this.state === ROTATE) {
      this.#pointerTracker.getCenterPoint(_pointer1);
      this.#pointerTracker.getPreviousCenterPoint(_pointer2);
      if (!_pointer1.equals(_pointer2)) {
        _pointer
          .subVectors(_pointer2, _pointer1)
          .multiplyScalar((2 * Math.PI) / this.#domElement.clientHeight);
        this.#rotate(_pointer);
        this.#rotateInertia.copy(_pointer);
        this.#inertiaValue = 1;
        this.#zoomInertia = 0;
        this.#dragInertia.set(0, 0, 0);
        this.#finalizeCamera();
        this.dispatchEvent(UPDATE_EVENT);
      }
    } else if (this.state === DRAG) {
      this.#pointerTracker.getCenterPoint(_pointer1);
      this.#pointerTracker.getPreviousCenterPoint(_pointer2);
      if (!_pointer1.equals(_pointer2) && this.#hit && this.#hit.distance > 0) {
        mouseToCoords(_pointer1.x, _pointer1.y, this.#domElement, _pointer1);
        mouseToCoords(_pointer2.x, _pointer2.y, this.#domElement, _pointer2);
        this.#restoreDragStartCamera();
        if (
          this.#intersectDragPlane(_pointer1, _vec1) &&
          this.#intersectDragPlane(_pointer2, _vec2)
        ) {
          _vec.subVectors(_vec1, this.#dragAnchorPoint);
          _vec5.subVectors(_vec1, _vec2);
          if (this.#shouldDragModified()) {
            this.#modifiedDrag(_vec);
          } else {
            this.#camera.position.sub(_vec);
          }
          this.#dragInertia.copy(_vec5);
          this.#inertiaValue = 1;
          this.#rotateInertia.set(0, 0);
          this.#zoomInertia = 0;
          this.#finalizeCamera();
          this.dispatchEvent(UPDATE_EVENT);
        }
      }
    } else if (this.state === IDLE) {
      if (this.enableDamping) {
        if (this.#rotateInertia.lengthSq() > 0 && this.#inertiaValue > 0) {
          _pointer.copy(this.#rotateInertia).multiplyScalar(this.#inertiaValue);
          this.#rotate(_pointer);
          this.#finalizeCamera();
        } else if (this.#dragInertia.lengthSq() > 0 && this.#inertiaValue > 0) {
          if (this.#shouldDragModified()) {
            _vec.copy(this.#dragInertia).multiplyScalar(this.#inertiaValue);
            this.#modifiedDrag(_vec);
            this.#finalizeCamera();
          } else {
            _vec.copy(this.#dragInertia).multiplyScalar(this.#inertiaValue);
            this.#camera.position.sub(_vec);
            this.#finalizeCamera();
          }
        }
        if (
          (this.#rotateInertia.lengthSq() === 0 &&
            this.#dragInertia.lengthSq() === 0) ||
          this.#inertiaValue === 0
        ) {
          this.#rotateInertia.set(0, 0);
          this.#dragInertia.set(0, 0, 0);
          if (!this.zooming) {
            this.#resetState();
            this.dispatchEvent(UPDATE_EVENT);
            this.dispatchEvent(FINISH_EVENT);
          } else {
            this.#setState(NONE);
            this.dispatchEvent(UPDATE_EVENT);
          }
        } else {
          this.dispatchEvent(UPDATE_EVENT);
        }
      } else {
        this.#rotateInertia.set(0, 0);
        this.#dragInertia.set(0, 0, 0);
        this.#zoomInertia = 0;
        this.#resetState();
        this.dispatchEvent(UPDATE_EVENT);
        this.dispatchEvent(FINISH_EVENT);
      }
    }
    if (this.zooming) {
      if (this.touchZooming) {
        const previousDistance =
          this.#pointerTracker.getPreviousTouchPointerDistance();
        const currentDistance = this.#pointerTracker.getTouchPointerDistance();
        const delta =
          (currentDistance - previousDistance) /
          Math.sqrt(
            this.#domElement.clientWidth ** 2 +
              this.#domElement.clientHeight ** 2,
          );
        this.#zoomDelta = delta * 4000;
      }
      if (this.#zoomDelta !== 0) {
        if (this.#zoomTimeout !== null) {
          clearTimeout(this.#zoomTimeout);
          this.#zoomTimeout = null;
        }
        if (this.#zoomDelta <= 0 && this.#reachCameraMaxDistance()) {
          this.#zoomDelta = 0;
        } else {
          this.#applyZoom(this.#zoomDelta);
        }
        this.#zoomInertia = this.#zoomDelta;
        this.#inertiaValue = 1;
        this.#zoomDelta = 0;
        this.dispatchEvent(UPDATE_EVENT);
        if (!this.enableDamping) {
          this.#zoomTimeout = setTimeout(() => {
            this.#zoomInertia = 0;
            this.#zoomTimeout = null;
            if (this.state === NONE || this.state === IDLE) {
              this.#resetState();
              this.dispatchEvent(UPDATE_EVENT);
              this.dispatchEvent(FINISH_EVENT);
            }
          }, 500);
        }
      } else if (this.enableDamping && this.#inertiaValue > 0) {
        if (this.#zoomInertia <= 0 && this.#reachCameraMaxDistance()) {
          this.#zoomInertia = 0;
          this.dispatchEvent(UPDATE_EVENT);
        } else {
          if (
            this.#zoomInertia !== 0 &&
            this.#inertiaValue > 0 &&
            this.#hit &&
            this.#hit.distance > 0
          ) {
            this.#applyZoom(this.#zoomInertia * this.#inertiaValue);
            this.dispatchEvent(UPDATE_EVENT);
          } else {
            this.#zoomInertia = 0;
            if (this.state === NONE) {
              this.#resetState();
              this.dispatchEvent(UPDATE_EVENT);
              this.dispatchEvent(FINISH_EVENT);
            }
          }
        }
      } else if (this.enableDamping && this.state === NONE) {
        this.#resetState();
        this.dispatchEvent(UPDATE_EVENT);
        this.dispatchEvent(FINISH_EVENT);
      } else {
        this.#zoomDelta = 0;
        this.#zoomInertia = 0;
      }
    }
    this.#pointerTracker.updateFrame();
  }
  dispose() {
    if (this.#zoomTimeout !== null) {
      clearTimeout(this.#zoomTimeout);
      this.#zoomTimeout = null;
    }
    this.#domElement.removeEventListener('contextmenu', this.#contextMenuEvent);
    this.#domElement.removeEventListener('pointerdown', this.#pointerDownEvent);
    this.#domElement.removeEventListener('pointermove', this.#pointerMoveEvent);
    this.#domElement.removeEventListener('pointerup', this.#pointerUpEvent);
    this.#domElement.removeEventListener('wheel', this.#wheelEvent);
    this.#domElement.removeEventListener(
      'pointerenter',
      this.#pointerEnterEvent,
    );
    this.#domElement.style.touchAction = '';
    this.#enabled = false;
    this.#ellipsoid = null;
  }
  #bindEvents() {
    this.#domElement.addEventListener('contextmenu', this.#contextMenuEvent);
    this.#domElement.addEventListener('pointerdown', this.#pointerDownEvent);
    this.#domElement.addEventListener('pointermove', this.#pointerMoveEvent);
    this.#domElement.addEventListener('pointerup', this.#pointerUpEvent);
    this.#domElement.addEventListener('wheel', this.#wheelEvent);
    this.#domElement.addEventListener('pointerenter', this.#pointerEnterEvent);
  }
  #contextMenu = (e) => {
    e.preventDefault();
  };
  #pointerDown = (e) => {
    if (!this.#enabled) {
      return;
    }
    this.#pointerTracker.addPointer(e);
    if (
      (this.#pointerTracker.getPointerCount() === 2 &&
        this.#pointerTracker.isPointerTouch()) ||
      (!this.#pointerTracker.isPointerTouch() &&
        this.#pointerTracker.isRightClicked()) ||
      (this.#pointerTracker.isLeftClicked() && e.shiftKey)
    ) {
      this.#setState(DRAG);
      this.#setZooming(false);
    } else if (
      (this.#pointerTracker.getPointerCount() === 1 &&
        this.#pointerTracker.isPointerTouch()) ||
      (!this.#pointerTracker.isPointerTouch() &&
        this.#pointerTracker.isLeftClicked() &&
        !e.shiftKey)
    ) {
      this.#setState(ROTATE);
      this.#setZooming(false);
    }
    if (
      this.#pointerTracker.getPointerCount() === 2 &&
      this.#pointerTracker.isPointerTouch()
    ) {
      this.#setZooming(true, true);
    }
    if (this.state === NONE) {
      this.#setState(IDLE);
    }
    if (this.state === ROTATE || this.state === DRAG || this.zooming) {
      this.#pointerTracker.getCenterPoint(_pointer1);
      mouseToCoords(_pointer1.x, _pointer1.y, this.#domElement, _pointer1);
      setRaycasterFromCamera(this.#raycaster, _pointer1, this.#camera);
      this.#hit = this.#raycast(this.#raycaster);
      if (this.state === DRAG && this.#hit.distance > 0) {
        this.#initializeDragAnchor();
      }
      this.dispatchEvent(START_EVENT);
    }
    this.#rotateInertia.set(0, 0);
    this.#dragInertia.set(0, 0, 0);
    this.#zoomInertia = 0;
    this.#zoomDelta = 0;
    this.dispatchEvent(UPDATE_EVENT);
  };
  #pointerMove = (e) => {
    e.preventDefault();
    if (!this.#enabled) {
      return;
    }
    this.#pointerTracker.setHoverEvent(e);
    if (!this.#pointerTracker.updatePointer(e)) {
      return;
    }
  };
  #pointerUp = (e) => {
    this.#pointerTracker.deletePointer(e);
    if (!this.#enabled) {
      return;
    }
    if (this.zooming || this.state !== NONE) {
      this.#setState(IDLE);
    }
    this.dispatchEvent(UPDATE_EVENT);
  };
  #wheel = (e) => {
    e.preventDefault();
    if (!this.#enabled) {
      return;
    }
    const tooClose = this.#hit
      ? this.#hit.point.distanceTo(this.#camera.position) <= this.#camera.near
      : false;
    if (!this.zooming || tooClose) {
      this.#rotateInertia.set(0, 0);
      this.#dragInertia.set(0, 0, 0);
      this.#zoomInertia = 0;
      this.#zoomDelta = 0;
    }
    this.#pointerTracker.setHoverEvent(e);
    this.#pointerTracker.updatePointer(e);
    this.#pointerTracker.getLatestPoint(_pointer1);
    mouseToCoords(_pointer1.x, _pointer1.y, this.#domElement, _pointer1);
    setRaycasterFromCamera(this.#raycaster, _pointer1, this.#camera);
    if ((!this.zooming && this.state === NONE) || tooClose) {
      this.#hit = this.#raycast(this.#raycaster);
    }
    let delta = 0;
    switch (e.deltaMode) {
      case 2: // Pages
        delta = e.deltaY * 800;
        break;
      case 1: // Lines
        delta = e.deltaY * 40;
        break;
      case 0: // Pixels
        delta = e.deltaY;
        break;
    }
    // use LOG to scale the scroll delta and hopefully normalize them across platforms
    const deltaSign = Math.sign(delta);
    const normalizedDelta = Math.max(40, Math.abs(delta));
    this.#zoomDelta =
      -0.8 *
      deltaSign *
      normalizedDelta *
      (this.enableDamping ? 1 - this.dampingFactor : 1);
    this.#setZooming(true);
    this.dispatchEvent(START_EVENT);
    this.dispatchEvent(UPDATE_EVENT);
  };
  #pointerEnter = (e) => {
    if (!this.#enabled) {
      return;
    }
    if (e.buttons !== this.#pointerTracker.getPointerButtons()) {
      this.#pointerTracker.deletePointer(e);
      this.#resetState();
      this.dispatchEvent(UPDATE_EVENT);
      this.dispatchEvent(FINISH_EVENT);
    }
  };
  #finalizeCamera() {
    this.#limitCameraDistance();
    this.#keepCameraUp();
    this.#camera.updateMatrixWorld();
  }
  #alignCameraRightToXYPlane() {
    this.#camera.getWorldDirection(_forward);
    _up.copy(this.#camera.up).transformDirection(this.#camera.matrixWorld);
    _vec1.crossVectors(_forward, _up).normalize();
    _right.copy(_vec1).projectOnPlane(_worldZ);
    if (_right.lengthSq() <= THRESHOLD * THRESHOLD) {
      _right.crossVectors(_forward, _worldZ);
    }
    if (_right.lengthSq() <= THRESHOLD * THRESHOLD) {
      return;
    }
    _right.normalize();
    if (_right.dot(_vec1) < 0) {
      _right.negate();
    }
    _localUp.crossVectors(_right, _forward).normalize();
    _vec2.copy(_forward).negate();
    _rotMatrix.makeBasis(_right, _localUp, _vec2);
    this.#camera.quaternion.setFromRotationMatrix(_rotMatrix);
  }
  #rotateNearAnchor(rotateVec) {
    if (!this.#hit) {
      return;
    }
    this.#camera.getWorldDirection(_forward);
    const cameraVerticalAngle = Math.PI - _forward.angleTo(_worldZ);
    const maxVerticalAngle = Math.PI - THRESHOLD;
    const minVerticalAngle = THRESHOLD;
    const verticalAngle = Math.min(
      Math.max(rotateVec.y, minVerticalAngle - cameraVerticalAngle),
      maxVerticalAngle - cameraVerticalAngle,
    );
    const horizontalAngle = rotateVec.x;
    _quaternion.setFromAxisAngle(_worldZ, horizontalAngle);
    makeRotateAroundPoint(this.#hit.point, _quaternion, _rotMatrix);
    this.#camera.matrixWorld.premultiply(_rotMatrix);
    this.#camera.matrixWorld.decompose(
      this.#camera.position,
      this.#camera.quaternion,
      _vec6,
    );
    this.#camera.getWorldDirection(_forward);
    _up.copy(this.#camera.up).transformDirection(this.#camera.matrixWorld);
    _vec1.crossVectors(_forward, _up).normalize();
    _right.copy(_vec1).projectOnPlane(_worldZ);
    if (_right.lengthSq() <= THRESHOLD * THRESHOLD) {
      _right.crossVectors(_forward, _worldZ);
    }
    if (_right.lengthSq() <= THRESHOLD * THRESHOLD) {
      return;
    }
    _right.normalize();
    if (_right.dot(_vec1) < 0) {
      _right.negate();
    }
    _quaternion.setFromAxisAngle(_right, verticalAngle);
    makeRotateAroundPoint(this.#hit.point, _quaternion, _rotMatrix);
    this.#camera.matrixWorld.premultiply(_rotMatrix);
    this.#camera.matrixWorld.decompose(
      this.#camera.position,
      this.#camera.quaternion,
      _vec6,
    );
  }
  #clampVerticalRotateAngle(axis, pivotPoint, verticalAngle) {
    if (verticalAngle <= 0) {
      return verticalAngle;
    }
    _up.copy(this.#camera.up).transformDirection(this.#camera.matrixWorld);
    const axisDotUp = axis.dot(_up);
    const axisDotPivot = axis.dot(pivotPoint);
    const axisProjection = axisDotPivot * axisDotUp;
    const a = pivotPoint.dot(_up) - axisProjection;
    const b = pivotPoint.dot(_vec1.crossVectors(axis, _up));
    const d = this.#camera.position.dot(_up) - a;
    const amplitude = Math.hypot(a, b);
    if (amplitude <= THRESHOLD) {
      return verticalAngle;
    }
    const cosValue = -d / amplitude;
    if (cosValue < -1 - THRESHOLD || cosValue > 1 + THRESHOLD) {
      return verticalAngle;
    }
    const clampedCosValue = Math.min(1, Math.max(-1, cosValue));
    const phase = Math.atan2(b, a);
    const delta = Math.acos(clampedCosValue);
    let result = verticalAngle;
    for (const candidate of [phase - delta, phase + delta]) {
      for (const offset of [-2 * Math.PI, 0, 2 * Math.PI]) {
        const angle = candidate + offset;
        if (angle > THRESHOLD && angle < result && angle <= verticalAngle) {
          result = angle;
        }
      }
    }
    return result;
  }
  #rotate(rotateVec) {
    if (!this.#hit) {
      return;
    }
    if (this.#isCameraCenterMode()) {
      this.#rotateNearAnchor(rotateVec);
      return;
    }
    this.#camera.getWorldDirection(_forward);
    _up.copy(this.#camera.up).transformDirection(this.#camera.matrixWorld);
    _right.crossVectors(_forward, _up).normalize();
    _localUp.copy(this.#hit.point).normalize();
    _vec6.copy(this.#camera.position).normalize();
    const cameraVerticalAngle = Math.PI - _forward.angleTo(_vec6);
    const maxVerticalAngle = Math.PI - THRESHOLD;
    const minVerticalAngle = THRESHOLD;
    let verticalAngle = Math.min(
      Math.max(rotateVec.y, minVerticalAngle - cameraVerticalAngle),
      maxVerticalAngle - cameraVerticalAngle,
    );
    _ray.set(
      _pivotPoint
        .copy(this.#hit.point)
        .sub(_vec6.copy(_right).multiplyScalar(MAX)),
      _right,
    );
    _plane.setFromNormalAndCoplanarPoint(_right, this.#camera.position);
    _ray.intersectPlane(_plane, _pivotPoint);
    verticalAngle = this.#clampVerticalRotateAngle(
      _right,
      _pivotPoint,
      verticalAngle,
    );
    // Rotate around the right axis
    _quaternion.setFromAxisAngle(_right, verticalAngle);
    makeRotateAroundPoint(_pivotPoint, _quaternion, _rotMatrix);
    this.#camera.matrixWorld.premultiply(_rotMatrix);
    // Rotate around the up axis
    const horizontalAngle = rotateVec.x;
    _quaternion.setFromAxisAngle(_localUp, horizontalAngle);
    makeRotateAroundPoint(this.#hit.point, _quaternion, _rotMatrix);
    this.#camera.matrixWorld.premultiply(_rotMatrix);
    // Explicitly set the quaternion before decomposing
    this.#camera.matrixWorld.decompose(
      this.#camera.position,
      this.#camera.quaternion,
      _vec6,
    );
  }
  #initializeDragAnchor() {
    if (!this.#hit || this.#hit.distance <= 0) {
      return;
    }
    this.#dragAnchorPoint.copy(this.#hit.point);
    this.#dragStartPosition.copy(this.#camera.position);
    this.#dragStartQuaternion.copy(this.#camera.quaternion);
    this.#camera.getWorldDirection(this.#dragPlaneNormal);
  }
  #restoreDragStartCamera() {
    this.#camera.position.copy(this.#dragStartPosition);
    this.#camera.quaternion.copy(this.#dragStartQuaternion);
    this.#camera.updateMatrixWorld();
  }
  #intersectDragPlane(pointer, target) {
    _plane.setFromNormalAndCoplanarPoint(
      this.#dragPlaneNormal,
      this.#dragAnchorPoint,
    );
    setRaycasterFromCamera(this.#raycaster, pointer, this.#camera);
    return this.#raycaster.ray.intersectPlane(_plane, target) !== null;
  }
  #modifiedDrag(rotateVec) {
    if (!this.#hit || this.#hit.distance <= 0) {
      return;
    }
    this.#camera.getWorldDirection(_forward);
    _up.copy(this.#camera.up).transformDirection(this.#camera.matrixWorld);
    _right.crossVectors(_forward, _up).normalize();
    _vec1.copy(rotateVec).projectOnVector(_right);
    _vec2.copy(rotateVec).projectOnVector(_up);
    const length = this.#hit.point.length();
    let verticalAngle =
      Math.atan2(_vec2.length(), length) * Math.sign(_vec2.dot(_up));
    let horizontalAngle =
      -Math.atan2(_vec1.length(), length) * Math.sign(_vec1.dot(_right));
    this.#camera.getWorldDirection(_vec4).negate();
    const angle = _vec4.angleTo(this.#camera.position);
    const cos = Math.cos(angle);
    verticalAngle /= cos;
    horizontalAngle /= cos;
    // Rotate around the right axis
    _quaternion.setFromAxisAngle(_right, verticalAngle);
    makeRotateAroundPoint(_vec3.set(0, 0, 0), _quaternion, _rotMatrix);
    this.#camera.matrixWorld.premultiply(_rotMatrix);
    // Rotate around the up axis
    _quaternion.setFromAxisAngle(_up, horizontalAngle);
    makeRotateAroundPoint(_vec3.set(0, 0, 0), _quaternion, _rotMatrix);
    this.#camera.matrixWorld.premultiply(_rotMatrix);
    // Explicitly set the quaternion before decomposing
    this.#camera.matrixWorld.decompose(
      this.#camera.position,
      this.#camera.quaternion,
      _vec3,
    );
  }
  #keepCameraUpAtFixedPoint(fixedPoint) {
    this.#camera.updateMatrixWorld();
    _invMatrix.copy(this.#camera.matrixWorld).invert();
    _vec1.copy(fixedPoint).applyMatrix4(_invMatrix);
    this.#keepCameraUp();
    this.#camera.updateMatrixWorld();
    _vec2.copy(_vec1).applyMatrix4(this.#camera.matrixWorld);
    this.#camera.position.add(_vec3.subVectors(fixedPoint, _vec2));
  }
  #getZoomOutMetrics(source, hit, baseScale = 1) {
    const metrics = _zoomOutMetrics;
    const minScale = 0;
    metrics.distanceScale = baseScale;
    metrics.transitionWeight = 0;
    if (!this.#ellipsoid) {
      return metrics;
    }
    const taperStartRadius = this.#ellipsoidMaxRadius * 1.5;
    const maxRadius = this.#ellipsoidMaxRadius * 2;
    const currentDistance = source.length();
    if (currentDistance > taperStartRadius) {
      if (currentDistance >= maxRadius) {
        metrics.distanceScale = minScale;
      } else {
        const factor =
          (maxRadius - currentDistance) / (maxRadius - taperStartRadius);
        metrics.distanceScale = minScale + (baseScale - minScale) * factor;
      }
    }
    if (!hit || hit.virtual) {
      return metrics;
    }
    const distanceASquared = hit.point.distanceToSquared(source);
    const distanceBSquared = hit.point.lengthSq();
    const distanceCSquared = source.lengthSq();
    const denominator =
      2 * Math.sqrt(distanceASquared) * Math.sqrt(distanceBSquared);
    if (denominator <= THRESHOLD) {
      return metrics;
    }
    const angleCos =
      (distanceASquared + distanceBSquared - distanceCSquared) / denominator;
    if (angleCos <= ZOOM_OUT_TRANSITION_COS_THRESHOLD) {
      return metrics;
    }
    if (angleCos >= ZOOM_OUT_TRANSITION_COS_MAX_THRESHOLD) {
      metrics.transitionWeight = 1;
      return metrics;
    }
    metrics.transitionWeight =
      (angleCos - ZOOM_OUT_TRANSITION_COS_THRESHOLD) /
      (ZOOM_OUT_TRANSITION_COS_MAX_THRESHOLD -
        ZOOM_OUT_TRANSITION_COS_THRESHOLD);
    return metrics;
  }
  #getScaledZoomTarget(hit, zoomFactor, source, distanceScale, target) {
    target
      .copy(source)
      .sub(hit.point)
      .multiplyScalar(1 + (zoomFactor - 1) * distanceScale)
      .add(hit.point);
  }
  #getZoomOutTransitionTarget(hit, zoomFactor, source, distanceScale, target) {
    this.#camera.getWorldDirection(_forward);
    target
      .copy(source)
      .addScaledVector(
        _forward,
        -source.distanceTo(hit.point) * (zoomFactor - 1) * distanceScale,
      );
  }
  #getZoomPosition(hit, zoomAmount, zoomFactor, target) {
    const source = _vec4.copy(this.#camera.position);
    let distanceScale = 1;
    let transitionWeight = 0;
    if (zoomAmount < 0 && this.#ellipsoid) {
      const metrics = this.#getZoomOutMetrics(source, hit.virtual ? null : hit);
      distanceScale = metrics.distanceScale;
      transitionWeight = metrics.transitionWeight;
    }
    this.#getScaledZoomTarget(hit, zoomFactor, source, distanceScale, target);
    if (transitionWeight <= 0) {
      return;
    }
    const transitionDistanceScale = this.#getZoomOutMetrics(
      source,
      null,
      1 / 3,
    ).distanceScale;
    this.#getZoomOutTransitionTarget(
      hit,
      zoomFactor,
      source,
      transitionDistanceScale,
      _vec6,
    );
    target.lerp(_vec6, transitionWeight);
  }
  #applyZoom(zoomAmount) {
    const hit = this.#hit;
    if (!hit || hit.distance <= 0) return;
    // Regenerate virtual hit at 50m from current camera position
    if (hit.virtual) {
      _forward.subVectors(hit.point, this.#camera.position).normalize();
      hit.point
        .copy(this.#camera.position)
        .addScaledVector(_forward, VIRTUAL_HIT_DISTANCE);
      hit.distance = VIRTUAL_HIT_DISTANCE;
    }
    let zoomFactor = Math.exp(-zoomAmount * 0.001);
    if (this.minDistance > 0 && zoomFactor < 1) {
      const distance = this.#camera.position.distanceTo(hit.point);
      if (distance * zoomFactor < this.minDistance) {
        zoomFactor = this.minDistance / distance;
      }
    }
    //@ts-expect-error
    if (this.#camera.isOrthographicCamera) {
      this.#camera.zoom /= zoomFactor;
      this.#camera.updateProjectionMatrix();
    }
    this.#getZoomPosition(hit, zoomAmount, zoomFactor, this.#camera.position);
    this.#limitCameraDistance(hit.point);
    if (this.#isCameraCenterMode()) {
      this.#camera.updateMatrixWorld();
    } else {
      this.#keepCameraUpAtFixedPoint(hit.point);
    }
    this.#camera.updateMatrixWorld();
    if (this.state === DRAG) {
      this.#initializeDragAnchor();
    }
  }
  #reachCameraMaxDistance() {
    return (
      !!this.#ellipsoid &&
      !this.#isCameraCenterMode() &&
      this.#camera.position.length() >= this.#ellipsoidMaxRadius * 2
    );
  }
  #isCameraCenterMode() {
    return this.#camera.position.lengthSq() <= CAMERA_CENTER_MODE_DISTANCE_SQ;
  }
  #limitCameraDistance(pivotPosition) {
    if (!this.#ellipsoid || this.#isCameraCenterMode()) return;
    const maxRadius = this.#ellipsoidMaxRadius * 2;
    const currentDistance = this.#camera.position.length();
    if (currentDistance <= maxRadius) return;
    if (pivotPosition) {
      _vec6.subVectors(this.#camera.position, pivotPosition);
      const a = _vec6.lengthSq();
      if (a <= THRESHOLD * THRESHOLD) {
        this.#camera.position.setLength(maxRadius);
        return;
      }
      // Solve for t in: |pivotPosition + t * (cameraPosition - pivotPosition)| = maxRadius
      const b = 2 * pivotPosition.dot(_vec6);
      const c = pivotPosition.lengthSq() - maxRadius ** 2;
      const discriminant = b * b - 4 * a * c;
      if (discriminant < 0) {
        this.#camera.position.setLength(maxRadius);
        return;
      }
      const sqrtDiscriminant = Math.sqrt(discriminant);
      const t0 = (-b - sqrtDiscriminant) / (2 * a);
      const t1 = (-b + sqrtDiscriminant) / (2 * a);
      // Clamp to the intersection on the pivot->camera segment; fall back to
      // radial clamp if the line only intersects outside the segment, to avoid
      // large jumps when the pivot is virtual or near-tangent.
      let t = Number.NaN;
      if (t0 >= 0 && t0 <= 1) {
        t = t0;
      }
      if (t1 >= 0 && t1 <= 1) {
        t = Number.isNaN(t) ? t1 : Math.max(t, t1);
      }
      if (Number.isNaN(t)) {
        this.#camera.position.setLength(maxRadius);
        return;
      }
      this.#camera.position.copy(pivotPosition).addScaledVector(_vec6, t);
    } else {
      this.#camera.position.setLength(maxRadius);
    }
  }
  #shouldDragModified() {
    return (
      !!this.#ellipsoid &&
      !this.#isCameraCenterMode() &&
      this.#camera.position.length() >= this.#ellipsoidMaxRadius + 100000
    );
  }
  #keepCameraUp() {
    _vec6.copy(this.#camera.position);
    const cameraPositionLength = _vec6.length();
    if (cameraPositionLength < CAMERA_CENTER_MODE_DISTANCE) {
      this.#alignCameraRightToXYPlane();
      return;
    }
    _localForward.copy(this.#camera.position).normalize();
    this.#camera.getWorldDirection(_forward);
    _up.copy(this.#camera.up).transformDirection(this.#camera.matrixWorld);
    _right.crossVectors(_forward, _up).normalize();
    _localRight.crossVectors(_up, _localForward);
    if (_localRight.dot(_right) < 0) {
      _localRight.negate();
    }
    _quaternion.setFromUnitVectors(_right, _localRight);
    this.#camera.quaternion.premultiply(_quaternion);
    // _localForward unchanged (position didn't change, only quaternion)
    this.#camera.getWorldDirection(_forward);
    _up.copy(this.#camera.up).transformDirection(this.#camera.matrixWorld);
    _right.crossVectors(_forward, _up).normalize();
    _localUp.crossVectors(_forward, _localForward);
    if (_localUp.dot(_right) < 0) {
      const forwardAngle = _forward.angleTo(_localForward);
      if (forwardAngle < Math.PI / 2) {
        _axis.crossVectors(_forward, _localForward).normalize();
        _quaternion.setFromAxisAngle(_axis, forwardAngle);
        this.#camera.quaternion.premultiply(_quaternion);
      } else {
        _axis
          .crossVectors(_forward, _vec4.copy(_localForward).negate())
          .normalize();
        const negatedAngle = _forward.angleTo(_vec4);
        _quaternion.setFromAxisAngle(_axis, negatedAngle);
        this.#camera.quaternion.premultiply(_quaternion);
      }
    }
  }
  #normalRaycastClosest(raycaster, objects) {
    const targets = Array.isArray(objects) ? objects : [objects];
    if (targets.length === 0) return null;
    const intersects = raycaster.intersectObjects(targets, true);
    if (intersects.length > 0) {
      return {
        point: intersects[0].point.clone(),
        distance: intersects[0].point.distanceTo(this.#camera.position),
      };
    }
    return null;
  }
  #raycast(raycaster) {
    const sceneHit = this.#normalRaycastClosest(raycaster, this.#scene);
    if (sceneHit) {
      return sceneHit;
    }
    const result = this.#isCameraCenterMode()
      ? undefined
      : this.#ellipsoid?.intersectRay(raycaster.ray, _vec6);
    this.#ellipsoid?.getPositionToNormal(_vec6, _vec5);
    const distance = _vec6.distanceTo(this.#camera.position);
    if (result) {
      return {
        point: _vec6.clone(),
        distance: distance,
        onGlobe: true,
      };
    }
    return {
      point: raycaster.ray.at(VIRTUAL_HIT_DISTANCE, _vec6).clone(),
      distance: VIRTUAL_HIT_DISTANCE,
      onGlobe: true,
      virtual: true,
    };
  }
}

export { CameraController };
