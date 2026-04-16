const fs = require('fs');
const path = require('path');

const { ConversionError, ensure } = require('./parser');

const GLTF_ACCESSOR_COMPONENTS = {
  SCALAR: 1,
  VEC2: 2,
  VEC3: 3,
  VEC4: 4,
  MAT2: 4,
  MAT3: 9,
  MAT4: 16,
};

const Z_UP_TO_Y_UP = [
  [1.0, 0.0, 0.0],
  [0.0, 0.0, 1.0],
  [0.0, -1.0, 0.0],
];

function pad4Length(length) {
  const rem = length % 4;
  return rem === 0 ? 0 : 4 - rem;
}

function padLength(length, alignment) {
  const rem = length % alignment;
  return rem === 0 ? 0 : alignment - rem;
}

function mat4ToGltfColumnMajorList(m) {
  return [
    m[0],
    m[4],
    m[8],
    m[12],
    m[1],
    m[5],
    m[9],
    m[13],
    m[2],
    m[6],
    m[10],
    m[14],
    m[3],
    m[7],
    m[11],
    m[15],
  ];
}

function make3DTilesGltfRootMatrix(translation, sourceUpAxis) {
  const t0 = translation[0];
  const t1 = translation[1];
  const t2 = translation[2];
  let t = [t0, t1, t2];
  let r;
  if (sourceUpAxis === 'z') {
    r = [
      Z_UP_TO_Y_UP[0][0],
      Z_UP_TO_Y_UP[0][1],
      Z_UP_TO_Y_UP[0][2],
      Z_UP_TO_Y_UP[1][0],
      Z_UP_TO_Y_UP[1][1],
      Z_UP_TO_Y_UP[1][2],
      Z_UP_TO_Y_UP[2][0],
      Z_UP_TO_Y_UP[2][1],
      Z_UP_TO_Y_UP[2][2],
    ];
    const nt0 = r[0] * t0 + r[1] * t1 + r[2] * t2;
    const nt1 = r[3] * t0 + r[4] * t1 + r[5] * t2;
    const nt2 = r[6] * t0 + r[7] * t1 + r[8] * t2;
    t = [nt0, nt1, nt2];
  } else if (sourceUpAxis === 'y') {
    r = [1, 0, 0, 0, 1, 0, 0, 0, 1];
  } else {
    throw new ConversionError(`Unknown source_up_axis: ${sourceUpAxis}`);
  }

  const m = [
    r[0],
    r[1],
    r[2],
    t[0],
    r[3],
    r[4],
    r[5],
    t[1],
    r[6],
    r[7],
    r[8],
    t[2],
    0,
    0,
    0,
    1,
  ];
  return mat4ToGltfColumnMajorList(m);
}

class GltfBuilder {
  constructor() {
    this.bufferParts = [];
    this.byteLength = 0;
    this.bufferViews = [];
    this.accessors = [];
  }

  addBufferView(data) {
    const pad = pad4Length(this.byteLength);
    if (pad > 0) {
      this.bufferParts.push(Buffer.alloc(pad));
      this.byteLength += pad;
    }
    const buf = Buffer.from(data);
    const byteOffset = this.byteLength;
    this.bufferParts.push(buf);
    this.byteLength += buf.length;
    this.bufferViews.push({
      buffer: 0,
      byteOffset,
      byteLength: buf.length,
    });
    return this.bufferViews.length - 1;
  }

  addAccessor({
    array,
    accessorType,
    componentType = 5126,
    includeMinMax = false,
    withBufferView = true,
    normalized = false,
    count = null,
    minValue = null,
    maxValue = null,
  }) {
    const comps = GLTF_ACCESSOR_COMPONENTS[accessorType];
    ensure(!!comps, `Unsupported accessor type: ${accessorType}`);
    const accessor = {
      componentType,
      count: count !== null ? count : 0,
      type: accessorType,
    };
    let arr = array;
    if (!(arr instanceof Float32Array) && componentType === 5126) {
      arr = Float32Array.from(array);
    }
    if (!componentType || ![5126, 5121].includes(componentType)) {
      componentType = 5126;
    }

    if (componentType === 5126) {
      const farr = arr instanceof Float32Array ? arr : Float32Array.from(arr);
      if (accessor.count === 0) {
        accessor.count = farr.length / comps;
      }
      if (includeMinMax && farr.length > 0) {
        const mn = new Array(comps).fill(Infinity);
        const mx = new Array(comps).fill(-Infinity);
        for (let i = 0; i < accessor.count; i++) {
          for (let c = 0; c < comps; c++) {
            const v = farr[i * comps + c];
            if (v < mn[c]) mn[c] = v;
            if (v > mx[c]) mx[c] = v;
          }
        }
        accessor.min = mn;
        accessor.max = mx;
      } else if (minValue != null && maxValue != null) {
        accessor.min = Array.from(minValue);
        accessor.max = Array.from(maxValue);
      }
      if (withBufferView) {
        const buf = Buffer.from(farr.buffer, farr.byteOffset, farr.byteLength);
        accessor.bufferView = this.addBufferView(buf);
      }
    } else {
      const u8 = arr instanceof Uint8Array ? arr : Uint8Array.from(arr);
      if (accessor.count === 0) {
        accessor.count = u8.length / comps;
      }
      if (includeMinMax && u8.length > 0) {
        accessor.min = [0];
        accessor.max = [255];
      } else if (minValue != null && maxValue != null) {
        accessor.min = Array.from(minValue);
        accessor.max = Array.from(maxValue);
      }
      if (withBufferView) {
        const buf = Buffer.from(u8);
        accessor.bufferView = this.addBufferView(buf);
      }
    }

    if (normalized) {
      accessor.normalized = true;
    }
    this.accessors.push(accessor);
    return this.accessors.length - 1;
  }

  writeSpzStreamGlb(
    filePath,
    spzBytes,
    cloud,
    colorSpace,
    translation,
    sourceUpAxis,
  ) {
    const bufferViewIndex = this.addBufferView(Buffer.from(spzBytes));
    const n = cloud.length;
    const attributes = {};

    const posPlaceholder = this.addAccessor({
      array: new Float32Array(0),
      accessorType: 'VEC3',
      componentType: 5126,
      withBufferView: false,
      count: n,
      minValue: [-1.0, -1.0, -1.0],
      maxValue: [1.0, 1.0, 1.0],
    });
    attributes.POSITION = posPlaceholder;

    const colorPlaceholder = this.addAccessor({
      array: new Uint8Array(0),
      accessorType: 'VEC4',
      componentType: 5121,
      normalized: true,
      withBufferView: false,
      count: n,
    });
    attributes.COLOR_0 = colorPlaceholder;

    const scalePlaceholder = this.addAccessor({
      array: new Float32Array(0),
      accessorType: 'VEC3',
      withBufferView: false,
      count: n,
    });
    attributes['KHR_gaussian_splatting:SCALE'] = scalePlaceholder;

    const rotPlaceholder = this.addAccessor({
      array: new Float32Array(0),
      accessorType: 'VEC4',
      withBufferView: false,
      count: n,
    });
    attributes['KHR_gaussian_splatting:ROTATION'] = rotPlaceholder;

    if (cloud.shDegree >= 1) {
      for (let i = 0; i < 3; i++) {
        const h = this.addAccessor({
          array: new Float32Array(0),
          accessorType: 'VEC4',
          componentType: 5126,
          withBufferView: false,
          count: n,
        });
        attributes[`KHR_gaussian_splatting:SH_DEGREE_1_COEF_${i}`] = h;
      }
    }
    if (cloud.shDegree >= 2) {
      for (let i = 0; i < 5; i++) {
        const h = this.addAccessor({
          array: new Float32Array(0),
          accessorType: 'VEC4',
          componentType: 5126,
          withBufferView: false,
          count: n,
        });
        attributes[`KHR_gaussian_splatting:SH_DEGREE_2_COEF_${i}`] = h;
      }
    }
    if (cloud.shDegree >= 3) {
      for (let i = 0; i < 7; i++) {
        const h = this.addAccessor({
          array: new Float32Array(0),
          accessorType: 'VEC4',
          componentType: 5126,
          withBufferView: false,
          count: n,
        });
        attributes[`KHR_gaussian_splatting:SH_DEGREE_3_COEF_${i}`] = h;
      }
    }

    const gsExt = {
      kernel: 'ellipse',
      projection: 'perspective',
      extensions: {
        KHR_gaussian_splatting_compression_spz_2: {
          bufferView: bufferViewIndex,
        },
      },
    };
    if (colorSpace !== 'srgb_rec709_display') {
      gsExt.colorSpace = colorSpace;
    }

    const gltf = {
      asset: { version: '2.0' },
      extensionsUsed: [
        'KHR_gaussian_splatting',
        'KHR_gaussian_splatting_compression_spz_2',
        'KHR_materials_unlit',
      ],
      extensionsRequired: [
        'KHR_gaussian_splatting',
        'KHR_gaussian_splatting_compression_spz_2',
      ],
      scene: 0,
      scenes: [{ nodes: [0] }],
      nodes: [
        {
          mesh: 0,
          matrix: make3DTilesGltfRootMatrix(translation, sourceUpAxis),
        },
      ],
      materials: [{ extensions: { KHR_materials_unlit: {} } }],
      meshes: [
        {
          primitives: [
            {
              mode: 0,
              attributes,
              material: 0,
              extensions: {
                KHR_gaussian_splatting: gsExt,
              },
            },
          ],
        },
      ],
      buffers: [{ byteLength: 0 }],
      bufferViews: this.bufferViews,
      accessors: this.accessors,
    };
    this._writeGLB(filePath, gltf);
  }

  _writeGLB(filePath, gltf) {
    const binPadLength = pad4Length(this.byteLength);
    const binDataLength = this.byteLength + binPadLength;
    gltf.buffers[0].byteLength = binDataLength;
    const jsonChunk = Buffer.from(JSON.stringify(gltf), 'utf8');
    const jsonPadLength = pad4Length(jsonChunk.length);
    const jsonDataLength = jsonChunk.length + jsonPadLength;

    const totalLen = 12 + 8 + jsonDataLength + 8 + binDataLength;
    const out = Buffer.alloc(totalLen);
    out.write('glTF', 0, 'ascii');
    out.writeUInt32LE(2, 4);
    out.writeUInt32LE(totalLen, 8);
    out.writeUInt32LE(jsonDataLength, 12);
    out.write('JSON', 16, 'ascii');

    const jsonOffset = 20;
    jsonChunk.copy(out, jsonOffset);
    if (jsonPadLength > 0) {
      out.fill(
        0x20,
        jsonOffset + jsonChunk.length,
        jsonOffset + jsonDataLength,
      );
    }

    const binHeaderOffset = jsonOffset + jsonDataLength;
    out.writeUInt32LE(binDataLength, binHeaderOffset);
    out.write('BIN', binHeaderOffset + 4, 'ascii');
    out[binHeaderOffset + 7] = 0x00;

    let binOffset = binHeaderOffset + 8;
    for (const part of this.bufferParts) {
      part.copy(out, binOffset);
      binOffset += part.length;
    }

    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    fs.writeFileSync(filePath, out);
  }
}

module.exports = {
  GltfBuilder,
  mat4ToGltfColumnMajorList,
  make3DTilesGltfRootMatrix,
};
