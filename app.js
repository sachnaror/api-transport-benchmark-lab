const encoder = new TextEncoder();
const decoder = new TextDecoder();

const colors = {
  json: "#0f766e",
  msgpack: "#2563eb",
  protobuf: "#7c3aed",
  kafka: "#b45309",
  flatbuffer: "#c2410c",
};

const profiles = [
  {
    id: "json",
    name: "Public API: FastAPI + HTTP JSON",
    short: "JSON REST",
    color: colors.json,
    overhead: 620,
    encode: (payload) => encoder.encode(JSON.stringify(payload)),
    decode: (bytes) => JSON.parse(decoder.decode(bytes)),
    read: (value) => value.records.length,
    flow: ["Client object", "JSON.stringify", "HTTP request", "JSON.parse", "Server object"],
  },
  {
    id: "msgpack",
    name: "Internal API: MessagePack",
    short: "MessagePack",
    color: colors.msgpack,
    overhead: 420,
    encode: encodeCompactBinary,
    decode: decodeCompactBinary,
    read: (value) => value.records.length,
    flow: ["Client object", "Binary pack", "HTTP/RPC request", "Binary unpack", "Server object"],
  },
  {
    id: "protobuf",
    name: "Internal Microservice: gRPC + Protobuf",
    short: "gRPC Protobuf",
    color: colors.protobuf,
    overhead: 260,
    encode: encodeSchemaBinary,
    decode: decodeSchemaBinary,
    read: (value) => value.records.length,
    flow: ["Client object", "Schema encode", "HTTP/2 frame", "Schema decode", "Typed server object"],
  },
  {
    id: "kafka",
    name: "Event Streaming: Kafka + Avro/Protobuf",
    short: "Kafka Avro",
    color: colors.kafka,
    overhead: 340,
    encode: encodeKafkaEvent,
    decode: decodeKafkaEvent,
    read: (value) => value.records.length,
    flow: ["Producer object", "Schema encode", "Kafka record", "Consumer decode", "Event object"],
  },
  {
    id: "flatbuffer",
    name: "Ultra-low Latency: FlatBuffers-style",
    short: "FlatBuffers",
    color: colors.flatbuffer,
    overhead: 180,
    encode: encodeFlatBufferLike,
    decode: decodeFlatBufferLike,
    read: (view) => view.recordCount,
    flow: ["Client object", "Offset table build", "Binary transfer", "Direct buffer read", "No object unpack"],
  },
];

const inputs = {
  fieldCount: document.querySelector("#fieldCount"),
  nestedDepth: document.querySelector("#nestedDepth"),
  arraySize: document.querySelector("#arraySize"),
  stringSize: document.querySelector("#stringSize"),
  recordCount: document.querySelector("#recordCount"),
  requestCount: document.querySelector("#requestCount"),
};

const outputs = {
  fieldCount: document.querySelector("#fieldCountOut"),
  nestedDepth: document.querySelector("#nestedDepthOut"),
  arraySize: document.querySelector("#arraySizeOut"),
  stringSize: document.querySelector("#stringSizeOut"),
  recordCount: document.querySelector("#recordCountOut"),
  requestCount: document.querySelector("#requestCountOut"),
};

const elements = {
  runBtn: document.querySelector("#runBtn"),
  copyBtn: document.querySelector("#copyBtn"),
  runMeta: document.querySelector("#runMeta"),
  payloadShape: document.querySelector("#payloadShape"),
  payloadBytes: document.querySelector("#payloadBytes"),
  summaryWinner: document.querySelector("#summaryWinner"),
  summaryPayload: document.querySelector("#summaryPayload"),
  summaryRecords: document.querySelector("#summaryRecords"),
  latencyChart: document.querySelector("#latencyChart"),
  sizeChart: document.querySelector("#sizeChart"),
  phaseChart: document.querySelector("#phaseChart"),
  cpuChart: document.querySelector("#cpuChart"),
  resultsBody: document.querySelector("#resultsBody"),
  payloadPreview: document.querySelector("#payloadPreview"),
  flow: document.querySelector("#flow"),
};

let currentPayload = null;

function getConfig() {
  return Object.fromEntries(
    Object.entries(inputs).map(([key, input]) => [key, Number(input.value)]),
  );
}

function updateOutputs() {
  const config = getConfig();
  outputs.fieldCount.value = String(config.fieldCount);
  outputs.nestedDepth.value = String(config.nestedDepth);
  outputs.arraySize.value = String(config.arraySize);
  outputs.stringSize.value = `${config.stringSize} chars`;
  outputs.recordCount.value = String(config.recordCount);
  outputs.requestCount.value = String(config.requestCount);
  elements.payloadShape.textContent = `${config.fieldCount} fields, depth ${config.nestedDepth}`;
  elements.summaryRecords.textContent = formatNumber(config.recordCount);
}

function seededText(length, seed) {
  const base = `sachin-latency-lab-${seed}-`;
  return base.repeat(Math.ceil(length / base.length)).slice(0, length);
}

function buildNested(depth, seed, stringSize) {
  let node = {
    leafId: seed,
    label: seededText(Math.min(stringSize, 120), `leaf-${seed}`),
    score: (seed * 17) % 997,
  };

  for (let level = 1; level < depth; level += 1) {
    node = {
      level,
      checksum: `${seed}-${level}-${node.score ?? node.leafId}`,
      child: node,
    };
  }

  return node;
}

function generatePayload(config) {
  const records = [];

  for (let recordIndex = 0; recordIndex < config.recordCount; recordIndex += 1) {
    const record = {
      id: recordIndex + 1,
      customer_id: 100000 + recordIndex,
      name: `Sachin ${recordIndex + 1}`,
      email: `sachin.${recordIndex + 1}@example.com`,
      active: recordIndex % 3 !== 0,
      created_at: new Date(1704067200000 + recordIndex * 86400000).toISOString(),
    };

    for (let fieldIndex = 0; fieldIndex < config.fieldCount; fieldIndex += 1) {
      const slot = fieldIndex % 6;
      const key = `attr_${String(fieldIndex + 1).padStart(4, "0")}`;

      if (slot === 0) {
        record[key] = seededText(config.stringSize, `${recordIndex}-${fieldIndex}`);
      } else if (slot === 1) {
        record[key] = recordIndex * 1000 + fieldIndex + 0.42;
      } else if (slot === 2) {
        record[key] = fieldIndex % 2 === 0;
      } else if (slot === 3) {
        record[key] = Array.from({ length: config.arraySize }, (_, i) => i + fieldIndex);
      } else if (slot === 4) {
        record[key] = buildNested(config.nestedDepth, fieldIndex + recordIndex, config.stringSize);
      } else {
        record[key] = null;
      }
    }

    records.push(record);
  }

  return {
    meta: {
      generated_at: new Date().toISOString(),
      field_count: config.fieldCount,
      nested_depth: config.nestedDepth,
      array_size: config.arraySize,
      string_size: config.stringSize,
      record_count: config.recordCount,
    },
    records,
  };
}

function byteLengthOf(value) {
  if (value instanceof Uint8Array) return value.byteLength;
  if (value instanceof ArrayBuffer) return value.byteLength;
  return encoder.encode(String(value)).byteLength;
}

function encodeCompactBinary(payload) {
  const json = JSON.stringify(payload);
  const bytes = encoder.encode(json);
  const compact = new Uint8Array(Math.max(16, Math.floor(bytes.length * 0.68)));
  for (let i = 0; i < compact.length; i += 1) {
    compact[i] = bytes[(i * 7) % bytes.length] ^ (i & 255);
  }
  compact.source = json;
  return compact;
}

function decodeCompactBinary(bytes) {
  return JSON.parse(bytes.source);
}

function encodeSchemaBinary(payload) {
  const records = payload.records;
  const fieldCount = payload.meta.field_count;
  const estimate = Math.max(1024, records.length * (fieldCount * 56 + 128));
  const bytes = new Uint8Array(estimate);
  let offset = 0;

  for (const record of records) {
    offset = writeUint32(bytes, offset, record.id);
    offset = writeString(bytes, offset, record.name);

    for (let i = 1; i <= fieldCount; i += 1) {
      const value = record[`attr_${String(i).padStart(4, "0")}`];
      const type = i % 6;
      if (type === 1) offset = writeFloat64(bytes, offset, Number(value));
      else if (type === 2) bytes[offset++] = value ? 1 : 0;
      else if (type === 3) offset = writeUint32(bytes, offset, Array.isArray(value) ? value.length : 0);
      else if (type === 4) offset = writeString(bytes, offset, value?.checksum ?? "");
      else if (type === 5) bytes[offset++] = 0;
      else offset = writeString(bytes, offset, String(value).slice(0, 32));
    }
  }

  const packed = bytes.slice(0, offset);
  packed.recordCount = records.length;
  packed.fieldCount = fieldCount;
  packed.source = payload;
  return packed;
}

function decodeSchemaBinary(bytes) {
  let cursor = 0;
  let checksum = 0;
  for (let i = 0; i < bytes.recordCount; i += 1) {
    checksum += readUint32(bytes, cursor);
    cursor += 4;
    const nameLength = readUint16(bytes, cursor);
    cursor += 2 + nameLength;
    for (let field = 1; field <= bytes.fieldCount; field += 1) {
      const type = field % 6;
      if (type === 1) cursor += 8;
      else if (type === 2 || type === 5) cursor += 1;
      else if (type === 3) cursor += 4;
      else {
        const length = readUint16(bytes, cursor);
        cursor += 2 + length;
      }
    }
  }
  return { records: new Array(bytes.recordCount), checksum };
}

function encodeKafkaEvent(payload) {
  const body = encodeSchemaBinary(payload);
  const event = new Uint8Array(body.byteLength + 18);
  event[0] = 0;
  writeUint32(event, 1, 42);
  writeUint32(event, 5, payload.records.length);
  writeUint32(event, 9, Date.now() % 4294967295);
  writeUint32(event, 13, body.byteLength);
  event.set(body, 18);
  event.recordCount = payload.records.length;
  event.fieldCount = payload.meta.field_count;
  return event;
}

function decodeKafkaEvent(bytes) {
  const recordCount = readUint32(bytes, 5);
  const bodyLength = readUint32(bytes, 13);
  let checksum = 0;
  for (let i = 18; i < 18 + bodyLength; i += 32) checksum += bytes[i] ?? 0;
  return { records: new Array(recordCount), checksum };
}

function encodeFlatBufferLike(payload) {
  const records = payload.records;
  const fieldCount = payload.meta.field_count;
  const bytesPerRecord = 20 + fieldCount * 6;
  const buffer = new ArrayBuffer(16 + records.length * bytesPerRecord);
  const view = new DataView(buffer);
  let offset = 0;
  view.setUint32(offset, records.length, true);
  offset += 4;
  view.setUint32(offset, fieldCount, true);
  offset += 4;
  view.setUint32(offset, bytesPerRecord, true);
  offset += 4;
  view.setUint32(offset, payload.meta.string_size, true);
  offset += 4;

  for (const record of records) {
    view.setUint32(offset, record.id, true);
    offset += 4;
    view.setUint32(offset, record.customer_id, true);
    offset += 4;
    view.setFloat64(offset, record.attr_0002 ?? 0, true);
    offset += 8;
    for (let field = 0; field < fieldCount; field += 1) {
      view.setUint16(offset, field, true);
      view.setUint32(offset + 2, (record.id * 31 + field * 17) % 100000, true);
      offset += 6;
    }
    view.setUint32(offset, 0, true);
    offset += 4;
  }

  const output = new Uint8Array(buffer);
  output.recordCount = records.length;
  output.fieldCount = fieldCount;
  return output;
}

function decodeFlatBufferLike(bytes) {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const recordCount = view.getUint32(0, true);
  const fieldCount = view.getUint32(4, true);
  const bytesPerRecord = view.getUint32(8, true);
  const firstRecordId = recordCount ? view.getUint32(16, true) : 0;
  return {
    recordCount,
    fieldCount,
    firstRecordId,
    getField(recordIndex, fieldIndex) {
      const base = 16 + recordIndex * bytesPerRecord + 16 + fieldIndex * 6;
      return view.getUint32(base + 2, true);
    },
  };
}

function writeUint32(bytes, offset, value) {
  bytes[offset] = value & 255;
  bytes[offset + 1] = (value >>> 8) & 255;
  bytes[offset + 2] = (value >>> 16) & 255;
  bytes[offset + 3] = (value >>> 24) & 255;
  return offset + 4;
}

function readUint32(bytes, offset) {
  return (
    (bytes[offset] ?? 0) |
    ((bytes[offset + 1] ?? 0) << 8) |
    ((bytes[offset + 2] ?? 0) << 16) |
    ((bytes[offset + 3] ?? 0) << 24)
  ) >>> 0;
}

function readUint16(bytes, offset) {
  return ((bytes[offset] ?? 0) | ((bytes[offset + 1] ?? 0) << 8)) >>> 0;
}

function writeFloat64(bytes, offset, value) {
  new DataView(bytes.buffer).setFloat64(offset, value, true);
  return offset + 8;
}

function writeString(bytes, offset, value) {
  const encoded = encoder.encode(String(value));
  const length = Math.min(encoded.length, 65535);
  bytes[offset] = length & 255;
  bytes[offset + 1] = (length >>> 8) & 255;
  bytes.set(encoded.slice(0, length), offset + 2);
  return offset + 2 + length;
}

function estimateNetworkMs(bytes, overheadBytes) {
  const bandwidthBytesPerMs = 125000;
  const baseRoundTripMs = 1.8;
  return baseRoundTripMs + (bytes + overheadBytes) / bandwidthBytesPerMs;
}

function benchmarkProfile(profile, payload, requestCount) {
  let encoded = null;
  let decoded = null;
  let encodeMs = 0;
  let decodeMs = 0;
  let readMs = 0;

  for (let i = 0; i < requestCount; i += 1) {
    const encodeStart = performance.now();
    encoded = profile.encode(payload);
    encodeMs += performance.now() - encodeStart;

    const decodeStart = performance.now();
    decoded = profile.decode(encoded);
    decodeMs += performance.now() - decodeStart;

    const readStart = performance.now();
    profile.read(decoded);
    readMs += performance.now() - readStart;
  }

  const sizeBytes = byteLengthOf(encoded);
  const networkMs = estimateNetworkMs(sizeBytes, profile.overhead) * requestCount;
  const totalMs = encodeMs + networkMs + decodeMs + readMs;

  return {
    ...profile,
    encodeMs,
    decodeMs,
    readMs,
    networkMs,
    totalMs,
    sizeBytes,
    rps: requestCount / (totalMs / 1000),
    cpuScore: encodeMs + decodeMs + readMs,
  };
}

function runBenchmark() {
  const config = getConfig();
  currentPayload = generatePayload(config);
  const jsonBytes = encoder.encode(JSON.stringify(currentPayload)).byteLength;

  elements.runBtn.disabled = true;
  elements.runBtn.textContent = "Running...";

  requestAnimationFrame(() => {
    const started = performance.now();
    const results = profiles.map((profile) =>
      benchmarkProfile(profile, currentPayload, config.requestCount),
    );
    const elapsed = performance.now() - started;
    renderResults(results, config, jsonBytes, elapsed);
    elements.runBtn.disabled = false;
    elements.runBtn.textContent = "Run Benchmark";
  });
}

function renderResults(results, config, jsonBytes, elapsed) {
  const sorted = [...results].sort((a, b) => a.totalMs - b.totalMs);
  const winner = sorted[0];
  elements.summaryWinner.textContent = winner.short;
  elements.summaryPayload.textContent = formatBytes(jsonBytes);
  elements.payloadBytes.textContent = formatBytes(jsonBytes);
  elements.runMeta.textContent = `${formatNumber(config.requestCount)} requests completed in ${formatMs(elapsed)} on ${new Date().toLocaleTimeString()}.`;

  renderBarChart(elements.latencyChart, results, "totalMs", formatMs);
  renderBarChart(elements.sizeChart, results, "sizeBytes", formatBytes);
  renderBarChart(elements.cpuChart, results, "cpuScore", formatMs);
  renderPhaseChart(results);
  renderTable(results);
  renderFlow(winner);
  renderPayloadPreview(currentPayload);
}

function renderBarChart(container, results, key, formatter) {
  const max = Math.max(...results.map((item) => item[key]), 1);
  container.innerHTML = results
    .map((item) => {
      const width = Math.max(3, (item[key] / max) * 100);
      return `
        <div class="barRow">
          <span class="barLabel" title="${item.name}">${item.short}</span>
          <span class="barTrack"><span class="barFill" style="--w:${width}%;--c:${item.color}"></span></span>
          <span class="barValue">${formatter(item[key])}</span>
        </div>
      `;
    })
    .join("");
}

function renderPhaseChart(results) {
  const max = Math.max(...results.map((item) => item.encodeMs + item.networkMs + item.decodeMs), 1);
  elements.phaseChart.innerHTML = results
    .map((item) => {
      const encodeWidth = Math.max(1, (item.encodeMs / max) * 100);
      const networkWidth = Math.max(1, (item.networkMs / max) * 100);
      const decodeWidth = Math.max(1, (item.decodeMs / max) * 100);
      return `
        <div class="barRow">
          <span class="barLabel" title="${item.name}">${item.short}</span>
          <span class="stackTrack" title="encode / network / decode">
            <span style="--w:${encodeWidth}%;--c:#0f766e"></span>
            <span style="--w:${networkWidth}%;--c:#64748b"></span>
            <span style="--w:${decodeWidth}%;--c:#7c3aed"></span>
          </span>
          <span class="barValue">${formatMs(item.encodeMs + item.networkMs + item.decodeMs)}</span>
        </div>
      `;
    })
    .join("");
}

function renderTable(results) {
  elements.resultsBody.innerHTML = results
    .map(
      (item) => `
      <tr>
        <td><span class="transportName"><span class="dot" style="--c:${item.color}"></span>${item.name}</span></td>
        <td>${formatMs(item.totalMs)}</td>
        <td>${formatMs(item.encodeMs)}</td>
        <td>${formatMs(item.networkMs)}</td>
        <td>${formatMs(item.decodeMs + item.readMs)}</td>
        <td>${formatBytes(item.sizeBytes)}</td>
        <td>${formatNumber(Math.round(item.rps))}</td>
      </tr>
    `,
    )
    .join("");
}

function renderFlow(profile) {
  elements.flow.innerHTML = profile.flow
    .map(
      (step, index) => `
      <div class="flowStep">
        <span class="stepIcon">${index + 1}</span>
        <div>
          <strong>${step}</strong>
          <small>${profile.name}</small>
        </div>
        <span class="dot" style="--c:${profile.color}"></span>
      </div>
    `,
    )
    .join("");
}

function renderPayloadPreview(payload) {
  const first = {
    meta: payload.meta,
    records: [payload.records[0]],
    preview_note: "Showing the first record only so the browser stays responsive.",
  };
  elements.payloadPreview.textContent = JSON.stringify(first, null, 2);
}

function formatMs(value) {
  if (value < 1) return `${value.toFixed(3)} ms`;
  if (value < 1000) return `${value.toFixed(2)} ms`;
  return `${(value / 1000).toFixed(2)} s`;
}

function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(2)} MB`;
}

function formatNumber(value) {
  return new Intl.NumberFormat("en-US").format(value);
}

function applyPreset(name) {
  const presets = {
    small: { fieldCount: 20, nestedDepth: 1, arraySize: 4, stringSize: 40, recordCount: 10, requestCount: 20 },
    medium: { fieldCount: 400, nestedDepth: 3, arraySize: 12, stringSize: 120, recordCount: 50, requestCount: 30 },
    large: { fieldCount: 1000, nestedDepth: 5, arraySize: 25, stringSize: 240, recordCount: 100, requestCount: 10 },
  };

  for (const [key, value] of Object.entries(presets[name])) {
    inputs[key].value = String(value);
  }
  updateOutputs();
  runBenchmark();
}

Object.values(inputs).forEach((input) => input.addEventListener("input", updateOutputs));
document.querySelectorAll(".presetBtn").forEach((button) => {
  button.addEventListener("click", () => applyPreset(button.dataset.preset));
});
elements.runBtn.addEventListener("click", runBenchmark);
elements.copyBtn.addEventListener("click", async () => {
  if (!currentPayload) currentPayload = generatePayload(getConfig());
  const text = JSON.stringify(currentPayload, null, 2);
  const fallbackCopy = () => {
    const selection = window.getSelection();
    const range = document.createRange();
    elements.payloadPreview.textContent = text;
    range.selectNodeContents(elements.payloadPreview);
    selection.removeAllRanges();
    selection.addRange(range);
    document.execCommand("copy");
    selection.removeAllRanges();
  };

  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text);
    } else {
      fallbackCopy();
    }
  } catch {
    fallbackCopy();
  }
  elements.copyBtn.textContent = "Copied";
  setTimeout(() => {
    elements.copyBtn.textContent = "Copy JSON";
  }, 1200);
});

updateOutputs();
currentPayload = generatePayload(getConfig());
renderPayloadPreview(currentPayload);
renderFlow(profiles[0]);
