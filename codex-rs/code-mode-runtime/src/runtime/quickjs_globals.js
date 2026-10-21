(() => {
  const hostCallback = __codexHost;
  const catalog = __codexTools;
  delete globalThis.__codexHost;
  delete globalThis.__codexTools;
  delete globalThis.console;
  delete globalThis.Atomics;
  delete globalThis.SharedArrayBuffer;
  delete globalThis.WebAssembly;
  const pending = new Map();
  const timers = new Map();
  let nextTimer = 1;

  function host(kind, payload) {
    return JSON.parse(hostCallback(kind, JSON.stringify(payload)));
  }
  function outputText(value) {
    if (value === null || !["object", "function"].includes(typeof value)) {
      return String(value);
    }
    const json = JSON.stringify(value);
    return json === undefined ? String(value) : json;
  }
  function text(value) {
    host("output", { type: "input_text", text: outputText(value) });
  }
  function notify(value) {
    const message = outputText(value);
    if (!message.trim()) throw new TypeError("notify expects non-empty text");
    host("notify", message);
  }
  function image(value, detailOverride) {
    const expected = "image expects a non-empty image URL string, an object with image_url and optional detail, or a raw MCP image block";
    let imageUrl;
    let detail;
    if (typeof value === "string") imageUrl = value;
    else if (value && typeof value === "object" && !Array.isArray(value)) {
      if (value.image_url !== undefined) {
        imageUrl = value.image_url;
        detail = value.detail;
        if (typeof imageUrl !== "string") throw new TypeError(expected);
        if (detail != null && typeof detail !== "string") throw new TypeError("image detail must be a string when provided");
      } else {
        const block = JSON.parse(JSON.stringify(value));
        if (typeof block.type !== "string") throw new TypeError(expected);
        if (block.type !== "image") throw new TypeError(`image only accepts MCP image blocks, got "${block.type}"`);
        if (typeof block.data !== "string" || !block.data) throw new TypeError("image expected MCP image data");
        const mime = block.mimeType || block.mime_type || "application/octet-stream";
        imageUrl = /^data:/i.test(block.data) ? block.data : `data:${mime};base64,${block.data}`;
        const metadata = block._meta?.["codex/imageDetail"];
        if (["auto", "low", "high", "original"].includes(metadata)) detail = metadata;
      }
    }
    if (typeof imageUrl !== "string" || !imageUrl) throw new TypeError(expected);
    if (/^https?:/i.test(imageUrl)) throw new TypeError("Tool call failed: remote image URLs are not supported in tool outputs. Pass a base64 data URI instead");
    if (!/^data:/i.test(imageUrl)) throw new TypeError("Tool call failed: invalid image output. Pass a base64 data URI instead");
    if (detailOverride != null && typeof detailOverride !== "string") throw new TypeError("image detail must be a string when provided");
    detail = detailOverride ?? detail ?? "high";
    detail = detail.toLowerCase();
    if (!["auto", "low", "high", "original"].includes(detail)) throw new TypeError("image detail must be one of: auto, low, high, original");
    host("output", { type: "input_image", image_url: imageUrl, detail });
  }
  function audio(value) {
    const expected = "audio expects a non-empty audio URL string, an object with audio_url, or a raw MCP audio block";
    let audioUrl;
    if (typeof value === "string") audioUrl = value;
    else if (value && typeof value === "object" && !Array.isArray(value)) {
      if (value.audio_url !== undefined) audioUrl = value.audio_url;
      else {
        const block = JSON.parse(JSON.stringify(value));
        if (typeof block.type !== "string") throw new TypeError(expected);
        if (block.type !== "audio") throw new TypeError(`audio only accepts MCP audio blocks, got "${block.type}"`);
        if (typeof block.data !== "string" || !block.data) throw new TypeError("audio expected MCP audio data");
        const mime = block.mimeType || block.mime_type || "application/octet-stream";
        audioUrl = /^data:/i.test(block.data) ? block.data : `data:${mime};base64,${block.data}`;
      }
    }
    if (typeof audioUrl !== "string" || !audioUrl) throw new TypeError(expected);
    if (!/^data:/i.test(audioUrl)) throw new TypeError("Tool call failed: invalid audio output. Pass a base64 data URI instead");
    host("output", { type: "input_audio", audio_url: audioUrl });
  }
  function generatedImage(value) {
    if (!value || typeof value !== "object") throw new TypeError("generatedImage expects an image generation result object");
    const hint = value.output_hint;
    if (hint !== undefined && typeof hint !== "string") throw new TypeError("generatedImage output_hint must be a string when provided");
    image(value);
    if (hint !== undefined) text(hint);
  }
  function store(key, value) {
    key = String(key);
    const json = JSON.stringify(value);
    if (json === undefined) throw new TypeError(`Unable to store ${JSON.stringify(key)}. Only plain serializable objects can be stored.`);
    host("store", { key, value: JSON.parse(json) });
  }
  function load(key) {
    const loaded = host("load", String(key));
    return loaded.present ? loaded.value : undefined;
  }
  function setTimeout(callback, delay) {
    if (typeof callback !== "function") throw new TypeError("setTimeout expects a function callback");
    delay = Number(delay);
    delay = !Number.isFinite(delay) || delay <= 0 ? 0 : Math.trunc(delay);
    const id = nextTimer++;
    timers.set(id, callback);
    host("setTimeout", { id, delay });
    return id;
  }
  function clearTimeout(id) {
    if (id == null) return;
    id = Number(id);
    if (!Number.isFinite(id) || id <= 0) return;
    id = Math.trunc(id);
    timers.delete(id);
    host("clearTimeout", id);
  }
  function yield_control() {
    host("yield", null);
  }
  function exit() {
    host("exit", null);
    // Enter the interpreter interrupt path, which cannot be caught by user JS.
    for (;;) {}
  }
  const tools = {};
  for (let index = 0; index < catalog.length; index++) {
    tools[catalog[index].name] = function (input) {
      const id = host("tool", { index, input });
      return new Promise((resolve, reject) => { pending.set(id, { resolve, reject }); });
    };
  }
  const globals = { tools, ALL_TOOLS: catalog, text, image, audio, generatedImage,
    store, load, notify, setTimeout, clearTimeout, yield_control, exit };
  for (const [name, value] of Object.entries(globals)) globalThis[name] = value;
  return {
    resolve(json) {
      const [id, success, value] = JSON.parse(json);
      const delivery = pending.get(id);
      if (!delivery) throw new Error(`unknown tool call \`${id}\``);
      pending.delete(id);
      if (success) delivery.resolve(value);
      else delivery.reject(value);
    },
    timeout(json) {
      const id = JSON.parse(json)[0];
      const callback = timers.get(id);
      timers.delete(id);
      if (callback) callback();
    },
  };
})()
