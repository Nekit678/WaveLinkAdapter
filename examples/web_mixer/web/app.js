const $ = (selector, root = document) => root.querySelector(selector);
const clamp = (value, min = 0, max = 1) => Math.min(max, Math.max(min, Number(value) || 0));

const elements = {
  activeMix: $("#active-mix-readout"),
  applicationChannelSelect: $("#application-channel-select"),
  applicationSelect: $("#application-select"),
  appsDialog: $("#apps-dialog"),
  bank: $("#channel-bank"),
  channelDialog: $("#channel-dialog"),
  channelInspector: $("#channel-inspector"),
  channelCount: $("#channel-count"),
  connectButton: $("#connect-button"),
  connectionButton: $("#connection-button"),
  connectionLabel: $("#connection-label"),
  empty: $("#empty-state"),
  emptyHeading: $("#empty-state h1"),
  emptyText: $("#empty-state p"),
  footerLed: $("#footer-led"),
  footerStatus: $("#footer-status"),
  focusedAppChannel: $("#focused-app-channel"),
  focusedAppName: $("#focused-app-name"),
  inputsDialog: $("#inputs-dialog"),
  inputsPanel: $("#inputs-panel"),
  mixTabs: $("#mix-tabs"),
  outputSelect: $("#output-select"),
  outputsPanel: $("#outputs-panel"),
  pluginDevices: $("#plugin-devices"),
  reconnect: $("#auto-reconnect"),
  refresh: $("#refresh-button"),
  routingDialog: $("#routing-dialog"),
  serverUrl: $("#server-url"),
  settings: $("#settings-dialog"),
  settingsForm: $("#settings-form"),
  toastRegion: $("#toast-region"),
};

const state = {
  socket: null,
  socketGeneration: 0,
  nextRpcId: 1,
  pending: new Map(),
  reconnectTimer: null,
  reconnectAttempt: 0,
  waveLinkConnected: false,
  hydrated: false,
  hydrating: false,
  channels: [],
  mixes: [],
  inputDevices: [],
  outputDevices: [],
  mainOutput: null,
  applicationInfo: null,
  focusedApp: null,
  selectedMixId: null,
  meters: new Map(),
  meterSubscriptions: new Set(),
  stripByChannel: new Map(),
  throttles: new Map(),
};

const palette = ["#17d3a2", "#1bb4e9", "#9d78ee", "#ec7cb2", "#f1a748", "#6fcf65", "#e76d62"];

function defaultServerUrl() {
  if (location.protocol === "http:" || location.protocol === "https:") {
    return `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}`;
  }
  return "ws://127.0.0.1:8765";
}

function savedServerUrl() {
  return localStorage.getItem("wavelink.serverUrl") || defaultServerUrl();
}

function normaliseWebSocketUrl(raw) {
  const text = raw.trim();
  const withScheme = /^(wss?):\/\//i.test(text) ? text : `ws://${text}`;
  const url = new URL(withScheme);
  if (url.protocol !== "ws:" && url.protocol !== "wss:") throw new Error("Используйте адрес ws:// или wss://");
  return url.toString().replace(/\/$/, "");
}

function showToast(message, type = "warning") {
  const toast = document.createElement("div");
  toast.className = `toast is-${type}`;
  toast.textContent = message;
  elements.toastRegion.append(toast);
  window.setTimeout(() => toast.remove(), 4200);
}

function rpcErrorMessage(error) {
  if (!error) return "Неизвестная ошибка";
  if (error.message) return error.message;
  return String(error);
}

function setConnectionUi(mode, detail) {
  elements.connectionButton.className = `status-pill is-${mode}`;
  elements.footerLed.className = `mini-led${mode === "online" ? " is-online" : mode === "error" ? " is-error" : ""}`;
  const labels = { online: "Подключено", connecting: "Подключение", error: "Нет связи" };
  elements.connectionLabel.textContent = labels[mode] || "Нет связи";
  elements.footerStatus.textContent = detail;

  if (!state.hydrated || !state.channels.length) {
    elements.emptyHeading.textContent = mode === "online" ? "В миксе пока нет каналов" : mode === "connecting" ? "Подключаемся к Wave Link" : "Нет связи с Wave Link";
    elements.emptyText.textContent = mode === "online"
      ? "Добавьте аудиоканалы в приложении Wave Link — они появятся здесь автоматически."
      : mode === "connecting"
        ? "Запустите Wave Link и оставьте эту страницу открытой — консоль появится автоматически."
        : "Проверьте, что сервер запущен и в настройках указан правильный адрес компьютера.";
  }
}

function rejectPending(reason) {
  for (const { reject, timeout } of state.pending.values()) {
    clearTimeout(timeout);
    reject(reason);
  }
  state.pending.clear();
}

function sendRpc(method, params, timeoutMs = 6500) {
  if (!state.socket || state.socket.readyState !== WebSocket.OPEN) {
    return Promise.reject(new Error("WebSocket не подключён"));
  }
  const id = state.nextRpcId++;
  const payload = { jsonrpc: "2.0", id, method };
  if (params !== undefined) payload.params = params;
  return new Promise((resolve, reject) => {
    const timeout = window.setTimeout(() => {
      state.pending.delete(id);
      reject(new Error(`Сервер не ответил на ${method}`));
    }, timeoutMs);
    state.pending.set(id, { resolve, reject, timeout });
    state.socket.send(JSON.stringify(payload));
  });
}

function connect({ announce = false } = {}) {
  clearTimeout(state.reconnectTimer);
  state.reconnectTimer = null;
  let url;
  try {
    url = normaliseWebSocketUrl(elements.serverUrl.value || savedServerUrl());
  } catch (error) {
    setConnectionUi("error", "Некорректный адрес сервера");
    if (announce) showToast(rpcErrorMessage(error), "error");
    return;
  }

  localStorage.setItem("wavelink.serverUrl", url);
  const generation = ++state.socketGeneration;
  if (state.socket) {
    state.socket.onclose = null;
    state.socket.close();
  }
  rejectPending(new Error("Соединение перезапущено"));
  state.waveLinkConnected = false;
  setConnectionUi("connecting", `Подключение к ${new URL(url).host}…`);

  const socket = new WebSocket(url);
  state.socket = socket;

  socket.addEventListener("open", async () => {
    if (generation !== state.socketGeneration) return;
    state.reconnectAttempt = 0;
    setConnectionUi("connecting", "Сервер подключён, ожидаем Wave Link…");
    if (announce) showToast("WebSocket-сервер подключён", "success");
    try {
      const status = await sendRpc("server.getStatus");
      handleConnectionStatus(status);
    } catch (error) {
      showToast(rpcErrorMessage(error), "error");
    }
  });

  socket.addEventListener("message", ({ data }) => {
    if (generation !== state.socketGeneration) return;
    try {
      const message = JSON.parse(data);
      (Array.isArray(message) ? message : [message]).forEach(handleMessage);
    } catch {
      showToast("Сервер прислал некорректный JSON", "error");
    }
  });

  socket.addEventListener("close", () => {
    if (generation !== state.socketGeneration) return;
    rejectPending(new Error("WebSocket-соединение закрыто"));
    state.waveLinkConnected = false;
    setConnectionUi("error", "WebSocket-сервер недоступен");
    scheduleReconnect();
  });

  socket.addEventListener("error", () => {
    if (generation === state.socketGeneration) setConnectionUi("error", `Не удаётся подключиться к ${new URL(url).host}`);
  });
}

function scheduleReconnect() {
  if (!elements.reconnect.checked || state.reconnectTimer) return;
  const delay = Math.min(12000, 900 * 2 ** state.reconnectAttempt++);
  state.reconnectTimer = window.setTimeout(() => connect(), delay);
}

function handleMessage(message) {
  if (message && Object.hasOwn(message, "id")) {
    const pending = state.pending.get(message.id);
    if (!pending) return;
    state.pending.delete(message.id);
    clearTimeout(pending.timeout);
    if (message.error) {
      const error = new Error(message.error.message || "JSON-RPC error");
      error.code = message.error.code;
      error.data = message.error.data;
      pending.reject(error);
    } else pending.resolve(message.result);
    return;
  }
  if (message?.method) handleNotification(message.method, message.params || {});
}

function handleNotification(method, params) {
  switch (method) {
    case "server.connectionChanged": handleConnectionStatus(params); break;
    case "channelsChanged": state.channels = params.channels || (Array.isArray(params) ? params : []); renderConsole(); renderApplications(); reconcileMeterSubscriptions(); break;
    case "channelChanged": mergeEntity(state.channels, params); updateChannel(params.id); renderApplications(); break;
    case "mixesChanged": state.mixes = params.mixes || (Array.isArray(params) ? params : []); ensureSelectedMix(); renderAll(); reconcileMeterSubscriptions(); break;
    case "mixChanged": mergeEntity(state.mixes, params); updateMaster(); renderMixTabs(); break;
    case "inputDevicesChanged": state.inputDevices = params.inputDevices || (Array.isArray(params) ? params : []); renderInputsPanel(); reconcileMeterSubscriptions(); break;
    case "inputDeviceChanged": mergeInputDevice(params); renderInputsPanel(); reconcileMeterSubscriptions(); break;
    case "outputDevicesChanged": applyOutputPayload(params); renderOutputs(); reconcileMeterSubscriptions(); break;
    case "outputDeviceChanged": mergeOutputDevice(params); renderOutputs(); reconcileMeterSubscriptions(); break;
    case "focusedAppChanged": state.focusedApp = params; renderApplications(); break;
    case "createProfileRequested": showToast("Wave Link запросил создание профиля устройства", "warning"); break;
    case "levelMeterChanged": updateMeters(params); break;
    default: break;
  }
}

function handleConnectionStatus(status) {
  const wasConnected = state.waveLinkConnected;
  state.waveLinkConnected = Boolean(status?.connected);
  if (state.waveLinkConnected) {
    setConnectionUi("online", `Wave Link ${status.waveLinkPort ? `:${status.waveLinkPort}` : "подключён"}`);
    if (!wasConnected || !state.hydrated) hydrate();
  } else {
    state.meterSubscriptions.clear();
    setConnectionUi("connecting", "Сервер работает — ожидаем запуска Wave Link");
  }
}

async function hydrate({ announce = false } = {}) {
  if (state.hydrating || !state.waveLinkConnected) return;
  state.hydrating = true;
  elements.refresh.classList.add("is-spinning");
  try {
    const snapshot = await sendRpc("server.refreshState", undefined, 10000);
    applySnapshot(snapshot);
    await subscribeToEvents();
    await syncPluginInfo();
    if (announce) showToast("Состояние микшера обновлено", "success");
  } catch (error) {
    showToast(`Не удалось обновить микшер: ${rpcErrorMessage(error)}`, "error");
  } finally {
    state.hydrating = false;
    elements.refresh.classList.remove("is-spinning");
  }
}

function applySnapshot(snapshot) {
  state.channels = snapshot?.channels || [];
  state.mixes = snapshot?.mixes || [];
  state.inputDevices = snapshot?.inputDevices || [];
  state.outputDevices = snapshot?.outputDevices || [];
  state.mainOutput = snapshot?.mainOutput || null;
  state.applicationInfo = snapshot?.applicationInfo || null;
  state.focusedApp = snapshot?.focusedApp || state.focusedApp;
  state.hydrated = true;
  ensureSelectedMix();
  if (snapshot?.levelMeters) updateMeters(snapshot.levelMeters);
  renderAll();
  renderSystemInfo();
}

async function subscribeToEvents() {
  await Promise.allSettled([
    reconcileMeterSubscriptions(),
    sendRpc("setSubscription", { focusedAppChanged: { isEnabled: true } }),
  ]);
}

function meterTargets() {
  return [
    ...state.inputDevices.flatMap((device) => (device.inputs || []).map((input) => ({ type: "input", id: input.id }))),
    ...state.outputDevices.flatMap((device) => (device.outputs || []).map((output) => ({ type: "output", id: output.id }))),
    ...state.channels.map((channel) => ({ type: "channel", id: channel.id })),
    ...state.mixes.map((mix) => ({ type: "mix", id: mix.id })),
  ];
}

async function reconcileMeterSubscriptions() {
  if (!state.waveLinkConnected || !state.socket || state.socket.readyState !== WebSocket.OPEN) return;
  const targets = new Map(meterTargets().map((target) => [`${target.type}:${target.id}`, target]));
  const requests = [];
  for (const key of state.meterSubscriptions) {
    if (targets.has(key)) continue;
    const separator = key.indexOf(":");
    const type = key.slice(0, separator);
    const id = key.slice(separator + 1);
    requests.push(sendRpc("setSubscription", { levelMeterChanged: { type, id, isEnabled: false } })
      .then(() => state.meterSubscriptions.delete(key)));
  }
  for (const [key, target] of targets) {
    if (state.meterSubscriptions.has(key)) continue;
    requests.push(sendRpc("setSubscription", { levelMeterChanged: { ...target, isEnabled: true } })
      .then(() => state.meterSubscriptions.add(key)));
  }
  await Promise.allSettled(requests);
}

function mergeEntity(collection, patch) {
  if (!patch?.id) return;
  const index = collection.findIndex((item) => item.id === patch.id);
  if (index < 0) collection.push(patch);
  else collection[index] = deepMerge(collection[index], patch);
}

function deepMerge(base, patch) {
  const result = { ...base, ...patch };
  if (patch && Object.hasOwn(patch, "mixes")) {
    // channelChanged присылает актуальный полный список миксов канала.
    // Слияние по id сохраняло удалённые привязки навсегда.
    result.mixes = Array.isArray(patch.mixes) ? [...patch.mixes] : patch.mixes;
  }
  if (base?.effects && patch?.effects) {
    result.effects = [...base.effects];
    for (const item of patch.effects) {
      const index = result.effects.findIndex((candidate) => candidate.id === item.id);
      if (index < 0) result.effects.push(item); else result.effects[index] = { ...result.effects[index], ...item };
    }
  }
  return result;
}

function ensureSelectedMix() {
  if (state.mixes.some((mix) => mix.id === state.selectedMixId)) return;
  const stored = localStorage.getItem("wavelink.selectedMix");
  state.selectedMixId = state.mixes.find((mix) => mix.id === stored)?.id || state.mixes[0]?.id || null;
}

function selectedMix() {
  return state.mixes.find((mix) => mix.id === state.selectedMixId) || null;
}

function channelMix(channel) {
  return channel.mixes?.find((mix) => (mix.id ?? mix.mixId) === state.selectedMixId) || null;
}

function channelValue(channel, key) {
  if (!state.selectedMixId) return channel[key];
  return channelMix(channel)?.[key];
}

function visibleChannels() {
  if (!state.selectedMixId) return [];
  return state.channels.filter((channel) => channelMix(channel) !== null);
}

function renderAll() {
  renderMixTabs();
  renderOutputs();
  renderInputsPanel();
  renderApplications();
  renderConsole();
}

function renderMixTabs() {
  elements.mixTabs.replaceChildren();
  if (!state.mixes.length) {
    const placeholder = document.createElement("span");
    placeholder.className = "toolbar-placeholder";
    placeholder.textContent = "Миксы не найдены";
    elements.mixTabs.append(placeholder);
  }
  for (const mix of state.mixes) {
    const button = document.createElement("button");
    button.className = "mix-tab";
    button.type = "button";
    button.role = "tab";
    button.dataset.mixId = mix.id;
    button.ariaSelected = String(mix.id === state.selectedMixId);
    button.textContent = mix.name || "Без названия";
    button.addEventListener("click", () => {
      state.selectedMixId = mix.id;
      localStorage.setItem("wavelink.selectedMix", mix.id);
      renderMixTabs();
      renderConsole();
    });
    elements.mixTabs.append(button);
  }
  elements.activeMix.textContent = selectedMix()?.name || "NO MIX SELECTED";
}

function outputKey(deviceId, outputId) { return `${deviceId}::${outputId}`; }

function renderOutputs() {
  elements.outputSelect.replaceChildren();
  let count = 0;
  for (const device of state.outputDevices) {
    for (const output of device.outputs || []) {
      const option = document.createElement("option");
      option.value = outputKey(device.id, output.id);
      option.textContent = output.name || device.name || "Output";
      option.dataset.deviceId = device.id;
      option.dataset.outputId = output.id;
      elements.outputSelect.append(option);
      count += 1;
    }
  }
  elements.outputSelect.disabled = count === 0;
  if (!count) elements.outputSelect.add(new Option("Не найден", ""));
  if (state.mainOutput) elements.outputSelect.value = outputKey(state.mainOutput.outputDeviceId, state.mainOutput.outputId);
  renderOutputsPanel();
}

function applyOutputPayload(payload) {
  state.mainOutput = payload.mainOutput || state.mainOutput;
  state.outputDevices = payload.outputDevices || state.outputDevices;
}

function mergeInputDevice(patch) {
  if (!patch?.id) return;
  const index = state.inputDevices.findIndex((device) => device.id === patch.id);
  if (index < 0) {
    state.inputDevices.push(patch);
    return;
  }
  const device = { ...state.inputDevices[index], ...patch };
  if (patch.inputs) {
    device.inputs = [...(state.inputDevices[index].inputs || [])];
    for (const inputPatch of patch.inputs) {
      const inputIndex = device.inputs.findIndex((input) => input.id === inputPatch.id);
      if (inputIndex < 0) device.inputs.push(inputPatch);
      else device.inputs[inputIndex] = deepMerge(device.inputs[inputIndex], inputPatch);
    }
  }
  state.inputDevices[index] = device;
}

function mergeOutputDevice(patch) {
  if (!patch?.id) return;
  const index = state.outputDevices.findIndex((device) => device.id === patch.id);
  if (index < 0) {
    state.outputDevices.push(patch);
    return;
  }
  const device = { ...state.outputDevices[index], ...patch };
  if (patch.outputs) {
    device.outputs = [...(state.outputDevices[index].outputs || [])];
    for (const outputPatch of patch.outputs) {
      const outputIndex = device.outputs.findIndex((output) => output.id === outputPatch.id);
      if (outputIndex < 0) device.outputs.push(outputPatch);
      else device.outputs[outputIndex] = { ...device.outputs[outputIndex], ...outputPatch };
    }
  }
  state.outputDevices[index] = device;
}

function emptyPanel(container, message) {
  const empty = document.createElement("div");
  empty.className = "panel-empty";
  empty.textContent = message;
  container.append(empty);
}

function createDeviceCard(device, kind) {
  const card = document.createElement("article");
  card.className = "device-card";
  const heading = document.createElement("header");
  heading.className = "device-card-heading";
  const name = document.createElement("strong");
  name.textContent = device.name || (kind === "input" ? "Input device" : "Output device");
  const type = document.createElement("small");
  type.textContent = (device.deviceType || (device.isWaveDevice ? "WAVE DEVICE" : kind)).toUpperCase();
  heading.append(name, type);
  card.append(heading);
  return card;
}

function createIoHeader(name, subtitle, meterId, muted, onMute) {
  const header = document.createElement("header");
  header.className = "io-card-title";
  const meter = document.createElement("span");
  meter.className = "mini-meter";
  meter.dataset.meterId = meterId;
  meter.innerHTML = '<i aria-hidden="true"></i><i aria-hidden="true"></i>';
  const identity = document.createElement("span");
  const strong = document.createElement("strong");
  strong.textContent = name;
  const small = document.createElement("small");
  small.textContent = subtitle;
  identity.append(strong, small);
  const mute = document.createElement("button");
  mute.type = "button";
  mute.className = "compact-action";
  mute.textContent = "MUTE";
  mute.ariaPressed = String(Boolean(muted));
  mute.addEventListener("click", onMute);
  header.append(meter, identity, mute);
  applyMiniMeter(meter, state.meters.get(meterId));
  return header;
}

function createRangeControl(label, value, formatter, onCommit, options = {}) {
  const row = document.createElement("div");
  row.className = "control-row";
  const caption = document.createElement("label");
  caption.textContent = label;
  const range = document.createElement("input");
  range.type = "range";
  range.min = String(options.min ?? 0);
  range.max = String(options.max ?? 1);
  range.step = String(options.step ?? .01);
  range.value = String(clamp(value));
  range.setAttribute("aria-label", label);
  const output = document.createElement("output");
  output.textContent = formatter(Number(range.value));
  range.addEventListener("input", () => { output.textContent = formatter(Number(range.value)); });
  range.addEventListener("change", () => onCommit(Number(range.value)));
  row.append(caption, range, output);
  return row;
}

function createToggleControl(label, checked, onChange) {
  const row = document.createElement("label");
  row.className = "toggle-control";
  const text = document.createElement("span");
  text.textContent = label;
  const input = document.createElement("input");
  input.type = "checkbox";
  input.checked = Boolean(checked);
  input.addEventListener("change", () => onChange(input.checked));
  row.append(text, input);
  return row;
}

function renderInputsPanel() {
  elements.inputsPanel.replaceChildren();
  if (!state.inputDevices.length) {
    emptyPanel(elements.inputsPanel, "Входные устройства не найдены");
    return;
  }
  for (const device of state.inputDevices) {
    const card = createDeviceCard(device, "input");
    for (const input of device.inputs || []) {
      const section = document.createElement("section");
      section.className = "io-card";
      section.append(createIoHeader(
        input.name || "Input",
        input.id,
        input.id,
        input.isMuted,
        () => setInputValue(device, input, "isMuted", !input.isMuted),
      ));
      if (input.gain) {
        section.append(createRangeControl("GAIN", input.gain.value, formatPercent, (value) => {
          setInputValue(device, input, "gain", { value });
        }));
      }
      if (input.micPcMix) {
        section.append(createRangeControl("MIC / PC", input.micPcMix.value, formatBalance, (value) => {
          setInputValue(device, input, "micPcMix", { value });
        }));
      }
      if (input.isGainLockOn != null) {
        section.append(createToggleControl("Блокировка усиления", input.isGainLockOn, (enabled) => {
          setInputValue(device, input, "isGainLockOn", enabled);
        }));
      }
      section.append(createInputEffects(device, input));
      card.append(section);
    }
    elements.inputsPanel.append(card);
  }
}

function createInputEffects(device, input) {
  const rack = document.createElement("div");
  rack.className = "io-effects";
  for (const [collection, label] of [["effects", "VST"], ["dspEffects", "DSP"]]) {
    for (const effect of input[collection] || []) {
      if (effect.isSupported === false) continue;
      const button = document.createElement("button");
      button.type = "button";
      button.className = `io-effect${effect.isEnabled ? " is-enabled" : ""}`;
      button.textContent = effect.name || "Effect";
      const kind = document.createElement("small");
      kind.textContent = label;
      button.append(kind);
      button.ariaPressed = String(Boolean(effect.isEnabled));
      button.addEventListener("click", () => {
        setInputEffect(device, input, collection, effect, !effect.isEnabled);
      });
      rack.append(button);
    }
  }
  return rack;
}

async function setInputEffect(device, input, collection, effect, enabled) {
  const previous = effect.isEnabled;
  effect.isEnabled = enabled;
  renderInputsPanel();
  try {
    await sendRpc("setInputDevice", {
      id: device.id,
      inputs: [{ id: input.id, [collection]: [{ id: effect.id, isEnabled: enabled }] }],
    });
  } catch (error) {
    effect.isEnabled = previous;
    renderInputsPanel();
    showToast(`Эффект входа не переключён: ${rpcErrorMessage(error)}`, "error");
  }
}

async function setInputValue(device, input, key, value) {
  const previous = input[key];
  input[key] = value;
  renderInputsPanel();
  try {
    await sendRpc("setInputDevice", { id: device.id, inputs: [{ id: input.id, [key]: value }] });
  } catch (error) {
    input[key] = previous;
    renderInputsPanel();
    showToast(`Вход не обновлён: ${rpcErrorMessage(error)}`, "error");
  }
}

function renderOutputsPanel() {
  elements.outputsPanel.replaceChildren();
  if (!state.outputDevices.length) {
    emptyPanel(elements.outputsPanel, "Выходные устройства не найдены");
    return;
  }
  for (const device of state.outputDevices) {
    const card = createDeviceCard(device, "output");
    for (const output of device.outputs || []) {
      const section = document.createElement("section");
      section.className = "io-card";
      const isMain = state.mainOutput?.outputDeviceId === device.id && state.mainOutput?.outputId === output.id;
      const heading = createIoHeader(
        output.name || "Output",
        output.id,
        output.id,
        output.isMuted,
        () => setOutputValue(device, output, "isMuted", !output.isMuted),
      );
      const main = document.createElement("button");
      main.type = "button";
      main.className = `compact-action${isMain ? " is-main" : ""}`;
      main.textContent = isMain ? "MAIN" : "SET MAIN";
      main.addEventListener("click", () => setMainOutput(device.id, output.id));
      heading.append(main);
      section.append(heading);
      section.append(createRangeControl("LEVEL", output.level, formatDb, (value) => setOutputValue(device, output, "level", value)));
      const route = document.createElement("div");
      route.className = "select-row";
      const label = document.createElement("label");
      label.textContent = "MIX";
      const select = document.createElement("select");
      select.setAttribute("aria-label", `Микс выхода ${output.name || output.id}`);
      select.add(new Option("Не назначен", ""));
      for (const mix of state.mixes) select.add(new Option(mix.name || mix.id, mix.id));
      select.value = output.mixId || "";
      select.addEventListener("change", () => setOutputValue(device, output, "mixId", select.value));
      route.append(label, select);
      section.append(route);
      card.append(section);
    }
    elements.outputsPanel.append(card);
  }
}

async function setOutputValue(device, output, key, value) {
  const previous = output[key];
  output[key] = value;
  renderOutputsPanel();
  const outputPatch = { id: output.id, [key]: value };
  try {
    try {
      await sendRpc("setOutputDevice", { outputDevice: { id: device.id, outputs: [outputPatch] } });
    } catch (error) {
      if (error.code !== -32602) throw error;
      await sendRpc("setOutputDevice", { id: device.id, outputs: [outputPatch] });
    }
  } catch (error) {
    output[key] = previous;
    renderOutputsPanel();
    showToast(`Выход не обновлён: ${rpcErrorMessage(error)}`, "error");
  }
}

async function setMainOutput(deviceId, outputId) {
  const previous = state.mainOutput;
  const mainOutput = { outputDeviceId: deviceId, outputId };
  state.mainOutput = mainOutput;
  renderOutputs();
  try {
    try {
      await sendRpc("setOutputDevice", { mainOutput });
    } catch (error) {
      if (error.code !== -32602) throw error;
      await sendRpc("setOutputDevice", mainOutput);
    }
  } catch (error) {
    state.mainOutput = previous;
    renderOutputs();
    showToast(`Главный выход не выбран: ${rpcErrorMessage(error)}`, "error");
  }
}

function allApplications() {
  const applications = new Map();
  if (state.focusedApp?.id) applications.set(state.focusedApp.id, state.focusedApp);
  for (const channel of state.channels) {
    for (const app of channel.apps || []) applications.set(app.id, app);
  }
  return [...applications.values()];
}

function renderApplications() {
  const focused = state.focusedApp;
  elements.focusedAppName.textContent = focused?.name || "Не определено";
  const focusedChannel = state.channels.find((channel) => channel.id === focused?.channel?.id);
  elements.focusedAppChannel.textContent = focusedChannel ? `Канал: ${focusedChannel.name || focusedChannel.id}` : "Канал не назначен";
  const selectedApp = elements.applicationSelect.value;
  elements.applicationSelect.replaceChildren();
  for (const app of allApplications()) elements.applicationSelect.add(new Option(app.name || app.id, app.id));
  if (selectedApp) elements.applicationSelect.value = selectedApp;
  elements.applicationChannelSelect.replaceChildren();
  for (const channel of state.channels) elements.applicationChannelSelect.add(new Option(channel.name || channel.id, channel.id));
}

function renderSystemInfo() {
  const info = state.applicationInfo || {};
  $("#info-version").textContent = info.version || info.build || "—";
  $("#info-interface").textContent = info.interfaceRevision ?? "—";
  $("#info-platform").textContent = info.operatingSystem || "—";
}

async function syncPluginInfo() {
  const devices = elements.pluginDevices.value.split(",").map((item) => item.trim()).filter(Boolean);
  try {
    await sendRpc("setPluginInfo", { connectedDevices: devices });
  } catch (error) {
    showToast(`Plugin info не обновлён: ${rpcErrorMessage(error)}`, "error");
  }
}

function formatPercent(value) { return `${Math.round(clamp(value) * 100)}%`; }
function formatBalance(value) {
  const percent = Math.round(clamp(value) * 100);
  if (percent === 50) return "CENTER";
  return percent < 50 ? `MIC ${100 - percent}` : `PC ${percent}`;
}

function renderConsole() {
  const channels = visibleChannels();
  state.stripByChannel.clear();
  elements.bank.replaceChildren();
  const hasConsole = channels.length > 0;
  elements.empty.hidden = hasConsole;
  elements.bank.hidden = !hasConsole;
  elements.channelCount.textContent = `${channels.length} ${pluralChannels(channels.length)}`;
  if (!hasConsole) {
    if (state.waveLinkConnected && state.selectedMixId) {
      elements.emptyHeading.textContent = "В этом миксе нет каналов";
      elements.emptyText.textContent = "Добавьте канал в выбранный микс через Wave Link или выберите другой микс сверху.";
    }
    setConnectionUi(state.waveLinkConnected ? "online" : "connecting", state.waveLinkConnected ? "Wave Link подключён" : "Ожидаем Wave Link");
    return;
  }

  channels.forEach((channel, index) => {
    const strip = createChannelStrip(channel, index);
    state.stripByChannel.set(channel.id, strip);
    elements.bank.append(strip);
  });
  const mix = selectedMix();
  if (mix) elements.bank.append(createMasterStrip(mix));
}

function createChannelStrip(channel, index) {
  const strip = $("#channel-template").content.firstElementChild.cloneNode(true);
  strip.dataset.channelId = channel.id;
  strip.style.setProperty("--strip-accent", channelColour(channel, index));
  $(".channel-number", strip).textContent = String(index + 1).padStart(2, "0");
  $(".channel-name", strip).textContent = channel.name || `Channel ${index + 1}`;
  $(".channel-type", strip).textContent = channelSubtitle(channel);
  $(".channel-icon", strip).innerHTML = iconForChannel(channel);
  $(".channel-identity", strip).addEventListener("click", () => openChannelInspector(channel.id));
  renderEffects(strip, channel);
  wireStripControls(strip, {
    getLevel: () => clamp(channelValue(currentChannel(channel.id), "level")),
    setLevel: (level, commit) => setChannelControl(channel.id, "level", level, commit),
    getMuted: () => Boolean(channelValue(currentChannel(channel.id), "isMuted")),
    setMuted: (muted) => setChannelControl(channel.id, "isMuted", muted, true),
  });
  updateStripVisual(strip, channelValue(channel, "level"), channelValue(channel, "isMuted"));
  applyMeterVisual(strip, state.meters.get(channel.id));
  return strip;
}

function openChannelInspector(channelId) {
  const channel = currentChannel(channelId);
  if (!channel) return;
  $("#channel-dialog-name").textContent = channel.name || "Канал";
  elements.channelInspector.replaceChildren();
  const section = document.createElement("section");
  section.className = "io-card";
  section.append(createIoHeader(
    channel.name || "Channel",
    "ОБЩИЙ УРОВЕНЬ ИСТОЧНИКА",
    channel.id,
    channel.isMuted,
    () => setChannelGlobalValue(channel, "isMuted", !channel.isMuted),
  ));
  section.append(createRangeControl("LEVEL", channel.level, formatDb, (value) => {
    setChannelGlobalValue(channel, "level", value);
  }));
  const effects = document.createElement("div");
  effects.className = "io-effects";
  for (const effect of channel.effects || []) {
    if (effect.isSupported === false) continue;
    const button = document.createElement("button");
    button.type = "button";
    button.className = `io-effect${effect.isEnabled ? " is-enabled" : ""}`;
    button.textContent = effect.name || "Effect";
    button.ariaPressed = String(Boolean(effect.isEnabled));
    button.addEventListener("click", () => setChannelGlobalEffect(channel, effect, !effect.isEnabled));
    effects.append(button);
  }
  section.append(effects);
  elements.channelInspector.append(section);
  if (!elements.channelDialog.open) elements.channelDialog.showModal();
}

async function setChannelGlobalValue(channel, key, value) {
  const previous = channel[key];
  channel[key] = value;
  openChannelInspector(channel.id);
  try {
    await sendRpc("setChannel", { id: channel.id, [key]: value });
  } catch (error) {
    channel[key] = previous;
    openChannelInspector(channel.id);
    showToast(`Канал не обновлён: ${rpcErrorMessage(error)}`, "error");
  }
}

async function setChannelGlobalEffect(channel, effect, enabled) {
  const previous = effect.isEnabled;
  effect.isEnabled = enabled;
  openChannelInspector(channel.id);
  try {
    await sendRpc("setChannel", { id: channel.id, effects: [{ id: effect.id, isEnabled: enabled }] });
  } catch (error) {
    effect.isEnabled = previous;
    openChannelInspector(channel.id);
    showToast(`Эффект канала не переключён: ${rpcErrorMessage(error)}`, "error");
  }
}

function createMasterStrip(mix) {
  const strip = $("#master-template").content.firstElementChild.cloneNode(true);
  strip.dataset.mixId = mix.id;
  $(".channel-name", strip).textContent = mix.name || "Main Mix";
  wireStripControls(strip, {
    getLevel: () => clamp(selectedMix()?.level),
    setLevel: (level, commit) => setMasterControl("level", level, commit),
    getMuted: () => Boolean(selectedMix()?.isMuted),
    setMuted: (muted) => setMasterControl("isMuted", muted, true),
  });
  updateStripVisual(strip, mix.level, mix.isMuted);
  applyMeterVisual(strip, state.meters.get(mix.id));
  return strip;
}

function wireStripControls(strip, controls) {
  const fader = $(".fader", strip);
  const mute = $(".mute-button", strip);
  let activePointer = null;

  const updateFromPointer = (event, commit = false) => {
    const rect = fader.getBoundingClientRect();
    const pad = Math.min(30, rect.height * .09);
    const level = clamp((rect.bottom - pad - event.clientY) / Math.max(1, rect.height - pad * 2));
    controls.setLevel(level, commit);
    updateStripVisual(strip, level, controls.getMuted());
  };

  fader.addEventListener("pointerdown", (event) => {
    activePointer = event.pointerId;
    fader.setPointerCapture(event.pointerId);
    updateFromPointer(event);
  });
  fader.addEventListener("pointermove", (event) => {
    if (event.pointerId === activePointer) updateFromPointer(event);
  });
  const endPointer = (event) => {
    if (event.pointerId !== activePointer) return;
    updateFromPointer(event, true);
    activePointer = null;
  };
  fader.addEventListener("pointerup", endPointer);
  fader.addEventListener("pointercancel", () => { activePointer = null; });
  fader.addEventListener("keydown", (event) => {
    const deltas = { ArrowUp: .01, ArrowRight: .01, ArrowDown: -.01, ArrowLeft: -.01, PageUp: .05, PageDown: -.05 };
    let next;
    if (event.key === "Home") next = 1;
    else if (event.key === "End") next = 0;
    else if (deltas[event.key] != null) next = clamp(controls.getLevel() + deltas[event.key]);
    else return;
    event.preventDefault();
    controls.setLevel(next, true);
    updateStripVisual(strip, next, controls.getMuted());
  });
  mute.addEventListener("click", () => controls.setMuted(!controls.getMuted()));
}

function setChannelControl(channelId, key, value, commit) {
  const channel = currentChannel(channelId);
  if (!channel) return;
  const mix = channelMix(channel);
  let params;
  if (state.selectedMixId) {
    if (!mix) return;
    const idKey = Object.hasOwn(mix, "mixId") && !Object.hasOwn(mix, "id") ? "mixId" : "id";
    Object.assign(mix, { [key]: value });
    params = { id: channelId, mixes: [{ [idKey]: state.selectedMixId, [key]: value }] };
  } else {
    channel[key] = value;
    params = { id: channelId, [key]: value };
  }
  const strip = state.stripByChannel.get(channelId);
  if (strip) updateStripVisual(strip, channelValue(channel, "level"), channelValue(channel, "isMuted"));
  scheduleControl(`channel:${channelId}:${key}`, "setChannel", params, commit);
}

function setMasterControl(key, value, commit) {
  const mix = selectedMix();
  if (!mix) return;
  mix[key] = value;
  updateMaster();
  scheduleControl(`mix:${mix.id}:${key}`, "setMix", { id: mix.id, [key]: value }, commit);
}

function scheduleControl(key, method, params, immediate) {
  const existing = state.throttles.get(key);
  if (existing) {
    clearTimeout(existing.timer);
    existing.params = params;
  }
  const send = async () => {
    const current = state.throttles.get(key);
    if (!current) return;
    state.throttles.delete(key);
    try {
      await sendRpc(method, current.params);
    } catch (error) {
      showToast(`Команда не выполнена: ${rpcErrorMessage(error)}`, "error");
      hydrate();
    }
  };
  if (immediate) {
    state.throttles.set(key, { params, timer: 0 });
    send();
  } else {
    const item = existing || { params, timer: 0 };
    item.params = params;
    item.timer = window.setTimeout(send, 55);
    state.throttles.set(key, item);
  }
}

function updateChannel(channelId) {
  const strip = state.stripByChannel.get(channelId);
  const channel = currentChannel(channelId);
  if (!channel) {
    renderConsole();
    return;
  }
  const belongsToActiveMix = channelMix(channel) !== null;
  if (Boolean(strip) !== belongsToActiveMix) {
    renderConsole();
    return;
  }
  if (!strip) return;
  updateStripVisual(strip, channelValue(channel, "level"), channelValue(channel, "isMuted"));
  renderEffects(strip, channel);
}

function updateMaster() {
  const strip = $(".master-strip", elements.bank);
  const mix = selectedMix();
  if (!strip || !mix) return;
  $(".channel-name", strip).textContent = mix.name || "Main Mix";
  updateStripVisual(strip, mix.level, mix.isMuted);
}

function updateStripVisual(strip, rawLevel, muted) {
  const level = clamp(rawLevel);
  const percentage = level * 100;
  const fader = $(".fader", strip);
  $(".fader-cap", strip).style.bottom = `clamp(29px, ${percentage}%, calc(100% - 29px))`;
  $(".fader-progress", strip).style.height = `${percentage}%`;
  $(".level-readout", strip).innerHTML = `${formatDb(level)} <small>dB</small>`;
  fader.setAttribute("aria-valuenow", String(Math.round(percentage)));
  fader.setAttribute("aria-valuetext", `${formatDb(level)} децибел`);
  const mute = $(".mute-button", strip);
  mute.ariaPressed = String(Boolean(muted));
  strip.classList.toggle("is-muted", Boolean(muted));
}

function renderEffects(strip, channel) {
  const rack = $(".effect-rack", strip);
  rack.replaceChildren();
  for (const [index, effect] of (channel.effects || []).slice(0, 4).entries()) {
    if (effect.isSupported === false) continue;
    const button = document.createElement("button");
    button.type = "button";
    button.className = `effect-button${effect.isEnabled ? " is-enabled" : ""}`;
    button.textContent = effectShortName(effect.name, index);
    button.title = effect.name || `Effect ${index + 1}`;
    button.ariaPressed = String(Boolean(effect.isEnabled));
    button.addEventListener("click", async () => {
      const next = !effect.isEnabled;
      effect.isEnabled = next;
      button.classList.toggle("is-enabled", next);
      button.ariaPressed = String(next);
      try {
        await sendRpc("setChannel", { id: channel.id, effects: [{ id: effect.id, isEnabled: next }] });
      } catch (error) {
        effect.isEnabled = !next;
        button.classList.toggle("is-enabled", !next);
        showToast(`Эффект не переключён: ${rpcErrorMessage(error)}`, "error");
      }
    });
    rack.append(button);
  }
}

function updateMeters(payload) {
  const groups = [payload.channels, payload.mixes, payload.inputDevices, payload.outputDevices];
  for (const entries of groups) {
    for (const meter of entries || []) {
      state.meters.set(meter.id, meter);
      const strip = state.stripByChannel.get(meter.id) || $(`.master-strip[data-mix-id="${CSS.escape(meter.id)}"]`, elements.bank);
      if (strip) applyMeterVisual(strip, meter);
      for (const mini of document.querySelectorAll(".mini-meter[data-meter-id]")) {
        if (mini.dataset.meterId === meter.id) applyMiniMeter(mini, meter);
      }
    }
  }
}

function applyMiniMeter(element, meter) {
  if (!element || !meter) return;
  const fills = element.querySelectorAll("i");
  if (fills[0]) fills[0].style.height = `${meterHeight(meter.levelLeftPercentage)}%`;
  if (fills[1]) fills[1].style.height = `${meterHeight(meter.levelRightPercentage ?? meter.levelLeftPercentage)}%`;
}

function applyMeterVisual(strip, meter) {
  if (!strip || !meter) return;
  const left = meterHeight(meter.levelLeftPercentage);
  const right = meterHeight(meter.levelRightPercentage ?? meter.levelLeftPercentage);
  const fills = strip.querySelectorAll(".meter-fill");
  if (fills[0]) fills[0].style.height = `${left}%`;
  if (fills[1]) fills[1].style.height = `${right}%`;
  const max = Math.max(left, right);
  const led = $(".signal-led", strip);
  led.classList.toggle("is-live", max > 1 && max < 88);
  led.classList.toggle("is-hot", max >= 88);
  const peak = $(".meter-peak", strip);
  if (peak && max > Number(peak.dataset.value || 0)) {
    peak.dataset.value = String(max);
    peak.style.bottom = `calc(${max}% - 2px)`;
    peak.style.opacity = "1";
    clearTimeout(peak._decayTimer);
    peak._decayTimer = setTimeout(() => { peak.dataset.value = "0"; peak.style.opacity = "0"; }, 900);
  }
}

function meterHeight(rawValue) {
  const value = Number(rawValue);
  if (!Number.isFinite(value) || value <= 0) return 0;

  // Wave Link 3.x на практике отдаёт нормализованную амплитуду 0…1,
  // несмотря на слово Percentage в имени поля. Старые варианты API могли
  // присылать уже готовое значение 0…100, поэтому поддерживаем обе формы.
  if (value > 1) return clamp(value, 0, 100);

  // Логарифмическая шкала −60…0 dB соответствует поведению аппаратного meter.
  const decibels = 20 * Math.log10(value);
  return clamp((decibels + 60) / 60) * 100;
}

function currentChannel(id) { return state.channels.find((channel) => channel.id === id); }

function formatDb(level) {
  if (level <= .001) return "−∞";
  const db = Math.max(-60, 20 * Math.log10(level));
  return `${db > -10 ? db.toFixed(1) : Math.round(db)}`.replace("-", "−");
}

function pluralChannels(count) {
  const mod10 = count % 10, mod100 = count % 100;
  if (mod10 === 1 && mod100 !== 11) return "канал";
  if (mod10 >= 2 && mod10 <= 4 && !(mod100 >= 12 && mod100 <= 14)) return "канала";
  return "каналов";
}

function channelColour(channel, index) {
  const type = `${channel.type || ""} ${channel.name || ""}`.toLowerCase();
  if (/mic|мик|voice|голос/.test(type)) return "#17d3a2";
  if (/music|музык|browser|брауз/.test(type)) return "#1bb4e9";
  if (/game|игр/.test(type)) return "#9d78ee";
  return palette[index % palette.length];
}

function channelSubtitle(channel) {
  if (channel.apps?.length) return channel.apps.map((app) => app.name).filter(Boolean).join(" · ").toUpperCase();
  return (channel.type || "AUDIO CHANNEL").replaceAll("_", " ").toUpperCase();
}

function effectShortName(name, index) {
  if (!name) return `FX${index + 1}`;
  const words = name.match(/[\p{L}\p{N}]+/gu) || [];
  return (words.length > 1 ? words.map((word) => word[0]).join("") : words[0]?.slice(0, 3) || "FX").toUpperCase().slice(0, 3);
}

function iconForChannel(channel) {
  const type = `${channel.type || ""} ${channel.name || ""}`.toLowerCase();
  if (/mic|мик|voice|голос/.test(type)) return '<svg viewBox="0 0 24 24"><rect x="8" y="3" width="8" height="13" rx="4"/><path d="M5 11a7 7 0 0 0 14 0M12 18v3M9 21h6"/></svg>';
  if (/music|музык/.test(type)) return '<svg viewBox="0 0 24 24"><path d="M9 18V6l11-2v12"/><circle cx="6" cy="18" r="3"/><circle cx="17" cy="16" r="3"/></svg>';
  if (/game|игр/.test(type)) return '<svg viewBox="0 0 24 24"><path d="M8 8h8a5 5 0 0 1 4.7 3.3l1.1 3.2a3 3 0 0 1-5.1 3l-1.3-1.5H8.6l-1.3 1.5a3 3 0 0 1-5.1-3l1.1-3.2A5 5 0 0 1 8 8Z"/><path d="M7 11v4M5 13h4M16.5 12h.01M19 14h.01"/></svg>';
  if (/browser|брауз|system|систем/.test(type)) return '<svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="M3 12h18M12 3a14 14 0 0 1 0 18M12 3a14 14 0 0 0 0 18"/></svg>';
  return '<svg viewBox="0 0 24 24"><path d="M4 9v6M8 6v12M12 3v18M16 7v10M20 10v4"/></svg>';
}

elements.settingsButton = $("#settings-button");
elements.settingsButton.addEventListener("click", () => elements.settings.showModal());
elements.connectionButton.addEventListener("click", () => elements.settings.showModal());
$("#inputs-button").addEventListener("click", () => { renderInputsPanel(); elements.inputsDialog.showModal(); });
$("#routing-button").addEventListener("click", () => { renderOutputsPanel(); elements.routingDialog.showModal(); });
$("#apps-button").addEventListener("click", () => { renderApplications(); elements.appsDialog.showModal(); });
for (const button of document.querySelectorAll("[data-close-dialog]")) {
  button.addEventListener("click", () => document.getElementById(button.dataset.closeDialog)?.close());
}
$("#empty-settings-button").addEventListener("click", () => elements.settings.showModal());
$("#reset-url-button").addEventListener("click", () => { elements.serverUrl.value = defaultServerUrl(); });
elements.settingsForm.addEventListener("submit", (event) => {
  const submitter = event.submitter;
  if (submitter?.value === "cancel") return;
  event.preventDefault();
  localStorage.setItem("wavelink.pluginDevices", elements.pluginDevices.value);
  elements.settings.close();
  connect({ announce: true });
});
elements.refresh.addEventListener("click", () => hydrate({ announce: true }));
elements.outputSelect.addEventListener("change", () => {
  const option = elements.outputSelect.selectedOptions[0];
  if (!option?.dataset.deviceId) return;
  setMainOutput(option.dataset.deviceId, option.dataset.outputId);
});
$("#route-app-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const appId = elements.applicationSelect.value;
  const channelId = elements.applicationChannelSelect.value;
  if (!appId || !channelId) {
    showToast("Выберите приложение и канал", "warning");
    return;
  }
  try {
    await sendRpc("addToChannel", { appId, channelId });
    showToast("Приложение назначено каналу", "success");
    await hydrate();
  } catch (error) {
    showToast(`Приложение не назначено: ${rpcErrorMessage(error)}`, "error");
  }
});
document.addEventListener("visibilitychange", () => {
  if (!document.hidden && (!state.socket || state.socket.readyState > WebSocket.OPEN)) connect();
});

elements.serverUrl.value = savedServerUrl();
elements.pluginDevices.value = localStorage.getItem("wavelink.pluginDevices") || "";
connect();
