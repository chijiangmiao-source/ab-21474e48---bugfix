/* 低温阵列扫描监控 —— 监看页逻辑。
 *
 * 关键保证：
 * - 每个事件都校验会话令牌 + 代号：旧连接的迟到事件、代际不符事件一律忽略，
 *   绝不改写当前画面。
 * - 帧序号必须连续（lastSeq + 1）；发现缺口立即带游标重连修复。
 * - 服务端仅保留最近 32 帧；游标过旧时会收到 reset 事件，客户端原子替换全部状态。
 * - 不可恢复响应（409 异参复用 / 410 代已关闭 / 404 代号不存在）显示明确状态。
 */
"use strict";

const MAX_FRAMES_SHOWN = 50;
const MAX_LOG_LINES = 100;

const state = {
  code: null,       // 当前采集代号
  lastSeq: 0,       // 已连续消费到的序号（高水位）
  totals: {},       // 各通道累计值
  frames: [],       // 最近帧（展示用，新的在前）
  session: 0,       // 会话令牌：新开采集时递增，迟到事件据此被忽略
  es: null,         // 当前 EventSource
  lastAppend: null, // 上次成功提交的 {operationId, deltas}，供幂等重发
};

function $(id) {
  return document.getElementById(id);
}

function setStatus(kind, text) {
  const cls = {
    idle: "badge badge-idle",
    ok: "badge badge-ok",
    info: "badge badge-info",
    warn: "badge badge-warn",
    err: "badge badge-err",
  }[kind] || "badge";
  const el = $("conn-status");
  el.className = cls;
  el.textContent = text;
}

function logEvent(text, important) {
  const li = document.createElement("li");
  li.textContent = `[${new Date().toLocaleTimeString()}] ${text}`;
  if (important) li.className = "important";
  const log = $("event-log");
  log.prepend(li);
  while (log.children.length > MAX_LOG_LINES) log.removeChild(log.lastChild);
}

/* 事件门卫：会话令牌与代号双重校验，迟到/代际不符事件不得改写画面。 */
function guard(token, eventCode) {
  if (token !== state.session) {
    return false; // 旧连接的迟到事件，静默丢弃
  }
  if (eventCode && eventCode !== state.code) {
    setStatus("warn", `代际不符：事件属于 ${eventCode}，已忽略`);
    logEvent(`代际不符事件已忽略（属于 ${eventCode}）`, true);
    return false;
  }
  return true;
}

/* ------------------------------------------------------------ 渲染 */

function fmtNum(n) {
  if (typeof n !== "number") return String(n);
  return Number.isInteger(n) ? String(n) : n.toPrecision(6).replace(/\.?0+$/, "");
}

function render() {
  $("gen-code").textContent = state.code || "—";
  $("high-water").textContent = String(state.lastSeq);

  const totalsBody = document.querySelector("#totals-table tbody");
  totalsBody.textContent = "";
  const channels = Object.keys(state.totals).sort();
  if (channels.length === 0) {
    totalsBody.innerHTML = '<tr class="empty"><td colspan="2">暂无数据</td></tr>';
  } else {
    for (const ch of channels) {
      const tr = document.createElement("tr");
      const tdName = document.createElement("td");
      const tdVal = document.createElement("td");
      tdName.textContent = ch;
      tdVal.textContent = fmtNum(state.totals[ch]);
      tr.append(tdName, tdVal);
      totalsBody.appendChild(tr);
    }
  }

  const framesBody = document.querySelector("#frames-table tbody");
  framesBody.textContent = "";
  if (state.frames.length === 0) {
    framesBody.innerHTML = '<tr class="empty"><td colspan="3">暂无帧</td></tr>';
  } else {
    for (const f of state.frames) {
      const tr = document.createElement("tr");
      const tdSeq = document.createElement("td");
      const tdOp = document.createElement("td");
      const tdDelta = document.createElement("td");
      tdSeq.textContent = String(f.seq);
      tdOp.textContent = f.operationId;
      const deltaText = Object.entries(f.deltas)
        .map(([k, v]) => `${k}:${fmtNum(v)}`)
        .join("  ");
      const totalText = Object.entries(f.totals || {})
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([k, v]) => `${k}:${fmtNum(v)}`)
        .join("  ");
      tdDelta.textContent = totalText ? `Δ ${deltaText} ｜ Σ ${totalText}` : deltaText;
      tr.append(tdSeq, tdOp, tdDelta);
      framesBody.appendChild(tr);
    }
  }
}

/* ------------------------------------------------------------ 事件流 */

function closeStream() {
  if (state.es) {
    state.es.close();
    state.es = null;
  }
}

/* cursor 为 null 表示全新连接（服务端先发快照）；否则按游标补发缺口。 */
function connect(cursor) {
  closeStream();
  const token = state.session;
  const code = state.code;
  if (!code) return;

  setStatus("info", cursor === null ? "连接中…" : `重连中（游标 ${cursor}）…`);
  let url = `/api/acquisitions/${encodeURIComponent(code)}/stream`;
  if (cursor !== null) url += `?cursor=${cursor}`;

  const es = new EventSource(url);
  state.es = es;

  es.onopen = () => {
    if (token === state.session) setStatus("ok", "已连接");
  };

  es.onerror = () => {
    if (token !== state.session) return;
    if (es.readyState === EventSource.CLOSED) {
      setStatus("err", "连接已关闭且不可恢复");
    } else {
      setStatus("warn", "连接中断，自动重连中…");
    }
  };

  es.addEventListener("snapshot", (ev) => {
    const data = JSON.parse(ev.data);
    if (!guard(token, data.code)) return;
    applySnapshot(data);
    logEvent(`快照已加载：高水位 ${data.highWater}`);
    setStatus("ok", "已连接");
  });

  es.addEventListener("reset", (ev) => {
    const data = JSON.parse(ev.data);
    if (!guard(token, data.code)) return;
    applySnapshot(data);
    logEvent(`游标过旧：已接收完整重置并原子替换（高水位 ${data.highWater}）`, true);
    setStatus("ok", "已连接（已重置）");
  });

  es.addEventListener("frame", (ev) => {
    const data = JSON.parse(ev.data);
    if (!guard(token, data.code)) return;
    applyFrame(data);
  });

  es.addEventListener("superseded", (ev) => {
    if (token !== state.session) return;
    let data = {};
    try { data = JSON.parse(ev.data); } catch (e) { /* 忽略解析失败 */ }
    if (data.code && data.code !== state.code) return;
    setStatus("warn", "本代采集已被新代替换");
    logEvent("本代采集已被新代替换，流已关闭", true);
    es.close();
  });
}

/* 快照/重置：原子替换全部展示状态。 */
function applySnapshot(data) {
  state.totals = Object.assign({}, data.totals);
  state.lastSeq = data.highWater;
  const recent = Array.isArray(data.frames) ? data.frames : [];
  state.frames = recent
    .filter((frame) => frame && frame.seq <= data.highWater)
    .sort((a, b) => b.seq - a.seq)
    .slice(0, MAX_FRAMES_SHOWN)
    .map((frame) => ({
      seq: frame.seq,
      operationId: frame.operationId,
      deltas: Object.assign({}, frame.deltas),
      totals: Object.assign({}, frame.totals),
    }));
  render();
}

/* 增量帧：严格校验序号连续性。 */
function applyFrame(data) {
  const seq = data.seq;
  if (seq <= state.lastSeq) {
    logEvent(`重复帧 #${seq} 已忽略`);
    return;
  }
  if (seq > state.lastSeq + 1) {
    setStatus("warn", `序号缺口：期望 ${state.lastSeq + 1}，收到 ${seq}，修复中…`);
    logEvent(`检测到序号缺口（${state.lastSeq + 1}..${seq - 1}），带游标重连修复`, true);
    connect(state.lastSeq);
    return;
  }
  for (const [ch, v] of Object.entries(data.deltas)) {
    state.totals[ch] = (state.totals[ch] || 0) + v;
  }
  state.lastSeq = seq;
  state.frames.unshift({
    seq,
    operationId: data.operationId,
    deltas: data.deltas,
    totals: Object.assign({}, data.totals || state.totals),
  });
  if (state.frames.length > MAX_FRAMES_SHOWN) state.frames.pop();
  render();
}

/* ------------------------------------------------------------ API */

async function apiJson(method, url, body) {
  const resp = await fetch(url, {
    method,
    headers: body ? { "Content-Type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  let data = null;
  try { data = await resp.json(); } catch (e) { /* 非 JSON 响应 */ }
  return { status: resp.status, data };
}

async function startNewGeneration() {
  state.session += 1;   // 使旧连接的全部迟到事件失效
  closeStream();
  setStatus("info", "正在新开采集…");
  let res;
  try {
    res = await apiJson("POST", "/api/acquisitions");
  } catch (e) {
    setStatus("err", "不可恢复：无法创建新采集（网络错误）");
    return;
  }
  if (res.status !== 201 || !res.data || !res.data.code) {
    setStatus("err", "不可恢复：创建新采集被拒绝");
    return;
  }
  state.code = res.data.code;
  state.lastSeq = 0;
  state.totals = {};
  state.frames = [];
  render();
  logEvent(`新一代采集已开始：${state.code}`, true);
  connect(null);
}

async function appendFrame(operationId, deltas) {
  if (!state.code) {
    setStatus("warn", "请先新开采集");
    return;
  }
  let res;
  try {
    res = await apiJson("POST", `/api/acquisitions/${encodeURIComponent(state.code)}/frames`, {
      operationId,
      deltas,
    });
  } catch (e) {
    setStatus("err", "追加失败：网络错误，请重试");
    return;
  }
  const d = res.data || {};
  if (res.status === 201) {
    logEvent(`帧 #${d.seq} 已受理（操作 ${operationId}）`);
    state.lastAppend = { operationId, deltas };
    $("btn-replay").disabled = false;
    regenOperationId();
  } else if (res.status === 200 && d.replayed) {
    logEvent(`幂等重放：操作 ${operationId} 返回原序号 #${d.seq}，未产生新帧`, true);
  } else if (res.status === 409) {
    setStatus("err", "不可恢复：操作标识被异参复用，已稳定拒绝");
    logEvent(`冲突：${d.message || "操作标识异参复用"}`, true);
  } else if (res.status === 410) {
    setStatus("err", "不可恢复：采集代已关闭");
    logEvent("追加被拒绝：采集代已关闭", true);
  } else if (res.status === 404) {
    setStatus("err", "不可恢复：代号不存在");
    logEvent("追加被拒绝：代号不存在", true);
  } else {
    setStatus("err", `追加失败：${(d && d.message) || `HTTP ${res.status}`}`);
  }
}

/* ------------------------------------------------------------ 表单 */

function regenOperationId() {
  $("op-id").value = `op-${Date.now().toString(36)}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;
}

function addDeltaRow(name, value) {
  const row = document.createElement("div");
  row.className = "delta-row";
  const nameInput = document.createElement("input");
  nameInput.type = "text";
  nameInput.placeholder = "通道名";
  nameInput.value = name || "";
  const valInput = document.createElement("input");
  valInput.type = "number";
  valInput.step = "any";
  valInput.placeholder = "增量";
  valInput.value = value === undefined ? "" : String(value);
  const removeBtn = document.createElement("button");
  removeBtn.type = "button";
  removeBtn.className = "remove";
  removeBtn.textContent = "×";
  removeBtn.addEventListener("click", () => row.remove());
  row.append(nameInput, valInput, removeBtn);
  $("delta-rows").appendChild(row);
}

function collectDeltas() {
  const deltas = {};
  for (const row of document.querySelectorAll("#delta-rows .delta-row")) {
    const [nameInput, valInput] = row.querySelectorAll("input");
    const name = nameInput.value.trim();
    if (!name) continue;
    const value = Number(valInput.value);
    if (valInput.value.trim() === "" || !Number.isFinite(value)) {
      setStatus("warn", `通道 ${name} 的增量无效`);
      return null;
    }
    deltas[name] = value;
  }
  if (Object.keys(deltas).length === 0) {
    setStatus("warn", "至少填写一个通道增量");
    return null;
  }
  return deltas;
}

/* ------------------------------------------------------------ 启动 */

function init() {
  $("btn-new-gen").addEventListener("click", startNewGeneration);
  $("btn-regen-op").addEventListener("click", regenOperationId);
  $("btn-add-row").addEventListener("click", () => addDeltaRow("", ""));
  $("btn-append").addEventListener("click", () => {
    const operationId = $("op-id").value.trim();
    if (!operationId) {
      setStatus("warn", "请填写操作标识");
      return;
    }
    const deltas = collectDeltas();
    if (deltas) appendFrame(operationId, deltas);
  });
  $("btn-replay").addEventListener("click", () => {
    if (state.lastAppend) {
      appendFrame(state.lastAppend.operationId, state.lastAppend.deltas);
    }
  });

  regenOperationId();
  addDeltaRow("ch0", "");
  addDeltaRow("ch1", "");
  addDeltaRow("ch2", "");
  setStatus("idle", "未连接");
  render();
  startNewGeneration(); // 打开页面即开始一代采集
}

document.addEventListener("DOMContentLoaded", init);
