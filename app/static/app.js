/* Webhook Replay Lab — frontend (vanilla JS) */
"use strict";

const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g,
  (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

const SAMPLE = [
  { event_id: "evt-001", type: "invoice.created", customer: "Acme", amount: 100, currency: "CNY" },
  { event_id: "evt-002", type: "invoice.paid", customer: "Acme", amount: 100, currency: "CNY", paid_at: "2026-09-22T10:00:00Z" },
  { event_id: "evt-003", type: "invoice.refunded", customer: "Acme", amount: 50, currency: "CNY" },
];

const STATUS_LABEL = {
  pending: "待发送", queued: "排队中", running: "发送中…",
  success: "成功", failed: "失败",
};

let lastEvents = [];
let targetsConfig = { mode: "single", targets: [], count: 0, legacy_default: {} };

async function api(path, opts = {}) {
  const resp = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...opts,
  });
  const text = await resp.text();
  const data = text ? JSON.parse(text) : {};
  if (!resp.ok) throw new Error(typeof data.detail === "string" ? data.detail : JSON.stringify(data.detail));
  return data;
}

function flash(el, text, ok = true) {
  el.textContent = text;
  el.style.color = ok ? "var(--green)" : "var(--red)";
  setTimeout(() => { el.textContent = ""; }, 5000);
}

// ---------- settings: legacy single target + named fanout targets ----------

async function loadSettings() {
  const s = await api("/api/settings");
  $("target_url").value = s.target_url;
  $("hmac_secret").value = s.hmac_secret;
  $("max_retries").value = s.max_retries;
  $("timeout_seconds").value = s.timeout_seconds;
  await loadTargets();
}

$("save-settings").addEventListener("click", async () => {
  try {
    await api("/api/settings", {
      method: "POST",
      body: JSON.stringify({
        target_url: $("target_url").value.trim(),
        hmac_secret: $("hmac_secret").value,
        max_retries: Number($("max_retries").value),
        timeout_seconds: Number($("timeout_seconds").value),
      }),
    });
    flash($("settings-msg"), "已保存 ✓");
    await loadTargets();
  } catch (e) { flash($("settings-msg"), e.message, false); }
});

$("tab-single").addEventListener("click", () => switchPane("single"));
$("tab-fanout").addEventListener("click", () => switchPane("fanout"));

function switchPane(mode) {
  const fan = mode === "fanout";
  $("pane-single").classList.toggle("hidden", fan);
  $("pane-fanout").classList.toggle("hidden", !fan);
  $("tab-single").classList.toggle("active", !fan);
  $("tab-fanout").classList.toggle("active", fan);
}

function targetRowHtml(t = {}) {
  return `<tr class="tgt-row">
    <td><input class="t-name" value="${esc(t.name || "")}" placeholder="billing" maxlength="64"/></td>
    <td><input class="t-url" value="${esc(t.url || "")}" placeholder="http://127.0.0.1:8000/recv"/></td>
    <td><input class="t-secret" value="${esc(t.hmac_secret ?? "")}" placeholder="shhh"/></td>
    <td><input class="t-retries" type="number" min="0" max="10" value="${esc(t.max_retries ?? 3)}"/></td>
    <td><input class="t-timeout" type="number" min="0.5" max="60" step="0.5" value="${esc(t.timeout_seconds ?? 5)}"/></td>
    <td><button class="mini danger t-del" type="button">✕</button></td>
  </tr>`;
}

async function loadTargets() {
  targetsConfig = await api("/api/targets");
  const body = $("targets-body");
  if (targetsConfig.targets.length) {
    body.innerHTML = targetsConfig.targets.map(targetRowHtml).join("");
  } else {
    const d = targetsConfig.legacy_default || {};
    body.innerHTML = [0, 1].map((i) => targetRowHtml(i === 0
      ? { name: "primary", url: d.target_url || "http://127.0.0.1:8000/recv",
          hmac_secret: d.hmac_secret || "shhh", max_retries: d.max_retries ?? 3,
          timeout_seconds: d.timeout_seconds ?? 5 }
      : { name: "secondary", url: "http://127.0.0.1:8000/recv",
          hmac_secret: d.hmac_secret || "shhh", max_retries: d.max_retries ?? 3,
          timeout_seconds: d.timeout_seconds ?? 5 })).join("");
  }
  switchPane(targetsConfig.mode === "fanout" ? "fanout" : "single");
}

$("target-add").addEventListener("click", () => {
  if ($("targets-body").querySelectorAll(".tgt-row").length >= 5) {
    flash($("targets-msg"), "最多配置 5 个目标", false);
    return;
  }
  $("targets-body").insertAdjacentHTML("beforeend", targetRowHtml());
});

$("targets-body").addEventListener("click", (ev) => {
  if (ev.target.classList.contains("t-del")) {
    const rows = $("targets-body").querySelectorAll(".tgt-row");
    if (rows.length <= 2) {
      flash($("targets-msg"), "扇出至少需要 2 个目标；如要单目标请切回单目标模式", false);
      return;
    }
    ev.target.closest(".tgt-row").remove();
  }
});

function collectTargets() {
  return [...$("targets-body").querySelectorAll(".tgt-row")].map((row) => ({
    name: row.querySelector(".t-name").value.trim(),
    url: row.querySelector(".t-url").value.trim(),
    hmac_secret: row.querySelector(".t-secret").value,
    max_retries: Number(row.querySelector(".t-retries").value),
    timeout_seconds: Number(row.querySelector(".t-timeout").value),
  }));
}

$("targets-save").addEventListener("click", async () => {
  const targets = collectTargets();
  if (targets.length < 2 || targets.length > 5) {
    flash($("targets-msg"), `需要 2–5 个目标，当前 ${targets.length} 个`, false);
    return;
  }
  if (targets.some((t) => !t.name || !t.url)) {
    flash($("targets-msg"), "每个目标的名称和 URL 都不能为空", false);
    return;
  }
  try {
    targetsConfig = await api("/api/targets", {
      method: "POST",
      body: JSON.stringify({ targets }),
    });
    flash($("targets-msg"), `已保存 ${targetsConfig.count} 个命名目标 ✓`);
    refreshAll();
  } catch (e) { flash($("targets-msg"), e.message, false); }
});

$("targets-clear").addEventListener("click", async () => {
  if (!confirm("确定删除全部命名目标并回到单目标兼容模式？\n命名目标相关的投递状态与尝试记录会被一并清除。")) return;
  try {
    targetsConfig = await api("/api/targets", { method: "DELETE" });
    await loadTargets();
    flash($("targets-msg"), "已回到单目标模式 ✓");
    refreshAll();
  } catch (e) { flash($("targets-msg"), e.message, false); }
});

// ---------- import ----------

$("load-sample").addEventListener("click", () => {
  $("payload").value = JSON.stringify(SAMPLE, null, 2);
});

$("file").addEventListener("change", async (ev) => {
  const f = ev.target.files[0];
  if (!f) return;
  $("payload").value = await f.text();
});

$("import").addEventListener("click", async () => {
  let doc;
  try {
    doc = JSON.parse($("payload").value);
  } catch (e) {
    flash($("import-msg"), "JSON 解析失败: " + e.message, false);
    return;
  }
  try {
    const r = await api("/api/events/import", { method: "POST", body: JSON.stringify(doc) });
    flash($("import-msg"), `入库 ${r.imported} 条（新增 ${r.created.length} / 覆盖 ${r.updated.length}）✓`);
    refreshAll();
  } catch (e) { flash($("import-msg"), e.message, false); }
});

// ---------- events ----------

function deliveryBadgesHtml(e, deliveriesByEvent) {
  const ds = deliveriesByEvent.get(e.event_id) || [];
  if (!ds.length) {
    if (e.status === "success") return '<span class="badge success">已送达（单目标）</span>';
    if (e.status === "failed") return '<span class="badge failed">失败（单目标）</span>';
    return '<span class="muted">—</span>';
  }
  return ds.map((d) => {
    const cls = d.status === "success" ? "success"
      : d.status === "failed" ? "failed"
      : (d.status === "queued" || d.status === "running") ? "running" : "pending";
    const code = d.last_http_status == null ? "·" : d.last_http_status;
    return `<span class="badge ${cls} tgt-badge" title="${esc(d.target_name)}：${esc(d.status)}，尝试 ${d.attempts_count} 次">` +
      `${esc(d.target_name)} ${code}</span>`;
  }).join(" ");
}

async function refreshEvents() {
  const [{ events }, { deliveries }] = await Promise.all([
    api("/api/events"), api("/api/deliveries").catch(() => ({ deliveries: [] })),
  ]);
  if (!Array.isArray(deliveries)) {
    // endpoint missing (should not happen) -> fetch per event lazily below
  }
  const byEvent = new Map();
  for (const d of deliveries || []) {
    if (!byEvent.has(d.event_id)) byEvent.set(d.event_id, []);
    byEvent.get(d.event_id).push(d);
  }
  lastEvents = events;
  const body = $("events-body");
  if (!events.length) {
    body.innerHTML = '<tr><td colspan="6" class="muted center">暂无事件</td></tr>';
    return;
  }
  body.innerHTML = events.map((e) => {
    const busy = e.status === "queued" || e.status === "running";
    const label = STATUS_LABEL[e.status] || e.status;
    const count = e.attempt_count ? `${e.attempt_count} 次` : "—";
    return `<tr>
      <td>${e.seq}</td>
      <td class="mono"><a class="link" onclick="showEvent('${encodeURIComponent(e.event_id)}')">${esc(e.event_id)}</a></td>
      <td><span class="badge ${e.status}">${label}</span></td>
      <td><div class="badge-wrap">${deliveryBadgesHtml(e, byEvent)}</div></td>
      <td>${count}</td>
      <td>
        <button class="mini" onclick="showEvent('${encodeURIComponent(e.event_id)}')">详情</button>
        <button class="mini" ${busy ? "disabled" : ""} onclick="replayOne('${encodeURIComponent(e.event_id)}', 'all')">全部重放</button>
        <button class="mini" ${busy || e.status !== "failed" ? "disabled" : ""}
          onclick="replayOne('${encodeURIComponent(e.event_id)}', 'failed')">只补失败目标</button>
      </td>
    </tr>`;
  }).join("");
}

async function replayOne(encId, scope) {
  try {
    await api(`/api/events/${encId}/replay`, {
      method: "POST",
      body: JSON.stringify({ scope: scope || "all" }),
    });
    refreshAll();
  } catch (e) { alert(e.message); }
}

async function showEvent(encId) {
  const d = await api(`/api/events/${encId}`);
  $("modal-title").textContent = `事件 ${d.event_id}（#${d.seq}，${d.status}）`;
  const out = {
    event_id: d.event_id,
    status: d.status,
    created_at: d.created_at,
    updated_at: d.updated_at,
    target_deliveries: (d.deliveries || []).map((x) => ({
      target: x.target_name,
      status: x.status,
      replay: x.replay_no,
      attempts: x.attempts_count,
      last_http_status: x.last_http_status,
      last_outcome: x.last_outcome,
      last_attempt_at: x.last_attempt_at,
    })),
    raw_payload: d.payload_parsed,
    attempts: d.timeline.map((t) => ({
      target: t.target_name || "(单目标)",
      replay: t.replay_no, attempt: t.attempt_no,
      at: t.request_at, http_status: t.http_status, outcome: t.outcome,
      elapsed_ms: t.elapsed_ms, idempotency_key: t.idem_key,
      response: t.resp_excerpt,
    })),
  };
  $("modal-body").textContent = JSON.stringify(out, null, 2);
  $("modal").classList.remove("hidden");
}

$("modal-close").addEventListener("click", () => $("modal").classList.add("hidden"));
$("modal").addEventListener("click", (e) => { if (e.target.id === "modal") $("modal").classList.add("hidden"); });

$("replay-all").addEventListener("click", async () => {
  const r = await api("/api/replay", { method: "POST", body: "{}" });
  alert(`已入队 ${r.queued.length} 条（跳过 ${r.skipped} 条进行中的事件）`);
  refreshAll();
});
$("replay-failed").addEventListener("click", async () => {
  const r = await api("/api/replay", { method: "POST",
    body: JSON.stringify({ only_failed: true }) });
  alert(`已入队 ${r.queued.length} 条失败事件（仅补投尚未成功的目标，跳过 ${r.skipped} 条）`);
  refreshAll();
});
$("refresh").addEventListener("click", refreshAll);

$("clear").addEventListener("click", async () => {
  if (!confirm("确定删除全部事件、尝试与投递记录？（目标配置保留）")) return;
  await api("/api/events", { method: "DELETE" });
  await fetch("/recv/logs", { method: "DELETE" });
  refreshAll();
});

// ---------- snapshot backup / restore ----------

let restoreFile = null;

$("restore-file").addEventListener("change", (ev) => {
  restoreFile = ev.target.files[0] || null;
  $("restore-filename").textContent = restoreFile ? restoreFile.name : "未选择文件";
  $("restore").disabled = !restoreFile;
});

$("restore").addEventListener("click", async () => {
  if (!restoreFile) return;
  if (!confirm(`将用快照「${restoreFile.name}」原子替换全部事件、尝试、投递与命名目标记录。\n\n` +
      "恢复前会完整校验（manifest / SHA-256 / 外键 / 载荷），任一错误都拒绝写入；\n" +
      "当前单目标 URL、HMAC 密钥、重试与超时配置会保留；命名目标组随快照恢复。\n\n确定继续？")) return;
  const btn = $("restore");
  btn.disabled = true;
  btn.textContent = "校验并恢复中…";
  try {
    const resp = await fetch("/api/restore", { method: "POST", body: restoreFile });
    const text = await resp.text();
    let data = {};
    try { data = text ? JSON.parse(text) : {}; } catch (_) { data = { detail: text }; }
    if (!resp.ok) {
      showSummary(false, { status: resp.status, ...data });
    } else {
      showSummary(true, data);
      await loadSettings();
      refreshAll();
    }
  } catch (e) {
    showSummary(false, { detail: e.message });
  } finally {
    btn.textContent = "⬆ 校验并恢复";
    btn.disabled = !restoreFile;
  }
});

function kvRow(k, v, cls) {
  return `<div class="sum-row"><span class="sum-k">${esc(k)}</span>` +
         `<span class="sum-v ${cls || ""}">${v}</span></div>`;
}

function showSummary(ok, r) {
  $("summary-title").textContent = ok ? "✅ 恢复成功 — 结果摘要" : "❌ 恢复被拒绝（未写入任何数据）";
  const box = $("summary-body");
  if (!ok) {
    const detail = typeof r.detail === "string" ? r.detail : JSON.stringify(r.detail, null, 2);
    box.innerHTML =
      `<div class="sum-banner fail">校验未通过，数据库保持原样。</div>` +
      (r.status ? kvRow("HTTP", String(r.status)) : "") +
      `<pre class="sum-errors">${esc(detail || "未知错误")}</pre>`;
  } else {
    const c = r.counts || {};
    const s = r.current_settings || {};
    box.innerHTML =
      `<div class="sum-banner ok">已在单事务内整体替换数据，单目标配置保留。</div>` +
      kvRow("恢复完成时间", esc(r.restored_at || "")) +
      kvRow("快照创建时间", esc(r.snapshot_created_at || "")) +
      kvRow("快照格式 / 数据库版本", `v${esc(r.format_version)} / v${esc(r.db_version)}`) +
      kvRow("投递模式", r.mode === "fanout"
        ? `多目标扇出（${c.targets_after ?? 0} 个命名目标）`
        : "单目标兼容模式") +
      kvRow("事件数量", `${c.events_before ?? "?"} → <b>${c.events_after ?? "?"}</b>`) +
      kvRow("尝试记录数量", `${c.attempts_after ?? "?"}`) +
      kvRow("命名目标 / 投递状态数量",
        `${c.targets_after ?? "?"} / ${c.deliveries_after ?? "?"}`) +
      (c.statuses_normalized_to_pending || c.deliveries_normalized_to_pending
        ? kvRow("状态归一化",
            `${c.statuses_normalized_to_pending || 0} 条事件、` +
            `${c.deliveries_normalized_to_pending || 0} 条投递已重置为 pending`)
        : "") +
      `<div class="sum-section">保留的当前单目标配置</div>` +
      kvRow("目标 URL", `<span class="mono">${esc(s.target_url)}</span>`) +
      kvRow("HMAC 密钥", `<span class="mono">${esc(s.hmac_secret)}</span>`) +
      kvRow("最大重试 / 超时", `${esc(s.max_retries)} 次 / ${esc(s.timeout_seconds)} 秒`) +
      `<div class="muted" style="margin-top:8px">${esc(r.snapshot_settings_note || "")}</div>`;
  }
  $("summary-modal").classList.remove("hidden");
}

$("summary-close").addEventListener("click", () => $("summary-modal").classList.add("hidden"));
$("summary-modal").addEventListener("click", (e) => {
  if (e.target.id === "summary-modal") $("summary-modal").classList.add("hidden");
});

// ---------- timeline ----------

function outcomeBadge(o) {
  const map = {
    delivered: ["success", "已送达"],
    retriable: ["running", "可重试错误"],
    non_retriable: ["failed", "不可重试"],
  };
  const [cls, label] = map[o] || ["pending", o];
  return `<span class="badge ${cls}">${label}</span>`;
}

async function refreshTimeline() {
  const { attempts } = await api("/api/timeline?limit=200");
  $("timeline-body").innerHTML = attempts.length ? attempts.map((t) => {
    const short = t.idem_key ? t.idem_key.slice(0, 10) + "…" : "";
    const status = t.http_status == null
      ? '<span class="badge failed">无响应</span>'
      : `<span class="badge ${t.http_status < 300 ? "success" : "failed"}">${t.http_status}</span>`;
    return `<tr>
      <td class="mono">${esc((t.request_at || "").replace("T", " ").slice(11, 23))}</td>
      <td class="mono"><a class="link" onclick="showEvent('${encodeURIComponent(t.event_id)}')">${esc(t.event_id)}</a></td>
      <td>${t.target_name ? `<span class="badge pending">${esc(t.target_name)}</span>` : '<span class="muted">单目标</span>'}</td>
      <td>${t.replay_no}-${t.attempt_no}<div class="muted" title="${esc(t.idem_key)}">idem ${esc(short)}</div></td>
      <td>${status}</td>
      <td>${outcomeBadge(t.outcome)}</td>
      <td>${t.elapsed_ms ?? ""} ms</td>
      <td><div class="excerpt" title="${esc(t.resp_excerpt || "")}">${esc(t.resp_excerpt || "")}</div></td>
    </tr>`;
  }).join("") : '<tr><td colspan="8" class="muted center">暂无请求记录</td></tr>';
}

async function refreshStatus() {
  const s = await api("/api/status");
  const c = s.counts;
  const dc = s.delivery_counts || {};
  const modePill = s.mode === "fanout"
    ? `<span class="pill">扇出 <b>${s.targets}</b> 目标</span>`
      + `<span class="pill">目标送达 <b style="color:var(--green)">${dc.success || 0}</b>` +
        `/<b style="color:var(--red)">${dc.failed || 0}</b></span>`
    : `<span class="pill">单目标</span>`;
  $("status-pills").innerHTML =
    modePill +
    `<span class="pill">总数 <b>${s.total}</b></span>` +
    `<span class="pill">待发送 <b>${c.pending}</b></span>` +
    `<span class="pill">队列/发送中 <b>${c.queued + c.running}</b></span>` +
    `<span class="pill">事件成功 <b style="color:var(--green)">${c.success}</b></span>` +
    `<span class="pill">事件失败 <b style="color:var(--red)">${c.failed}</b></span>`;
}

function refreshAll() {
  refreshStatus();
  refreshEvents();
  refreshTimeline();
}

refreshAll();
loadSettings();
setInterval(() => { refreshStatus(); refreshEvents(); refreshTimeline(); }, 2500);
