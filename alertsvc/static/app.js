"use strict";

const POLL_MS = 2000;
let eventOffset = 0;
const EVENT_LIMIT = 20;
let eventTotal = 0;
let pollTimer = null;
let lastServerTime = null;

const $ = (id) => document.getElementById(id);

function fmtTime(ts) {
  if (!ts) return "—";
  const d = new Date(ts * 1000);
  const p = (n, w = 2) => String(n).padStart(w, "0");
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${p(d.getMilliseconds(), 3)}`;
}

function fmtNum(n, digits = 0) {
  if (n === null || n === undefined) return "—";
  return Number(n).toLocaleString("zh-CN", { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

async function api(path, options = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  let body = null;
  try {
    body = await res.json();
  } catch (_e) {
    /* non-JSON */
  }
  if (!res.ok) {
    const msg = (body && body.error) || `${res.status} ${res.statusText}`;
    throw new Error(msg);
  }
  return body;
}

let toastTimer = null;
function toast(message, isError = false) {
  const el = $("toast");
  el.textContent = message;
  el.classList.toggle("error", isError);
  el.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { el.hidden = true; }, 4200);
}

function setTop(status) {
  const dot = $("live-dot");
  dot.classList.toggle("alert", !!status.state.in_alert);
  dot.classList.toggle("live", !status.state.in_alert);
  lastServerTime = status.server_time;
  const rule = status.active_rule;
  $("topstatus").textContent =
    `v${rule.version} · ${rule.threshold_ms}ms × ${rule.consecutive} · ` +
    (status.state.in_alert ? "告警中" : "正常");
}

function renderStatus(status) {
  setTop(status);
  const rule = status.active_rule;
  const st = status.state;

  $("st-version").textContent = `v${rule.version}`;
  $("st-threshold").textContent = `${fmtNum(rule.threshold_ms, 1)} ms`;
  $("st-consecutive-req").textContent = rule.consecutive;
  $("st-consecutive").textContent = st.consecutive;
  $("server-time").textContent = `服务器时间 ${fmtTime(status.server_time)}`;

  const pillar = $("st-alert");
  pillar.textContent = st.in_alert ? "告警中" : "正常";
  pillar.className = "pill " + (st.in_alert ? "bad" : "ok");

  const pct = Math.min(100, (st.consecutive / Math.max(1, rule.consecutive)) * 100);
  $("breach-progress").style.width = `${pct}%`;
  $("breach-progress-label").textContent =
    `连续超限 ${st.consecutive} / ${rule.consecutive}` +
    (st.open_episode ? ` · 断连 #${st.open_episode.id} 起于 ${fmtTime(st.open_episode.started_ts)}` : "");

  $("st-samples").textContent = fmtNum(status.samples.count);
  $("st-retention").textContent = fmtNum(status.samples.retention);
  $("st-events").textContent = fmtNum(status.events_total);
  $("st-episodes").textContent = fmtNum(status.episodes_total);
  $("st-last-sample").textContent = status.samples.last_id ?? "—";

  if (!status.trial && prefilledVersion !== rule.version) {
    // Prefill the trial form from the live rule so the demo starts from a
    // meaningful baseline, without clobbering values the operator typed.
    $("trial-threshold").value = rule.threshold_ms;
    $("trial-consecutive").value = rule.consecutive;
    prefilledVersion = rule.version;
  }
  renderTrial(status.trial);
}

function renderTrial(trial) {
  const createBtn = $("trial-create");
  const cancelBtn = $("trial-cancel");
  const activateBtn = $("trial-activate");
  const badge = $("trial-badge");

  if (!trial) {
    badge.hidden = true;
    cancelBtn.hidden = true;
    activateBtn.hidden = true;
    createBtn.disabled = false;
    $("trial-empty").hidden = false;
    $("trial-body").hidden = true;
    return;
  }

  badge.hidden = false;
  cancelBtn.hidden = false;
  activateBtn.hidden = false;
  createBtn.disabled = true;
  $("trial-empty").hidden = true;
  $("trial-body").hidden = false;

  const rule = trial.rule;
  $("tr-version").textContent = `v${rule.version}（待启用）`;
  $("tr-samples").textContent = fmtNum(rule.preview.sample_count);
  $("tr-alerts").textContent = fmtNum(rule.preview.alert_count);
  $("tr-recoveries").textContent = fmtNum(rule.preview.recovery_count);
  $("tr-endstate").textContent = trial.state.in_alert
    ? `告警中（连续 ${trial.state.consecutive}）`
    : `正常（连续 ${trial.state.consecutive}）`;
  $("tr-shadow").textContent =
    `${fmtNum(rule.shadow.sample_count)} 条（暂扣告警 ${fmtNum(rule.shadow.alert_count)} / 恢复 ${fmtNum(rule.shadow.recovery_count)}）`;
}

let lastTrialId = null;
let prefilledVersion = null;

async function refreshTrialEvents(trial) {
  if (!trial) {
    lastTrialId = null;
    $("trial-events").querySelector("tbody").innerHTML = "";
    return;
  }
  const id = trial.rule.id;
  if (id !== lastTrialId) {
    lastTrialId = id;
    $("trial-events").querySelector("tbody").innerHTML = "";
  }
  const data = await api(`/api/trials/${id}/events?limit=60`);
  const tbody = $("trial-events").querySelector("tbody");
  const rows = data.withheld_events.items;
  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="5" class="muted">暂无试算事件（尚未达到告警条件）</td></tr>`;
    return;
  }
  tbody.innerHTML = rows
    .map((e) => {
      const cls = e.kind === "alert" ? "kind-preview" : "kind-recovery";
      const label = e.kind === "alert" ? "告警" : "恢复";
      return `<tr><td class="${cls}">${label}</td><td>${e.sample_id}</td>` +
        `<td>${fmtNum(e.duration_ms, 1)}</td><td>${e.consecutive}</td>` +
        `<td>${e.origin === "preview" ? "回放" : e.origin === "shadow" ? "实时暂扣" : "补算"}</td></tr>`;
    })
    .join("");
}

async function refreshStatus() {
  try {
    const status = await api("/api/status");
    renderStatus(status);
    if (status.trial) await refreshTrialEvents(status.trial).catch(() => {});
    else await refreshTrialEvents(null);
  } catch (err) {
    $("topstatus").textContent = `连接失败：${err.message}`;
    $("live-dot").className = "dot";
  }
}

async function refreshEvents() {
  const kind = $("event-kind").value;
  const qs = new URLSearchParams({ limit: EVENT_LIMIT, offset: eventOffset });
  if (kind) qs.set("kind", kind);
  const data = await api(`/api/events?${qs}`);
  eventTotal = data.total;
  const tbody = $("events-table").querySelector("tbody");
  if (!data.items.length) {
    tbody.innerHTML = `<tr><td colspan="9" class="muted">暂无记录</td></tr>`;
  } else {
    tbody.innerHTML = data.items
      .map((e) => {
        const cls = e.kind === "alert" ? "kind-alert" : "kind-recovery";
        const label = e.kind === "alert" ? "告警" : "恢复";
        const cause = e.cause === "activation" ? "换版补记" : "实时";
        return `<tr><td>${e.id}</td><td class="${cls}">${label}</td>` +
          `<td>v${e.rule_version}</td><td>${fmtTime(e.ts)}</td>` +
          `<td>${fmtNum(e.duration_ms, 1)}</td><td>${fmtNum(e.threshold_ms, 1)}</td>` +
          `<td>${e.consecutive}</td><td>${cause}</td><td>${e.episode_id ?? "—"}</td></tr>`;
      })
      .join("");
  }
  const page = Math.floor(eventOffset / EVENT_LIMIT) + 1;
  const pages = Math.max(1, Math.ceil(eventTotal / EVENT_LIMIT));
  $("page-label").textContent = `第 ${page} / ${pages} 页 · 共 ${eventTotal} 条`;
  $("prev-page").disabled = eventOffset <= 0;
  $("next-page").disabled = eventOffset + EVENT_LIMIT >= eventTotal;
}

async function refreshRules() {
  const data = await api("/api/rules?limit=30");
  const tbody = $("rules-table").querySelector("tbody");
  const labels = { active: "生效中", retired: "已退役", trial: "试算中", cancelled: "已取消" };
  tbody.innerHTML = data.items
    .map((r) => {
      const cls = r.status === "active" ? "kind-recovery" : "";
      return `<tr><td class="${cls}">v${r.version}</td><td>${fmtNum(r.threshold_ms, 1)}</td>` +
        `<td>${r.consecutive}</td><td>${labels[r.status] || r.status}</td>` +
        `<td>${fmtTime(r.created_at)}</td></tr>`;
    })
    .join("");
}

async function refreshSamples() {
  const data = await api("/api/samples?limit=15");
  const tbody = $("samples-table").querySelector("tbody");
  tbody.innerHTML = data.items
    .map((s) => `<tr><td>${s.id}</td><td>${fmtNum(s.duration_ms, 1)}</td><td>${fmtTime(s.received_at)}</td></tr>`)
    .join("");
}

async function refreshAll() {
  await refreshStatus();
  await Promise.all([refreshEvents(), refreshRules(), refreshSamples()]);
}

function startPolling() {
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = setInterval(async () => {
    await refreshStatus();
    await Promise.all([refreshEvents(), refreshRules(), refreshSamples()]);
  }, POLL_MS);
}

/* ---------------- actions ---------------- */

$("ingest-form").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const value = Number($("duration").value);
  try {
    const res = await api("/api/samples", {
      method: "POST",
      body: JSON.stringify({ duration_ms: value }),
    });
    const r = res.results[0];
    if (r.event) {
      toast(r.event.kind === "alert" ? `已触发告警（断连 #${r.event.episode_id}）` : `已恢复（断连 #${r.event.episode_id}）`);
    } else {
      toast(`已上报 ${value}ms（连续超限 ${r.state.consecutive}）`);
    }
    await refreshAll();
  } catch (err) {
    toast(err.message, true);
  }
});

$("seq-form").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const values = $("seq").value
    .split(",")
    .map((v) => v.trim())
    .filter(Boolean)
    .map(Number);
  if (!values.length || values.some((v) => !Number.isFinite(v) || v < 0)) {
    toast("序列需为逗号分隔的非负数", true);
    return;
  }
  try {
    const res = await api("/api/samples/seq", {
      method: "POST",
      body: JSON.stringify({ values }),
    });
    const issued = res.results.filter((r) => r.event);
    toast(`已上报 ${res.ingested} 条${issued.length ? `，新增 ${issued.length} 条告警/恢复记录` : ""}`);
    await refreshAll();
  } catch (err) {
    toast(err.message, true);
  }
});

$("burst").addEventListener("click", async () => {
  try {
    const res = await api("/api/demo/seed", { method: "POST", body: JSON.stringify({ rounds: 6 }) });
    toast(`演示流量已灌入 ${res.ingested} 条`);
    await refreshAll();
  } catch (err) {
    toast(err.message, true);
  }
});

$("trial-form").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  try {
    const res = await api("/api/trials", {
      method: "POST",
      body: JSON.stringify({
        threshold_ms: Number($("trial-threshold").value),
        consecutive: Number($("trial-consecutive").value),
      }),
    });
    const p = res.preview;
    toast(
      `v${res.rule.version} 试算完成：回放 ${p.sample_count} 条，预计 ${p.alert_count} 次告警 / ${p.recovery_count} 次恢复，已暂扣`
    );
    await refreshAll();
  } catch (err) {
    toast(err.message, true);
  }
});

$("trial-activate").addEventListener("click", async () => {
  const btn = $("trial-activate");
  btn.disabled = true;
  try {
    const status = await api("/api/status");
    if (!status.trial) return;
    const rule = status.trial.rule;
    if (!confirm(`启用规则 v${rule.version}（${rule.threshold_ms}ms × ${rule.consecutive}）？\n启用后将直接接管最新样本状态，不重扫历史。`)) return;
    const res = await api(`/api/trials/${rule.id}/activate`, { method: "POST" });
    const map = { continued: "沿用原告警", recovered: "补记恢复", alerted: "补记告警", unchanged: "状态不变" };
    toast(`已启用 v${res.activated.version}：${map[res.transition] || res.transition}${res.catch_up_samples ? `，补算 ${res.catch_up_samples} 条` : ""}`);
    await refreshAll();
  } catch (err) {
    toast(`启用失败：${err.message}`, true);
    await refreshAll().catch(() => {});
  } finally {
    btn.disabled = false;
  }
});

$("trial-cancel").addEventListener("click", async () => {
  const btn = $("trial-cancel");
  btn.disabled = true;
  try {
    const status = await api("/api/status");
    if (!status.trial) return;
    if (!confirm(`取消试算 v${status.trial.rule.version}？原规则 v${status.active_rule.version} 继续生效。`)) return;
    const res = await api(`/api/trials/${status.trial.rule.id}/cancel`, { method: "POST" });
    toast(`已取消试算，v${res.active.version} 继续生效`);
    await refreshAll();
  } catch (err) {
    toast(`取消失败：${err.message}`, true);
    await refreshAll().catch(() => {});
  } finally {
    btn.disabled = false;
  }
});

$("events-refresh").addEventListener("click", () => refreshEvents().catch((e) => toast(e.message, true)));
$("event-kind").addEventListener("change", () => { eventOffset = 0; refreshEvents().catch((e) => toast(e.message, true)); });
$("prev-page").addEventListener("click", () => { eventOffset = Math.max(0, eventOffset - EVENT_LIMIT); refreshEvents().catch((e) => toast(e.message, true)); });
$("next-page").addEventListener("click", () => { eventOffset += EVENT_LIMIT; refreshEvents().catch((e) => toast(e.message, true)); });

/* Demo reset: only available when the server exposes it; hides otherwise. */
const resetBtn = $("demo-reset");
resetBtn.addEventListener("click", async () => {
  if (!confirm("清空全部样本、告警记录并恢复默认规则？此操作不可撤销。")) return;
  try {
    await api("/api/demo/reset", { method: "POST" });
    eventOffset = 0;
    toast("演示数据已清空");
    await refreshAll();
  } catch (err) {
    toast(err.message, true);
  }
});

(async function boot() {
  try {
    await refreshAll();
  } catch (err) {
    toast(err.message, true);
  }
  startPolling();
})();
