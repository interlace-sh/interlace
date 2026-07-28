// Every view: async factories returning {html, mount} — the router awaits them.
// Data comes from data.js caches; actions call the API and invalidate.
import { APIError, api } from "./api.js";
import {
  currentEnv, invalidate, loadChecks, loadDetail, loadEnvironments,
  loadModels, loadPlan, loadRuns, loadStreams, toCols,
} from "./data.js";
import { mountDag } from "./dag.js";
import { compactDiff, lineDiff } from "./diff.js";
import { feedComponent, unwireFeed, wireFeed } from "./live.js";
import {
  $, $$, MAT_GLYPH, STATE_COLOR, STATE_LABEL, changeState, esc, fmtAgo, fmtSchedule,
  fmtTime, highlightSql, runColor, runTrigger, shortFp, toast,
} from "./util.js";

export const views = {};
export const ui = { selected: null, navigate: null, confirm: null }; // wired by app.js

const errorBox = (err) => `<div class="error-box">✕ ${esc(err.detail || err.message || err)}</div>`;

/* ---------------- shared: model detail rail ---------------- */
async function modelDetailHTML(name, { extras = false } = {}) {
  const { byName } = await loadModels();
  const m = byName[name];
  if (!m) return `<div class="empty">unknown model</div>`;
  let detail = null;
  try { detail = await loadDetail(name); } catch { /* rail degrades */ }
  const cols = detail
    ? toCols(detail).map(([out, src, col]) =>
        col === "—" || !src
          ? `<div class="col-row"><span class="out">${esc(out)}</span><span class="from">←</span><span class="none">literal / aggregate</span></div>`
          : `<div class="col-row"><span class="out">${esc(out)}</span><span class="from">←</span><span class="src">${esc(src)}.${esc(col)}</span></div>`)
      .join("")
    : `<div class="none" style="font-family:var(--font-mono);font-size:11px">lineage unavailable</div>`;
  let extra = "";
  if (extras) {
    let checks = [];
    try { checks = await api.checks(name); } catch { /* optional */ }
    const checkRows = checks.length
      ? checks.slice(0, 6).map((c) => `<div class="col-row"><span class="sdot" style="background:${c.status === "passed" ? "var(--green)" : c.status === "error" ? "var(--warn)" : "var(--coral)"};margin-top:5px"></span><span class="out" style="min-width:118px">${esc(c.check_name)}</span><span class="src">${esc(c.status)}${c.failures ? ` · ${c.failures} failing` : ""}</span></div>`).join("")
      : '<div class="none" style="font-family:var(--font-mono);font-size:11px;padding:2px 0">no recorded results</div>';
    extra = `<div class="sub">Checks</div><div class="cols">${checkRows}</div>`;
  }
  const upstream = detail ? detail.upstream : m.depends_on;
  const downstream = detail ? detail.downstream : m.down;
  return `<div class="eyebrow">Model</div><h2>${esc(name)}</h2>
    <div class="badges">
      <span class="badge ${m.running ? "running" : m.state}">${m.running ? "running" : STATE_LABEL[m.state]}</span>
      <span class="badge">${MAT_GLYPH[m.mat] || ""} ${esc(m.mat)}</span>
      ${m.strategy && m.strategy !== "full" ? `<span class="badge">${esc(m.strategy)}</span>` : ""}
    </div>
    <dl class="kv"><dt>fingerprint</dt><dd>${shortFp(m.fingerprint)}</dd><dt>schedule</dt><dd>${esc(fmtSchedule(m.schedule))}</dd>
      <dt>owner</dt><dd>${esc(m.owner || "—")}</dd><dt>tags</dt><dd>${esc(m.tags.join(", ") || "—")}</dd>
      <dt>upstream</dt><dd>${esc(upstream.join(", ") || "—")}</dd><dt>downstream</dt><dd>${esc(downstream.join(", ") || "—")}</dd></dl>
    <div class="sub">Column lineage</div><div class="cols">${cols}</div>
    ${extra}
    ${detail?.sql ? `<div class="sub">Definition</div><pre class="sql">${highlightSql(detail.sql)}</pre>` : ""}`;
}

/* ---------------- lineage ---------------- */
views.lineage = async () => {
  const { byName, names } = await loadModels();
  let plan = null;
  try { plan = await loadPlan(); } catch { /* header degrades */ }
  const breaking = plan ? plan.changes.filter((c) => changeState(c) === "breaking" && !c.reused).length : 0;
  const changed = plan ? plan.changes.filter((c) => !c.reused && c.change_type !== "removed").length : 0;
  if (!ui.selected || !byName[ui.selected]) ui.selected = names[names.length - 1] || null;
  return {
    html: `
      <div class="vhead">
        <h1>Lineage</h1><span class="sub">${names.length} models · ${esc(currentEnv())}</span>
        <div class="right">
          ${breaking ? `<span class="badge breaking">${breaking} breaking</span>` : ""}
          ${changed - breaking > 0 ? `<span class="badge nonbreaking">${changed - breaking} changed</span>` : ""}
          <button class="btn sm" id="expandAll">Expand columns</button>
          <button class="btn sm" id="collapseAll">Collapse</button>
          <button class="btn" data-go="plan">Open plan →</button>
        </div>
      </div>
      <div class="split">
        <div class="canvas-wrap">
          <canvas id="dag"></canvas>
          <div class="hint">double-click a model to expand columns · hover a column to trace its lineage · drag to pan</div>
          <div class="legend">
            <span><i style="background:var(--green)"></i>added</span><span><i style="background:var(--coral)"></i>breaking</span>
            <span><i style="background:var(--accent)"></i>non-breaking</span><span><i style="background:var(--faint)"></i>unchanged</span>
            <span><i style="background:var(--cyan)"></i>running</span>
          </div>
        </div>
        <aside class="rail"><div class="detail pad" id="detail"></div>${feedComponent()}</aside>
      </div>`,
    mount() {
      const paint = () => { if (ui.selected) modelDetailHTML(ui.selected).then((h) => { const d = $("#detail"); if (d) d.innerHTML = h; }); };
      paint();
      wireFeed();
      const dag = mountDag({
        wrap: $(".canvas-wrap"), canvas: $("#dag"), models: byName, names,
        selected: ui.selected, onSelect: (n) => { ui.selected = n; paint(); },
      });
      if (ui.expandOnMount && byName[ui.expandOnMount]) { dag.expand(ui.expandOnMount); ui.expandOnMount = null; }
      $("#expandAll").onclick = () => dag.expandAll();
      $("#collapseAll").onclick = () => dag.collapseAll();
      return () => { dag.destroy(); unwireFeed(); };
    },
  };
};

/* ---------------- models catalog ---------------- */
const modelFilter = { q: "", mat: "all" };
const MATS = ["all", "table", "view", "ephemeral", "sink"];
views.models = async () => {
  const { byName, names } = await loadModels();
  let checks = [];
  try { checks = await loadChecks(); } catch { /* column degrades */ }
  const summary = (n) => {
    const cs = checks.filter((c) => c.model === n);
    const fail = cs.filter((c) => c.status === "failed").length;
    const err = cs.filter((c) => c.status === "error").length;
    return !cs.length ? '<span class="dim m">—</span>'
      : fail ? `<span class="badge fail">${fail} fail</span>`
      : err ? `<span class="badge warn">${err} error</span>`
      : `<span class="badge pass">${cs.length} pass</span>`;
  };
  const rows = () => {
    const q = modelFilter.q.toLowerCase();
    return names
      .filter((n) => (modelFilter.mat === "all" || byName[n].mat === modelFilter.mat) && n.toLowerCase().includes(q))
      .map((n) => {
        const m = byName[n];
        return `<tr data-click data-model="${esc(n)}" class="${n === ui.selected ? "sel" : ""}">
          <td><span class="sdot" style="background:${m.running ? "var(--cyan)" : STATE_COLOR[m.state]}"></span> <span class="m">${esc(n)}</span></td>
          <td class="m">${MAT_GLYPH[m.mat] || ""} ${esc(m.mat)}</td><td class="dim m">${esc(m.strategy)}</td>
          <td class="dim m">${esc(fmtSchedule(m.schedule))}</td><td class="dim m">${esc(m.owner || "—")}</td>
          <td>${summary(n)}</td><td class="dim m">${shortFp(m.fingerprint)}</td></tr>`;
      }).join("");
  };
  if (!ui.selected || !byName[ui.selected]) ui.selected = names[0] || null;
  return {
    html: `
      <div class="vhead"><h1>Models</h1><span class="sub">catalog · ${names.length} models</span>
        <div class="right">
          ${MATS.map((x) => `<button class="btn sm matf ${x === modelFilter.mat ? "primary" : ""}" data-mat="${x}">${x}</button>`).join("")}
          <button class="btn sm" id="toGraph">Graph →</button>
        </div></div>
      <div class="split"><div class="main">
        <div style="padding:10px 14px;border-bottom:1px solid var(--line)"><input id="mq" placeholder="filter models…" value="${esc(modelFilter.q)}" style="width:100%;background:var(--ground-2);border:1px solid var(--line);border-radius:var(--r);color:var(--text);font-family:var(--font-mono);font-size:12px;padding:7px 10px;outline:none"></div>
        <table class="tbl"><thead><tr><th>Model</th><th>Materialisation</th><th>Strategy</th><th>Schedule</th><th>Owner</th><th>Checks</th><th>Fingerprint</th></tr></thead>
        <tbody id="modelRows">${rows()}</tbody></table></div>
        <aside class="rail"><div class="detail pad" id="modelDetail"></div></aside></div>`,
    mount() {
      const paint = () => { if (ui.selected) modelDetailHTML(ui.selected, { extras: true }).then((h) => { const d = $("#modelDetail"); if (d) d.innerHTML = h; }); };
      paint();
      const wire = () => $$("#modelRows [data-model]").forEach((tr) => (tr.onclick = () => {
        ui.selected = tr.dataset.model;
        $$("#modelRows tr").forEach((x) => x.classList.remove("sel"));
        tr.classList.add("sel");
        paint();
      }));
      wire();
      $$(".matf").forEach((b) => (b.onclick = () => {
        modelFilter.mat = b.dataset.mat;
        $$(".matf").forEach((x) => x.classList.toggle("primary", x.dataset.mat === modelFilter.mat));
        $("#modelRows").innerHTML = rows(); wire();
      }));
      $("#mq").oninput = () => { modelFilter.q = $("#mq").value; $("#modelRows").innerHTML = rows(); wire(); };
      $("#toGraph").onclick = () => { ui.expandOnMount = ui.selected; ui.navigate("lineage"); };
    },
  };
};

/* ---------------- plan + apply ---------------- */
views.plan = async () => {
  const env = currentEnv();
  let plan;
  try { plan = await loadPlan(); } catch (err) { return { html: `<div class="vhead"><h1>Plan</h1></div>${errorBox(err)}` }; }
  const { byName } = await loadModels().catch(() => ({ byName: {} }));
  const counts = { breaking: 0, nonbreaking: 0, added: 0, reused: 0 };
  for (const c of plan.changes) {
    if (c.reused) counts.reused++;
    else if (c.change_type === "added") counts.added++;
    else if (c.category === "breaking") counts.breaking++;
    else counts.nonbreaking++;
  }
  const rows = plan.changes.map((c) => {
    const state = c.reused ? "unchanged" : changeState(c);
    const m = byName[c.name];
    const diff = c.previous_sql || c.new_sql
      ? compactDiff(lineDiff(c.previous_sql, c.new_sql)).map(([k, l]) => `<div class="ln ${k}">${esc(l)}</div>`).join("")
      : "";
    const impact = c.reused
      ? "output provably identical — reuses the existing table, no rebuild"
      : c.category === "forward_only"
        ? "forward-only: history carries to the new version, checks gate before views move"
        : c.impacted_columns.length ? `impacted columns: <code>${esc(c.impacted_columns.join(", "))}</code>` : "";
    return `<details class="change"><summary><span class="chev">▸</span>
        <span class="nm">${esc(c.name)}</span>
        <span class="badge ${state}">${c.reused ? "reuse" : c.category === "forward_only" ? "forward-only" : STATE_LABEL[state]}</span>
        <span class="meta">${m ? `${MAT_GLYPH[m.mat] || ""} ${esc(m.mat)} · ` : ""}${shortFp(c.previous_fingerprint)} → ${shortFp(c.new_fingerprint)}</span></summary>
      <div class="body">${impact ? `<div class="impact">${impact}</div>` : ""}${diff ? `<div class="diff">${diff}</div>` : ""}</div></details>`;
  }).join("");
  const transfers = plan.transfers.map((t) => `<div class="impact" style="color:var(--cyan)">⇄ ${esc(t)}</div>`).join("");
  return {
    html: `
      <div class="vhead"><h1>Plan</h1><span class="sub">→ ${esc(env)}</span>
        <div class="right counts">
          <div class="count-chip breaking"><span class="n">${counts.breaking}</span><span class="l">breaking</span></div>
          <div class="count-chip nonbreaking"><span class="n">${counts.nonbreaking}</span><span class="l">non-breaking</span></div>
          <div class="count-chip added"><span class="n">${counts.added}</span><span class="l">added</span></div>
          <div class="count-chip unchanged"><span class="n">${counts.reused}</span><span class="l">reused</span></div>
          ${plan.changes.length ? `<button class="btn primary" id="applyBtn">Apply to ${esc(env)} →</button>` : ""}
        </div></div>
      <div class="vbody pad">
        ${transfers}
        <div class="sectitle">${plan.changes.length - counts.reused} models to build · ${counts.reused} reused</div>
        ${rows || `<div class="empty">No changes — ${esc(env)} is up to date.</div>`}
      </div>`,
    mount() {
      const btn = $("#applyBtn");
      if (!btn) return;
      btn.onclick = async (force = false) => {
        btn.disabled = true; btn.textContent = "Applying…";
        try {
          const result = await api.apply({ environment: env, force: force === true });
          const built = Object.entries(result.rows || {}).map(([n, r]) => {
            const bits = [r.inserted && `+${r.inserted}`, r.updated && `~${r.updated}`, r.deleted && `-${r.deleted}`].filter(Boolean).join(" ");
            return `${n}${bits ? ` ${bits}` : ""}`;
          });
          toast(`Applied to ${env}`, `built ${result.built.length} · promoted ${result.promoted}${built.length ? ` · ${built.slice(0, 4).join(", ")}` : ""}`);
          invalidate(); ui.navigate("plan");
        } catch (err) {
          if (err instanceof APIError && err.status === 400 && /breaking/.test(err.detail || "")) {
            ui.confirm({
              title: "± Breaking changes", danger: true,
              body: `<div class="impact">${esc(err.detail)}</div><div class="impact">Downstream history-keeping models rebuild from scratch unless applied forward-only. Apply anyway?</div>`,
              action: "Force apply",
              onConfirm: () => btn.onclick(true),
            });
          } else toast("Apply failed", err.detail || err.message);
          btn.disabled = false; btn.textContent = `Apply to ${env} →`;
        }
      };
    },
  };
};

/* ---------------- runs ---------------- */
let selRun = null;
views.runs = async () => {
  const runs = await loadRuns();
  if (!runs.find((r) => r.id === selRun)) selRun = runs[0]?.id ?? null;
  const rows = runs.map((r) => `<tr data-click data-run="${r.id}" class="${r.id === selRun ? "sel" : ""}">
      <td><span class="sdot" style="background:${runColor(r.state)}"></span> <span class="m">${esc(r.state)}</span></td>
      <td class="m">#${r.id}</td><td class="m">${esc(r.flow_selector.join(", "))}</td>
      <td class="dim m">${esc(runTrigger(r.idempotency_key))}</td><td class="dim m">${fmtTime(r.enqueued_at)}</td>
      <td class="dim m">${r.restate ? "restate" : r.partition ? "window" : "—"}</td>
      <td class="m">${r.attempts}</td></tr>`).join("");
  return {
    html: `
      <div class="vhead"><h1>Runs</h1><span class="sub">durable queue · scheduler + api + streams</span>
        <div class="right"><button class="btn primary" id="newRun">Run…</button></div></div>
      <div class="split"><div class="main">
        <table class="tbl"><thead><tr><th>Status</th><th>Run</th><th>Models</th><th>Trigger</th><th>Enqueued</th><th>Kind</th><th>Attempts</th></tr></thead>
        <tbody id="runRows">${rows || ""}</tbody></table>
        ${runs.length ? "" : `<div class="empty">No runs recorded — the queue holds daemon-triggered work (schedules, streams, POST /runs).</div>`}</div>
        <aside class="rail"><div class="pad" id="runDetail"></div></aside></div>`,
    mount() {
      const paintDetail = async () => {
        const d = $("#runDetail");
        if (!d || selRun == null) { if (d) d.innerHTML = `<div class="empty">no run selected</div>`; return; }
        let run;
        try { run = await api.run(selRun); } catch (err) { d.innerHTML = errorBox(err); return; }
        const success = run.events.find((e) => e.type === "run.succeeded");
        const timings = success?.payload?.timings || {};
        const steps = Object.keys(timings).length
          ? Object.entries(timings).map(([m, s]) => `<div class="col-row"><span class="sdot" style="background:var(--green);margin-top:5px"></span><span class="out" style="min-width:130px">${esc(m)}</span><span class="from" style="margin-left:auto">${s}s</span></div>`).join("")
          : run.events.map((e) => `<div class="col-row"><span class="sdot" style="background:${e.type.endsWith("failed") ? "var(--coral)" : e.type.endsWith("succeeded") ? "var(--green)" : "var(--faint)"};margin-top:5px"></span><span class="out" style="min-width:130px">${esc(e.type)}</span><span class="src">${fmtTime(e.ts)}</span></div>`).join("") || `<div class="empty">queued — no events yet</div>`;
        const cancellable = run.state === "queued" || run.state === "running";
        d.innerHTML = `<div class="eyebrow">Run</div><h2>#${run.id}</h2>
          <div class="badges"><span class="badge ${esc(run.state)}">${esc(run.state)}</span><span class="badge">${esc(runTrigger(run.idempotency_key))}</span>${run.restate ? `<span class="badge forward">restate</span>` : ""}</div>
          <dl class="kv"><dt>enqueued</dt><dd>${fmtTime(run.enqueued_at)}</dd><dt>models</dt><dd>${esc(run.flow_selector.join(", "))}</dd>
            ${run.partition ? `<dt>window</dt><dd>${esc(run.partition[0] || "…")} → ${esc(run.partition[1] || "…")}</dd>` : ""}
            <dt>attempts</dt><dd>${run.attempts}</dd></dl>
          ${run.error ? `<div class="impact" style="color:var(--coral)">✕ ${esc(run.error)}</div>` : ""}
          ${cancellable ? `<button class="btn sm" id="cancelRun">Cancel run</button>` : ""}
          <div class="sub" style="margin-top:14px">Timeline</div><div class="cols">${steps}</div>`;
        const cancel = $("#cancelRun");
        if (cancel) cancel.onclick = async () => {
          try { const out = await api.cancelRun(run.id); toast("Cancel", `run #${run.id} → ${out.state}`); invalidate("runs"); ui.navigate("runs"); }
          catch (err) { toast("Cancel failed", err.detail || err.message); }
        };
      };
      paintDetail();
      $$("#runRows [data-run]").forEach((tr) => (tr.onclick = () => {
        selRun = +tr.dataset.run;
        $$("#runRows tr").forEach((x) => x.classList.remove("sel"));
        tr.classList.add("sel");
        paintDetail();
      }));
      $("#newRun").onclick = async () => {
        const { names } = await loadModels();
        ui.confirm({
          title: "▸ Run models",
          body: `<div class="field"><label>Selectors (space-separated · name, +name, name+, tag:x — empty = all)</label><input id="runSel" placeholder="${esc(names.slice(0, 2).join(" "))}"></div>
            <div class="field"><label>Environment</label><input id="runEnv" value="${esc(currentEnv())}"></div>
            <div style="display:flex;gap:14px"><div class="field" style="flex:1"><label>Start (ISO, optional)</label><input id="runStart" placeholder="2026-07-01T00:00:00"></div>
            <div class="field" style="flex:1"><label>End (ISO, optional)</label><input id="runEnd" placeholder=""></div></div>
            <div class="field"><label><input type="checkbox" id="runRestate" style="width:auto;margin-right:6px">restate — rewrite filled windows</label></div>`,
          action: "Enqueue run",
          onConfirm: async () => {
            const body = {
              selectors: $("#runSel").value.trim() ? $("#runSel").value.trim().split(/\s+/) : [],
              environment: $("#runEnv").value.trim() || undefined,
              start: $("#runStart").value.trim() || undefined,
              end: $("#runEnd").value.trim() || undefined,
              restate: $("#runRestate").checked,
            };
            try {
              const out = await api.createRun(body);
              toast("Run enqueued", `${out.models.length} model(s) — a running scheduler drains it`);
              invalidate("runs"); ui.navigate("runs");
            } catch (err) { toast("Enqueue failed", err.detail || err.message); }
          },
        });
      };
    },
  };
};

/* ---------------- streams ---------------- */
let selStream = null;
views.streams = async () => {
  const streams = await loadStreams();
  if (!streams.find((s) => s.name === selStream)) selStream = streams[0]?.name ?? null;
  const cards = streams.map((s) => {
    const pending = Math.max(0, s.head - s.watermark);
    return `<div class="card click" data-stream="${esc(s.name)}">
      <h3>≈ ${esc(s.name)} <span class="badge ${pending ? "warn" : "live"}" style="margin-left:auto">${pending ? `${pending} pending` : "live"}</span></h3>
      <div class="dim m" style="font-size:12px;color:var(--muted)">→ ${esc(s.table)}</div>
      <div class="metricrow"><div class="metric"><div class="v">${s.head}</div><div class="k">head</div></div>
        <div class="metric"><div class="v">${s.watermark}</div><div class="k">watermark</div></div>
        <div class="metric"><div class="v">${esc(s.on_schema_drift)}</div><div class="k">on drift</div></div></div></div>`;
  }).join("");
  return {
    html: `
      <div class="vhead"><h1>Streams</h1><span class="sub">durable ingestion · ${streams.length} declared</span></div>
      <div class="split"><div class="main pad">${streams.length ? `<div class="cards">${cards}</div>` : `<div class="empty">No streams declared — add an @stream model.</div>`}</div>
        <aside class="rail"><div class="pad" id="streamDetail"></div></aside></div>`,
    mount() {
      const paint = async () => {
        const d = $("#streamDetail");
        if (!d || !selStream) { if (d) d.innerHTML = `<div class="empty">no stream selected</div>`; return; }
        let s;
        try { s = await api.stream(selStream); } catch (err) { d.innerHTML = errorBox(err); return; }
        const schema = Object.entries(s.schema).map(([n, t]) => `<div class="col-row"><span class="out" style="min-width:120px">${esc(n)}</span><span class="src">${esc(t)}</span></div>`).join("");
        const recent = s.recent.slice(-5).reverse().map((r) => `<pre class="sql" style="margin-bottom:5px">${esc(JSON.stringify(r))}</pre>`).join("") || `<div class="empty">no events yet</div>`;
        const pending = Math.max(0, s.head - s.watermark);
        d.innerHTML = `<div class="eyebrow">Stream</div><h2>${esc(s.name)}</h2>
          <div class="badges"><span class="badge ${pending ? "warn" : "live"}">${pending ? `${pending} pending` : "live"}</span><span class="badge">→ ${esc(s.table)}</span></div>
          <dl class="kv"><dt>head</dt><dd>${s.head}</dd><dt>watermark</dt><dd>${s.watermark}</dd>
            <dt>dedup key</dt><dd>${esc(s.idempotency_key || "—")}</dd></dl>
          <div class="sub">Schema</div><div class="cols">${schema}</div>
          <div class="sub">Publish test event</div>
          <div class="field"><textarea id="pubBody" style="min-height:64px">${esc(JSON.stringify(Object.fromEntries(Object.keys(s.schema).map((k) => [k, null]))))}</textarea></div>
          <button class="btn sm" id="pubBtn">POST /streams/${esc(s.name)}</button>
          <div class="sub" style="margin-top:16px">Recent events</div>${recent}`;
        $("#pubBtn").onclick = async () => {
          try {
            const out = await api.publish(s.name, JSON.parse($("#pubBody").value));
            toast("Published", `accepted ${out.accepted} · deduplicated ${out.deduplicated}${out.quarantined ? ` · quarantined ${out.quarantined}` : ""}`);
            invalidate("stream"); setTimeout(paint, 400); // let the flusher land it
          } catch (err) { toast("Publish failed", err.detail || err.message); }
        };
      };
      paint();
      $$("[data-stream]").forEach((c) => (c.onclick = () => { selStream = c.dataset.stream; paint(); }));
    },
  };
};

/* ---------------- environments ---------------- */
views.environments = async () => {
  const envs = await loadEnvironments();
  const cards = envs.map((e) => `<div class="card">
      <h3>⊞ ${esc(e.name)} ${e.name === "prod" ? '<span class="badge live" style="margin-left:auto">production</span>' : ""}</h3>
      <div class="dim" style="color:var(--muted);font-size:12px">promoted ${fmtAgo(e.promoted_at)}${e.changed ? ` · ${e.changed} model(s) drifted` : " · up to date"}</div>
      <div class="metricrow"><div class="metric"><div class="v">${e.models}</div><div class="k">models</div></div>
        <div class="metric"><div class="v">${e.changed || "—"}</div><div class="k">drift</div></div></div>
      <div style="margin-top:14px;display:flex;gap:8px">
        <button class="btn sm" data-plan-env="${esc(e.name)}">Plan →</button>
        <button class="btn sm" data-drop="${esc(e.name)}">Drop</button></div></div>`).join("");
  return {
    html: `
      <div class="vhead"><h1>Environments</h1><span class="sub">virtual data environments · views over snapshots</span></div>
      <div class="vbody pad">
        ${envs.length ? `<div class="cards">${cards}</div>` : `<div class="empty">No environments promoted yet — run an apply.</div>`}
      </div>`,
    mount() {
      $$("[data-plan-env]").forEach((b) => (b.onclick = () => { ui.setEnv(b.dataset.planEnv); ui.navigate("plan"); }));
      $$("[data-drop]").forEach((b) => (b.onclick = () => {
        const name = b.dataset.drop;
        const isProd = name === "prod";
        ui.confirm({
          title: "⊞ Drop environment", danger: true,
          body: `<div class="impact">Views for <code>${esc(name)}</code> are removed and its snapshots become reclaimable by gc.${isProd ? " <b>This is the production environment — consumers lose their views.</b>" : ""}</div>
            ${isProd ? `<div class="field"><label>Type the environment name to confirm</label><input id="dropConfirm" placeholder="prod"></div>` : ""}`,
          action: "Drop",
          onConfirm: async () => {
            if (isProd && $("#dropConfirm")?.value !== name) { toast("Not dropped", "confirmation text did not match"); return; }
            try {
              const out = await api.dropEnvironment(name, isProd);
              toast("Environment dropped", `${name} · ${out.dropped_views.length} view(s) removed`);
              invalidate(); ui.navigate("environments");
            } catch (err) { toast("Drop failed", err.detail || err.message); }
          },
        });
      }));
    },
  };
};

/* ---------------- checks ---------------- */
views.checks = async () => {
  const checks = await loadChecks();
  const passed = checks.filter((c) => c.status === "passed").length;
  const failed = checks.filter((c) => c.status === "failed").length;
  const errored = checks.filter((c) => c.status === "error").length;
  const colour = { passed: "pass", failed: "fail", error: "warn" };
  const rows = checks.map((c) => `<tr><td class="dim m">${esc(c.environment)}</td><td class="m">${esc(c.model)}</td><td class="m">${esc(c.check_name)}</td>
      <td><span class="badge ${c.severity === "error" ? "" : "warn"}">${esc(c.severity)}</span></td>
      <td><span class="badge ${colour[c.status] || ""}">${esc(c.status)}</span></td>
      <td class="dim m">${c.failures ? `${c.failures} failing` : esc(c.message || "—")}</td>
      <td class="dim m">${fmtTime(c.executed_at)}</td></tr>`).join("");
  return {
    html: `
      <div class="vhead"><h1>Checks</h1><span class="sub">data quality · gate promotion</span>
        <div class="right"><span class="badge pass">${passed} pass</span>${failed ? `<span class="badge fail">${failed} fail</span>` : ""}${errored ? `<span class="badge warn">${errored} error</span>` : ""}</div></div>
      <div class="vbody pad">
        ${checks.length ? `<table class="tbl"><thead><tr><th>Env</th><th>Model</th><th>Check</th><th>Severity</th><th>Status</th><th>Detail</th><th>At</th></tr></thead><tbody>${rows}</tbody></table>`
          : `<div class="empty">No check results recorded — checks run with every apply, or via \`interlace checks run\`.</div>`}
      </div>`,
  };
};

/* ---------------- settings ---------------- */
views.settings = async () => {
  const { getToken, setToken } = await import("./api.js");
  let health = null;
  try { health = await api.health(); } catch { /* offline */ }
  return {
    html: `
      <div class="vhead"><h1>Settings</h1><span class="sub">daemon · ${esc(health?.version || "unreachable")}</span></div>
      <div class="vbody pad" style="max-width:760px">
        <div class="sectitle">API access</div>
        <div class="impact">The API is open until a key exists (create one: <code>interlace apikey create &lt;name&gt; --scope admin</code>).
          With keys configured, paste one here — it is stored in this browser only. The live feed falls back to polling when keyed.</div>
        <div class="field" style="margin-top:12px"><label>Bearer token</label><input id="tokenInput" placeholder="ilk_…" value="${esc(getToken())}"></div>
        <div style="display:flex;gap:8px"><button class="btn primary" id="saveToken">Save token</button><button class="btn" id="clearToken">Clear</button></div>
        <div class="sectitle">Endpoints</div>
        <table class="tbl"><tbody>
          <tr><td class="m">API docs</td><td class="dim m"><a href="/schema/scalar" target="_blank">/schema/scalar</a> · OpenAPI</td></tr>
          <tr><td class="m">Event stream</td><td class="dim m">/events/stream · SSE with Last-Event-ID replay</td></tr>
          <tr><td class="m">Health</td><td class="dim m">/health · ${health ? `<span class="badge ok">ok</span>` : `<span class="badge fail">unreachable</span>`}</td></tr>
        </tbody></table>
      </div>`,
    mount() {
      $("#saveToken").onclick = async () => {
        setToken($("#tokenInput").value.trim());
        try { await api.models(); toast("Token saved", "verified against GET /models"); }
        catch (err) { toast("Token saved (unverified)", err.detail || err.message); }
      };
      $("#clearToken").onclick = () => { setToken(""); $("#tokenInput").value = ""; toast("Token cleared", "back to open-mode requests"); };
    },
  };
};
