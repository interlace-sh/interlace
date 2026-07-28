// The lineage canvas, ported from the prototype: layered layout, violet weave
// edges (the logo gradient), column-level expansion with cross-graph thread
// tracing, pan, per-node running halo. Column data hydrates lazily on expand.
import { loadDetail, toCols } from "./data.js";
import { MAT_GLYPH, STATE_COLOR, css, reduce, shortFp } from "./util.js";

export function mountDag({ wrap, canvas, models, names, selected, onSelect }) {
  const ctx = canvas.getContext("2d");
  let W = 0, H = 0, dpr = 1, raf = 0, drag = null;
  const pan = { x: 0, y: 0 };
  const pos = {};
  const expanded = new Set();
  const cols = {}; // name -> [[out, srcTable, srcCol]] once hydrated
  let hoverCol = null;
  let sel = selected;
  const COLL = 50, HEADER = 30, ROW = 17, NODE_W = 148;

  async function hydrate(name) {
    if (cols[name]) return;
    try { cols[name] = toCols(await loadDetail(name)); } catch { cols[name] = []; }
  }

  function layout() {
    const r = wrap.getBoundingClientRect();
    dpr = Math.min(devicePixelRatio || 1, 2);
    W = r.width; H = r.height;
    canvas.width = W * dpr; canvas.height = H * dpr;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    const layers = {};
    names.forEach((n) => (layers[models[n].layer] ||= []).push(n));
    const ks = Object.keys(layers), nL = Math.max(ks.length, 2), padX = 30, vGap = 20, padTop = 22;
    const colGap = (W - padX * 2 - NODE_W) / (nL - 1);
    ks.forEach((L) => {
      const ns = layers[L];
      const hs = ns.map((n) => (expanded.has(n) && cols[n] ? HEADER + cols[n].length * ROW + 10 : COLL));
      const total = hs.reduce((a, b) => a + b, 0) + vGap * (ns.length - 1);
      let y = Math.max(padTop, (H - total) / 2);
      ns.forEach((n, i) => {
        const p = { x: padX + (+L) * colGap, y, w: NODE_W, h: hs[i] };
        if (expanded.has(n) && cols[n])
          p.cols = cols[n].map((c, ci) => ({ name: c[0], src: c[1], srcCol: c[2], y: y + HEADER + ci * ROW + ROW / 2 }));
        pos[n] = p; y += hs[i] + vGap;
      });
    });
  }

  const rr = (x, y, w, h, r) => { ctx.beginPath(); ctx.roundRect(x, y, w, h, r); };
  const aOut = (n, col) => { const p = pos[n]; if (col && p.cols) { const c = p.cols.find((c) => c.name === col); if (c) return { x: p.x + p.w, y: c.y }; } return { x: p.x + p.w, y: p.cols ? p.y + HEADER / 2 : p.y + p.h / 2 }; };
  const aIn = (n, col) => { const p = pos[n]; if (col && p.cols) { const c = p.cols.find((c) => c.name === col); if (c) return { x: p.x, y: c.y }; } return { x: p.x, y: p.cols ? p.y + HEADER / 2 : p.y + p.h / 2 }; };
  const strand = (a, b) => { const mx = (a.x + b.x) / 2; ctx.moveTo(a.x, a.y); ctx.bezierCurveTo(mx, a.y, mx, b.y, b.x, b.y); };

  function edge(a, b, mode) { // 0 base · 1 selected-path · 2 hovered column thread
    if (mode) {
      const g = ctx.createLinearGradient(a.x, a.y, b.x, b.y);
      g.addColorStop(0, "#8B5CF6"); g.addColorStop(0.5, "#6366F1"); g.addColorStop(1, "#A855F7");
      ctx.strokeStyle = g; ctx.globalAlpha = mode === 2 ? 1 : 0.9; ctx.lineWidth = mode === 2 ? 2.8 : 2.3;
    } else { ctx.strokeStyle = "rgba(139,92,246,.14)"; ctx.globalAlpha = 1; ctx.lineWidth = 1.3; }
    ctx.beginPath(); strand(a, b); ctx.stroke();
    ctx.strokeStyle = mode === 2 ? "rgba(250,250,250,.5)" : mode === 1 ? "rgba(250,250,250,.32)" : "rgba(139,92,246,.05)";
    ctx.lineWidth = mode === 2 ? 0.9 : mode === 1 ? 0.7 : 0.5;
    ctx.save(); ctx.translate(0, -1.3); ctx.beginPath(); strand(a, b); ctx.stroke(); ctx.restore();
    ctx.globalAlpha = 1;
  }

  function draw(t) {
    ctx.clearRect(0, 0, W, H); ctx.save(); ctx.translate(pan.x, pan.y);
    ctx.strokeStyle = "rgba(255,255,255,.02)"; ctx.lineWidth = 1;
    for (let x = 0; x < W; x += 46) { ctx.beginPath(); ctx.moveTo(x, -pan.y); ctx.lineTo(x, H - pan.y); ctx.stroke(); }
    const selModel = models[sel];
    const onPath = new Set(selModel ? [sel, ...selModel.depends_on, ...selModel.down] : []);
    names.forEach((n) => {
      const m = models[n];
      m.depends_on.forEach((d) => {
        if (!pos[d] || !pos[n]) return;
        const lit = onPath.has(n) && onPath.has(d);
        const cs = (expanded.has(n) && cols[n] ? cols[n] : []).filter((c) => c[1] === d);
        if ((!expanded.has(n) && !expanded.has(d)) || !cs.length) { edge(aOut(d, null), aIn(n, null), lit ? 1 : 0); return; }
        cs.forEach((c) => {
          const hv = hoverCol && ((hoverCol.node === d && hoverCol.name === c[2]) || (hoverCol.node === n && hoverCol.name === c[0]));
          edge(aOut(d, c[2]), aIn(n, c[0]), hv ? 2 : lit ? 1 : 0);
        });
      });
    });
    names.forEach((n) => {
      const m = models[n], p = pos[n];
      if (!p) return;
      const color = STATE_COLOR[m.state], dim = onPath.size && !onPath.has(n);
      ctx.globalAlpha = dim ? 0.5 : 1;
      rr(p.x, p.y, p.w, p.h, 8); ctx.fillStyle = n === sel ? "#27272A" : "#18181B"; ctx.fill();
      ctx.lineWidth = n === sel ? 1.6 : 1; ctx.strokeStyle = n === sel ? css("--accent") : "#2E2E36"; ctx.stroke();
      ctx.fillStyle = m.running ? css("--cyan") : color; rr(p.x, p.y, 3.5, p.h, 8); ctx.fill();
      if (m.running && !reduce) {
        const k = (Math.sin(t / 420) + 1) / 2;
        ctx.globalAlpha = (dim ? 0.5 : 1) * (0.55 - k * 0.45);
        ctx.strokeStyle = css("--cyan"); ctx.lineWidth = 1.5;
        rr(p.x - 3 - k * 4, p.y - 3 - k * 4, p.w + 6 + k * 8, p.h + 6 + k * 8, 11); ctx.stroke();
        ctx.globalAlpha = dim ? 0.5 : 1;
      }
      ctx.textBaseline = "middle";
      ctx.fillStyle = css("--text"); ctx.font = "600 12px ui-monospace,Menlo,monospace";
      ctx.fillText(n.length > 17 ? n.slice(0, 16) + "…" : n, p.x + 13, p.y + (p.cols ? 15 : 17));
      if (!p.cols) {
        ctx.fillStyle = css("--muted"); ctx.font = "10px ui-monospace,Menlo,monospace";
        ctx.fillText(`${MAT_GLYPH[m.mat] || "·"} ${m.mat}`, p.x + 13, p.y + 33);
        ctx.fillStyle = css("--faint"); ctx.textAlign = "right"; ctx.fillText(shortFp(m.fingerprint), p.x + p.w - 11, p.y + 33); ctx.textAlign = "left";
      } else {
        ctx.fillStyle = css("--faint"); ctx.font = "10px ui-monospace,Menlo,monospace";
        ctx.textAlign = "right"; ctx.fillText(shortFp(m.fingerprint), p.x + p.w - 22, p.y + 15); ctx.textAlign = "left";
        ctx.strokeStyle = "#2E2E36"; ctx.lineWidth = 1; ctx.beginPath(); ctx.moveTo(p.x + 8, p.y + HEADER); ctx.lineTo(p.x + p.w - 8, p.y + HEADER); ctx.stroke();
        p.cols.forEach((c) => {
          const hv = hoverCol && hoverCol.node === n && hoverCol.name === c.name;
          if (hv) { ctx.fillStyle = "rgba(139,92,246,.12)"; ctx.fillRect(p.x + 4, c.y - ROW / 2, p.w - 8, ROW); }
          ctx.fillStyle = hv ? css("--text") : css("--muted"); ctx.font = "10.5px ui-monospace,Menlo,monospace";
          ctx.fillText(c.name.length > 18 ? c.name.slice(0, 17) + "…" : c.name, p.x + 14, c.y);
          ctx.fillStyle = hv ? css("--accent") : "rgba(139,92,246,.3)";
          if (m.depends_on.length) ctx.fillRect(p.x + 1, c.y - 2, 3, 4);
          if (m.down.length) ctx.fillRect(p.x + p.w - 4, c.y - 2, 3, 4);
        });
      }
      ctx.globalAlpha = dim ? 0.55 : 0.8; ctx.fillStyle = css("--faint"); ctx.font = "13px ui-monospace,Menlo,monospace"; ctx.textAlign = "center";
      ctx.fillText(p.cols ? "–" : "+", p.x + p.w - 10, p.y + (p.cols ? 15 : 12)); ctx.textAlign = "left"; ctx.globalAlpha = 1;
    });
    ctx.restore();
    raf = requestAnimationFrame(draw);
  }

  const at = (e) => { const r = canvas.getBoundingClientRect(); return { x: e.clientX - r.left - pan.x, y: e.clientY - r.top - pan.y }; };
  const nodeAt = (m) => names.find((n) => { const p = pos[n]; return p && m.x >= p.x && m.x <= p.x + p.w && m.y >= p.y && m.y <= p.y + p.h; });
  const toggle = async (n) => {
    if (expanded.has(n)) expanded.delete(n);
    else { await hydrate(n); expanded.add(n); }
    layout();
  };

  canvas.addEventListener("mousedown", (e) => {
    const m = at(e);
    const tog = names.find((n) => { const p = pos[n]; return p && m.x >= p.x + p.w - 18 && m.x <= p.x + p.w - 2 && m.y >= p.y + 2 && m.y <= p.y + 20; });
    if (tog) { toggle(tog); return; }
    const hit = nodeAt(m);
    if (hit) { sel = hit; onSelect(hit); }
    else { drag = { x: e.clientX - pan.x, y: e.clientY - pan.y }; canvas.style.cursor = "grabbing"; }
  });
  canvas.addEventListener("dblclick", (e) => { const hit = nodeAt(at(e)); if (hit) { sel = hit; onSelect(hit); toggle(hit); } });
  const mv = (e) => {
    if (drag) { pan.x = e.clientX - drag.x; pan.y = e.clientY - drag.y; return; }
    const m = at(e);
    let hc = null;
    for (const n of names) {
      const p = pos[n];
      if (!p || !p.cols) continue;
      if (m.x >= p.x && m.x <= p.x + p.w) for (const c of p.cols) { if (Math.abs(m.y - c.y) <= ROW / 2) { hc = { node: n, name: c.name }; break; } }
      if (hc) break;
    }
    hoverCol = hc; canvas.style.cursor = hc ? "pointer" : "grab";
  };
  const up = () => { drag = null; canvas.style.cursor = hoverCol ? "pointer" : "grab"; };
  addEventListener("mousemove", mv);
  addEventListener("mouseup", up);

  const ro = new ResizeObserver(layout);
  ro.observe(wrap);
  layout();
  raf = requestAnimationFrame(draw);

  return {
    select(name) { sel = name; },
    async expand(name) { await hydrate(name); expanded.add(name); layout(); },
    async expandAll() { await Promise.all(names.map(hydrate)); names.forEach((n) => expanded.add(n)); layout(); },
    collapseAll() { expanded.clear(); layout(); },
    destroy() { cancelAnimationFrame(raf); ro.disconnect(); removeEventListener("mousemove", mv); removeEventListener("mouseup", up); },
  };
}
