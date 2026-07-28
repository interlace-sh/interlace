// Line diff (LCS) for plan SQL: previous_sql vs new_sql → [["ctx"|"add"|"del", line]].
export function lineDiff(before, after) {
  const a = (before || "").split("\n");
  const b = (after || "").split("\n");
  if (!before) return b.map((l) => ["add", l]);
  if (!after) return a.map((l) => ["del", l]);
  const n = a.length, m = b.length;
  const lcs = Array.from({ length: n + 1 }, () => new Uint16Array(m + 1));
  for (let i = n - 1; i >= 0; i--)
    for (let j = m - 1; j >= 0; j--)
      lcs[i][j] = a[i] === b[j] ? lcs[i + 1][j + 1] + 1 : Math.max(lcs[i + 1][j], lcs[i][j + 1]);
  const out = [];
  let i = 0, j = 0;
  while (i < n && j < m) {
    if (a[i] === b[j]) { out.push(["ctx", a[i]]); i++; j++; }
    else if (lcs[i + 1][j] >= lcs[i][j + 1]) out.push(["del", a[i++]]);
    else out.push(["add", b[j++]]);
  }
  while (i < n) out.push(["del", a[i++]]);
  while (j < m) out.push(["add", b[j++]]);
  return out;
}

// Collapse long unchanged stretches to 2 lines of context around each change.
export function compactDiff(diff, context = 2) {
  const keep = new Array(diff.length).fill(false);
  diff.forEach(([kind], idx) => {
    if (kind === "ctx") return;
    for (let k = Math.max(0, idx - context); k <= Math.min(diff.length - 1, idx + context); k++) keep[k] = true;
  });
  const out = [];
  let skipping = false;
  diff.forEach((entry, idx) => {
    if (keep[idx]) { out.push(entry); skipping = false; }
    else if (!skipping) { out.push(["ctx", "…"]); skipping = true; }
  });
  return out;
}
