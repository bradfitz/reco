"use strict";

const $ = (id) => document.getElementById(id);
const sets = new Set(["a", "b", "union"]);
const values = new Map();
const versions = new Map();
const dependencies = {union: ["a", "b"], details: ["union"], totalRunes: ["details"], score: ["totalRunes", "weight"], summary: ["label", "totalRunes", "score"]};
let socket, reconnectTimer, retry = 0, paused = false, live = false, revision = null;
let transaction = null, nextRequest = 0, highlightedDep = null;
const leafIDs = ["a", "b", "label", "weight"];
let peerInfo = null, peerStatus = null;

function owns(id) { return !peerInfo || peerInfo.owners[id] === peerInfo.local; }

function editable(el) {
  const id = el.closest(".node")?.id.slice(5);
  return !id || owns(id);
}

function status(text, online) {
  live = online;
  $("status").textContent = text;
  $("status").dataset.state = online ? "live" : "offline";
  $("stale").hidden = online || values.size === 0;
  document.querySelectorAll("[data-edit]").forEach((el) => { el.disabled = !online || !!transaction?.request || !editable(el); });
  $("connection-toggle").textContent = paused ? "Reconnect" : "Disconnect";
  updateTxControls();
  renderPeerStatus();
}

function configurePeer(info) {
  peerInfo = info || null;
  $("peer-panel").hidden = $("peer-activity").hidden = !peerInfo;
  if (!peerInfo) return;
  document.title = `reco · process ${info.local.toUpperCase()}`;
  $("graph-mode").textContent = `Process ${info.local.toUpperCase()} / Two processes / One metagraph / In memory`;
  $("peer-title").textContent = `METAGRAPH ${info.metagraph} · PROCESS ${info.local.toUpperCase()}`;
  $("peer-link").href = info.peerURL;
  $("peer-link").textContent = `Open process ${info.remote.toUpperCase()} ↗`;
  for (const id of leafIDs) {
    const card = $("node-" + id);
    card.classList.toggle("remote", !owns(id));
    let badge = card.querySelector(".ownership");
    if (!badge) { badge = document.createElement("p"); badge.className = "ownership"; card.querySelector(".node-heading").after(badge); }
    badge.textContent = owns(id) ? `Owned here · process ${info.local.toUpperCase()}` : `Remote mirror · owned by process ${info.remote.toUpperCase()}`;
  }
}

function renderPeerStatus() {
  if (!peerInfo) { $("peer-stale").hidden = true; return; }
  const st = peerStatus;
  const fresh = live && st?.connected && st.ready === st.watching;
  $("peer-status").textContent = !live ? "Browser disconnected · peer status unknown" : !st?.connected ? "Peer disconnected · remote values are stale" : !fresh ? `Receiving snapshots · ${st.ready}/${st.watching} watches ready` : `Connected to ${st.remote.toUpperCase()} · ${st.ready} remote watches · ${st.serving} exports`;
  $("peer-status").title = st?.epoch ? `Remote process epoch: ${st.epoch}` : "";
  $("peer-stale").hidden = fresh;
  $("peer-stale").textContent = live ? "Remote state is not fresh. Owned leaves remain editable. Cached mirrors and derived values may be stale; missing values wait for their first snapshot. Reconnect replaces remote state before streaming deltas again." : "This browser is disconnected. Both graph values and peer status shown here are cached.";
  $("peer-panel").dataset.state = fresh ? "live" : "stale";
}

function peerWire(wire) {
  $("peer-wire").textContent = `${wire.direction.toUpperCase()}\n${wire.data}`;
  const li = document.createElement("li");
  const time = document.createElement("time"); time.textContent = new Date().toLocaleTimeString();
  const body = document.createElement("span");
  try {
    const frame = JSON.parse(wire.data);
    body.textContent = `${wire.direction} · ${frame.type}${frame.id ? ` #${frame.id}` : ""}${frame.type === "value" ? frame.full ? " · snapshot" : ` · delta ${frame.base} → ${frame.version}` : ""} · ${new TextEncoder().encode(wire.data).length} bytes`;
  } catch { body.textContent = `${wire.direction} · large message (preview truncated)`; }
  li.append(time, body); $("peer-log").prepend(li);
  while ($("peer-log").children.length > 30) $("peer-log").lastChild.remove();
}

function log(text) {
  const item = document.createElement("li");
  const time = document.createElement("time");
  time.textContent = new Date().toLocaleTimeString();
  const body = document.createElement("span");
  body.textContent = text;
  item.append(time, body);
  $("activity-log").prepend(item);
  while ($("activity-log").children.length > 30) $("activity-log").lastChild.remove();
}

function send(command) {
  if (!live || socket?.readyState !== WebSocket.OPEN || transaction?.request) return false;
  $("error").hidden = true;
  if (command.node && !owns(command.node)) { showError("Edit that leaf in its owning process."); return false; }
  if (transaction) {
    if (transaction.edits.length >= 128) { showError("A transaction can contain at most 128 edits."); return false; }
    // Normalize/validate only the edited leaf. The server validates again at
    // commit against current state; this browser preview is never authoritative.
    if (command.op === "add" || command.op === "remove") {
      command = {...command, item: command.item.trim()};
      if (!command.item || new TextEncoder().encode(command.item).length > 48) { showError("Words must contain 1–48 bytes after trimming spaces."); return false; }
    }
    transaction.edits.push(command);
    const draft = draftValues();
    if (draft.get("a").size > 64 || draft.get("b").size > 64) {
      transaction.edits.pop();
      showError("Sets are limited to 64 words in this demo.");
      return false;
    }
    render(leafIDs, true);
    updateTxControls();
    return true;
  }
  socket.send(JSON.stringify(command));
  return true;
}

function showError(text) { $("error").textContent = text; $("error").hidden = false; }

// Drafts are a browser-only preview. Derived nodes remain on committed state.
// Rebase the ordered operations on the latest snapshot when another tab edits.
function draftValues() {
  const draft = new Map(values);
  if (!transaction) return draft;
  draft.set("a", new Set(values.get("a")));
  draft.set("b", new Set(values.get("b")));
  for (const edit of transaction.edits) {
    switch (edit.op) {
      case "set": draft.set(edit.node, edit.value); break;
      case "add": draft.get(edit.node).add(edit.item); break;
      case "remove": draft.get(edit.node).delete(edit.item); break;
      case "clear": draft.set(edit.node, new Set()); break;
      case "replace": draft.set(edit.node, new Set(edit.items)); break;
    }
  }
  return draft;
}

function updateTxControls() {
  $("tx-start").disabled = !live || !!transaction;
  $("tx-rollback").disabled = !transaction || !!transaction.request;
  $("tx-commit").disabled = !live || !transaction?.edits.length || !!transaction.request;
  $("tx-status").textContent = !transaction ? "Auto-commit: each edit is one transaction." : transaction.request ? "Committing the batch…" : `Tx mode · ${transaction.edits.length} staged edits. Leaves preview drafts; functions show committed state. Commit applies to the latest server state.`;
  $("tx-edits").hidden = !transaction?.edits.length;
  $("tx-edits").replaceChildren();
  for (const edit of transaction?.edits || []) {
    const li = document.createElement("li");
    li.textContent = `${edit.op} ${edit.node || "example"}${edit.item !== undefined ? `: ${edit.item}` : edit.value !== undefined ? `: ${JSON.stringify(edit.value)}` : edit.items ? `: {${edit.items.join(", ")}}` : ""}`;
    $("tx-edits").append(li);
  }
  for (const id of leafIDs) {
    $("node-" + id).classList.toggle("draft", owns(id) && !!transaction?.edits.some((edit) => edit.node === id));
  }
}

function discardTx(reason) {
  if (!transaction) return;
  transaction = null;
  render(leafIDs, true);
  updateTxControls();
  if (reason) log(reason);
}

function connect() {
  clearTimeout(reconnectTimer);
  status("Connecting…", false);
  const url = new URL("/ws", location.href);
  url.protocol = location.protocol === "https:" ? "wss:" : "ws:";
  const ws = new WebSocket(url);
  socket = ws;
  ws.onopen = () => { if (socket === ws && !paused) status("Syncing…", false); };
  ws.onmessage = (event) => {
    if (socket !== ws || paused) return;
    try { receive(JSON.parse(event.data)); }
    catch (error) { status("Resynchronizing…", false); log(`Cannot apply message: ${error.message}. Requesting a fresh snapshot.`); ws.close(); }
  };
  ws.onclose = () => {
    if (socket !== ws) return;
    discardTx(transaction?.request ? "Connection lost during commit: outcome unknown until resync. The batch will NOT be replayed." : "Discarded uncommitted transaction on disconnect.");
    status(paused ? "Disconnected" : "Reconnecting…", false);
    log("Connection closed. Cached values are now stale; no edits will be replayed.");
    if (!paused) {
      const delay = Math.min(500 * 2 ** retry++, 5000) + Math.random() * 250;
      reconnectTimer = setTimeout(connect, delay);
    }
  };
  ws.onerror = () => ws.close();
}

function receive(message) {
  if (message.type === "peer") { peerStatus = message.peer; renderPeerStatus(); return; }
  if (message.type === "peer-wire") { peerWire(message.wire); return; }
  if (message.type !== "committed") $("wire-message").textContent = JSON.stringify(message, null, 2);
  if (message.type === "error") {
    if (transaction && transaction.request !== null && transaction.request === message.request) { transaction.request = null; status("Live", true); }
    showError(message.error);
    log(`Rejected: ${message.error}`);
    return;
  }
  if (message.type === "committed") {
    if (transaction && transaction.request !== null && transaction.request === message.request) {
      discardTx(`Committed the whole batch as transaction ${message.revision}.`);
      status("Live", true);
    }
    return;
  }
  if (message.type !== "snapshot" && message.type !== "update") throw new Error("unknown message type");
  const initial = message.type === "snapshot";
  if (!initial && (!live || message.revision !== revision + 1)) throw new Error("revision gap");
  if (initial) { values.clear(); versions.clear(); configurePeer(message.info); peerStatus = message.peer || null; }
  const changed = [];
  for (const node of message.nodes || []) {
    if (Object.hasOwn(node, "value")) {
      values.set(node.id, sets.has(node.id) ? new Set(node.value) : node.id === "details" ? new Map(Object.entries(node.value)) : node.value);
    } else if (node.set) {
      const set = values.get(node.id);
      for (const key of node.set.remove) set.delete(key);
      for (const key of node.set.add) set.add(key);
    } else if (node.map) {
      const map = values.get(node.id);
      for (const key of node.map.remove) map.delete(key);
      for (const [key, value] of Object.entries(node.map.put)) map.set(key, value);
    } else { throw new Error("missing node value or delta"); }
    versions.set(node.id, node.version);
    changed.push(node.id);
  }
  // Apply the entire transaction before rendering any node.
  revision = message.revision;
  retry = 0;
  status("Live", true);
  $("revision").textContent = `transaction ${revision}`;
  render(initial ? [...leafIDs, ...Object.keys(dependencies)] : changed, initial);
  updateTxControls();
  log(initial ? `Snapshot · ${changed.length} nodes · transaction ${revision}. Fully synchronized.` : `Transaction ${revision} · ${changed.length ? changed.join(" → ") : "no value changes"} · applied atomically.`);
}

function render(changed, initial) {
  const display = draftValues();
  // Repaint shared-membership hints when either source set changes.
  const redraw = new Set(changed);
  if (redraw.has("a") || redraw.has("b")) { redraw.add("a"); redraw.add("b"); }
  for (const id of redraw) {
    const value = display.get(id);
    if (sets.has(id)) renderSet(id, value, display);
    else if (id === "details") renderMap(value);
    else if (id === "label" || id === "weight") {
      // Do not discard another tab's in-progress draft on unrelated updates.
      if (initial || document.activeElement !== $(id)) $(id).value = value ?? "";
    } else $("value-" + id).textContent = value ?? "Waiting for remote state…";
    const version = document.querySelector(`[data-version="${id}"]`);
    if (version) version.textContent = versions.has(id) ? `v${versions.get(id)}` : "awaiting snapshot";
  }
  for (const id of changed) {
    const el = $("node-" + id);
    el.classList.remove("flash");
    void el.offsetWidth;
    el.classList.add("flash");
  }
  requestAnimationFrame(() => drawEdges(changed));
}

function renderSet(id, value, display) {
  const target = $("value-" + id);
  target.replaceChildren();
  if (!value?.size) {
    const empty = document.createElement("span");
    empty.className = "empty";
    empty.textContent = value ? "∅ empty set" : "Waiting for remote state…";
    target.append(empty);
    return;
  }
  for (const key of [...value].sort()) {
    const chip = document.createElement("span");
    chip.className = "chip";
    const text = document.createElement("span");
    text.textContent = key;
    chip.append(text);
    if (id !== "union") {
      if (display.get(id === "a" ? "b" : "a")?.has(key)) {
        chip.classList.add("shared");
        chip.title = "Present in both input sets";
      }
      const remove = document.createElement("button");
      remove.type = "button";
      remove.textContent = "×";
      remove.setAttribute("aria-label", `Remove ${key} from set ${id.toUpperCase()}`);
      remove.setAttribute("data-edit", "");
      remove.disabled = !live || !!transaction?.request || !owns(id);
      remove.addEventListener("click", () => send({op: "remove", node: id, item: key}));
      chip.append(remove);
    }
    target.append(chip);
  }
}

function renderMap(value) {
  const target = $("value-details");
  target.replaceChildren();
  for (const [key, word] of [...(value || [])].sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)) {
    const row = document.createElement("tr");
    for (const cell of [key, word.upper, word.runes]) {
      const td = document.createElement("td");
      td.textContent = cell;
      row.append(td);
    }
    target.append(row);
  }
  if (!value?.size) {
    const row = document.createElement("tr"), cell = document.createElement("td");
    cell.colSpan = 3;
    cell.className = "empty";
    cell.textContent = value ? "∅ empty map" : "Waiting for remote state…";
    row.append(cell);
    target.append(row);
  }
}

function drawEdges(changed = []) {
  const svg = $("edges"), board = $("board").getBoundingClientRect();
  svg.replaceChildren();
  if (window.innerWidth <= 800) return;
  for (const [to, froms] of Object.entries(dependencies)) {
    for (const from of froms) {
      const a = $("node-" + from).getBoundingClientRect();
      const b = $("node-" + to).getBoundingClientRect();
      const sameColumn = Math.abs(a.left - b.left) < 5;
      let d;
      if (sameColumn) {
        const start = a.right - board.left, end = b.right - board.left;
        const x = Math.max(start, end) + 8;
        d = `M ${start} ${a.top - board.top + 32} H ${x} V ${b.top - board.top + 32} H ${end}`;
      } else {
        const x = a.right - board.left, y = a.top - board.top + 32;
        const endX = b.left - board.left, endY = b.top - board.top + 32;
        const mid = (x + endX) / 2;
        d = `M ${x} ${y} C ${mid} ${y}, ${mid} ${endY}, ${endX} ${endY}`;
      }
      const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
      path.setAttribute("d", d);
      path.dataset.from = from;
      path.dataset.to = to;
      if (changed.includes(from) && changed.includes(to)) path.classList.add("active");
      svg.append(path);
    }
  }
  paintDependency();
}

function paintDependency() {
  document.querySelectorAll(".dep-highlight").forEach((el) => el.classList.remove("dep-highlight"));
  if (!highlightedDep) return;
  const {from, to} = highlightedDep;
  $("node-" + from).classList.add("dep-highlight");
  document.querySelector(`#edges path[data-from="${from}"][data-to="${to}"]`)?.classList.add("dep-highlight");
}

// The dependency list is also the diagram's source of truth. Keep the labels
// keyboard-focusable, and use click to scroll to a dependency on narrow screens.
for (const [to, froms] of Object.entries(dependencies)) {
  const label = $("node-" + to).querySelector("footer > span");
  label.replaceChildren(document.createTextNode("DEPENDS ON "));
  froms.forEach((from, i) => {
    if (i) label.append(document.createTextNode(", "));
    const button = document.createElement("button");
    button.type = "button"; button.className = "dep-link"; button.textContent = from;
    button.dataset.from = from; button.dataset.to = to;
    button.setAttribute("aria-label", `Highlight ${from}, a dependency of ${to}`);
    const highlight = () => { highlightedDep = {from, to}; paintDependency(); };
    const clear = () => {
      if (button.matches(":hover") || document.activeElement === button) return;
      if (highlightedDep?.from === from && highlightedDep?.to === to) { highlightedDep = null; paintDependency(); }
    };
    button.addEventListener("mouseenter", highlight);
    button.addEventListener("mouseleave", clear);
    button.addEventListener("focus", highlight);
    button.addEventListener("blur", clear);
    button.addEventListener("click", () => $("node-" + from).scrollIntoView({block: "center", behavior: "smooth"}));
    label.append(button);
  });
}

$("tx-start").addEventListener("click", () => {
  if (!live || transaction) return;
  transaction = {edits: [], request: null};
  $("error").hidden = true;
  updateTxControls();
});
$("tx-rollback").addEventListener("click", () => {
  if (transaction?.request) return;
  discardTx("Rolled back staged edits. The server graph was not changed.");
  $("error").hidden = true;
});
$("tx-commit").addEventListener("click", () => {
  if (!live || !transaction?.edits.length || transaction.request) return;
  transaction.request = ++nextRequest;
  socket.send(JSON.stringify({op: "batch", commands: transaction.edits, request: transaction.request}));
  status("Live", true);
});

document.querySelectorAll("[data-add]").forEach((form) => {
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    const input = form.elements.namedItem("item");
    if (send({op: "add", node: form.dataset.add, item: input.value})) input.value = "";
  });
});
document.querySelectorAll("[data-clear]").forEach((button) => button.addEventListener("click", () => send({op: "clear", node: button.dataset.clear})));
document.querySelectorAll("[data-replace]").forEach((button) => button.addEventListener("click", () => send({op: "replace", node: button.dataset.replace, items: ["fern", "moss"]})));
$("weight-form").addEventListener("submit", (event) => { event.preventDefault(); send({op: "set", node: "weight", value: Number($("weight").value)}); });
$("label-form").addEventListener("submit", (event) => { event.preventDefault(); send({op: "set", node: "label", value: $("label").value}); });
$("clear-log").addEventListener("click", () => $("activity-log").replaceChildren());
$("clear-peer-log").addEventListener("click", () => $("peer-log").replaceChildren());
$("connection-toggle").addEventListener("click", () => {
  paused = !paused;
  clearTimeout(reconnectTimer);
  if (paused) {
    discardTx(transaction?.request ? "Disconnected during commit; resync will show the outcome. No replay." : "Discarded uncommitted transaction on disconnect.");
    status("Disconnected", false); socket?.close();
  }
  else { socket?.close(); connect(); }
});
window.addEventListener("online", () => { if (!paused && !live) { socket?.close(); connect(); } });
new ResizeObserver(() => drawEdges()).observe($("board"));
connect();
