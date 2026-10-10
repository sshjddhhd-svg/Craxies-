"use strict";

const fs   = require("fs-extra");
const path = require("path");

const CONFIG_PATH = path.join(__dirname, "../../config.json");

let _timer   = null;
let _running = false;
let _busy    = false;
const _repliedMsgIDs = new Set();
const _lastSeenTs    = new Map();
const _lastReplyAt   = new Map();
const _startedAt     = Date.now();
const _msgQueue      = [];

function withTimeout(promise, ms, fallback) {
  return new Promise((resolve) => {
    let done = false;
    const t = setTimeout(() => { if (!done) { done = true; resolve(fallback); } }, ms);
    Promise.resolve(promise).then(
      (v) => { if (!done) { done = true; clearTimeout(t); resolve(v); } },
      ()  => { if (!done) { done = true; clearTimeout(t); resolve(fallback); } }
    );
  });
}
const SEND_TIMEOUT_MS    = 45000;
const HISTORY_TIMEOUT_MS = 20000;
const SCAN_WATCHDOG_MS   = 180000;
let _busySince = 0;
const _histFails = new Map();

const log = (m) => (global.log?.info || console.log)(`👁️ [مراقبة] ${m}`);
const warn = (m) => (global.log?.warn || console.warn)(`👁️ [مراقبة] ${m}`);

function getWatchers() {
  const list = global.config?.watchers;
  return Array.isArray(list) ? list : [];
}

function saveWatchers(list) {
  global.config = global.config || {};
  global.config.watchers = list;
  try {
    const cfg = fs.existsSync(CONFIG_PATH) ? fs.readJsonSync(CONFIG_PATH) : {};
    cfg.watchers = list;
    if (global.config.watcherIntervalSec) cfg.watcherIntervalSec = global.config.watcherIntervalSec;
    fs.writeJsonSync(CONFIG_PATH, cfg, { spaces: 2 });
  } catch (e) { warn(`فشل حفظ config.json: ${e.message}`); }
  return list;
}

function findWatcher(threadID) {
  return getWatchers().find(w => String(w.threadID) === String(threadID) && w.enabled !== false);
}

function sendReply(api, w, msg) {
  const p = new Promise((res) => {
    const text = String(w.reply || "").trim();
    if (!text) return res(false);
    try {
      api.sendMessage({ body: text }, String(w.threadID), (err) => {
        if (err) { warn(`فشل الرد على ${w.name || w.threadID}: ${err.error || err.message || err}`); return res(false); }
        log(`✔️ تم الرد على ${w.name || w.threadID} فوراً`);
        try { global._dashIO?.emit("watcher-reply", { threadID: w.threadID, name: w.name, ts: Date.now(), body: msg?.body || "" }); } catch (_) {}
        res(true);
      }, msg?.messageID);
    } catch (e) { warn(`استثناء أثناء الإرسال: ${e.message}`); res(false); }
  });
  return withTimeout(p, SEND_TIMEOUT_MS, false).then((ok) => {
    if (!ok) warn(`لم يكتمل الرد على ${w.name || w.threadID} — سيُعاد في الفحص القادم`);
    return ok;
  });
}

const _inFlight   = new Set();
const _idleSentFor = new Map();

function rememberReplied(mid) {
  if (!mid) return;
  _repliedMsgIDs.add(mid);
  if (_repliedMsgIDs.size > 5000) {
    const arr = [..._repliedMsgIDs]; _repliedMsgIDs.clear();
    arr.slice(-2500).forEach(x => _repliedMsgIDs.add(x));
  }
}

async function handleIncoming(api, msg, source, opts = {}) {
  if (!api || !msg) return false;
  const tid = String(msg.threadID || "");
  const w = findWatcher(tid);
  if (!w) return false;

  const me = String(api.getCurrentUserID?.() || "");
  const sender = String(msg.senderID || msg.authorID || "");
  if (!sender || sender === me) return false;
  const mid = msg.messageID;
  if (mid && _repliedMsgIDs.has(mid)) return false;
  const ts = parseInt(msg.timestamp) || Date.now();
  if (!opts.force && ts < _startedAt - 1000) return false;
  if (ts < (w.addedAt || 0)) return false;
  if (_inFlight.has(tid)) return false;

  const cooldown = Math.max(0, Number(w.cooldownSec) || 0) * 1000;
  const last = _lastReplyAt.get(tid) || 0;
  if (cooldown && Date.now() - last < cooldown) return false;

  _inFlight.add(tid);
  try {
    log(`رسالة جديدة في ${w.name || tid} (${source}) — جارٍ الرد…`);
    const ok = await sendReply(api, w, msg);
    if (ok) { rememberReplied(mid); _lastReplyAt.set(tid, Date.now()); }
    return ok;
  } finally { _inFlight.delete(tid); }
}

function sendText(api, tid, text) {
  return withTimeout(new Promise((res) => {
    try { api.sendMessage({ body: text }, String(tid), (err) => res(!err)); } catch { res(false); }
  }), SEND_TIMEOUT_MS, false);
}

function onEvent(api, event) {
  if (!event) return;
  if (event.type !== "message" && event.type !== "message_reply") return;
  if (!(event.body || (event.attachments && event.attachments.length))) return;
  
  _msgQueue.push({ api, event, timestamp: Date.now() });
  handleIncoming(api, event, "فوري").catch(() => {});
}

function getHistoryOnce(api, threadID, amount) {
  return withTimeout(new Promise((res) => {
    if (typeof api.getThreadHistory !== "function") return res(null);
    try {
      api.getThreadHistory(threadID, amount, undefined, (err, msgs) => res(err ? null : (msgs || [])));
    } catch { res(null); }
  }), HISTORY_TIMEOUT_MS, null);
}

async function getHistory(api, threadID, amount) {
  let r = await getHistoryOnce(api, threadID, amount);
  if (r === null) {
    await new Promise(x => setTimeout(x, 1500));
    r = await getHistoryOnce(api, threadID, amount);
  }
  return r;
}

function isRealMessage(m) {
  if (!m || !(m.senderID || m.authorID)) return false;
  if (m.type && !/^message/.test(String(m.type))) return false;
  return !!(m.body || (m.attachments && m.attachments.length) || m.messageID);
}

async function scanThread(api, w, me) {
  const tid = String(w.threadID);
  const hist = await getHistory(api, tid, 15);
  if (hist === null) {
    const n = (_histFails.get(tid) || 0) + 1;
    _histFails.set(tid, n);
    return;
  }
  _histFails.delete(tid);

  const msgs = hist
    .filter(isRealMessage)
    .sort((x, y) => (parseInt(x.timestamp) || 0) - (parseInt(y.timestamp) || 0));
  if (!msgs.length) return;

  const since = Math.max(_lastSeenTs.get(tid) || 0, _startedAt, w.addedAt || 0);
  let maxTs = since;
  for (const m of msgs) {
    const ts = parseInt(m.timestamp) || 0;
    if (ts <= since) continue;
    if (ts > maxTs) maxTs = ts;
    await handleIncoming(api, { ...m, threadID: tid }, "مراقبة");
  }
  _lastSeenTs.set(tid, maxTs);
}

async function scanOnce() {
  const api = global.api;
  if (!api) return;
  if (_busy) return;
  const list = getWatchers().filter(w => w.enabled !== false);
  if (!list.length) return;
  _busy = true; _busySince = Date.now();
  const me = String(api.getCurrentUserID?.() || "");
  try {
    for (const w of list) {
      if (!findWatcher(w.threadID)) continue;
      try { await scanThread(api, w, me); } catch (e) {}
    }
  } finally { _busy = false; }
}

function schedule() {
  if (!_running) return;
  const sec = Math.max(1, Number(global.config?.watcherIntervalSec) || 2);
  _timer = setTimeout(async () => { try { await scanOnce(); } catch (e) {} schedule(); }, sec * 1000);
}

function start() {
  if (_running) return;
  _running = true;
  schedule();
}

function stop() {
  _running = false;
  if (_timer) clearTimeout(_timer);
  _timer = null;
}

function getInbox(api, limit = 60) {
  return new Promise((res, rej) => {
    if (typeof api.getThreadList !== "function") return rej(new Error("getThreadList غير متاح"));
    api.getThreadList(limit, null, ["INBOX"], (err, threads) => {
      if (err) return rej(new Error(err.message || String(err)));
      const me = String(api.getCurrentUserID?.() || "");
      const out = (threads || []).map(t => {
        const isGroup = !!t.isGroup;
        let name = t.name || t.threadName || "";
        let avatar = t.imageSrc || null;
        if (!isGroup && Array.isArray(t.userInfo)) {
          const other = t.userInfo.find(u => String(u.id) !== me) || t.userInfo[0];
          if (other) { name = name || other.name; avatar = avatar || other.thumbSrc || other.profilePicture || null; }
        }
        return {
          threadID: String(t.threadID),
          name: name || String(t.threadID),
          isGroup,
          avatar,
          snippet: String(t.snippet || "").slice(0, 80),
          timestamp: parseInt(t.timestamp) || 0,
          unread: t.unreadCount || 0,
        };
      });
      res(out);
    });
  });
}

module.exports = { start, stop, onEvent, getWatchers, saveWatchers, getInbox, scanOnce };
