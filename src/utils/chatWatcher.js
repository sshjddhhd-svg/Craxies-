"use strict";
/**
 * chatWatcher.js — مراقبة محادثات خاصة محددة والرد التلقائي الفوري
 *
 * يعمل بطريقتين معاً:
 *  1) فوري: عند وصول حدث رسالة من MQTT/Long-Poll (onEvent)
 *  2) مراقبة نشطة: يفحص سجل كل محادثة مراقبة كل بضع ثوانٍ عبر getThreadHistory
 *     حتى لو لم يصل أي إشعار/حدث للبوت.
 *
 * الإعدادات تُحفظ في config.json تحت المفتاح "watchers":
 *  [{ threadID, name, reply, enabled, cooldownSec }]
 * وفاصل الفحص في "watcherIntervalSec" (افتراضي 4 ثوانٍ).
 */

const fs   = require("fs-extra");
const path = require("path");

const CONFIG_PATH = path.join(__dirname, "../../config.json");

let _timer   = null;
let _running = false;
let _busy    = false;
const _repliedMsgIDs = new Set();       // رسائل تم الرد عليها
const _lastSeenTs    = new Map();       // threadID → آخر timestamp تمت معالجته
const _lastReplyAt   = new Map();       // threadID → وقت آخر رد
const _startedAt     = Date.now();

const log = (m) => (global.log?.info || console.log)(`👁️ [مراقبة] ${m}`);
const warn = (m) => (global.log?.warn || console.warn)(`👁️ [مراقبة] ${m}`);

// ─── Config helpers ───────────────────────────────────────────────────────────
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

// ─── Reply logic ──────────────────────────────────────────────────────────────
function sendReply(api, w, msg) {
  return new Promise((res) => {
    const text = String(w.reply || "").trim();
    if (!text) return res(false);
    api.sendMessage({ body: text }, String(w.threadID), (err) => {
      if (err) { warn(`فشل الرد على ${w.name || w.threadID}: ${err.error || err.message || err}`); return res(false); }
      log(`تم الرد على ${w.name || w.threadID} ✔`);
      try { global._dashIO?.emit("watcher-reply", { threadID: w.threadID, name: w.name, ts: Date.now(), body: msg?.body || "" }); } catch (_) {}
      res(true);
    }, msg?.messageID);
  });
}

async function handleIncoming(api, msg, source) {
  if (!api || !msg) return;
  const tid = String(msg.threadID || "");
  const w = findWatcher(tid);
  if (!w) return;

  const me = String(api.getCurrentUserID?.() || "");
  const sender = String(msg.senderID || msg.authorID || "");
  if (!sender || sender === me) return;              // تجاهل رسائلي
  const mid = msg.messageID;
  if (mid && _repliedMsgIDs.has(mid)) return;        // تم الرد مسبقاً
  const ts = parseInt(msg.timestamp) || Date.now();
  if (ts < _startedAt - 1000) return;                // تجاهل الرسائل القديمة قبل التشغيل
  if (ts < (w.addedAt || 0)) return;                 // تجاهل ما قبل إضافة المراقبة

  if (mid) _repliedMsgIDs.add(mid);
  if (_repliedMsgIDs.size > 5000) {
    const arr = [..._repliedMsgIDs]; _repliedMsgIDs.clear();
    arr.slice(-2500).forEach(x => _repliedMsgIDs.add(x));
  }

  const cooldown = Math.max(0, Number(w.cooldownSec) || 0) * 1000;
  const last = _lastReplyAt.get(tid) || 0;
  if (cooldown && Date.now() - last < cooldown) return;
  _lastReplyAt.set(tid, Date.now());

  log(`رسالة جديدة في ${w.name || tid} (${source}) — جارٍ الرد…`);
  await sendReply(api, w, msg);
}

// ─── 1) Instant path: called from handlerEvents ──────────────────────────────
function onEvent(api, event) {
  if (!event) return;
  if (event.type !== "message" && event.type !== "message_reply") return;
  handleIncoming(api, event, "فوري").catch(() => {});
}

// ─── 2) Active monitoring: poll history of each watched thread ───────────────
function getHistory(api, threadID, amount) {
  return new Promise((res) => {
    if (typeof api.getThreadHistory !== "function") return res([]);
    try {
      api.getThreadHistory(threadID, amount, undefined, (err, msgs) => res(err ? [] : (msgs || [])));
    } catch { res([]); }
  });
}

async function scanOnce() {
  const api = global.api;
  if (!api || _busy) return;
  const list = getWatchers().filter(w => w.enabled !== false);
  if (!list.length) return;
  _busy = true;
  try {
    for (const w of list) {
      const tid = String(w.threadID);
      const msgs = await getHistory(api, tid, 5);
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
  } catch (e) {
    warn(`خطأ في الفحص: ${e.message}`);
  } finally { _busy = false; }
}

function schedule() {
  if (!_running) return;
  const sec = Math.max(2, Number(global.config?.watcherIntervalSec) || 4);
  _timer = setTimeout(async () => { await scanOnce(); schedule(); }, sec * 1000);
}

function start() {
  if (_running) return;
  _running = true;
  log("محرك المراقبة يعمل");
  schedule();
}

function stop() {
  _running = false;
  if (_timer) clearTimeout(_timer);
  _timer = null;
}

// ─── Inbox listing for dashboard ─────────────────────────────────────────────
function getInbox(api, limit = 60) {
  return new Promise((res, rej) => {
    if (typeof api.getThreadList !== "function") return rej(new Error("getThreadList غير متاح"));
    api.getThreadList(limit, null, ["INBOX"], (err, threads) => {
      if (err) return rej(new Error(err.error || err.message || String(err)));
      const me = String(api.getCurrentUserID?.() || "");
      const out = (threads || []).map(t => {
        const isGroup = !!t.isGroup;
        let name = t.name || t.threadName || "";
        let avatar = t.imageSrc || null;
        if (!isGroup && Array.isArray(t.userInfo)) {
          const other = t.userInfo.find(u => String(u.id) !== me) || t.userInfo[0];
          if (other) { name = name || other.name; avatar = avatar || other.thumbSrc || other.profilePicture || null; }
        }
        const snippet = t.snippet || t.lastMessage?.body || "";
        return {
          threadID: String(t.threadID),
          name: name || String(t.threadID),
          isGroup,
          avatar,
          snippet: String(snippet).slice(0, 80),
          timestamp: parseInt(t.timestamp) || 0,
          unread: t.unreadCount || 0,
        };
      });
      res(out);
    });
  });
}

module.exports = { start, stop, onEvent, getWatchers, saveWatchers, getInbox, scanOnce };
