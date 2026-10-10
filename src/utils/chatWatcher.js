"use strict";
/**
 * chatWatcher.js — مراقبة الغروبات والرد التلقائي بدون تفويت أي رسالة.
 *
 * طريقتان تعملان معاً:
 *  1) فوري: كل رسالة تصل عبر المستمع يُرد عليها مباشرة.
 *  2) فحص دوري: يقرأ آخر الرسائل من كل غروب مُراقَب ويرد على أي رسالة لم يُرد عليها بعد.
 *
 * الرسالة لا تُعتبر "منتهية" إلا بعد نجاح الرد عليها فعلاً، لذا إن فشل الإرسال أو
 * كان البوت مشغولاً بالرد على رسالة أخرى في نفس الغروب، يعيد المحاولة في الفحص التالي
 * بدل أن تضيع الرسالة حتى يرسل أحدهم رسالة جديدة.
 */

const fs   = require("fs-extra");
const path = require("path");

const CONFIG_PATH = path.join(__dirname, "../../config.json");

let _timer   = null;
let _running = false;
let _busy    = false;
let _busySince = 0;
const _startedAt = Date.now();

const _repliedMsgIDs = new Set();   // رسائل تم الرد عليها بنجاح
const _skippedMsgIDs = new Set();   // رسائل تُركت عمداً (تبريد / فشل متكرر)
const _failCount     = new Map();   // mid -> عدد مرات فشل الرد
const _lastReplyAt   = new Map();   // tid -> وقت آخر رد
const _floorTs       = new Map();   // tid -> لا نرد على رسائل أقدم من هذا الوقت
const _threadLocks   = new Map();   // tid -> Promise (طابور لكل غروب)
const _histFails     = new Map();

const SEND_TIMEOUT_MS    = 45000;
const HISTORY_TIMEOUT_MS = 20000;
const SCAN_WATCHDOG_MS   = 180000;
const HISTORY_AMOUNT     = 30;
const MAX_SEND_FAILS     = 5;
const CATCHUP_WINDOW_MS  = 10 * 60 * 1000; // عند بدء المراقبة: يرد على آخر رسالة إن كانت أحدث من 10 دقائق

const log  = (m) => (global.log?.info || console.log)(`👁️ [مراقبة] ${m}`);
const warn = (m) => (global.log?.warn || console.warn)(`👁️ [مراقبة] ${m}`);

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

function trimSet(set, max = 5000) {
  if (set.size <= max) return;
  const arr = [...set]; set.clear();
  arr.slice(-Math.floor(max / 2)).forEach(x => set.add(x));
}

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

/** طابور لكل غروب: الرسائل تنتظر دورها بدل أن تُرمى أثناء إرسال رد آخر */
function withThreadLock(tid, fn) {
  const prev = _threadLocks.get(tid) || Promise.resolve();
  const next = prev.then(fn, fn);
  const tail = next.catch(() => {});
  _threadLocks.set(tid, tail);
  tail.then(() => { if (_threadLocks.get(tid) === tail) _threadLocks.delete(tid); });
  return next;
}

function sendReply(api, w, msg) {
  const p = new Promise((res) => {
    const text = String(w.reply || "").trim();
    if (!text) return res(false);
    try {
      api.sendMessage({ body: text }, String(w.threadID), (err) => {
        if (err) { warn(`فشل الرد على ${w.name || w.threadID}: ${err.error || err.message || err}`); return res(false); }
        log(`✔️ تم الرد على ${w.name || w.threadID}`);
        try { global._dashIO?.emit("watcher-reply", { threadID: w.threadID, name: w.name, ts: Date.now(), body: msg?.body || "" }); } catch (_) {}
        res(true);
      }, msg?.messageID);
    } catch (e) { warn(`استثناء أثناء الإرسال: ${e.message}`); res(false); }
  });
  return withTimeout(p, SEND_TIMEOUT_MS, false);
}

function isHandled(mid) {
  return !!mid && (_repliedMsgIDs.has(mid) || _skippedMsgIDs.has(mid));
}

function threadFloor(tid, w) {
  return Math.max(_floorTs.get(tid) || (_startedAt - 1000), Number(w.addedAt) || 0);
}

/**
 * يعالج رسالة واحدة. النتيجة:
 *  "replied" تم الرد | "skip" لا تحتاج رداً | "defer" أُجّلت (تبريد) | "failed" فشل الإرسال وسيُعاد
 */
async function handleIncoming(api, msg, source, opts = {}) {
  if (!api || !msg) return "skip";
  const tid = String(msg.threadID || "");
  const w = findWatcher(tid);
  if (!w) return "skip";

  const me = String(api.getCurrentUserID?.() || "");
  const sender = String(msg.senderID || msg.authorID || "");
  if (!sender || sender === me) return "skip";
  const mid = msg.messageID;
  const ts = parseInt(msg.timestamp) || Date.now();
  if (!opts.force && ts < threadFloor(tid, w)) return "skip";
  if (isHandled(mid)) return "skip";

  return withThreadLock(tid, async () => {
    // أُعيد الفحص داخل الطابور: ربما رد عليها المسار الآخر أثناء الانتظار
    if (isHandled(mid)) return "skip";
    const ww = findWatcher(tid);
    if (!ww) return "skip";

    const cooldown = Math.max(0, Number(ww.cooldownSec) || 0) * 1000;
    const last = _lastReplyAt.get(tid) || 0;
    if (cooldown && Date.now() - last < cooldown) return "defer";

    log(`رسالة جديدة في ${ww.name || tid} (${source}) — جارٍ الرد…`);
    const ok = await sendReply(api, ww, msg);
    if (ok) {
      if (mid) { _repliedMsgIDs.add(mid); trimSet(_repliedMsgIDs); _failCount.delete(mid); }
      _lastReplyAt.set(tid, Date.now());
      return "replied";
    }
    if (mid) {
      const n = (_failCount.get(mid) || 0) + 1;
      _failCount.set(mid, n);
      if (n >= MAX_SEND_FAILS) {
        warn(`تعذّر الرد على رسالة في ${ww.name || tid} بعد ${n} محاولات — تم تجاوزها`);
        _skippedMsgIDs.add(mid); trimSet(_skippedMsgIDs); _failCount.delete(mid);
      } else {
        warn(`لم يكتمل الرد على ${ww.name || tid} — سيُعاد في الفحص القادم (${n}/${MAX_SEND_FAILS})`);
      }
    }
    return "failed";
  });
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
  if (!findWatcher(event.threadID)) return;
  handleIncoming(api, event, "فوري").catch(() => {});
  // فحص سريع إضافي بعد ثانيتين لالتقاط أي رسالة وصلت معها ولم تمر عبر المستمع
  if (!_quickScan) _quickScan = setTimeout(() => { _quickScan = null; scanOnce().catch(() => {}); }, 2000);
}
let _quickScan = null;

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
  const hist = await getHistory(api, tid, HISTORY_AMOUNT);
  if (hist === null) {
    const n = (_histFails.get(tid) || 0) + 1;
    _histFails.set(tid, n);
    if (n % 10 === 0) warn(`تعذّرت قراءة ${w.name || tid} (${n} مرة متتالية)`);
    return;
  }
  _histFails.delete(tid);

  const msgs = hist
    .filter(isRealMessage)
    .sort((x, y) => (parseInt(x.timestamp) || 0) - (parseInt(y.timestamp) || 0));

  // أول فحص لهذا الغروب (بدء البوت أو تفعيل المراقبة): لا نرد على القديم،
  // لكن إن كانت آخر رسالة من شخص آخر وحديثة نرد عليها كي لا تضيع
  let forceMid = null;
  if (!_floorTs.has(tid)) {
    const startFloor = (Date.now() - _startedAt < 120000) ? _startedAt - 1000 : Date.now() - 5000;
    _floorTs.set(tid, startFloor);
    const lastMsg = msgs[msgs.length - 1];
    if (lastMsg) {
      const s = String(lastMsg.senderID || lastMsg.authorID || "");
      const lts = parseInt(lastMsg.timestamp) || 0;
      if (s && s !== me && Date.now() - lts < CATCHUP_WINDOW_MS) forceMid = lastMsg.messageID;
    }
  }
  if (!msgs.length) return;

  const floor = threadFloor(tid, w);
  let pending = msgs.filter(m => {
    const s = String(m.senderID || m.authorID || "");
    if (!s || s === me) return false;
    if (isHandled(m.messageID)) return false;
    return m.messageID === forceMid || (parseInt(m.timestamp) || 0) >= floor;
  });
  if (!pending.length) return;

  // مع وجود تبريد: نرد على أحدث رسالة فقط ونعتبر ما قبلها مُعالَجاً
  if (Number(w.cooldownSec) > 0 && pending.length > 1) {
    pending.slice(0, -1).forEach(m => m.messageID && _skippedMsgIDs.add(m.messageID));
    trimSet(_skippedMsgIDs);
    pending = pending.slice(-1);
  }

  for (const m of pending) {
    const r = await handleIncoming(api, { ...m, threadID: tid }, "مراقبة", { force: m.messageID === forceMid });
    if (r === "defer" || r === "failed") break; // نعيد المحاولة في الفحص التالي بنفس الترتيب
  }
}

async function scanOnce() {
  const api = global.api;
  if (!api) return;
  if (_busy) {
    if (Date.now() - _busySince < SCAN_WATCHDOG_MS) return;
    warn("الفحص السابق علق — تم تحريره");
  }
  const all = getWatchers();
  // الغروبات المعطّلة: ننسى نقطة البداية كي لا نرد على رسائل فترة الإيقاف عند إعادة التفعيل
  for (const w of all) if (w.enabled === false) _floorTs.delete(String(w.threadID));
  for (const tid of [..._floorTs.keys()]) if (!all.some(w => String(w.threadID) === tid)) _floorTs.delete(tid);

  const list = all.filter(w => w.enabled !== false);
  if (!list.length) return;
  _busy = true; _busySince = Date.now();
  const me = String(api.getCurrentUserID?.() || "");
  try {
    for (const w of list) {
      if (!findWatcher(w.threadID)) continue;
      try { await scanThread(api, w, me); }
      catch (e) { warn(`خطأ في ${w.name || w.threadID}: ${e.message}`); }
    }
  } finally { _busy = false; }
}

function schedule() {
  if (!_running) return;
  const sec = Math.max(1, Number(global.config?.watcherIntervalSec) || 3);
  _timer = setTimeout(async () => {
    try { await scanOnce(); } catch (e) { warn(`خطأ: ${e.message}`); }
    schedule();
  }, sec * 1000);
}

function start() {
  if (_running) return;
  _running = true;
  log("🚀 المراقبة تعمل — رد فوري + فحص دوري لعدم تفويت أي رسالة");
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
