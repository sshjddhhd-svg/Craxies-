"use strict";
/**
 * chatWatcher.js — مراقبة الغروبات: البوت يكون دائماً صاحب آخر رسالة.
 *
 * الفكرة: لكل غروب حالة واحدة فقط "هل آخر رسالة ليست للبوت؟".
 *  - أي رسالة من شخص آخر  → الغروب يصبح "يحتاج رداً".
 *  - أي رسالة من البوت     → الغروب "منتهٍ" (لا إرسال).
 *  - عامل إرسال واحد لكل غروب يرسل رداً واحداً على أحدث رسالة، ثم يتحقق:
 *    إن وصلت رسائل جديدة أثناء الإرسال يرد مرة أخرى، وإلا يتوقف.
 *  لذلك الرسائل السريعة جداً لا تُتجاهل ولا تسبب سيل ردود؛ تُدمج في رد واحد
 *  وتكون آخر رسالة دائماً للبوت.
 *
 *  الحماية من التوقف:
 *  - مهلة لكل إرسال وقراءة، ولا يوجد قفل عام يمكن أن يعلق.
 *  - لا تخلٍّ نهائي عن غروب بعد فشل: إعادة محاولة مع تأخير متزايد (حتى 60 ثانية).
 *  - فحص دوري للسجل يلتقط أي رسالة لم تصل عبر المستمع.
 *  - مراقب (watchdog) يعيد تشغيل الحلقة إن توقفت، ويحرر أي عامل عالق.
 *  - يستخدم دائماً global.api الحالي (يعمل بعد إعادة تسجيل الدخول).
 */

const fs   = require("fs-extra");
const path = require("path");

const CONFIG_PATH = path.join(__dirname, "../../config.json");

const SEND_TIMEOUT_MS    = 30000;
const HISTORY_TIMEOUT_MS = 15000;
const WORKER_STUCK_MS    = 90000;
const MIN_GAP_MS         = 1200;   // أقل فاصل بين ردّين في نفس الغروب (حماية من حظر فيسبوك)
const MAX_BACKOFF_MS     = 60000;
const HISTORY_AMOUNT     = 10;

const log  = (m) => (global.log?.info || console.log)(`👁️ [مراقبة] ${m}`);
const warn = (m) => (global.log?.warn || console.warn)(`👁️ [مراقبة] ${m}`);

/** tid -> { dirty, lastOtherTs, lastOtherMid, lastBotTs, sending, sendingSince, fails, nextTryAt, wake } */
const _state = new Map();
let _timer = null, _watchdog = null, _running = false, _scanning = false, _scanSince = 0, _lastTick = 0;

function S(tid) {
  tid = String(tid);
  let s = _state.get(tid);
  if (!s) {
    s = { dirty: false, lastOtherTs: 0, lastOtherMid: null, lastBotTs: 0, sending: false,
          sendingSince: 0, fails: 0, nextTryAt: 0, wake: null, initialized: false };
    _state.set(tid, s);
  }
  return s;
}

function api() { return global.api || null; }
function myID(a) { try { return String(a?.getCurrentUserID?.() || ""); } catch { return ""; } }

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
  // نظّف حالة الغروبات المحذوفة/المعطّلة
  for (const tid of [..._state.keys()]) {
    if (!list.some(w => String(w.threadID) === tid && w.enabled !== false)) _state.delete(tid);
  }
  return list;
}

function findWatcher(threadID) {
  return getWatchers().find(w => String(w.threadID) === String(threadID) && w.enabled !== false);
}

function tsOf(m) { return parseInt(m?.timestamp) || 0; }
function senderOf(m) { return String(m?.senderID || m?.authorID || ""); }

/* ───────────── تسجيل الرسائل ───────────── */

function noteOther(tid, ts, mid) {
  const s = S(tid);
  ts = ts || Date.now();
  if (ts < s.lastOtherTs) return;
  s.lastOtherTs = ts;
  if (mid) s.lastOtherMid = mid;
  if (ts > s.lastBotTs) { s.dirty = true; kick(tid); }
}

function noteBot(tid, ts) {
  const s = S(tid);
  ts = ts || Date.now();
  if (ts > s.lastBotTs) s.lastBotTs = ts;
  if (s.lastBotTs >= s.lastOtherTs) s.dirty = false;
}

/* ───────────── الإرسال ───────────── */

function rawSend(a, tid, text, replyTo) {
  return withTimeout(new Promise((res) => {
    try {
      const cb = (err, info) => res(err ? { ok: false, err } : { ok: true, info });
      if (replyTo) a.sendMessage({ body: text }, String(tid), cb, replyTo);
      else a.sendMessage({ body: text }, String(tid), cb);
    } catch (e) { res({ ok: false, err: e }); }
  }), SEND_TIMEOUT_MS, { ok: false, err: new Error("انتهت مهلة الإرسال") });
}

async function sendReply(a, w, replyTo) {
  const text = String(w.reply || "").trim();
  if (!text) return false;
  let r = await rawSend(a, w.threadID, text, replyTo);
  if (!r.ok && replyTo) r = await rawSend(a, w.threadID, text, null); // الرسالة ربما حُذفت
  if (!r.ok) {
    const e = r.err;
    warn(`فشل الرد على ${w.name || w.threadID}: ${e?.error || e?.message || e}`);
    return false;
  }
  log(`✔️ تم الرد على ${w.name || w.threadID}`);
  try { global._dashIO?.emit("watcher-reply", { threadID: w.threadID, name: w.name, ts: Date.now() }); } catch (_) {}
  return true;
}

/** يوقظ عامل الغروب (أو يجدول إيقاظه) */
function kick(tid, delay = 0) {
  tid = String(tid);
  const s = S(tid);
  if (s.sending) return; // العامل الحالي سيتحقق من الحالة بعد انتهائه
  if (s.wake) { clearTimeout(s.wake); s.wake = null; }
  s.wake = setTimeout(() => { s.wake = null; runWorker(tid).catch(e => warn(`خطأ العامل: ${e.message}`)); }, Math.max(0, delay));
}

async function runWorker(tid) {
  const s = S(tid);
  if (s.sending) return;
  s.sending = true; s.sendingSince = Date.now();
  try {
    while (_running) {
      const w = findWatcher(tid);
      if (!w) { s.dirty = false; break; }
      if (!s.dirty) break;

      const a = api();
      if (!a) break; // سيُعاد عبر المراقب الدوري

      const now = Date.now();
      const cooldown = Math.max(0, Number(w.cooldownSec) || 0) * 1000;
      const waitUntil = Math.max(s.nextTryAt, s.lastBotTs + Math.max(cooldown, MIN_GAP_MS));
      if (now < waitUntil) {
        s.sending = false;
        kick(tid, waitUntil - now + 50);
        return;
      }

      const startedAt = Date.now();
      const replyTo = s.lastOtherMid;
      const ok = await sendReply(a, w, replyTo);
      if (ok) {
        s.fails = 0; s.nextTryAt = 0;
        // نعتبر وقت الرد = بداية الإرسال؛ أي رسالة وصلت بعدها تُبقي الغروب "يحتاج رداً"
        noteBot(tid, Math.max(startedAt, s.lastBotTs));
        if (s.lastOtherTs > startedAt) s.dirty = true;
      } else {
        s.fails++;
        const back = Math.min(MAX_BACKOFF_MS, 2000 * Math.pow(2, Math.min(s.fails - 1, 5)));
        s.nextTryAt = Date.now() + back;
        warn(`سيُعاد الرد على ${w.name || tid} بعد ${Math.round(back / 1000)} ث (محاولة ${s.fails})`);
      }
    }
  } finally {
    s.sending = false;
  }
}

/* ───────────── المستمع الفوري ───────────── */

function onEvent(_api, event) {
  try {
    if (!event) return;
    if (event.type !== "message" && event.type !== "message_reply") return;
    const tid = String(event.threadID || "");
    if (!tid || !findWatcher(tid)) return;
    const a = api() || _api;
    const sender = senderOf(event);
    if (!sender) return;
    if (sender === myID(a)) { noteBot(tid, tsOf(event) || Date.now()); return; }
    noteOther(tid, Math.max(tsOf(event), 1) || Date.now(), event.messageID);
  } catch (e) { warn(`خطأ في المستمع: ${e.message}`); }
}

/* ───────────── الفحص الدوري (شبكة أمان) ───────────── */

function getHistory(a, threadID, amount) {
  return withTimeout(new Promise((res) => {
    if (typeof a.getThreadHistory !== "function") return res(null);
    try { a.getThreadHistory(threadID, amount, undefined, (err, msgs) => res(err ? null : (msgs || []))); }
    catch { res(null); }
  }), HISTORY_TIMEOUT_MS, null);
}

function isRealMessage(m) {
  if (!m || !senderOf(m)) return false;
  if (m.type && !/^message/.test(String(m.type))) return false;
  return true;
}

async function scanThread(a, w, me) {
  const tid = String(w.threadID);
  const s = S(tid);
  const hist = await getHistory(a, tid, HISTORY_AMOUNT);
  if (!hist) return;
  const msgs = hist.filter(isRealMessage).sort((x, y) => tsOf(x) - tsOf(y));
  const last = msgs[msgs.length - 1];
  s.initialized = true;
  if (!last) return;
  const lts = tsOf(last) || Date.now();
  if (senderOf(last) === me) noteBot(tid, lts);
  else noteOther(tid, lts, last.messageID);
  if (s.dirty && !s.sending && !s.wake) kick(tid);
}

async function scanOnce() {
  const a = api();
  if (!a) return;
  if (_scanning && Date.now() - _scanSince < 120000) return;
  _scanning = true; _scanSince = Date.now();
  try {
    const me = myID(a);
    const list = getWatchers().filter(w => w.enabled !== false);
    await Promise.all(list.map(w =>
      scanThread(a, w, me).catch(e => warn(`خطأ في ${w.name || w.threadID}: ${e.message}`))
    ));
  } finally { _scanning = false; }
}

function schedule() {
  if (!_running) return;
  if (_timer) clearTimeout(_timer);
  const sec = Math.max(2, Number(global.config?.watcherIntervalSec) || 4);
  _timer = setTimeout(async () => {
    _lastTick = Date.now();
    try { await scanOnce(); } catch (e) { warn(`خطأ: ${e.message}`); }
    schedule();
  }, sec * 1000);
}

function watchdog() {
  if (!_running) return;
  const sec = Math.max(2, Number(global.config?.watcherIntervalSec) || 4);
  // الحلقة الدورية توقفت؟ أعد تشغيلها
  if (Date.now() - _lastTick > sec * 1000 * 4 + 30000) {
    warn("الحلقة الدورية متوقفة — إعادة تشغيل");
    _scanning = false; _lastTick = Date.now(); schedule();
  }
  // عامل عالق؟ حرّره وأعد المحاولة
  for (const [tid, s] of _state) {
    if (s.sending && Date.now() - s.sendingSince > WORKER_STUCK_MS) {
      warn(`عامل الغروب ${tid} عالق — تم تحريره`);
      s.sending = false;
    }
    if (s.dirty && !s.sending && !s.wake) kick(tid);
  }
}

function start() {
  if (_running) return;
  _running = true;
  _lastTick = Date.now();
  log("🚀 المراقبة تعمل — البوت سيبقى صاحب آخر رسالة في كل غروب مُراقَب");
  schedule();
  _watchdog = setInterval(() => { try { watchdog(); } catch (e) { warn(e.message); } }, 10000);
  if (_watchdog.unref) _watchdog.unref();
  scanOnce().catch(() => {});
}

function stop() {
  _running = false;
  if (_timer) clearTimeout(_timer);
  if (_watchdog) clearInterval(_watchdog);
  _timer = null; _watchdog = null;
  for (const s of _state.values()) if (s.wake) { clearTimeout(s.wake); s.wake = null; }
}

// لا تدع خطأ غير معالج يُسقط المراقبة
if (!global.__chatWatcherGuards) {
  global.__chatWatcherGuards = true;
  process.on("unhandledRejection", (e) => warn(`رفض غير معالج: ${e?.message || e}`));
}

function getInbox(a, limit = 60) {
  return new Promise((res, rej) => {
    if (typeof a.getThreadList !== "function") return rej(new Error("getThreadList غير متاح"));
    a.getThreadList(limit, null, ["INBOX"], (err, threads) => {
      if (err) return rej(new Error(err.message || String(err)));
      const me = myID(a);
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
