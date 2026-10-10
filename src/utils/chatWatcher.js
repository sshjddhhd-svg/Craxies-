"use strict";
/**
 * chatWatcher.js — مراقبة الغروبات: البوت يبقى دائماً صاحب آخر رسالة.
 *
 * الطريقة (بسيطة ومباشرة كما طلبت):
 *  - كل 4 ثوانٍ يقرأ البوت سجل كل غروب مُراقَب من فيسبوك نفسه.
 *  - إن كانت آخر رسالة في الغروب للبوت → لا يفعل شيئاً.
 *  - إن كانت آخر رسالة لشخص آخر → يرسل الرد فوراً.
 *  - بالإضافة لذلك: أي رسالة جديدة تصل عبر المستمع تُطلق فحصاً فورياً،
 *    فلا ينتظر البوت الـ 4 ثوانٍ.
 *
 * لماذا لن يتجاهل الرسائل السريعة بعد الآن:
 *  - لا مقارنة بين ساعة السيرفر وساعة فيسبوك (كانت سبب تجاهل الرسائل).
 *    القرار يعتمد فقط على: "مَن صاحب آخر رسالة؟" ومعرّف الرسالة.
 *  - إن وصلت رسائل أثناء الإرسال، يُعاد الفحص مباشرة بعد انتهاء الإرسال.
 *
 * لماذا لن يتوقف:
 *  - مهلة لكل قراءة وإرسال، وكل غروب مستقل عن الآخر.
 *  - لا يتخلى عن أي غروب أبداً؛ عند الفشل يعيد المحاولة (أقصى انتظار 8 ثوانٍ).
 *  - إن فشلت قراءة السجل يعتمد على آخر رسالة وصلت عبر المستمع.
 *  - مراقب (watchdog) يعيد تشغيل الحلقة إن توقفت ويحرّر أي غروب عالق.
 *  - يستخدم دائماً global.api الحالي (يعمل بعد إعادة تسجيل الدخول).
 */

const fs   = require("fs-extra");
const path = require("path");

const CONFIG_PATH = path.join(__dirname, "../../config.json");

const DEFAULT_INTERVAL_SEC = 4;
const HISTORY_AMOUNT       = 8;
const HISTORY_TIMEOUT_MS   = 12000;
const SEND_TIMEOUT_MS      = 25000;
const BUSY_STUCK_MS        = 60000;
const MIN_GAP_MS           = 1500;   // أقل فاصل بين ردّين في نفس الغروب (حماية من حظر فيسبوك)
const RESEND_AFTER_MS      = 15000;  // إن لم يظهر ردّنا في السجل بعد هذه المدة نرسل مرة أخرى
const MAX_BACKOFF_MS       = 8000; 
const EVENT_TRUST_MS       = 20000;  // رسالة وصلت بالمستمع ولم تظهر بعد في السجل

const log  = (m) => { try { (global.log?.info || console.log)(`👁️ [مراقبة] ${m}`); } catch (_) {} };
const warn = (m) => { try { (global.log?.warn || console.warn)(`👁️ [مراقبة] ${m}`); } catch (_) {} };

/**
 * لكل غروب:
 *  last       : آخر رسالة معروفة { mid, sender, mine }
 *  evt        : آخر رسالة وصلت عبر المستمع من شخص آخر { mid, at }
 *  repliedTo  : معرّف الرسالة التي رددنا بعدها آخر مرة
 *  repliedAt  : وقت آخر رد ناجح (ساعة السيرفر — للفواصل فقط)
 *  busy/busySince/again : منع التداخل + إعادة الفحص إن وصلت رسائل أثناء العمل
 *  fails/nextTryAt      : إعادة المحاولة عند الفشل
 */
const _state = new Map();
let _timer = null, _watchdog = null, _running = false, _lastTick = 0;

function S(tid) {
  tid = String(tid);
  let s = _state.get(tid);
  if (!s) {
    s = { last: null, evt: null, repliedTo: null, repliedAt: 0, busy: false, busySince: 0,
          again: false, fails: 0, nextTryAt: 0, kickTimer: null };
    _state.set(tid, s);
  }
  return s;
}

function api() { return global.api || null; }
function myID(a) { try { return String(a?.getCurrentUserID?.() || ""); } catch { return ""; } }
function intervalMs() {
  const sec = Number(global.config?.watcherIntervalSec) || DEFAULT_INTERVAL_SEC;
  return Math.max(3, Math.min(60, sec)) * 1000;
}

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

/* ───────────── الإعدادات ───────────── */

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
  for (const tid of [..._state.keys()]) {
    if (!list.some(w => String(w.threadID) === tid && w.enabled !== false)) {
      const s = _state.get(tid);
      if (s?.kickTimer) clearTimeout(s.kickTimer);
      _state.delete(tid);
    }
  }
  // فحص فوري للغروبات الجديدة/المعدّلة
  for (const w of list) if (w.enabled !== false) kick(w.threadID, 200);
  return list;
}

function findWatcher(threadID) {
  return getWatchers().find(w => String(w.threadID) === String(threadID) && w.enabled !== false);
}

function tsOf(m) { return parseInt(m?.timestamp) || 0; }
function senderOf(m) { return String(m?.senderID || m?.authorID || m?.author || ""); }

/* ───────────── قراءة السجل ───────────── */

function getHistory(a, threadID) {
  return withTimeout(new Promise((res) => {
    if (typeof a.getThreadHistory !== "function") return res(null);
    try {
      const p = a.getThreadHistory(String(threadID), HISTORY_AMOUNT, undefined,
        (err, msgs) => res(err ? null : (Array.isArray(msgs) ? msgs : [])));
      if (p && typeof p.catch === "function") p.catch(() => res(null));
    } catch { res(null); }
  }), HISTORY_TIMEOUT_MS, null);
}

/** آخر رسالة حقيقية في السجل (نتجاهل أحداث النظام مثل "انضم فلان") */
function lastFromHistory(hist, me) {
  const msgs = hist
    .map((m, i) => ({ m, i }))
    .filter(({ m }) => m && !m.error && senderOf(m) && (!m.type || /^message/.test(String(m.type))));
  if (!msgs.length) return { last: null, mids: new Set() };
  msgs.sort((x, y) => (tsOf(x.m) - tsOf(y.m)) || (x.i - y.i));
  const l = msgs[msgs.length - 1].m;
  return {
    last: { mid: String(l.messageID || ""), sender: senderOf(l), mine: senderOf(l) === me },
    mids: new Set(msgs.map(({ m }) => String(m.messageID || ""))),
  };
}

/* ───────────── الإرسال ───────────── */

function rawSend(a, tid, text, replyTo) {
  return withTimeout(new Promise((res) => {
    try {
      const cb = (err, info) => res(err ? { ok: false, err } : { ok: true, info });
      const p = replyTo
        ? a.sendMessage({ body: text }, String(tid), cb, replyTo)
        : a.sendMessage({ body: text }, String(tid), cb);
      // المكتبة ترمي الخطأ أيضاً كـ Promise — نلتقطه حتى لا يصبح "رفضاً غير معالج"
      if (p && typeof p.catch === "function") p.catch((err) => res({ ok: false, err }));
    } catch (e) { res({ ok: false, err: e }); }
  }), SEND_TIMEOUT_MS, { ok: false, err: new Error("انتهت مهلة الإرسال") });
}

async function sendReply(a, w, replyTo) {
  const text = String(w.reply || "").trim();
  if (!text) return null;
  let r = await rawSend(a, w.threadID, text, replyTo);
  if (!r.ok && replyTo) r = await rawSend(a, w.threadID, text, null); // ربما حُذفت الرسالة
  if (!r.ok) {
    const e = r.err;
    warn(`فشل الرد على ${w.name || w.threadID}: ${e?.error || e?.message || e}`);
    return null;
  }
  log(`✔️ تم الرد على ${w.name || w.threadID}`);
  try { global._dashIO?.emit("watcher-reply", { threadID: w.threadID, name: w.name, ts: Date.now() }); } catch (_) {}
  return { mid: String(r.info?.messageID || "") };
}

/* ───────────── فحص غروب واحد ───────────── */

function kick(tid, delay = 0) {
  if (!_running) return;
  const s = S(tid);
  if (s.busy) { s.again = true; return; }
  if (s.kickTimer) return; // فحص مجدول بالفعل
  s.kickTimer = setTimeout(() => {
    s.kickTimer = null;
    checkThread(String(tid)).catch(e => warn(`خطأ في الفحص: ${e.message}`));
  }, Math.max(0, delay));
}

async function checkThread(tid) {
  const s = S(tid);
  if (s.busy) { s.again = true; return; }
  s.busy = true; s.busySince = Date.now(); s.again = false;
  let retryIn = 0;
  try {
    const w = findWatcher(tid);
    const a = api();
    if (!w || !a) return;
    const me = myID(a);
    if (!me) return; // لا نعرف حساب البوت بعد — لا نرسل حتى لا نرد على أنفسنا

    // 1) اقرأ السجل الحقيقي من فيسبوك
    const fetchStart = Date.now();
    const hist = await getHistory(a, tid);
    if (!_running) return;

    let last = s.last;
    if (hist) {
      const r = lastFromHistory(hist, me);
      if (r.last) last = r.last;
      // رسالة وصلت بالمستمع ولم تظهر بعد في السجل؟ اعتمد عليها
      if (s.evt && !r.mids.has(s.evt.mid) && s.evt.at >= fetchStart - EVENT_TRUST_MS) {
        last = { mid: s.evt.mid, sender: "event", mine: false };
      } else if (s.evt && r.mids.has(s.evt.mid)) {
        s.evt = null;
      }
    } else if (s.evt && (!s.last || s.last.mid !== s.evt.mid)) {
      // فشلت القراءة: استخدم آخر رسالة وصلت بالمستمع
      last = { mid: s.evt.mid, sender: "event", mine: false };
    }
    if (!last) return;
    s.last = last;

    // 2) آخر رسالة للبوت → لا شيء
    if (last.mine) { s.fails = 0; return; }

    // 3) آخر رسالة لشخص آخر → أرسل
    const now = Date.now();
    // رددنا للتو على نفس الرسالة والسجل لم يتحدث بعد؟ انتظر قليلاً قبل الإعادة
    if (s.repliedTo && s.repliedTo === last.mid && now - s.repliedAt < RESEND_AFTER_MS) return;

    const cooldown = Math.max(0, Number(w.cooldownSec) || 0) * 1000;
    const readyAt = Math.max(s.nextTryAt, s.repliedAt + Math.max(cooldown, MIN_GAP_MS));
    if (now < readyAt) { retryIn = readyAt - now + 50; return; }

    const sent = await sendReply(a, w, last.mid || undefined);
    if (sent) {
      s.fails = 0; s.nextTryAt = 0;
      s.repliedTo = last.mid; s.repliedAt = Date.now();
      s.last = { mid: sent.mid || `sent-${Date.now()}`, sender: me, mine: true };
      if (s.evt && s.evt.mid === last.mid) s.evt = null;
    } else {
      s.fails++;
      const back = Math.min(MAX_BACKOFF_MS, 2000 * Math.pow(2, Math.min(s.fails - 1, 4)));
      s.nextTryAt = Date.now() + back;
      retryIn = back + 50;
      warn(`سيُعاد الرد على ${w.name || tid} بعد ${Math.round(back / 1000)} ث (محاولة ${s.fails})`);
    }
  } finally {
    s.busy = false;
    // وصلت رسائل أثناء العمل؟ افحص فوراً حتى تبقى آخر رسالة للبوت
    if (s.again) { s.again = false; kick(tid, 300); }
    else if (retryIn) kick(tid, retryIn);
  }
}

/* ───────────── المستمع الفوري ───────────── */

function onEvent(_api, event) {
  try {
    if (!_running || !event) return;
    if (event.type !== "message" && event.type !== "message_reply") return;
    const tid = String(event.threadID || "");
    if (!tid || !findWatcher(tid)) return;
    const a = api() || _api;
    const sender = senderOf(event);
    if (!sender) return;
    const s = S(tid);
    if (sender === myID(a)) {
      s.last = { mid: String(event.messageID || ""), sender, mine: true };
      return;
    }
    const mid = String(event.messageID || `evt-${Date.now()}`);
    s.evt = { mid, at: Date.now() };
    s.last = { mid, sender, mine: false };
    kick(tid, 250); // تأخير بسيط لدمج الرسائل المتتالية السريعة في رد واحد
  } catch (e) { warn(`خطأ في المستمع: ${e.message}`); }
}

/* ───────────── الحلقة الدورية (كل 4 ثوانٍ) ───────────── */

async function scanOnce() {
  if (!api()) return;
  for (const w of getWatchers()) {
    if (w.enabled === false) continue;
    kick(w.threadID, 0);
  }
}

function loop() {
  if (!_running) return;
  if (_timer) clearTimeout(_timer);
  _timer = setTimeout(() => {
    _lastTick = Date.now();
    try { scanOnce(); } catch (e) { warn(`خطأ: ${e.message}`); }
    loop();
  }, intervalMs());
}

function watchdog() {
  if (!_running) return;
  if (Date.now() - _lastTick > intervalMs() * 3 + 5000) {
    warn("الحلقة الدورية متوقفة — إعادة تشغيل");
    _lastTick = Date.now();
    loop();
    scanOnce();
  }
  for (const [tid, s] of _state) {
    if (s.busy && Date.now() - s.busySince > BUSY_STUCK_MS) {
      warn(`الغروب ${tid} عالق — تم تحريره`);
      s.busy = false;
      kick(tid, 0);
    }
  }
}

function start() {
  if (_running) return;
  _running = true;
  _lastTick = Date.now();
  log(`🚀 المراقبة تعمل — فحص كل ${intervalMs() / 1000} ثوانٍ، البوت سيبقى صاحب آخر رسالة`);
  loop();
  _watchdog = setInterval(() => { try { watchdog(); } catch (e) { warn(e.message); } }, 5000);
  scanOnce();
}

function stop() {
  _running = false;
  if (_timer) clearTimeout(_timer);
  if (_watchdog) clearInterval(_watchdog);
  _timer = null; _watchdog = null;
  for (const s of _state.values()) if (s.kickTimer) { clearTimeout(s.kickTimer); s.kickTimer = null; }
}

// لا تدع خطأ غير معالج يُسقط البوت أو المراقبة
if (!global.__chatWatcherGuards) {
  global.__chatWatcherGuards = true;
  process.on("unhandledRejection", (e) => warn(`رفض غير معالج: ${e?.message || e?.error || e}`));
}

/* ───────────── قائمة المحادثات للوحة التحكم ───────────── */

function getInbox(a, limit = 60) {
  return new Promise((res, rej) => {
    if (typeof a.getThreadList !== "function") return rej(new Error("getThreadList غير متاح"));
    const p = a.getThreadList(limit, null, ["INBOX"], (err, threads) => {
      if (err) return rej(new Error(err.message || err.error || String(err)));
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
    if (p && typeof p.catch === "function") p.catch(() => {});
  });
}

module.exports = { start, stop, onEvent, getWatchers, saveWatchers, getInbox, scanOnce };
