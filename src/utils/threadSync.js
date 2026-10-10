"use strict";
/**
 * threadSync.js — مزامنة كل محادثات الحساب (خاص + مجموعات) مع لوحة التحكم
 *
 * فور تسجيل الدخول بالكوكيز يجلب قائمة المحادثات من فيسبوك (الوارد + طلبات
 * المراسلة) ويحفظها في قاعدة البيانات وفي ذاكرة مؤقتة، فتظهر محادثات الخاص
 * في صفحة "تفعيل مباشر" مباشرةً بدون انتظار وصول رسالة جديدة.
 * يعيد المزامنة تلقائياً كل دقيقة، وفوراً عند تغيير الحساب.
 */

const log  = (m) => (global.log?.info || console.log)(`🔄 [مزامنة المحادثات] ${m}`);
const warn = (m) => (global.log?.warn || console.warn)(`🔄 [مزامنة المحادثات] ${m}`);

const _cache = new Map();   // threadID → { threadID, name, isGroup, snippet, timestamp, avatar }
let _lastUID   = null;
let _lastSync  = 0;
let _syncing   = null;
let _timer     = null;
const FULL_SYNC_MS = 60 * 1000;

function _fetchFolder(api, folder, limit) {
  return new Promise((res) => {
    if (!api || typeof api.getThreadList !== "function") return res([]);
    let done = false;
    const t = setTimeout(() => { if (!done) { done = true; res([]); } }, 20000);
    try {
      api.getThreadList(limit, null, [folder], (err, threads) => {
        if (done) return; done = true; clearTimeout(t);
        if (err) { warn(`تعذر جلب ${folder}: ${err.error || err.message || err}`); return res([]); }
        res(Array.isArray(threads) ? threads : []);
      });
    } catch (e) { if (!done) { done = true; clearTimeout(t); res([]); } }
  });
}

function _normalize(api, t) {
  const me = String(api.getCurrentUserID?.() || "");
  const tid = String(t.threadID || t.threadFbId || "");
  if (!tid) return null;
  const parts = Array.isArray(t.participantIDs) ? t.participantIDs.map(String) : [];
  const isGroup = typeof t.isGroup === "boolean" ? t.isGroup : parts.length > 2;
  let name = t.name || t.threadName || "";
  let avatar = t.imageSrc || null;
  if (!isGroup && Array.isArray(t.userInfo)) {
    const other = t.userInfo.find(u => String(u.id) !== me) || t.userInfo[0];
    if (other) { name = name || other.name; avatar = avatar || other.thumbSrc || other.profilePicture || null; }
  }
  if (!isGroup && !name) {
    const otherID = parts.find(p => p !== me);
    name = (otherID && global._nameCache?.u?.[otherID]) || "";
  }
  return {
    threadID: tid,
    name: name || tid,
    isGroup,
    avatar,
    snippet: String(t.snippet || t.lastMessage?.body || "").slice(0, 80),
    timestamp: parseInt(t.timestamp) || parseInt(t.lastMessageTimestamp) || 0,
  };
}

async function syncNow() {
  const api = global.api;
  if (!api) return [];
  if (_syncing) return _syncing;
  _syncing = (async () => {
    try {
      const [inbox, pending, other] = await Promise.all([
        _fetchFolder(api, "INBOX", 100),
        _fetchFolder(api, "PENDING", 30),
        _fetchFolder(api, "OTHER", 30),
      ]);
      const all = [...inbox, ...pending, ...other].map(t => _normalize(api, t)).filter(Boolean);
      const { getOrCreateThread, Thread } = require("./database");
      for (const t of all) {
        _cache.set(t.threadID, { ...(_cache.get(t.threadID) || {}), ...t });
        if (!t.isGroup && global._nameCache) global._nameCache.t[t.threadID] = t.name;
        try {
          const row = await getOrCreateThread(t.threadID, t.name);
          if (row && t.name && t.name !== t.threadID && row.name !== t.name) {
            await Thread.update({ name: t.name }, { where: { threadID: t.threadID } });
          }
        } catch (_) {}
      }
      _lastSync = Date.now();
      const dms = all.filter(t => !t.isGroup).length;
      log(`تمت المزامنة: ${all.length} محادثة (${dms} خاص، ${all.length - dms} مجموعة)`);
      try { global._dashIO?.emit("threads-updated", { count: all.length }); } catch (_) {}
      return getAll();
    } catch (e) {
      warn(`فشل المزامنة: ${e.message}`);
      return getAll();
    } finally { _syncing = null; }
  })();
  return _syncing;
}

/** يُستدعى عند وصول أي رسالة — يضيف المحادثة فوراً إن لم تكن موجودة */
function touch(info) {
  if (!info || !info.threadID) return;
  const tid = String(info.threadID);
  const prev = _cache.get(tid);
  const isNew = !prev;
  _cache.set(tid, {
    threadID: tid,
    name: info.name || prev?.name || tid,
    isGroup: typeof info.isGroup === "boolean" ? info.isGroup : !!prev?.isGroup,
    avatar: prev?.avatar || null,
    snippet: String(info.snippet || "").slice(0, 80) || prev?.snippet || "",
    timestamp: info.timestamp || Date.now(),
  });
  if (isNew) { try { global._dashIO?.emit("threads-updated", { threadID: tid }); } catch (_) {} }
}

function get(threadID) { return _cache.get(String(threadID)) || null; }
function getAll() { return [..._cache.values()].sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0)); }
function lastSyncAt() { return _lastSync; }

function _tick() {
  const api = global.api;
  if (api) {
    let uid = null;
    try { uid = String(api.getCurrentUserID()); } catch (_) {}
    if (uid && uid !== _lastUID) {
      // حساب جديد (كوكيز جديدة) — امسح القديم وزامن فوراً
      _lastUID = uid;
      _cache.clear();
      log(`حساب جديد (${uid}) — جارٍ جلب كل المحادثات…`);
      syncNow();
    } else if (Date.now() - _lastSync > FULL_SYNC_MS) {
      syncNow();
    }
  }
  _timer = setTimeout(_tick, 3000);
}

function start() {
  if (_timer) return;
  _timer = setTimeout(_tick, 1000);
}

module.exports = { start, syncNow, touch, get, getAll, lastSyncAt };
