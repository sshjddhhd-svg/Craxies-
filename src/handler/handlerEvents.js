"use strict";
/**
 * handlerEvents.js — WHITE-V3 Style Event Handler
 * يعالج جميع أنواع الأحداث: رسائل، أحداث مجموعات، تفاعلات، إلخ
 */

const chalk = require("chalk");
const moment = require("moment-timezone");
const { getOrCreateUser, getOrCreateThread, logCommand } = require("../utils/database");

// ─── Helper ───────────────────────────────────────────────────────────────────
const _sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function _randInt(min, max) { return Math.floor(Math.random() * (max - min + 1)) + min; }

// ─── Anti-Spam / Flood Map ────────────────────────────────────────────────────
const _spamMap   = new Map(); // senderID → { count, resetAt }
const _warned    = new Set(); // senderIDs warned this window
const SPAM_LIMIT = 8;
const SPAM_WIN   = 10000; // 10s

function checkSpam(senderID) {
  const now = Date.now();
  let entry = _spamMap.get(senderID);
  if (!entry || now > entry.resetAt) {
    entry = { count: 0, resetAt: now + SPAM_WIN };
    _warned.delete(senderID);
  }
  entry.count++;
  _spamMap.set(senderID, entry);
  return {
    exceeded: entry.count > SPAM_LIMIT,
    warned:   _warned.has(senderID),
    setWarn:  () => _warned.add(senderID),
  };
}

// ─── Name Cache ───────────────────────────────────────────────────────────────
const _nc = { u: {}, t: {} };
global._nameCache = _nc;

async function resolveUser(api, uid) {
  if (_nc.u[uid]) return _nc.u[uid];
  try {
    const info = await new Promise((res, rej) =>
      api.getUserInfo(uid, (e, d) => e ? rej(e) : res(d || {})));
    _nc.u[uid] = info[uid]?.name || String(uid);
  } catch { _nc.u[uid] = String(uid); }
  return _nc.u[uid];
}

async function resolveThread(api, tid) {
  if (_nc.t[tid]) return _nc.t[tid];
  try {
    const info = await new Promise((res, rej) =>
      api.getThreadInfo(tid, (e, d) => e ? rej(e) : res(d || {})));
    _nc.t[tid] = info?.threadName || String(tid);
  } catch { _nc.t[tid] = String(tid); }
  return _nc.t[tid];
}

// ─── Console Logger ───────────────────────────────────────────────────────────
const ts = () => moment().tz(global.config?.timezone || "Africa/Algiers").format("HH:mm:ss");
function logMsg(senderName, threadName, body, isGroup, isCmd) {
  const icon  = isGroup ? chalk.blue("👥") : chalk.green("💬");
  const who   = chalk.bold.cyan(senderName);
  const where = isGroup ? chalk.bold.blue(`[${threadName}]`) : chalk.bold.green("DM");
  const prefix = isCmd ? chalk.magenta("⚡CMD ") : "";
  console.log(
    `${chalk.gray(ts())} ${icon} ${where} ${chalk.gray("←")} ${who}: ${prefix}${chalk.white(String(body||"").slice(0,120))}`
  );
}
function logEvent(type, threadName) {
  console.log(`${chalk.gray(ts())} ${chalk.yellow("⚡")} ${chalk.yellow(type)} @ ${chalk.cyan(threadName)}`);
}

// ─── Main Handler ─────────────────────────────────────────────────────────────
module.exports = async function handlerEvents(api, event, commands) {
  // مراقبة المحادثات المحددة — رد فوري
  try { require("../utils/chatWatcher").onEvent(api, event); } catch (_) {}

  if (!event) return;

  const io     = (() => { try { return require("../dashboard/server").getIO(); } catch { return null; } })();
  const prefix = global.commandPrefix || "/";
  const config = global.config || {};

  global._lastActivity = Date.now();
  try { require("../protection/mqttHealthCheck").onMqttActivity(); } catch (_) {}

  // ══ MESSAGE ══════════════════════════════════════════════════════════════════
  if (event.type === "message" || event.type === "message_reply") {
    const { body = "", threadID, senderID, messageID } = event;
    // بعض إصدارات المكتبة لا ترسل isGroup للخاص — في الخاص يكون threadID = ID الطرف الآخر
    const isGroup = typeof event.isGroup === "boolean"
      ? event.isGroup
      : String(threadID) !== String(senderID);

    // رسائلي أنا (من الهاتف مثلاً): تُعرض في اللوحة فقط بدون تنفيذ أوامر
    if (String(senderID) === String(api.getCurrentUserID())) {
      try {
        const known = require("../utils/threadSync").get(threadID);
        const selfMsg = { senderID: String(senderID), senderName: "أنا (البوت)", threadID: String(threadID),
          threadName: known?.name || String(threadID), body, isGroup, messageID, timestamp: Date.now() };
        require("../utils/threadSync").touch({ threadID, name: known?.name, isGroup, snippet: body });
        if (typeof global._bufferMsg === "function") global._bufferMsg(selfMsg);
        if (io) io.emit("message", selfMsg);
      } catch (_) {}
      return;
    }

    // ── Permissions (computed early for lock check) ────────────────────────
    const isOwner = global.isOwner ? global.isOwner(senderID) : String(senderID) === String(global.ownerID);
    const isAdmin = global.isAdmin ? global.isAdmin(senderID) : isOwner || (config.adminIDs||[]).map(String).includes(String(senderID));

    // Resolve names (non-blocking)
    // في المحادثات الخاصة نستخدم اسم المرسل كاسم للمحادثة حتى تظهر في لوحة التحكم
    const [senderName, threadName] = await Promise.all([
      resolveUser(api, senderID),
      isGroup ? resolveThread(api, threadID) : resolveUser(api, senderID),
    ]);

    // دراكاريس هو الأمر الوحيد الذي يعمل بدون بادئة، مع بقاء فحص الأدمن
    // إلزامياً قبل السماح له بالوصول إلى التنفيذ.
    const directCommand = body.trim().toLowerCase() === "دراكاريس";
    const isCmd = body.startsWith(prefix) || directCommand;
    logMsg(senderName, threadName, body, isGroup, isCmd);

    // Update DB (non-blocking)
    // نسجّل المحادثة دائماً (جماعية أو خاصة) حتى تظهر محادثات الخاص في قسم المراقبة
    Promise.all([
      getOrCreateUser(senderID, senderName).catch(() => {}),
      getOrCreateThread(threadID, threadName).catch(() => {}),
    ]);

    const msgData = { senderID: String(senderID), senderName, threadID: String(threadID), threadName, body, isGroup, messageID, timestamp: Date.now() };

    // أضف المحادثة فوراً لقائمة لوحة التحكم (خاص أو مجموعة)
    try { require("../utils/threadSync").touch({ threadID, name: threadName, isGroup, snippet: body, timestamp: msgData.timestamp }); } catch (_) {}

    // Buffer message for live dashboard page
    if (typeof global._bufferMsg === "function") global._bufferMsg(msgData);

    // Emit to dashboard
    if (io) io.emit("message", msgData);

    if (!isCmd) return;

    // ── Lock Check ────────────────────────────────────────────────────────────
    const _locked = global._lockedThreads || new Set();
    if ((global._globalLock || _locked.has(threadID)) && !isAdmin) return;

    // ── Command Dispatch ──────────────────────────────────────────────────────
    const commandText = directCommand ? body.trim() : body.slice(prefix.length).trim();
    const args    = commandText.split(/\s+/);
    const cmdName = args.shift().toLowerCase();
    const cmd     = commands.get(cmdName);
    if (!cmd) return;

    // ── تجاهل تام لأي شخص ليس أدمناً — بدون أي رد ────────────────────────────
    if (!isAdmin) return;

    // Permission checks — commandRoles system
    // ownerOnly is always respected
    if (cmd.config.ownerOnly && !isOwner)
      return api.sendMessage("❌ هذا الأمر للمالك فقط.", threadID);

    // Anti-spam (skip for admins)
    if (!isAdmin) {
      const spam = checkSpam(senderID);
      if (spam.exceeded) {
        if (!spam.warned) {
          spam.setWarn();
          api.sendMessage("⚠️ أنت تستخدم الأوامر بسرعة كبيرة، انتظر قليلاً!", threadID);
        }
        return;
      }
    }

    // Log command
    console.log(`${chalk.gray(ts())} ${chalk.magenta("›")} /${chalk.bold.magenta(cmdName)} | ${chalk.cyan(senderName)} @ ${chalk.cyan(threadName)}`);
    if (io) io.emit("command", { cmdName, senderID, senderName, threadID, threadName, args, timestamp: Date.now() });
    logCommand(senderID, threadID, cmdName, args).catch(() => {});

    // Run command
    // ملاحظة: مؤشر الكتابة يعمل تلقائياً داخل api.sendMessage (humanTyping wrapper)
    // لا حاجة لمحاكاة يدوية هنا — النظام يعمل على مستوى API مباشرةً
    try {
      await cmd.run({
        api, event, args,
        body, threadID, senderID,
        isGroup, isOwner, isAdmin,
        senderName, threadName,
        prefix, config,
        commands,
      });
    } catch (e) {
      console.error(`${chalk.red("✘")} ${cmdName} error: ${e.message}`);
      try { api.sendMessage(`❌ خطأ في الأمر \`${cmdName}\`: ${e.message}`, threadID); } catch (_) {}
    }

  // ══ GROUP EVENT ══════════════════════════════════════════════════════════════
  } else if (event.type === "event") {
    const { threadID, logMessageType, logMessageData } = event;
    const threadName = await resolveThread(api, threadID).catch(() => threadID);
    logEvent(logMessageType || "group_event", threadName);

    if (io) io.emit("group-event", {
      type: logMessageType,
      threadID, threadName,
      data: logMessageData,
      timestamp: Date.now(),
    });

    // Handle specific group events
    switch (logMessageType) {
      case "log:subscribe": {
        // Someone joined
        const names = (logMessageData?.addedParticipants || []).map(p => p.fullName || p.userFbId).join(", ");
        if (config.groupEvents?.welcomeMessage && names) {
          const msg = config.groupEvents.welcomeMessage.replace("{name}", names).replace("{thread}", threadName);
          setTimeout(() => api.sendMessage(msg, threadID, () => {}), 1500);
        }
        break;
      }
      case "log:unsubscribe": {
        // Someone left
        if (config.groupEvents?.leaveMessage) {
          const leftId = logMessageData?.leftParticipantFbId;
          if (leftId) {
            const leftName = await resolveUser(api, leftId).catch(() => leftId);
            const msg = config.groupEvents.leaveMessage.replace("{name}", leftName).replace("{thread}", threadName);
            setTimeout(() => api.sendMessage(msg, threadID, () => {}), 1500);
          }
        }
        break;
      }

      // ── قفل اسم المجموعة ─────────────────────────────────────────────────
      case "log:thread-name": {
        const botUID   = String(api.getCurrentUserID());
        const changer  = String(event.author || logMessageData?.author || "");
        const newTitle = String(logMessageData?.name || "");

        // تجاهل إذا كان البوت هو من غيّر الاسم
        if (changer === botUID) break;

        if (!global._lockedNames)     global._lockedNames     = new Map();
        if (!global._nameRestoringAt) global._nameRestoringAt = new Map();

        if (!global._lockedNames.has(threadID)) break;

        const lockedName = global._lockedNames.get(threadID);
        if (!lockedName) break;
        if (newTitle === lockedName) break;

        // استخدم دوال الأمر المشتركة إن كانت متاحة
        const _ops = global._nameOps || {};
        const _isR   = _ops._isRestoring   || ((tid) => { const t = global._nameRestoringAt?.get(tid); return !!t && (Date.now() - t) < 2500; });
        const _markR = _ops._markRestoring  || ((tid) => { if (!global._nameRestoringAt) global._nameRestoringAt = new Map(); global._nameRestoringAt.set(tid, Date.now()); });
        const _clearR= _ops._unmarkRestoring|| ((tid) => global._nameRestoringAt?.delete(tid));
        const _doR   = _ops._doRestore      || (async (tid, name) => {
          for (let i = 0; i < 5; i++) {
            const _a = global.api;
            if (!_a) { await _sleep(2000); continue; }
            if (!global._lockedNames?.has(tid)) return;
            try {
              await new Promise((res, rej) => _a.setTitle(name, tid, (e) => (e ? rej(e) : res())));
              return;
            } catch (_) { if (i < 4) await _sleep(1500 * (i + 1)); }
          }
        });

        // إذا كانت إعادة جارية (بدأت منذ أقل من 2.5 ث) → تخطَّ
        if (_isR(threadID)) break;

        // ابدأ الإعادة: تأخير عشوائي 2-5 ثوانٍ
        _markR(threadID);
        (async () => {
          try {
            await _sleep(_randInt(2000, 5000));
            if (!global._lockedNames?.has(threadID)) { _clearR(threadID); return; }
            await _doR(threadID, lockedName);
          } catch (_) {}
          _clearR(threadID);
        })();

        break;
      }

      // ── قفل الكنيات ──────────────────────────────────────────────────────
      case "log:user-nickname": {
        const botUID      = String(api.getCurrentUserID());
        const changer     = String(event.author || logMessageData?.author || "");
        const targetUID   = String(logMessageData?.participant_id || "");
        const newNickname = String(logMessageData?.nickname || "");

        // تجاهل إذا كان البوت هو من غيّر الكنية
        if (changer === botUID) break;

        if (!global._perMemberNicknames)  global._perMemberNicknames  = new Map();
        if (!global._nicknameJobs)        global._nicknameJobs        = new Map();
        if (!global._nickRestoring)       global._nickRestoring       = new Set();

        // هل يوجد قفل في هذه المجموعة؟
        const memberMap = global._perMemberNicknames.get(threadID);
        const hasLock   = memberMap && memberMap.size > 0;
        if (!hasLock && !global._nicknameJobs.has(threadID)) break;

        if (!targetUID) break;

        // القفل مطلق: حتى تغيير الأدمن لا يستبدل القيمة المقفلة.
        // ── أعد الكنية فوراً لأي مغيّر ─────────────────────────────────────
        const restoreKey  = `${threadID}:${targetUID}`;
        if (global._nickRestoring.has(restoreKey)) break;

        // ابحث عن الكنية المقفلة لهذا العضو
        const lockedNick = memberMap?.get(targetUID) ?? global._nicknameJobs.get(threadID);
        if (!lockedNick && lockedNick !== "") break;
        if (newNickname === lockedNick) break;

        global._nickRestoring.add(restoreKey);

        setTimeout(async () => {
          try {
            await _sleep(_randInt(500, 1800));

            const currentMap = global._perMemberNicknames?.get(threadID);
            if (!currentMap?.has(targetUID) && !global._nicknameJobs?.has(threadID)) return;

            const nick = currentMap?.get(targetUID) ?? global._nicknameJobs?.get(threadID);
            if (nick === undefined) return;

            await new Promise((res, rej) =>
              api.changeNickname(nick, threadID, targetUID, (e) => (e ? rej(e) : res()))
            );
          } catch (_) {}
          setTimeout(() => global._nickRestoring?.delete(restoreKey), 5000);
        }, _randInt(1500, 4000));

        break;
      }
    }

  // ══ TYPING ═══════════════════════════════════════════════════════════════════
  } else if (event.type === "typ") {
    if (io && event.isTyping) io.emit("typing", { from: event.from, threadID: event.threadID });

  // ══ REACTION ═════════════════════════════════════════════════════════════════
  } else if (event.type === "message_reaction") {
    if (io) io.emit("reaction", {
      reaction: event.reaction,
      senderID: event.senderID,
      threadID: event.threadID,
      messageID: event.messageID,
    });
    global._lastActivity = Date.now();

  // ══ UNSEND ═══════════════════════════════════════════════════════════════════
  } else if (event.type === "message_unsend") {
    if (io) io.emit("unsend", {
      senderID: event.senderID,
      threadID: event.threadID,
      messageID: event.messageID,
    });

  // ══ READ RECEIPT ══════════════════════════════════════════════════════════════
  } else if (event.type === "read_receipt") {
    global._lastActivity = Date.now();
  }
};
