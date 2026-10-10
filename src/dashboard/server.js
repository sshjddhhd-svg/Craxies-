const express    = require("express");
const http       = require("http");
const { Server } = require("socket.io");
const path       = require("path");
const bodyParser = require("body-parser");
const fs         = require("fs-extra");
const crypto     = require("crypto");
const multer     = require("multer");

const ACCOUNT_PATH  = path.join(__dirname, "../../account.txt");
const CONFIG_PATH   = path.join(__dirname, "../../config.json");
const COMMANDS_DIR  = path.join(__dirname, "../commands");
const UPLOADS_DIR   = path.join(__dirname, "public/uploads");
const DEFAULT_PASS  = "";

fs.ensureDirSync(UPLOADS_DIR);

let io;
const sessions  = new Map();
const _logBuf   = [];

// ─── Message Buffer (live dashboard page) ─────────────────────────────────────
const _msgBuf = new Map(); // threadID → [{...}]
const MSG_BUF_MAX = 60;
function _bufferMsg(msg) {
  const tid = String(msg.threadID);
  if (!_msgBuf.has(tid)) _msgBuf.set(tid, []);
  const buf = _msgBuf.get(tid);
  buf.push(msg);
  if (buf.length > MSG_BUF_MAX) buf.shift();
}
global._bufferMsg = _bufferMsg;
const _stripANSI = s => String(s)
  .replace(/\x1b\[[0-9;]*m/g, '')
  .replace(/\x1b\][^\x07]*\x07/g, '')
  .replace(/\x1b[^a-zA-Z]*[a-zA-Z]/g, '');

// Intercept console output and pipe to log buffer + socket
(function _captureLogs() {
  const _o = { log: console.log, warn: console.warn, error: console.error, info: console.info };
  function _cap(lvl) {
    return (...args) => {
      _o[lvl](...args);
      const msg = _stripANSI(args.map(a => (typeof a === 'object' ? JSON.stringify(a) : String(a))).join(' '));
      const entry = { level: lvl, msg, ts: Date.now() };
      _logBuf.push(entry);
      if (_logBuf.length > 600) _logBuf.shift();
      try { if (io) io.emit('log', entry); } catch (_) {}
    };
  }
  console.log   = _cap('log');
  console.warn  = _cap('warn');
  console.error = _cap('error');
  console.info  = _cap('info');
}());

// ─── Multer for media uploads ──────────────────────────────────────────────────
const storage = multer.diskStorage({
  destination: (_req, _file, cb) => cb(null, UPLOADS_DIR),
  filename: (_req, file, cb) => {
    const ext  = path.extname(file.originalname).toLowerCase();
    const base = path.basename(file.originalname, ext).replace(/[^a-z0-9_-]/gi, "_").slice(0, 40);
    cb(null, `${base}_${Date.now()}${ext}`);
  },
});
const upload = multer({
  storage,
  limits: { fileSize: 50 * 1024 * 1024 }, // 50 MB
  fileFilter: (_req, file, cb) => {
    const allowed = /\.(jpg|jpeg|png|gif|webp|mp4|mov|avi|mp3|ogg|wav|m4a|pdf|txt)$/i;
    cb(null, allowed.test(file.originalname));
  },
});

// ─── Helpers ───────────────────────────────────────────────────────────────────
function genToken() { return crypto.randomBytes(32).toString("hex"); }
function parseCookies(str = "") {
  const out = {};
  for (const part of str.split(";")) {
    const i = part.indexOf("=");
    if (i < 1) continue;
    out[part.slice(0, i).trim()] = part.slice(i + 1).trim();
  }
  return out;
}
function getDashPass() {
  if (fs.existsSync(CONFIG_PATH)) {
    try { return process.env.DASHBOARD_PASSWORD || fs.readJsonSync(CONFIG_PATH).dashboardPassword || DEFAULT_PASS; } catch {}
  }
  return DEFAULT_PASS;
}
function isAuth(req) {
  const sid = parseCookies(req.headers.cookie || "")["_fca_sid"];
  return sid && sessions.has(sid);
}
function authMW(req, res, next) {
  const open = ["/api/login", "/api/ping"];
  if (open.includes(req.path) || req.path.startsWith("/socket.io")) return next();
  if (isAuth(req)) return next();
  if (req.path.startsWith("/api/")) return res.status(401).json({ error: "Unauthorized" });
  next();
}

// ─── Cookie parsing (same as utils/cookieParser.js — standalone for server) ───
const { parseCookieInput, cookiesToString, hasMandatory, dedup } = require("../utils/cookieParser");
const checkLiveCookie = require("../utils/checkLiveCookie");

async function startDashboard(port) {
  const app    = express();
  const server = http.createServer(app);
  app.set("trust proxy", 1);

  io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] },
    transports: ["polling", "websocket"],
    allowEIO3: true,
  });

  app.use(bodyParser.json({ limit: "15mb" }));
  app.use((_req, res, next) => { res.set("Cache-Control", "no-store"); next(); });
  app.use(authMW);
  app.use(express.static(path.join(__dirname, "public")));

  // ── Auth ───────────────────────────────────────────────────────────────────
  app.post("/api/login", (req, res) => {
    const { password } = req.body || {};
    if (!password) return res.status(400).json({ error: "كلمة المرور مطلوبة" });
    if (password !== getDashPass()) return res.status(401).json({ error: "كلمة المرور غير صحيحة" });
    const token = genToken();
    sessions.set(token, { at: Date.now() });
    res.setHeader("Set-Cookie", `_fca_sid=${token}; Path=/; HttpOnly; SameSite=None; Secure; Max-Age=86400`);
    res.json({ success: true });
  });
  app.post("/api/logout", (req, res) => {
    const sid = parseCookies(req.headers.cookie || "")["_fca_sid"];
    if (sid) sessions.delete(sid);
    res.setHeader("Set-Cookie", "_fca_sid=; Path=/; Max-Age=0");
    res.json({ success: true });
  });

  // ── Ping ──────────────────────────────────────────────────────────────────
  app.get("/api/ping", (_req, res) => res.json({ ok: true, uptime: process.uptime() }));

  // ── Stats ──────────────────────────────────────────────────────────────────
  app.get("/api/stats", async (_req, res) => {
    try {
      const { getStats } = require("../utils/database");
      const stats = await getStats();
      const cfg   = fs.existsSync(CONFIG_PATH) ? fs.readJsonSync(CONFIG_PATH) : {};
      res.json({
        ...stats,
        uptime:      process.uptime(),
        commands:    global.commands ? global.commands.size : 0,
        botName:     global.botName  || "jarfis",
        prefix:      global.commandPrefix || "/",
        ownerID:     global.ownerID || "",
        adminCount:  (cfg.adminIDs || []).length,
        online:      !!global.api,
        uid:         global.api ? global.api.getCurrentUserID() : null,
        memMB:       Math.round(process.memoryUsage().rss / 1024 / 1024),
        nodeVersion: process.version,
      });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // ── Config ─────────────────────────────────────────────────────────────────
  app.get("/api/config", (_req, res) => {
    const cfg = fs.existsSync(CONFIG_PATH) ? fs.readJsonSync(CONFIG_PATH) : {};
    const safe = { ...cfg };
    delete safe.dashboardPassword;
    res.json(safe);
  });
  app.post("/api/config", (req, res) => {
    try {
      const cur = fs.existsSync(CONFIG_PATH) ? fs.readJsonSync(CONFIG_PATH) : {};
      const upd = { ...cur, ...req.body };
      if (!req.body.dashboardPassword) upd.dashboardPassword = cur.dashboardPassword;
      fs.writeJsonSync(CONFIG_PATH, upd, { spaces: 2 });
      global.config        = upd;
      global.botName       = upd.botName  || "jarfis";
      global.commandPrefix = upd.prefix   || "/";
      global.ownerID       = upd.ownerID  || "";
      res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });
  app.post("/api/config/password", (req, res) => {
    const { current, newPassword } = req.body || {};
    if (!current || !newPassword) return res.status(400).json({ error: "الحقول مطلوبة" });
    if (current !== getDashPass()) return res.status(401).json({ error: "كلمة المرور الحالية غير صحيحة" });
    try {
      const cfg = fs.existsSync(CONFIG_PATH) ? fs.readJsonSync(CONFIG_PATH) : {};
      cfg.dashboardPassword = newPassword;
      fs.writeJsonSync(CONFIG_PATH, cfg, { spaces: 2 });
      res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // ── Admins ─────────────────────────────────────────────────────────────────
  // يُحدِّث الملف + الذاكرة المباشرة + يُعيد بناء global.isAdmin فوراً
  function _flushAdmins(newIDs) {
    // 1. حدِّث global.config في الذاكرة
    if (global.config) global.config.adminIDs = newIDs.map(String);
    // 2. أعد بناء global.isAdmin لتضمن القراءة الفورية للقائمة الجديدة
    global.isAdmin = function(id) {
      const ownerId = global.ownerID || global.config?.ownerID || "";
      if (String(id) === String(ownerId)) return true;
      return (global.config?.adminIDs || []).map(String).includes(String(id));
    };
    // 3. بث تحديث لكل العملاء المتصلين عبر Socket
    if (io) io.emit("admins-updated", {
      adminIDs: newIDs.map(String),
      ownerID:  global.ownerID || global.config?.ownerID || "",
    });
    console.log(`[ADMINS] قائمة الأدمنز محدَّثة في الذاكرة: ${newIDs.length} أدمن`);
  }

  app.get("/api/admins", (_req, res) => {
    const cfg = fs.existsSync(CONFIG_PATH) ? fs.readJsonSync(CONFIG_PATH) : {};
    res.json({ ownerID: cfg.ownerID || "", adminIDs: cfg.adminIDs || [] });
  });
  app.post("/api/admins", (req, res) => {
    const { uid } = req.body || {};
    if (!uid) return res.status(400).json({ error: "uid مطلوب" });
    try {
      const cfg = fs.existsSync(CONFIG_PATH) ? fs.readJsonSync(CONFIG_PATH) : {};
      const ids = cfg.adminIDs || [];
      if (!ids.map(String).includes(String(uid))) ids.push(String(uid));
      cfg.adminIDs = ids.map(String);
      fs.writeJsonSync(CONFIG_PATH, cfg, { spaces: 2 });
      _flushAdmins(ids);
      res.json({ success: true, adminIDs: ids });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });
  app.delete("/api/admins/:uid", (req, res) => {
    try {
      const cfg = fs.existsSync(CONFIG_PATH) ? fs.readJsonSync(CONFIG_PATH) : {};
      const newIDs = (cfg.adminIDs || []).filter(id => id !== String(req.params.uid));
      cfg.adminIDs = newIDs.map(String);
      fs.writeJsonSync(CONFIG_PATH, cfg, { spaces: 2 });
      _flushAdmins(newIDs);
      res.json({ success: true, adminIDs: newIDs });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // ── Commands ───────────────────────────────────────────────────────────────
  app.get("/api/commands", (_req, res) => {
    if (!global.commands) return res.json([]);
    const seen = new Set();
    const list = [];
    for (const [, cmd] of global.commands) {
      if (!seen.has(cmd.config.name)) {
        seen.add(cmd.config.name);
        list.push({
          name:        cmd.config.name,
          description: cmd.config.description || "",
          usage:       cmd.config.usage || cmd.config.name,
          adminOnly:   !!cmd.config.adminOnly,
          ownerOnly:   !!cmd.config.ownerOnly,
          aliases:     cmd.config.aliases || [],
          category:    cmd.config.category || "general",
        });
      }
    }
    res.json(list);
  });
  app.get("/api/commands/:name/source", (req, res) => {
    const fp = path.join(COMMANDS_DIR, `${req.params.name}.js`);
    if (!fs.existsSync(fp)) return res.status(404).json({ error: "الأمر غير موجود" });
    try { res.json({ source: fs.readFileSync(fp, "utf8"), name: req.params.name }); }
    catch (e) { res.status(500).json({ error: e.message }); }
  });
  app.post("/api/commands", (req, res) => {
    const { name, source } = req.body || {};
    if (!name || !source) return res.status(400).json({ error: "name و source مطلوبان" });
    const safe = name.replace(/[^a-z0-9_-]/gi, "").toLowerCase();
    if (!safe) return res.status(400).json({ error: "اسم غير صالح" });
    const fp = path.join(COMMANDS_DIR, `${safe}.js`);
    if (fs.existsSync(fp)) return res.status(409).json({ error: "الأمر موجود بالفعل" });
    try {
      fs.writeFileSync(fp, source, "utf8");
      try {
        delete require.cache[require.resolve(fp)];
        const cmd = require(fp);
        if (cmd.config?.name)
          [cmd.config.name, ...(cmd.config.aliases || [])].forEach(n => global.commands?.set(n.toLowerCase(), cmd));
      } catch (_) {}
      res.json({ success: true, name: safe });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });
  app.put("/api/commands/:name", (req, res) => {
    const { source } = req.body || {};
    if (!source) return res.status(400).json({ error: "source مطلوب" });
    const fp = path.join(COMMANDS_DIR, `${req.params.name}.js`);
    if (!fs.existsSync(fp)) return res.status(404).json({ error: "الأمر غير موجود" });
    try {
      fs.writeFileSync(fp, source, "utf8");
      try {
        delete require.cache[require.resolve(fp)];
        const cmd = require(fp);
        if (cmd.config?.name)
          [cmd.config.name, ...(cmd.config.aliases || [])].forEach(n => global.commands?.set(n.toLowerCase(), cmd));
      } catch (_) {}
      res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });
  app.delete("/api/commands/:name", (req, res) => {
    const fp = path.join(COMMANDS_DIR, `${req.params.name}.js`);
    if (!fs.existsSync(fp)) return res.status(404).json({ error: "الأمر غير موجود" });
    try {
      fs.unlinkSync(fp);
      if (global.commands)
        for (const [k, cmd] of global.commands)
          if (cmd.config.name === req.params.name) global.commands.delete(k);
      res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // ── Send ───────────────────────────────────────────────────────────────────
  app.post("/api/send", async (req, res) => {
    const { threadID, message } = req.body || {};
    if (!global.api) return res.status(503).json({ error: "البوت غير متصل" });
    if (!threadID || !message) return res.status(400).json({ error: "threadID و message مطلوبان" });
    try {
      await new Promise((ok, fail) => global.api.sendMessage(message, threadID, e => e ? fail(e) : ok()));
      res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // ── Cookies (account.txt) ──────────────────────────────────────────────────
  // GET current cookie status
  app.get("/api/cookies", (_req, res) => {
    if (!fs.existsSync(ACCOUNT_PATH)) return res.json({ exists: false, count: 0, uid: "" });
    try {
      const raw = fs.readFileSync(ACCOUNT_PATH, "utf8").trim();
      if (!raw) return res.json({ exists: false, count: 0, uid: "" });

      // Is it a token?
      if (/^EAA[A-Za-z0-9]+$/.test(raw)) {
        return res.json({ exists: true, count: 0, uid: "", isToken: true, format: "token" });
      }

      // Try to parse
      const { parseCookieInput: parseCI } = require("../utils/cookieParser");
      const parsed = parseCI(raw);
      if (parsed.isToken) return res.json({ exists: true, count: 0, uid: "", isToken: true, format: "token" });
      const cookies = parsed.cookies || [];
      const cUser = cookies.find(c => c.key === "c_user");
      return res.json({
        exists: true,
        count: cookies.length,
        uid: cUser?.value || "",
        format: "cookies",
      });
    } catch {
      return res.json({ exists: true, count: 0, uid: "", format: "unknown" });
    }
  });

  // POST save cookies (all formats) + hot-swap bot account
  app.post("/api/cookies", async (req, res) => {
    try {
      let raw = req.body.cookies;
      if (typeof raw === "string") raw = raw.trim();
      if (!raw) return res.status(400).json({ error: "الكوكيز فارغة" });

      const userAgent = global.config?.userAgent ||
        "Mozilla/5.0 (Linux; Android 12; M2102J20SG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.0.0 Mobile Safari/537.36";

      // Parse
      let parsed;
      try { parsed = parseCookieInput(raw); }
      catch (e) { return res.status(400).json({ error: e.message }); }

      // ── مساعد: حفظ + hot-swap مع منع مراقب الملفات من تكرار الدخول ──────────
      function _saveCookiesAndRelogin(cookieArr, meta) {
        // اضبط علامة _dashCookieWrite لمنع مراقب account.txt من إطلاق reLoginBot ثانيةً
        global._dashCookieWrite = true;
        fs.writeFileSync(ACCOUNT_PATH, JSON.stringify(cookieArr, null, 2), "utf8");
        // أطلق hot-swap وحيداً من هنا بعد 600ms
        setTimeout(() => {
          global._dashCookieWrite = false;
          if (global.reLoginBot) {
            if (io) io.emit("bot-status", { status: "connecting", message: "جارٍ تغيير الحساب…" });
            global.reLoginBot();
          }
        }, 600);
        if (io) io.emit("cookies-updated", meta);
      }

      // If token — convert directly
      if (parsed.isToken) {
        const getFbstate = require("../utils/getFbstateFromToken");
        try {
          const cookies = await getFbstate(parsed.token);
          const deduped = dedup(cookies);
          if (!deduped.length) return res.status(400).json({ error: "التوكن لم يُرجع كوكيز" });
          const cUser = deduped.find(c => c.key === "c_user");
          _saveCookiesAndRelogin(deduped, { count: deduped.length, uid: cUser?.value || "", format: "token" });
          return res.json({ success: true, count: deduped.length, uid: cUser?.value || "", format: "token",
            message: "✅ التوكن حُوِّل — جارٍ تغيير الحساب…" });
        } catch (e) {
          return res.status(400).json({ error: "خطأ في التوكن: " + e.message });
        }
      }

      const cookies = parsed.cookies || [];
      if (!cookies.length) return res.status(400).json({ error: "لا توجد كوكيز صالحة" });
      if (!hasMandatory(cookies)) return res.status(400).json({ error: "c_user أو xs مفقود — الكوكيز ناقصة" });

      // Validate live
      const valid = await checkLiveCookie(cookiesToString(cookies), userAgent);
      const cUser = cookies.find(c => c.key === "c_user");

      _saveCookiesAndRelogin(cookies, { count: cookies.length, uid: cUser?.value || "", valid, format: "cookies" });

      res.json({
        success: true,
        count:   cookies.length,
        uid:     cUser?.value || "",
        valid,
        format: "cookies",
        message: valid ? "✅ الكوكيز صالحة — جارٍ تغيير الحساب…" : "⚠️ قد تكون منتهية الصلاحية — سيتم المحاولة",
      });
    } catch (e) { res.status(400).json({ error: e.message }); }
  });

  // Validate only (no save)
  app.post("/api/cookies/validate", async (req, res) => {
    try {
      let raw = req.body.cookies;
      if (typeof raw === "string") raw = raw.trim();
      const parsed = parseCookieInput(raw);
      if (parsed.isToken) return res.json({ valid: null, format: "token", message: "توكن — سيتم التحقق عند الحفظ" });
      const cookies = parsed.cookies || [];
      if (!hasMandatory(cookies)) return res.json({ valid: false, message: "c_user أو xs مفقود" });
      const cookieStr = cookiesToString(cookies);
      const userAgent = global.config?.userAgent ||
        "Mozilla/5.0 (Linux; Android 12; M2102J20SG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.0.0 Mobile Safari/537.36";
      const valid = await checkLiveCookie(cookieStr, userAgent);
      const cUser = cookies.find(c => c.key === "c_user");
      res.json({
        valid, count: cookies.length, uid: cUser?.value || "",
        message: valid ? "✅ الكوكيز صالحة وتعمل" : "⚠️ الكوكيز قد تكون منتهية الصلاحية",
      });
    } catch (e) { res.status(400).json({ valid: false, message: e.message }); }
  });

  // DELETE cookies
  app.delete("/api/cookies", (_req, res) => {
    try {
      fs.writeFileSync(ACCOUNT_PATH, "", "utf8");
      if (io) io.emit("cookies-updated", { count: 0, uid: "" });
      res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // ── Media Upload & Management ──────────────────────────────────────────────
  app.post("/api/media/upload", upload.single("file"), (req, res) => {
    if (!req.file) return res.status(400).json({ error: "لم يتم رفع أي ملف" });
    res.json({
      success: true,
      filename: req.file.filename,
      url: `/uploads/${req.file.filename}`,
      size: req.file.size,
      mimetype: req.file.mimetype,
    });
  });

  app.get("/api/media", (_req, res) => {
    try {
      const files = fs.readdirSync(UPLOADS_DIR).map(f => {
        const stat = fs.statSync(path.join(UPLOADS_DIR, f));
        const ext  = path.extname(f).toLowerCase();
        let type = "file";
        if (/\.(jpg|jpeg|png|gif|webp)$/i.test(f)) type = "image";
        else if (/\.(mp4|mov|avi)$/i.test(f)) type = "video";
        else if (/\.(mp3|ogg|wav|m4a)$/i.test(f)) type = "audio";
        return { filename: f, url: `/uploads/${f}`, size: stat.size, type, ext };
      }).filter(f => !f.filename.startsWith("."));
      res.json(files);
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  app.delete("/api/media/:filename", (req, res) => {
    const safe = path.basename(req.params.filename);
    const fp   = path.join(UPLOADS_DIR, safe);
    try {
      if (fs.existsSync(fp)) fs.unlinkSync(fp);
      res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // ── Protection ─────────────────────────────────────────────────────────────
  app.get("/api/protection", (_req, res) => {
    const safe = (fn) => { try { return fn(); } catch { return null; } };
    const cfg  = fs.existsSync(CONFIG_PATH) ? fs.readJsonSync(CONFIG_PATH) : {};
    res.json({
      // الأنظمة الأصلية
      stealth:          { enabled: cfg.stealth?.enable !== false,                    ...safe(() => require("../protection/stealth").getStatus()) },
      throttle:         { enabled: cfg.stealth?.outgoingThrottle?.enable !== false,  ...safe(() => require("../protection/outgoingThrottle").getStatus()) },
      humanTyping:      { enabled: cfg.humanTyping?.enable !== false },
      mqttHealth:       { enabled: cfg.mqttHealthCheck?.enable !== false },
      keepAlive:        { enabled: cfg.keepAlive?.enable !== false },
      // الأنظمة الجديدة — 10 أنظمة محاكاة الإنسان
      humanReadReceipt: { enabled: cfg.humanReadReceipt?.enable !== false,           ...safe(() => require("../protection/humanReadReceipt").getStatus?.()) },
      naturalPresence:  { enabled: cfg.naturalPresence?.enable !== false,            ...safe(() => require("../protection/naturalPresence").getStatus()) },
      scrollSimulator:  { enabled: cfg.scrollSimulator?.enable !== false,            ...safe(() => require("../protection/scrollSimulator").getStatus()) },
      antiDetection:    { enabled: cfg.antiDetection?.enable !== false,              ...safe(() => require("../protection/antiDetection").getStatus()) },
      sessionRefresher: { enabled: cfg.sessionRefresher?.enable !== false,           ...safe(() => require("../protection/sessionRefresher").getStatus()) },
      reactionDelay:    { enabled: cfg.reactionDelay?.enable !== false,              ...safe(() => require("../protection/reactionDelay").getStatus()) },
      connectionJitter: { enabled: cfg.connectionJitter?.enable !== false,           ...safe(() => require("../protection/connectionJitter").getStatus()) },
      duplicateGuard:   { enabled: cfg.duplicateGuard?.enable !== false,             ...safe(() => require("../protection/duplicateGuard").getStatus()) },
      typingVariator:   { enabled: cfg.typingVariator?.enable !== false,             ...safe(() => require("../protection/typingVariator").getStatus()) },
      behaviorScheduler:{ enabled: cfg.behaviorScheduler?.enable !== false,          ...safe(() => require("../protection/behaviorScheduler").getStatus()) },
    });
  });

  app.post("/api/protection/toggle", (req, res) => {
    const { system, enable } = req.body || {};
    try {
      const cfg = fs.existsSync(CONFIG_PATH) ? fs.readJsonSync(CONFIG_PATH) : {};
      const systems = [
        "stealth", "humanTyping", "mqttHealth", "keepAlive",
        "humanReadReceipt", "naturalPresence", "scrollSimulator", "antiDetection",
        "sessionRefresher", "reactionDelay", "connectionJitter", "duplicateGuard",
        "typingVariator", "behaviorScheduler",
      ];
      if (systems.includes(system)) {
        const key = system === "mqttHealth" ? "mqttHealthCheck" : system;
        if (!cfg[key]) cfg[key] = {};
        cfg[key].enable = !!enable;
      }
      // throttle خاص
      if (system === "throttle") {
        if (!cfg.stealth) cfg.stealth = {};
        if (!cfg.stealth.outgoingThrottle) cfg.stealth.outgoingThrottle = {};
        cfg.stealth.outgoingThrottle.enable = !!enable;
      }
      fs.writeJsonSync(CONFIG_PATH, cfg, { spaces: 2 });
      if (global.config) global.config = cfg;
      res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // ── Backup ─────────────────────────────────────────────────────────────────
  app.get("/api/backup", (_req, res) => {
    try { res.json(require("../utils/autoBackup").getStatus()); }
    catch (e) { res.status(500).json({ error: e.message }); }
  });
  app.post("/api/backup/now", async (_req, res) => {
    try {
      await require("../utils/autoBackup").doBackup();
      res.json({ success: true, ...require("../utils/autoBackup").getStatus() });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });
  app.post("/api/backup/restore/:ts", async (req, res) => {
    try {
      const n = await require("../utils/autoBackup").restoreBackup(req.params.ts);
      res.json({ success: true, restored: n });
      setTimeout(() => { if (global.reLoginBot) global.reLoginBot(); }, 1000);
    } catch (e) { res.status(400).json({ error: e.message }); }
  });
  app.get("/api/backup/download/:ts", (req, res) => {
    const { backupDir } = require("../utils/autoBackup").getStatus();
    const dir = require("path").join(backupDir, req.params.ts);
    if (!require("fs-extra").existsSync(dir)) return res.status(404).json({ error: "غير موجود" });
    const archive = require("archiver")("zip", { zlib: { level: 6 } });
    res.attachment(`backup_${req.params.ts}.zip`);
    archive.pipe(res);
    archive.directory(dir, false);
    archive.finalize();
  });

  // ── Command Roles ──────────────────────────────────────────────────────────
  app.get("/api/command-roles", (_req, res) => {
    res.json(global.config?.commandRoles || {});
  });

  app.post("/api/command-roles", (req, res) => {
    try {
      const { cmd, role } = req.body || {};
      if (!cmd) return res.status(400).json({ error: "cmd مطلوب" });
      if (!["admin", "member"].includes(role)) return res.status(400).json({ error: "role يجب أن يكون admin أو member" });
      const cfg = fs.existsSync(CONFIG_PATH) ? fs.readJsonSync(CONFIG_PATH) : {};
      if (!cfg.commandRoles) cfg.commandRoles = {};
      cfg.commandRoles[cmd] = role;
      fs.writeJsonSync(CONFIG_PATH, cfg, { spaces: 2 });
      if (global.config) global.config.commandRoles = cfg.commandRoles;
      res.json({ success: true, cmd, role });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // ── Chat Watchers (مراقبة المحادثات) ───────────────────────────────────────
  const watcher = require("../utils/chatWatcher");
  global._dashIO = io;

  const snap = require("../utils/chatSnapshot");
  app.get("/api/screenshots", (_req, res) => res.json(snap.list()));
  app.get("/api/screenshots/:file", (req, res) => {
    const f = path.basename(req.params.file);
    if (!/^[\w.-]+\.png$/.test(f)) return res.status(400).end();
    res.sendFile(path.join(snap.DIR, f), (e) => e && res.status(404).end());
  });
  app.delete("/api/screenshots/:file", (req, res) => { snap.remove(req.params.file); res.json({ success: true }); });

  app.get("/api/inbox", async (_req, res) => {
    if (!global.api) return res.status(503).json({ error: "البوت غير متصل" });
    try { res.json(await watcher.getInbox(global.api, 80)); }
    catch (e) { res.status(500).json({ error: e.message }); }
  });

  app.get("/api/watchers", (_req, res) => {
    res.json({ watchers: watcher.getWatchers(), intervalSec: global.config?.watcherIntervalSec || 4 });
  });

  app.post("/api/watchers", (req, res) => {
    const { threadID, name, reply, cooldownSec = 0, enabled = true } = req.body || {};
    if (!threadID) return res.status(400).json({ error: "اختر محادثة" });
    if (!reply || !String(reply).trim()) return res.status(400).json({ error: "اكتب نص الرد" });
    const list = watcher.getWatchers().slice();
    const i = list.findIndex(w => String(w.threadID) === String(threadID));
    const item = {
      threadID: String(threadID), name: name || String(threadID), reply: String(reply),
      cooldownSec: Math.max(0, parseInt(cooldownSec) || 0), enabled: !!enabled,
      addedAt: i >= 0 ? list[i].addedAt : Date.now(),
    };
    if (i >= 0) list[i] = item; else list.push(item);
    watcher.saveWatchers(list);
    res.json({ success: true, watchers: list });
  });

  app.post("/api/watchers/:id/toggle", (req, res) => {
    const list = watcher.getWatchers().slice();
    const w = list.find(x => String(x.threadID) === req.params.id);
    if (!w) return res.status(404).json({ error: "غير موجود" });
    w.enabled = w.enabled === false;
    if (w.enabled) w.addedAt = Date.now();
    watcher.saveWatchers(list);
    res.json({ success: true, watchers: list });
  });

  app.delete("/api/watchers/:id", (req, res) => {
    const list = watcher.getWatchers().filter(x => String(x.threadID) !== req.params.id);
    watcher.saveWatchers(list);
    res.json({ success: true, watchers: list });
  });

  app.post("/api/watchers/interval", (req, res) => {
    const sec = Math.min(60, Math.max(2, parseInt(req.body?.intervalSec) || 4));
    global.config.watcherIntervalSec = sec;
    watcher.saveWatchers(watcher.getWatchers());
    res.json({ success: true, intervalSec: sec });
  });

  // ── Thread Messages (live page) — من الذاكرة، وإن كانت فارغة نجلب السجل من فيسبوك ──
  app.get("/api/threads/:id/messages", async (req, res) => {
    const tid = String(req.params.id);
    const buf = _msgBuf.get(tid) || [];
    if (buf.length || !global.api || typeof global.api.getThreadHistory !== "function") return res.json(buf);
    try {
      const hist = await new Promise((resolve) => {
        const t = setTimeout(() => resolve([]), 15000);
        global.api.getThreadHistory(tid, 30, undefined, (err, msgs) => { clearTimeout(t); resolve(err ? [] : (msgs || [])); });
      });
      const me = String(global.api.getCurrentUserID?.() || "");
      const info = require("../utils/threadSync").get(tid);
      const nc = global._nameCache?.u || {};
      const out = hist
        .filter(m => m && (m.type === "message" || m.type === "message_reply" || m.body !== undefined))
        .map(m => {
          const sid = String(m.senderID || m.authorID || "");
          return {
            senderID: sid,
            senderName: sid === me ? "أنا (البوت)" : (nc[sid] || (info && !info.isGroup ? info.name : sid)),
            threadID: tid,
            body: m.body || (m.attachments?.length ? "📎 مرفق" : ""),
            messageID: m.messageID,
            timestamp: parseInt(m.timestamp) || Date.now(),
          };
        })
        .sort((a, b) => a.timestamp - b.timestamp);
      out.forEach(m => _bufferMsg(m));
      res.json(out);
    } catch (e) { res.json(buf); }
  });

  // ── Threads list: كل محادثات الحساب (خاص + مجموعات) من فيسبوك + قاعدة البيانات ──
  app.get("/api/threads", async (req, res) => {
    try {
      const sync = require("../utils/threadSync");
      if (global.api && (req.query.refresh === "1" || !sync.getAll().length)) {
        await sync.syncNow();
      }
      const live = sync.getAll();
      const seen = new Set(live.map(t => t.threadID));
      const out = live.map(t => ({ threadID: t.threadID, name: t.name || t.threadID, isGroup: !!t.isGroup, snippet: t.snippet || "", timestamp: t.timestamp || 0 }));
      // إن تعذر الجلب من فيسبوك نعرض المحفوظ في قاعدة البيانات
      if (!live.length) {
        const { Thread } = require("../utils/database");
        const threads = await Thread.findAll({ order: [["updatedAt", "DESC"]], limit: 200 });
        for (const t of threads) {
          if (seen.has(t.threadID)) continue;
          out.push({ threadID: t.threadID, name: t.name || t.threadID, isGroup: null, snippet: "", timestamp: 0 });
        }
      }
      res.json(out);
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // ── Execute command from dashboard ─────────────────────────────────────────
  app.post("/api/execute", async (req, res) => {
    const { command, args = [], threadID, allThreads = false } = req.body || {};
    if (!global.api) return res.status(503).json({ error: "البوت غير متصل" });
    if (!command) return res.status(400).json({ error: "command مطلوب" });

    const cmd = global.commands?.get(command.toLowerCase());
    if (!cmd) return res.status(404).json({ error: "الأمر غير موجود" });

    // Build thread list
    let threads = [];
    if (allThreads) {
      try {
        const { Thread } = require("../utils/database");
        const liveIDs = require("../utils/threadSync").getAll().filter(t => t.isGroup).map(t => t.threadID);
        const dbThreads = liveIDs.length ? [] : await Thread.findAll({ limit: 200 });
        threads = liveIDs.length ? liveIDs : dbThreads.map(t => t.threadID);
      } catch (e) { return res.status(500).json({ error: "خطأ في قاعدة البيانات: " + e.message }); }
    } else if (threadID) {
      threads = [String(threadID)];
    } else {
      return res.status(400).json({ error: "threadID أو allThreads مطلوب" });
    }

    if (!threads.length) return res.status(400).json({ error: "لا توجد محادثات مستهدفة" });

    const prefix  = global.commandPrefix || "/";
    const config  = global.config || {};

    // Always execute as owner — use configured ownerID, fall back to bot's own UID
    const botUID  = String(global.api.getCurrentUserID());
    const execUID = (global.ownerID && String(global.ownerID).trim()) ? String(global.ownerID) : botUID;

    // Temporarily patch global.isOwner so commands that call it directly also pass
    const _origIsOwner = global.isOwner;
    const _origIsAdmin = global.isAdmin;
    global.isOwner = (id) => String(id) === execUID || _origIsOwner?.(id) || false;
    global.isAdmin = (id) => global.isOwner(id) || _origIsAdmin?.(id) || false;

    const results = [];

    const _sync = require("../utils/threadSync");
    for (const tid of threads) {
      const _info = _sync.get(tid);
      const _isGroup = _info ? !!_info.isGroup : true;
      try {
        const parsedArgs = Array.isArray(args) ? args : String(args).split(/\s+/).filter(Boolean);
        const body = `${prefix}${command}${parsedArgs.length ? " " + parsedArgs.join(" ") : ""}`;
        const fakeEvent = {
          type:        "message",
          senderID:    execUID,
          body,
          threadID:    tid,
          messageID:   `dash_${Date.now()}_${Math.random().toString(36).slice(2)}`,
          timestamp:   Date.now(),
          attachments: [],
          isGroup:     _isGroup,
          mentions:    {},
        };
        await cmd.run({
          api:            global.api,
          event:          fakeEvent,
          args:           parsedArgs,
          body,
          threadID:       tid,
          senderID:       execUID,
          isGroup:        _isGroup,
          isOwner:        true,
          isAdmin:        true,
          senderName:     "لوحة التحكم",
          threadName:     _info?.name || tid,
          prefix,
          config,
          commands:       global.commands,
          simulateTyping: async () => {},
        });
        results.push({ threadID: tid, success: true });
      } catch (e) {
        results.push({ threadID: tid, success: false, error: e.message });
      }
    }

    // Restore original permission helpers
    global.isOwner = _origIsOwner;
    global.isAdmin = _origIsAdmin;

    const ok = results.filter(r => r.success).length;
    res.json({ success: true, results, total: threads.length, ok });
  });

  // ── Restart (hot-swap re-login only) ──────────────────────────────────────
  app.post("/api/restart", (_req, res) => {
    res.json({ success: true });
    setTimeout(() => process.exit(0), 400);
  });

  // Hot-swap (re-login without full restart)
  app.post("/api/relogin", async (_req, res) => {
    res.json({ success: true, message: "جارٍ إعادة تسجيل الدخول…" });
    setTimeout(() => { if (global.reLoginBot) global.reLoginBot(); }, 300);
  });

  // ── System Logs ───────────────────────────────────────────────────────────
  app.get("/api/logs", (_req, res) => res.json(_logBuf));

  // ── Socket ─────────────────────────────────────────────────────────────────
  io.on("connection", socket => {
    const uid = global.api ? global.api.getCurrentUserID() : null;
    socket.emit("bot-status", {
      status:  global.api ? "online" : "offline",
      message: global.api ? `متصل ✔ (${uid})` : "البوت غير متصل",
    });
  });

  await new Promise(resolve => server.listen(port, "0.0.0.0", resolve));
  return { app, server, io };
}

function getIO() { return io; }
module.exports = { startDashboard, getIO };
