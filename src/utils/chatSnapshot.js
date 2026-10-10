"use strict";
/** chatSnapshot.js — يرسم صورة (سكرين) لآخر رسائل المحادثة ويحفظها في data/screenshots */
const fs = require("fs-extra");
const path = require("path");

const DIR = path.join(__dirname, "../../data/screenshots");
const INDEX = path.join(DIR, "index.json");
const MAX_KEEP = 200;

function list() { try { return fs.readJsonSync(INDEX); } catch { return []; } }
function save(l) { fs.ensureDirSync(DIR); fs.writeJsonSync(INDEX, l.slice(0, MAX_KEEP), { spaces: 2 }); }

function getNames(api, ids) {
  return new Promise((res) => {
    const t = setTimeout(() => res({}), 10000);
    try {
      api.getUserInfo(ids, (err, info) => { clearTimeout(t); res(err || !info ? {} : info); });
    } catch { clearTimeout(t); res({}); }
  });
}

function wrap(ctx, text, maxW) {
  const out = [];
  for (const para of String(text).split("\n")) {
    let line = "";
    for (const word of para.split(" ")) {
      const t = line ? line + " " + word : word;
      if (ctx.measureText(t).width > maxW && line) { out.push(line); line = word; } else line = t;
    }
    out.push(line);
  }
  return out.slice(0, 12);
}

async function capture(api, threadID, title, msgs, me) {
  let createCanvas;
  try { ({ createCanvas } = require("canvas")); } catch { return null; }
  msgs = msgs.slice(-15);
  const ids = [...new Set(msgs.map(m => String(m.senderID || m.authorID || "")).filter(Boolean))];
  const info = await getNames(api, ids);
  const nameOf = (id) => (info[id] && info[id].name) || id;

  const W = 720, PAD = 18, BW = 470, LH = 24, FONT = "18px sans-serif";
  const measure = createCanvas(10, 10).getContext("2d"); measure.font = FONT;
  const items = msgs.map(m => {
    const sid = String(m.senderID || m.authorID || "");
    let body = m.body || "";
    if (!body && m.attachments && m.attachments.length) body = `[مرفق: ${m.attachments[0].type || "ملف"}]`;
    const lines = wrap(measure, body || "…", BW - 28);
    const w = Math.min(BW, Math.max(...lines.map(l => measure.measureText(l).width)) + 28);
    return { mine: sid === me, name: nameOf(sid), lines, w, h: lines.length * LH + 18, ts: parseInt(m.timestamp) || 0 };
  });
  const HEAD = 64;
  const H = HEAD + PAD + items.reduce((s, it) => s + it.h + (it.mine ? 10 : 30), 0) + PAD;

  const c = createCanvas(W, H), ctx = c.getContext("2d");
  ctx.fillStyle = "#18191a"; ctx.fillRect(0, 0, W, H);
  ctx.fillStyle = "#242526"; ctx.fillRect(0, 0, W, HEAD);
  ctx.fillStyle = "#e4e6eb"; ctx.font = "bold 20px sans-serif"; ctx.textAlign = "center";
  ctx.fillText(String(title || threadID).slice(0, 50), W / 2, 30);
  ctx.fillStyle = "#b0b3b8"; ctx.font = "13px sans-serif";
  ctx.fillText(new Date().toLocaleString("ar"), W / 2, 52);

  let y = HEAD + PAD;
  for (const it of items) {
    ctx.textAlign = "left";
    if (!it.mine) { ctx.fillStyle = "#b0b3b8"; ctx.font = "13px sans-serif"; ctx.fillText(it.name, PAD + 6, y + 13); y += 20; }
    const x = it.mine ? W - PAD - it.w : PAD;
    ctx.fillStyle = it.mine ? "#0084ff" : "#3a3b3c";
    ctx.beginPath(); ctx.roundRect ? ctx.roundRect(x, y, it.w, it.h, 16) : ctx.rect(x, y, it.w, it.h); ctx.fill();
    ctx.fillStyle = "#ffffff"; ctx.font = FONT; ctx.textAlign = "right";
    it.lines.forEach((l, i) => ctx.fillText(l, x + it.w - 14, y + 26 + i * LH));
    y += it.h + 10;
  }

  fs.ensureDirSync(DIR);
  const file = `${threadID}_${Date.now()}.png`;
  fs.writeFileSync(path.join(DIR, file), c.toBuffer("image/png"));
  const l = list(); l.unshift({ file, threadID: String(threadID), name: title || "", at: Date.now() });
  for (const old of l.slice(MAX_KEEP)) fs.remove(path.join(DIR, old.file)).catch(() => {});
  save(l);
  return file;
}

function remove(file) {
  const l = list().filter(x => x.file !== file); save(l);
  fs.remove(path.join(DIR, path.basename(file))).catch(() => {});
}

module.exports = { capture, list, remove, DIR };
