#!/usr/bin/env node
/**
 * prepare-storage.js — يربط ملفات الحالة (الكوكيز/الإعدادات/القاعدة/النسخ) بمجلد دائم.
 * على Railway: أنشئ Volume واجعل مساره /data وأضف المتغير DATA_DIR=/data
 * فلا تضيع الكوكيز وقاعدة البيانات عند كل نشر جديد.
 * بدون DATA_DIR لا يفعل شيئاً (يعمل محلياً كالمعتاد).
 */
const fs = require("fs");
const path = require("path");

const DATA_DIR = process.env.DATA_DIR;
if (!DATA_DIR) process.exit(0);

const ROOT = path.join(__dirname, "..");
fs.mkdirSync(DATA_DIR, { recursive: true });

const items = [
  { name: "account.txt",  dir: false },
  { name: "config.json",  dir: false },
  { name: "data",         dir: true  },
  { name: "backups",      dir: true  },
  { name: "Fca_Database", dir: true  },
];

for (const it of items) {
  const local  = path.join(ROOT, it.name);
  const stored = path.join(DATA_DIR, it.name);
  try {
    if (!fs.existsSync(stored)) {
      const lst = fs.existsSync(local) && !fs.lstatSync(local).isSymbolicLink() ? local : null;
      if (lst) fs.cpSync(lst, stored, { recursive: true });
      else if (it.dir) fs.mkdirSync(stored, { recursive: true });
    }
    if (fs.existsSync(local) || fs.lstatSync(local, { throwIfNoEntry: false })) {
      fs.rmSync(local, { recursive: true, force: true });
    }
    if (fs.existsSync(stored)) fs.symlinkSync(stored, local, it.dir ? "dir" : "file");
    console.log(`  ✔ ${it.name} → ${stored}`);
  } catch (e) {
    console.warn(`  ⚠ ${it.name}: ${e.message}`);
  }
}
