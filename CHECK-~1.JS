/* ==========================================================
   سكربت مراقبة حدود الحرارة/الرطوبة + إشعارات الحريق/الغاز/الحركة/الطاقة
   منصة Qamha Scada System

   نسخة Netlify Function - بديل GitHub Actions لأن جدولة GitHub تأخرت لساعات
   بدون سبب واضح (غير موثوقة لتطبيق سلامة). هذا الملف يشتغل كدالة HTTP عادية
   (مو مجدولة داخلياً)، ونستدعيها كل دقيقة من خدمة تنبيه خارجية مجانية
   (cron-job.org) - نفس فكرة GitHub Actions بس بموثوقية وسرعة أفضل.

   ما يحتاج أي خطة مدفوعة - firebase-admin يشتغل مجاني بخطة Spark، ونتلفاي
   عنده باقة مجانية كافية لهذا الاستخدام الخفيف.
   ========================================================== */

const admin = require("firebase-admin");

// بيانات حساب الخدمة تجي من متغير بيئة بإعدادات Netlify (Site settings ->
// Environment variables) بنفس اسم FIREBASE_SERVICE_ACCOUNT_KEY - نفس القيمة
// المستخدمة سابقاً بـ GitHub Secrets بالضبط (نفس ملف JSON لحساب الخدمة)
const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT_KEY);

// نتأكد ما نهيّئ التطبيق أكثر من مرة - الدوال بنتلفاي ممكن تعيد استخدام نفس
// العملية (warm invocation) بين استدعاء وآخر، فـ initializeApp() الثانية بترمي خطأ
if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    databaseURL: "https://qamha-metering-default-rtdb.europe-west1.firebasedatabase.app",
  });
}

// ⚠️ لازم تطابق أسماء المخازن هنا نفس WAREHOUSES بملف index.html
const WAREHOUSE_NAMES = {
  warehouse_1: { ar: "المخزن المجمد" },
  warehouse_2: { ar: "المخزن المبرد 1" },
  warehouse_3: { ar: "المخزن المبرد 2" },
  warehouse_4: { ar: "مخزن المواد الأولية 1" },
  warehouse_5: { ar: "مخزن المواد الأولية 2" },
  warehouse_6: { ar: "مخزن الطحين" },
  warehouse_7: { ar: "منطقة الكهرباء" },
};

// ==========================================================
// مسح سجل الأحداث بالكامل (/logs) مرة وحدة كل سنة - بدل الاحتفاظ المتجدد
// بفترة معينة، هنا نصفر كل شي دفعة وحدة (كل الأنظمة: مخازن + حريق + غاز +
// حركة + طاقة) بمجرد ما تمر سنة كاملة من آخر مسح، حتى ما تكبر قاعدة البيانات
// بلا حدود (خطة Spark المجانية سقفها 1GB تخزين).
// ==========================================================
const LOG_WIPE_INTERVAL_MS = 365 * 24 * 60 * 60 * 1000; // مرة كل سنة

async function wipeLogsYearly(db) {
  const metaSnap = await db.ref("meta/lastLogWipe").get();
  const lastWipe = metaSnap.val() || 0;
  const now = Date.now();

  if (lastWipe === 0) {
    await db.ref("meta/lastLogWipe").set(now);
    console.log("مسح السجل السنوي: أول تشغيل - سجّلنا نقطة البداية، ما مسحنا شي.");
    return;
  }

  if (now - lastWipe < LOG_WIPE_INTERVAL_MS) {
    return;
  }

  await db.ref("logs").remove();
  await db.ref("meta/lastLogWipe").set(now);
  console.log("مسح السجل السنوي: مرت سنة كاملة - تم تصفير /logs بالكامل (كل الأنظمة).");
}

// ==========================================================
// كشف انقطاع الاتصال (كهرباء أو انترنت) عن كل مخزن
// ==========================================================
const CONN_OFFLINE_MS = 3 * 60 * 1000; // 3 دقايق بدون بيانات = منقطع

async function checkConnectivity(db, warehouses) {
  const now = Date.now();
  const connStateSnap = await db.ref("connState").get();
  const connStateAll = connStateSnap.val() || {};
  const connUpdates = {};
  const connMessages = [];

  const statusByWarehouse = {};
  for (const warehouseId of Object.keys(warehouses)) {
    const data = warehouses[warehouseId] || {};
    const ts = Number(data.timestamp) || 0;
    if (ts <= 0) { statusByWarehouse[warehouseId] = null; continue; }
    statusByWarehouse[warehouseId] = (now - ts) > CONN_OFFLINE_MS;
  }

  for (const warehouseId of Object.keys(statusByWarehouse)) {
    const isOffline = statusByWarehouse[warehouseId];
    if (isOffline === null) continue;
    const prev = connStateAll[warehouseId] || {};
    const wasOffline = !!prev.offline;
    const name = (WAREHOUSE_NAMES[warehouseId] || { ar: warehouseId }).ar;

    if (isOffline && !wasOffline) {
      connMessages.push({
        title: `⛔ انقطع الاتصال — ${name}`,
        body: "تم قطع الاتصال مع منظومة متحسسات المنطقة.",
      });
      connUpdates[warehouseId] = { offline: true, since: now };
    } else if (!isOffline && wasOffline) {
      connMessages.push({
        title: `✅ عاد الاتصال — ${name}`,
        body: "رجعت البيانات توصل من هذا المخزن طبيعي.",
      });
      connUpdates[warehouseId] = { offline: false, since: now };
    }
  }

  if (Object.keys(connUpdates).length) {
    await db.ref("connState").update(connUpdates);
  }
  return connMessages;
}

// ==========================================================
// إشعارات push حقيقية لمنظومات الحريق/الغاز/الحركة/الطاقة
// ==========================================================

async function checkFireAlarms(db) {
  const messages = [];
  const [sysConfigSnap, zonesSnap, alertStateSnap] = await Promise.all([
    db.ref("fireSystemsConfig").get(),
    db.ref("fireZones").get(),
    db.ref("alertsSent/fire").get(),
  ]);
  const systems = sysConfigSnap.val() || [];
  const zonesObj = zonesSnap.val() || {};
  const alertState = alertStateSnap.val() || {};
  const newAlertState = {};

  for (const sys of systems) {
    const sysData = zonesObj[sys.id] || {};

    for (const zoneDef of sys.zones || []) {
      if (zoneDef.enabled === false) continue;
      const zData = sysData[zoneDef.id];
      const key = `${sys.id}_${zoneDef.id}`;
      const isAlarm = !!(zData && zData.status === "alarm");
      if (isAlarm && !alertState[key]) {
        messages.push({
          title: `🔥 حريق! — ${sys.name}`,
          body: `${zoneDef.name || "منطقة " + zoneDef.id}: تم رصد حريق`,
        });
      }
      newAlertState[key] = isAlarm;
    }

    const auxKey = `${sys.id}_aux`;
    const isAux = !!(sysData.auxFire && sysData.auxFire.active);
    if (isAux && !alertState[auxKey]) {
      messages.push({ title: `🚨 إنذار عام — ${sys.name}`, body: "تأكيد حريق من طرف لوحة السيطرة (AUX)." });
    }
    newAlertState[auxKey] = isAux;

    const faultKey = `${sys.id}_fault`;
    const isFault = !!(sysData.sysFault && sysData.sysFault.fault);
    if (isFault && !alertState[faultKey]) {
      messages.push({ title: `⚠️ عطل عام باللوحة — ${sys.name}`, body: "افحص اللوحة (مثلاً بطاريات احتياط ناقصة)." });
    }
    newAlertState[faultKey] = isFault;
  }

  if (Object.keys(newAlertState).length) await db.ref("alertsSent/fire").update(newAlertState);
  return messages;
}

async function checkGasAlarms(db) {
  const messages = [];
  const [zonesSnap, alertStateSnap] = await Promise.all([
    db.ref("gasZones").get(),
    db.ref("alertsSent/gas").get(),
  ]);
  const zonesObj = zonesSnap.val() || {};
  const alertState = alertStateSnap.val() || {};
  const newAlertState = {};

  for (const zoneId of Object.keys(zonesObj)) {
    const z = zonesObj[zoneId] || {};
    const isDanger = z.status === "danger";
    if (isDanger && !alertState[zoneId]) {
      const ppmTxt = z.ppm != null ? ` (${Math.round(z.ppm)} ppm)` : "";
      messages.push({ title: "🧪 تسرب غاز!", body: `منطقة ${zoneId}: تجاوز حد الأمان${ppmTxt}` });
    }
    newAlertState[zoneId] = isDanger;
  }

  if (Object.keys(newAlertState).length) await db.ref("alertsSent/gas").update(newAlertState);
  return messages;
}

async function checkMotionAlarms(db) {
  const messages = [];
  const [zonesSnap, alertStateSnap] = await Promise.all([
    db.ref("motionZones").get(),
    db.ref("alertsSent/motion").get(),
  ]);
  const zonesObj = zonesSnap.val() || {};
  const alertState = alertStateSnap.val() || {};
  const newAlertState = {};

  for (const zoneId of Object.keys(zonesObj)) {
    const z = zonesObj[zoneId] || {};
    const isAlarm = z.status === "alarm";
    if (isAlarm && !alertState[zoneId]) {
      messages.push({ title: "🚶 اختراق محيط!", body: `زوج المتحسسات ${zoneId}: تم رصد حركة/اختراق` });
    }
    newAlertState[zoneId] = isAlarm;
  }

  if (Object.keys(newAlertState).length) await db.ref("alertsSent/motion").update(newAlertState);
  return messages;
}

// ⚠️ لازم تطابق نفس القيم الافتراضية والمصادر الموجودة بملف index.html
const POWER_SOURCES_META = [
  { id: "grid", ar: "الكهرباء الوطنية" },
  { id: "gen", ar: "المولد" },
  { id: "solar", ar: "الطاقة الشمسية" },
];
const POWER_PHASES = ["L1", "L2", "L3"];
const POWER_LL_PAIRS = [
  { id: "L1L2", label: "L1-L2" },
  { id: "L2L3", label: "L2-L3" },
  { id: "L3L1", label: "L3-L1" },
];
const DEFAULT_POWER_THRESHOLDS = { vMin: 200, vMax: 240, hzMin: 47, hzMax: 53, vllMin: 380, vllMax: 415 };

async function checkPowerAlarms(db) {
  const messages = [];
  const [readingsSnap, thresholdsSnap, alertStateSnap] = await Promise.all([
    db.ref("powerReadings").get(),
    db.ref("powerThresholdsConfig").get(),
    db.ref("alertsSent/power").get(),
  ]);
  const readings = readingsSnap.val() || {};
  const th = Object.assign({}, DEFAULT_POWER_THRESHOLDS, thresholdsSnap.val() || {});
  const alertState = alertStateSnap.val() || {};
  const newAlertState = {};

  for (const src of POWER_SOURCES_META) {
    const srcData = readings[src.id] || {};

    for (const ph of POWER_PHASES) {
      const d = srcData[ph];
      const key = `${src.id}_${ph}`;
      let isAlarm = false;
      if (d && d.voltage != null) {
        isAlarm = d.voltage < th.vMin || d.voltage > th.vMax || (d.freq != null && (d.freq < th.hzMin || d.freq > th.hzMax));
      }
      if (isAlarm && !alertState[key]) {
        messages.push({ title: `⚡ انحراف كهربائي — ${src.ar}`, body: `الطور ${ph}: خارج الحدود المسموحة` });
      }
      newAlertState[key] = isAlarm;
    }

    const llData = srcData.LL || {};
    for (const pair of POWER_LL_PAIRS) {
      const raw = llData[pair.id];
      const v = raw && typeof raw === "object" ? raw.voltage : raw;
      const key = `${src.id}_LL_${pair.id}`;
      const isAlarm = v != null && (v < th.vllMin || v > th.vllMax);
      if (isAlarm && !alertState[key]) {
        messages.push({ title: `⚡ انحراف كهربائي — ${src.ar}`, body: `فولتية خط-خط ${pair.label}: خارج الحدود المسموحة` });
      }
      newAlertState[key] = isAlarm;
    }
  }

  if (Object.keys(newAlertState).length) await db.ref("alertsSent/power").update(newAlertState);
  return messages;
}

async function runCheck() {
  const db = admin.database();
  const logLines = [];
  const log = (s) => { console.log(s); logLines.push(s); };

  await wipeLogsYearly(db).catch((e) => log("فشل المسح السنوي للسجل: " + e));

  const [warehousesSnap, adminSettingsSnap, alertStateSnap, tokensSnap] = await Promise.all([
    db.ref("warehouses").get(),
    db.ref("adminSettings").get(),
    db.ref("alertsSent").get(),
    db.ref("fcmTokens").get(),
  ]);

  const warehouses = warehousesSnap.val() || {};
  const adminSettings = adminSettingsSnap.val() || {};
  const alertStateAll = alertStateSnap.val() || {};
  const tokensObj = tokensSnap.val() || {};
  const tokens = Object.keys(tokensObj);

  const alertUpdates = {};
  const messages = [];

  const connMessages = await checkConnectivity(db, warehouses).catch((e) => { log("فشل كشف انقطاع الاتصال: " + e); return []; });
  messages.push(...connMessages);

  const [fireMessages, gasMessages, motionMessages, powerMessages] = await Promise.all([
    checkFireAlarms(db).catch((e) => { log("فشل فحص إنذارات الحريق: " + e); return []; }),
    checkGasAlarms(db).catch((e) => { log("فشل فحص إنذارات الغاز: " + e); return []; }),
    checkMotionAlarms(db).catch((e) => { log("فشل فحص إنذارات الحركة: " + e); return []; }),
    checkPowerAlarms(db).catch((e) => { log("فشل فحص إنذارات الطاقة: " + e); return []; }),
  ]);
  messages.push(...fireMessages, ...gasMessages, ...motionMessages, ...powerMessages);

  for (const warehouseId of Object.keys(warehouses)) {
    const data = warehouses[warehouseId];
    const cfg = adminSettings[warehouseId];
    if (!data || !cfg) continue;

    const name = WAREHOUSE_NAMES[warehouseId] || { ar: warehouseId };
    const alertState = alertStateAll[warehouseId] || {};
    const newAlertState = Object.assign({}, alertState);

    const checks = [
      { key: "sensor1", label: "حساس 1", value: data.sensor1, min: cfg.tempMin, max: cfg.tempMax, unit: "°C" },
      { key: "sensor2", label: "حساس 2", value: data.sensor2, min: cfg.tempMin, max: cfg.tempMax, unit: "°C" },
      { key: "humidity", label: "الرطوبة", value: data.humidity, min: cfg.humMin, max: cfg.humMax, unit: "%" },
    ];

    const lines = [];
    for (const c of checks) {
      if (c.value === undefined || c.value === null || isNaN(c.value)) continue;
      if (c.key !== "humidity" && Number(c.value) <= -500) continue;
      const breach = c.value < c.min || c.value > c.max;
      const wasBreach = !!alertState[c.key];
      if (breach && !wasBreach) {
        const v = Number(c.value).toFixed(1);
        lines.push(`${c.label}: ${v}${c.unit} (المسموح ${c.min} إلى ${c.max}${c.unit})`);
      }
      newAlertState[c.key] = breach;
    }

    alertUpdates[warehouseId] = newAlertState;
    if (lines.length) {
      messages.push({ title: `⚠️ تنبيه حرارة — ${name.ar}`, body: lines.join("\n") });
    }
  }

  if (Object.keys(alertUpdates).length) {
    await db.ref("alertsSent").update(alertUpdates);
  }

  if (messages.length === 0) {
    log("لا يوجد تجاوز جديد.");
    return logLines.join("\n");
  }
  if (tokens.length === 0) {
    log(`فيه ${messages.length} تجاوز جديد بس ماكو أجهزة مسجلة للإشعارات.`);
    return logLines.join("\n");
  }

  for (const msg of messages) {
    const response = await admin.messaging().sendEachForMulticast({ tokens, notification: msg });
    log(`أرسلت: "${msg.title}" لعدد ${tokens.length} جهاز - نجح ${response.successCount}, فشل ${response.failureCount}`);

    const deletions = [];
    response.responses.forEach((r, i) => {
      if (!r.success && r.error && r.error.code === "messaging/registration-token-not-registered") {
        deletions.push(db.ref(`fcmTokens/${tokens[i]}`).remove());
      }
    });
    await Promise.all(deletions);
  }

  return logLines.join("\n");
}

// نقطة الدخول لدالة Netlify - نستدعيها كطلب HTTP عادي (GET) من خدمة تنبيه
// خارجية مجانية (cron-job.org) كل دقيقة، بدل الاعتماد على جدولة داخلية
exports.handler = async function (event, context) {
  try {
    const summary = await runCheck();
    return { statusCode: 200, body: summary || "ok" };
  } catch (e) {
    console.error("فشل الفحص:", e);
    return { statusCode: 500, body: "error: " + (e && e.message ? e.message : String(e)) };
  }
};
