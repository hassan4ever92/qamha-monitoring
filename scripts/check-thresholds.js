/* ==========================================================
   سكربت مراقبة حدود الحرارة/الرطوبة لمنصة Qamha Scada System
   يشتغل عن طريق GitHub Actions كل چند دقايق (بدل Cloud Function،
   لأن خطة Firebase Blaze تحتاج بطاقة دفع دولية غير متوفرة بالعراق حالياً)
   ما يحتاج أي خطة مدفوعة - firebase-admin يشتغل مجاني بخطة Spark
   ========================================================== */

const admin = require("firebase-admin");

// بيانات حساب الخدمة تجي من GitHub Secret (متغير بيئة FIREBASE_SERVICE_ACCOUNT_KEY)
const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT_KEY);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: "https://qamha-metering-default-rtdb.europe-west1.firebasedatabase.app",
});

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
// تنظيف سجل الأحداث القديم (/logs/{warehouse}) - نحتفظ بس بآخر LOG_RETENTION_DAYS يوم،
// حتى ما تكبر قاعدة البيانات بلا حدود. نشغلها بس مرة كل 24 ساعة (مو كل تشغيل للسكربت
// كل 5 دقايق) عن طريق علامة زمنية نخزنها بمسار /meta/lastLogCleanup
// ==========================================================
const LOG_RETENTION_DAYS = 60;
const CLEANUP_INTERVAL_MS = 24 * 60 * 60 * 1000; // مرة كل يوم

async function pruneOldLogs(db) {
  const metaSnap = await db.ref("meta/lastLogCleanup").get();
  const lastCleanup = metaSnap.val() || 0;
  const now = Date.now();
  if (now - lastCleanup < CLEANUP_INTERVAL_MS) {
    return; // ماكو داعي ننظف - آخر تنظيف كان أقل من 24 ساعة
  }

  const cutoff = now - LOG_RETENTION_DAYS * 24 * 60 * 60 * 1000;
  const logsSnap = await db.ref("logs").get();
  const logsObj = logsSnap.val() || {};
  let deletedCount = 0;

  for (const warehouseId of Object.keys(logsObj)) {
    const oldEntriesSnap = await db
      .ref(`logs/${warehouseId}`)
      .orderByKey()
      .endAt(String(cutoff))
      .get();
    const oldEntries = oldEntriesSnap.val() || {};
    const keys = Object.keys(oldEntries);
    if (keys.length === 0) continue;

    const updates = {};
    keys.forEach((k) => { updates[`logs/${warehouseId}/${k}`] = null; });
    await db.ref().update(updates);
    deletedCount += keys.length;
  }

  await db.ref("meta/lastLogCleanup").set(now);
  console.log(`تنظيف السجل: حذفنا ${deletedCount} قراءة أقدم من ${LOG_RETENTION_DAYS} يوم.`);
}

// ==========================================================
// كشف انقطاع الاتصال (كهرباء أو انترنت) عن كل مخزن
// نعتمد على حقل timestamp (وقت حقيقي متزامن NTP يرسله الجهاز مع كل قراءة) - لو
// الوقت الحالي أبعد من آخر timestamp بأكثر من CONN_OFFLINE_MS نعتبر المخزن منقطع.
// نحفظ آخر حالة معروفة بـ /connState/{warehouseId} حتى ما نرسل إشعار كل 5 دقايق وهو
// لسا منقطع - نرسل بس أول لحظة ينقطع (rising edge) وأول لحظة يرجع (recovery).
// ==========================================================
const CONN_OFFLINE_MS = 3 * 60 * 1000; // 3 دقايق بدون بيانات = منقطع (الجهاز يرسل كل 15 ثانية عادة)

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
    if (ts <= 0) { statusByWarehouse[warehouseId] = null; continue; } // NTP ما تزامن لهسة - نتجاهله هذا الدور
    statusByWarehouse[warehouseId] = (now - ts) > CONN_OFFLINE_MS;
  }

  for (const warehouseId of Object.keys(statusByWarehouse)) {
    const isOffline = statusByWarehouse[warehouseId];
    if (isOffline === null) continue;
    const prev = connStateAll[warehouseId] || {};
    const wasOffline = !!prev.offline;
    const name = (WAREHOUSE_NAMES[warehouseId] || { ar: warehouseId }).ar;

    if (isOffline && !wasOffline) {
      // لحظة انقطاع جديدة
      connMessages.push({
        title: `⛔ انقطع الاتصال — ${name}`,
        body: "تم قطع الاتصال مع منظومة متحسسات المنطقة.",
      });
      connUpdates[warehouseId] = { offline: true, since: now };
    } else if (!isOffline && wasOffline) {
      // رجع الاتصال
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

async function main() {
  const db = admin.database();

  await pruneOldLogs(db).catch((e) => console.error("فشل تنظيف السجل القديم:", e));

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

  const connMessages = await checkConnectivity(db, warehouses).catch((e) => {
    console.error("فشل كشف انقطاع الاتصال:", e);
    return [];
  });
  messages.push(...connMessages);

  for (const warehouseId of Object.keys(warehouses)) {
    const data = warehouses[warehouseId];
    const cfg = adminSettings[warehouseId];
    if (!data || !cfg) continue;

    const name = WAREHOUSE_NAMES[warehouseId] || { ar: warehouseId };
    const alertState = alertStateAll[warehouseId] || {};
    const newAlertState = Object.assign({}, alertState);

    // لو المتحسس الثاني لسا ما انتصب بهذا المخزن (adminSettings.sensor2Installed=false)، نتجاهل قراءته
    // كلياً حتى ما يرسل تنبيهات push كاذبة بسبب رقم عشوائي/خطأ من شريحة غير موصولة
    const checks = [
      { key: "sensor1", label: "حساس 1", value: data.sensor1, min: cfg.tempMin, max: cfg.tempMax, unit: "°C" },
      { key: "humidity", label: "الرطوبة", value: data.humidity, min: cfg.humMin, max: cfg.humMax, unit: "%" },
    ];
    if (cfg.sensor2Installed) {
      checks.push({ key: "sensor2", label: "حساس 2", value: data.sensor2, min: cfg.tempMin, max: cfg.tempMax, unit: "°C" });
    }

    const lines = [];
    for (const c of checks) {
      if (c.value === undefined || c.value === null || isNaN(c.value)) continue;
      const breach = c.value < c.min || c.value > c.max;
      const wasBreach = !!alertState[c.key];
      // نرسل بس أول لحظة تجاوز (rising edge) - مو كل مرة يشتغل السكربت وهي لسا خارج الرينج
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
    console.log("لا يوجد تجاوز جديد.");
    return;
  }
  if (tokens.length === 0) {
    console.log(`فيه ${messages.length} تجاوز جديد بس ماكو أجهزة مسجلة للإشعارات.`);
    return;
  }

  for (const msg of messages) {
    const response = await admin.messaging().sendEachForMulticast({ tokens, notification: msg });
    console.log(`أرسلت: "${msg.title}" لعدد ${tokens.length} جهاز - نجح ${response.successCount}, فشل ${response.failureCount}`);

    const deletions = [];
    response.responses.forEach((r, i) => {
      if (!r.success && r.error && r.error.code === "messaging/registration-token-not-registered") {
        deletions.push(db.ref(`fcmTokens/${tokens[i]}`).remove());
      }
    });
    await Promise.all(deletions);
  }
}

main()
  .then(() => process.exit(0))
  .catch((e) => {
    console.error("فشل السكربت:", e);
    process.exit(1);
  });
