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
// مسح سجل الأحداث بالكامل (/logs) مرة وحدة كل سنة - بدل الاحتفاظ المتجدد
// بفترة معينة، هنا نصفر كل شي دفعة وحدة (كل الأنظمة: مخازن + حريق + غاز +
// حركة + طاقة) بمجرد ما تمر سنة كاملة من آخر مسح، حتى ما تكبر قاعدة البيانات
// بلا حدود (خطة Spark المجانية سقفها 1GB تخزين).
// نتحقق كل تشغيل للسكربت (كل 5 دقايق) من علامة زمنية بمسار /meta/lastLogWipe،
// وما نمسح شي إلا إذا مرت سنة كاملة منها.
// ==========================================================
const LOG_WIPE_INTERVAL_MS = 365 * 24 * 60 * 60 * 1000; // مرة كل سنة

async function wipeLogsYearly(db) {
  const metaSnap = await db.ref("meta/lastLogWipe").get();
  const lastWipe = metaSnap.val() || 0;
  const now = Date.now();

  if (lastWipe === 0) {
    // أول تشغيل لهذا الفحص على الإطلاق - نسجل نقطة البداية بس، من دون ما نمسح
    // شي (البيانات لسا جديدة أصلاً وما فيه داعي نصفرها أول يوم)
    await db.ref("meta/lastLogWipe").set(now);
    console.log("مسح السجل السنوي: أول تشغيل - سجّلنا نقطة البداية، ما مسحنا شي.");
    return;
  }

  if (now - lastWipe < LOG_WIPE_INTERVAL_MS) {
    return; // لسا ما مرت سنة كاملة من آخر مسح
  }

  await db.ref("logs").remove();
  await db.ref("meta/lastLogWipe").set(now);
  console.log("مسح السجل السنوي: مرت سنة كاملة - تم تصفير /logs بالكامل (كل الأنظمة).");
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

  await wipeLogsYearly(db).catch((e) => console.error("فشل المسح السنوي للسجل:", e));

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

    // الجهاز يرسل -999 كعلامة "متحسس مو مركّب / RTD مقطوع" بدل رقم حرارة حقيقي - نتجاهل أي قراءة
    // أوطأ من -500° تلقائياً (حساس 1 أو حساس 2، مو بس الثاني) حتى ما نرسل تنبيه كاذب على متحسس لسا
    // ما تركب. بمجرد ما يوصل رقم حقيقي فوق -500° يدخل تلقائياً بالفحص من دون أي إعداد يدوي
    const checks = [
      { key: "sensor1", label: "حساس 1", value: data.sensor1, min: cfg.tempMin, max: cfg.tempMax, unit: "°C" },
      { key: "sensor2", label: "حساس 2", value: data.sensor2, min: cfg.tempMin, max: cfg.tempMax, unit: "°C" },
      { key: "humidity", label: "الرطوبة", value: data.humidity, min: cfg.humMin, max: cfg.humMax, unit: "%" },
    ];

    const lines = [];
    for (const c of checks) {
      if (c.value === undefined || c.value === null || isNaN(c.value)) continue;
      if (c.key !== "humidity" && Number(c.value) <= -500) continue; // متحسس مو مركّب لهسة
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
