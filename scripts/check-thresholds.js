const admin = require("firebase-admin");

const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT_KEY);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: "https://qamha-metering-default-rtdb.europe-west1.firebasedatabase.app",
});

const WAREHOUSE_NAMES = {
  warehouse_1: { ar: "المخزن المجمد" },
  warehouse_2: { ar: "المخزن المبرد 1" },
  warehouse_3: { ar: "المخزن المبرد 2" },
  warehouse_4: { ar: "مخزن المواد الأولية 1" },
  warehouse_5: { ar: "مخزن المواد الأولية 2" },
  warehouse_6: { ar: "مخزن الطحين" },
  warehouse_7: { ar: "منطقة الكهرباء" },
};

async function main() {
  const db = admin.database();

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
