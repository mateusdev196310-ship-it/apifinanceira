const functions = require('firebase-functions');
const admin = require('firebase-admin');
try {
  admin.initializeApp();
} catch (e) {}
exports.setCustomClaims = functions.https.onCall(async (data, context) => {
  if (!context.auth || !context.auth.token || !(context.auth.token.role === 'admin' || context.auth.token.admin === true)) {
    throw new functions.https.HttpsError('permission-denied', 'admin required');
  }
  const uid = String(data.uid || '');
  const role = String(data.role || 'user');
  if (!uid) {
    throw new functions.https.HttpsError('invalid-argument', 'uid required');
  }
  await admin.auth().setCustomUserClaims(uid, { role, admin: role === 'admin' });
  return { ok: true, uid, role };
});
exports.setAppCheckRequired = functions.https.onCall(async (data, context) => {
  if (!context.auth || !context.auth.token || !(context.auth.token.role === 'admin' || context.auth.token.admin === true)) {
    throw new functions.https.HttpsError('permission-denied', 'admin required');
  }
  const required = !!data.required;
  await admin.firestore().collection('config').doc('security').set({ appCheckRequired: required }, { merge: true });
  await admin.firestore().collection('audit_logs').add({
    type: 'config',
    action: 'setAppCheckRequired',
    required,
    ts: admin.firestore.FieldValue.serverTimestamp()
  });
  return { ok: true, appCheckRequired: required };
});
exports.backupFirestoreDaily = functions.pubsub.schedule('0 3 * * *').timeZone('America/Recife').onRun(async () => {
  const { google } = require('googleapis');
  const projectId = process.env.GCLOUD_PROJECT || process.env.FIREBASE_CONFIG ? JSON.parse(process.env.FIREBASE_CONFIG).projectId : null;
  let bucketName = process.env.BACKUP_BUCKET;
  if (!bucketName) {
    try {
      bucketName = admin.storage().bucket().name;
    } catch (e) {}
  }
  if (!projectId || !bucketName) return null;
  const auth = await google.auth.getClient({
    scopes: ['https://www.googleapis.com/auth/datastore', 'https://www.googleapis.com/auth/cloud-platform']
  });
  const firestore = google.firestore({ version: 'v1', auth });
  const dbName = `projects/${projectId}/databases/(default)`;
  const dateStr = new Date().toISOString().substring(0, 10);
  const outputUriPrefix = `gs://${bucketName}/firestore-backups/${dateStr}`;
  await firestore.projects.databases.exportDocuments({
    name: dbName,
    requestBody: {
      outputUriPrefix
    }
  });
  await admin.firestore().collection('audit_logs').add({
    type: 'backup',
    status: 'requested',
    outputUriPrefix,
    ts: admin.firestore.FieldValue.serverTimestamp()
  });
  return { ok: true, outputUriPrefix };
});
exports.auditLogTransactions = functions.firestore.document('transactions/{txId}').onWrite(async (change, context) => {
  const before = change.before.exists ? change.before.data() : null;
  const after = change.after.exists ? change.after.data() : null;
  const action = after && !before ? 'create' : (!after && before ? 'delete' : 'update');
  await admin.firestore().collection('audit_logs').add({
    type: 'transaction',
    action,
    txId: context.params.txId,
    before,
    after,
    ts: admin.firestore.FieldValue.serverTimestamp()
  });
});
exports.auditLogClienteItems = functions.firestore.document('clientes/{clienteId}/transacoes/{dr}/items/{itemId}').onWrite(async (change, context) => {
  const before = change.before.exists ? change.before.data() : null;
  const after = change.after.exists ? change.after.data() : null;
  const action = after && !before ? 'create' : (!after && before ? 'delete' : 'update');
  await admin.firestore().collection('audit_logs').add({
    type: 'cliente_item',
    action,
    clienteId: context.params.clienteId,
    dr: context.params.dr,
    itemId: context.params.itemId,
    before,
    after,
    ts: admin.firestore.FieldValue.serverTimestamp()
  });
});
