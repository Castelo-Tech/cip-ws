// Firestore session registry: /accounts/{accountId}/sessions/{label}
// Stores waId + createdAt + lastReadyAt + status (best-effort).
export function createSessionRegistry({ db }) {
  const acc = (accountId) => db.collection('accounts').doc(accountId);

  return {
    async setReady(accountId, label, waId) {
      await acc(accountId).collection('sessions').doc(label).set({
        waId: String(waId || ''),
        status: 'ready',
        lastReadyAt: new Date()
      }, { merge: true });
    },
    async setStatus(accountId, label, status) {
      await acc(accountId).collection('sessions').doc(label).set({
        status: String(status),
        createdAt: status === 'starting' ? new Date() : undefined
      }, { merge: true });
    },
    async remove(accountId, label) {
      await acc(accountId).collection('sessions').doc(label).delete().catch(()=>{});
    },
    async list(accountId) {
      const snap = await acc(accountId).collection('sessions').get();
      return snap.docs.map(d => ({
        accountId,
        label: d.id,
        waId: d.get('waId') || null,
        status: d.get('status') || 'idle',
        createdAt: d.get('createdAt')?.toDate?.() || null,
        lastReadyAt: d.get('lastReadyAt')?.toDate?.() || null
      })).sort((a,b) => a.label.localeCompare(b.label));
    },
    async getWaId(accountId, label) {
      const d = await acc(accountId).collection('sessions').doc(label).get();
      return d.exists ? (d.get('waId') || null) : null;
    }
  };
}
