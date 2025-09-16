// Firestore-backed RBAC + ACL helpers, per your model.
export function createRbac({ db }) {
  const acc = (accountId) => db.collection('accounts').doc(accountId);

  return {
    // Role from /accounts/{accountId}/members/{uid}.role
    async getRole(accountId, uid) {
      const snap = await acc(accountId).collection('members').doc(uid).get();
      if (!snap.exists) return null;
      const role = String(snap.get('role') || '').trim();
      return role || null;
    },

    // List ACL — /accounts/{accountId}/acl/*
    async listAcl(accountId) {
      const col = await acc(accountId).collection('acl').get();
      return col.docs.map(d => ({ uid: d.id, sessions: Array.isArray(d.get('sessions')) ? d.get('sessions') : [] }));
    },

    // Set ACL (overwrite sessions array; empty array = revoke)
    async setAcl(accountId, uid, sessions) {
      const doc = acc(accountId).collection('acl').doc(uid);
      await doc.set({ sessions: Array.from(new Set(sessions.map(String))) }, { merge: true });
    },

    // Get allowed sessions for a user, respecting role
    async allowedSessions(accountId, uid) {
      const role = await this.getRole(accountId, uid);
      if (!role) return { role: null, sessions: [] };

      if (role === 'Administrator') {
        // Admin sees all labels for the account
        const s = await acc(accountId).collection('sessions').get();
        return { role, sessions: s.docs.map(d => d.id) };
      }

      const a = await acc(accountId).collection('acl').doc(uid).get();
      const sessions = a.exists ? (Array.isArray(a.get('sessions')) ? a.get('sessions') : []) : [];
      return { role, sessions };
    },

    // Subscribe to ACL changes for a user; callback with new sessions[]
    // (Note: for Administrators we refresh on sessions collection changes)
    subscribeAllowed({ accountId, uid }, cb) {
      let unsubAcl = () => {};
      let unsubSessions = () => {};

      const acct = acc(accountId);

      // watch member role
      const unsubMember = acct.collection('members').doc(uid).onSnapshot(async (mSnap) => {
        const role = mSnap.exists ? String(mSnap.get('role') || '') : null;
        // clean old listeners
        unsubAcl(); unsubSessions = () => {};
        if (!role) return cb({ role: null, sessions: [] });

        if (role === 'Administrator') {
          // Admin -> follow sessions list
          unsubSessions = acct.collection('sessions').onSnapshot((sSnap) => {
            cb({ role, sessions: sSnap.docs.map(d => d.id) });
          });
        } else {
          // Non-admin -> follow ACL doc
          unsubAcl = acct.collection('acl').doc(uid).onSnapshot((aSnap) => {
            const sessions = aSnap.exists ? (Array.isArray(aSnap.get('sessions')) ? aSnap.get('sessions') : []) : [];
            cb({ role, sessions });
          });
        }
      });

      return () => { try { unsubMember(); } catch {} try { unsubAcl(); } catch {} try { unsubSessions(); } catch {} };
    }
  };
}
