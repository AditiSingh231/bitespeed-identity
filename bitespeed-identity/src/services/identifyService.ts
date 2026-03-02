import db from '../db';
import { Contact } from '../models/contact';

function nowIso() {
  return new Date().toISOString();
}

function findMatches(email?: string | null, phone?: string | null): Contact[] {
  const stmt = db.prepare(
    `SELECT * FROM contacts WHERE deletedAt IS NULL AND ((email IS NOT NULL AND email = ?) OR (phoneNumber IS NOT NULL AND phoneNumber = ?))`
  );
  return stmt.all(email ?? null, phone ?? null) as Contact[];
}

function getContactsByPrimaryId(primaryId: number): Contact[] {
  const stmt = db.prepare('SELECT * FROM contacts WHERE deletedAt IS NULL AND (id = ? OR linkedId = ?)');
  return stmt.all(primaryId, primaryId) as Contact[];
}

function upsertPrimaryAndSecondaries(allContacts: Contact[]) {
  if (allContacts.length === 0) return;
  // find oldest createdAt
  allContacts.sort((a, b) => (a.createdAt < b.createdAt ? -1 : 1));
  const primary = allContacts[0];

  const updatePrimary = db.prepare(
    'UPDATE contacts SET linkPrecedence = ?, linkedId = NULL, updatedAt = ? WHERE id = ?'
  );
  updatePrimary.run('primary', nowIso(), primary.id);

  const updateSecondary = db.prepare(
    'UPDATE contacts SET linkPrecedence = ?, linkedId = ?, updatedAt = ? WHERE id = ?'
  );

  for (let i = 1; i < allContacts.length; i++) {
    const c = allContacts[i];
    updateSecondary.run('secondary', primary.id, nowIso(), c.id);
  }
}

function collectLinkSet(matches: Contact[]): Contact[] {
  const primaryIds = new Set<number>();
  for (const m of matches) {
    if (m.linkPrecedence === 'primary') primaryIds.add(m.id);
    else if (m.linkedId) primaryIds.add(m.linkedId);
  }

  const all: Contact[] = [];
  for (const pid of primaryIds) {
    const group = getContactsByPrimaryId(pid);
    for (const g of group) all.push(g);
  }

  // remove duplicates by id
  const byId = new Map<number, Contact>();
  for (const c of all) byId.set(c.id, c);
  return Array.from(byId.values());
}

function createContact(email?: string | null, phone?: string | null, precedence: 'primary' | 'secondary' = 'primary', linkedId?: number | null) {
  const now = nowIso();
  const stmt = db.prepare(
    'INSERT INTO contacts (phoneNumber, email, linkedId, linkPrecedence, createdAt, updatedAt) VALUES (?,?,?,?,?,?)'
  );
  const info = stmt.run(phone ?? null, email ?? null, linkedId ?? null, precedence, now, now);
  const id = info.lastInsertRowid as number;
  return db.prepare('SELECT * FROM contacts WHERE id = ?').get(id) as Contact;
}

export function handleIdentifyRequest(payload: { email?: string | null; phoneNumber?: string | null }) {
  const email = payload.email ?? null;
  const phone = payload.phoneNumber ?? null;

  if (!email && !phone) {
    throw new Error('email or phoneNumber required');
  }

  // Run reconciliation inside a DB transaction to avoid races
  const result = db.transaction(() => {
    const matches = findMatches(email, phone);

    if (matches.length === 0) {
      // no existing contacts -> create primary
      const created = createContact(email, phone, 'primary', null);
      return buildResponse(created.id);
    }

    // collect full link set (only non-deleted rows)
    const linkSet = collectLinkSet(matches);

    // ensure correct primary/secondary assignments
    upsertPrimaryAndSecondaries(linkSet);

    // refresh after updates
    const ordered = linkSet.slice().sort((a, b) => (a.createdAt < b.createdAt ? -1 : 1));
    const primaryId = ordered[0].id;
    const fullSet = getContactsByPrimaryId(primaryId);

    // if incoming has any new info not present in fullSet, create a new secondary
    const emails = new Set<string>();
    const phones = new Set<string>();
    for (const c of fullSet) {
      if (c.email) emails.add(c.email);
      if (c.phoneNumber) phones.add(c.phoneNumber);
    }

    const needCreate = (email && !emails.has(email)) || (phone && !phones.has(phone));

    if (needCreate) {
      createContact(email, phone, 'secondary', primaryId);
    }

    return buildResponse(primaryId);
  })();

  return result;
}

function buildResponse(primaryId: number) {
  const contacts = getContactsByPrimaryId(primaryId);
  // determine primary row (oldest)
  contacts.sort((a, b) => (a.createdAt < b.createdAt ? -1 : 1));
  const primary = contacts[0];

  const emails: string[] = [];
  const phones: string[] = [];
  const secondaryIds: number[] = [];

  // primary's email/phone first as required
  if (primary.email) emails.push(primary.email);
  if (primary.phoneNumber) phones.push(primary.phoneNumber);

  for (const c of contacts) {
    if (c.id === primary.id) continue;
    if (c.email && !emails.includes(c.email)) emails.push(c.email);
    if (c.phoneNumber && !phones.includes(c.phoneNumber)) phones.push(c.phoneNumber);
    if (c.linkPrecedence === 'secondary') secondaryIds.push(c.id);
  }

  return {
    contact: {
      primaryContatctId: primary.id,
      emails,
      phoneNumbers: phones,
      secondaryContactIds: secondaryIds
    }
  };
}
