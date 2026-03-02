import db from '../db';
import { handleIdentifyRequest } from '../services/identifyService';

beforeEach(() => {
  db.prepare('DELETE FROM contacts').run();
});

test('creates primary contact', () => {
  const res = handleIdentifyRequest({ email: 'lorraine@hillvalley.edu', phoneNumber: '123456' });
  expect(res.contact.primaryContatctId).toBeGreaterThan(0);
  expect(res.contact.emails).toContain('lorraine@hillvalley.edu');
  expect(res.contact.phoneNumbers).toContain('123456');
  expect(res.contact.secondaryContactIds).toHaveLength(0);
});

test('creates secondary when same phone new email', () => {
  const p = handleIdentifyRequest({ email: 'lorraine@hillvalley.edu', phoneNumber: '123456' });
  const s = handleIdentifyRequest({ email: 'mcfly@hillvalley.edu', phoneNumber: '123456' });
  expect(s.contact.primaryContatctId).toBe(p.contact.primaryContatctId);
  expect(s.contact.emails).toEqual(expect.arrayContaining(['lorraine@hillvalley.edu','mcfly@hillvalley.edu']));
  expect(s.contact.secondaryContactIds.length).toBeGreaterThanOrEqual(1);
});

test('lookup by phone or email returns consolidated contact', () => {
  const p = handleIdentifyRequest({ email: 'lorraine@hillvalley.edu', phoneNumber: '123456' });
  handleIdentifyRequest({ email: 'mcfly@hillvalley.edu', phoneNumber: '123456' });

  const byPhone = handleIdentifyRequest({ phoneNumber: '123456' });
  const byEmail = handleIdentifyRequest({ email: 'mcfly@hillvalley.edu' });

  expect(byPhone.contact.primaryContatctId).toBe(p.contact.primaryContatctId);
  expect(byEmail.contact.primaryContatctId).toBe(p.contact.primaryContatctId);
});

test('merges primaries: george + biff example', () => {
  const a = handleIdentifyRequest({ email: 'george@hillvalley.edu', phoneNumber: '919191' });
  const b = handleIdentifyRequest({ email: 'biffsucks@hillvalley.edu', phoneNumber: '717171' });

  const merged = handleIdentifyRequest({ email: 'george@hillvalley.edu', phoneNumber: '717171' });

  expect(merged.contact.emails).toEqual(expect.arrayContaining(['george@hillvalley.edu','biffsucks@hillvalley.edu']));
  expect(merged.contact.phoneNumbers).toEqual(expect.arrayContaining(['919191','717171']));
  expect(merged.contact.secondaryContactIds.length).toBeGreaterThanOrEqual(1);
});
