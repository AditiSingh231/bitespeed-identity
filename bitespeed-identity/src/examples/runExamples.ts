import { handleIdentifyRequest } from '../services/identifyService';
import db from '../db';

function resetDb() {
  db.prepare('DELETE FROM contacts').run();
}

function pretty(obj: any) {
  console.log(JSON.stringify(obj, null, 2));
}

async function run() {
  console.log('Resetting DB');
  resetDb();

  console.log('\n1) Create primary contact (lorraine / 123456)');
  let r1 = handleIdentifyRequest({ email: 'lorraine@hillvalley.edu', phoneNumber: '123456' });
  pretty(r1);

  console.log('\n2) Create secondary (mcfly / 123456)');
  let r2 = handleIdentifyRequest({ email: 'mcfly@hillvalley.edu', phoneNumber: '123456' });
  pretty(r2);

  console.log('\n3) Lookup by phone only (123456)');
  let r3 = handleIdentifyRequest({ phoneNumber: '123456' });
  pretty(r3);

  console.log('\n4) Lookup by email only (lorraine@hillvalley.edu)');
  let r4 = handleIdentifyRequest({ email: 'lorraine@hillvalley.edu' });
  pretty(r4);

  console.log('\n5) Create two separate primaries (george / 919191) and (biff / 717171)');
  resetDb();
  let a = handleIdentifyRequest({ email: 'george@hillvalley.edu', phoneNumber: '919191' });
  let b = handleIdentifyRequest({ email: 'biffsucks@hillvalley.edu', phoneNumber: '717171' });
  pretty(a);
  pretty(b);

  console.log('\n6) Merge primaries by sending {email: george, phoneNumber: 717171}');
  let merged = handleIdentifyRequest({ email: 'george@hillvalley.edu', phoneNumber: '717171' });
  pretty(merged);

  console.log('\nFinal contacts table:');
  const all = db.prepare('SELECT * FROM contacts ORDER BY id').all();
  pretty(all);
}

run().catch((e) => {
  console.error(e);
  process.exit(1);
});
