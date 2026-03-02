# Bitespeed Identity Reconciliation

Local setup and run instructions

Prerequisites:
- Node.js (16+)

Install dependencies:

```bash
cd c:/Users/Jaswanth/Projects/bitespeed-identity
npm install
```

Run in development (auto-restart on changes):

```bash
npm run dev
```

The server listens on port 3000 by default.

Endpoints:
- POST /identify

Request body:
```json
{ "email": "optional", "phoneNumber": "optional" }
```

Example curl:

```bash
curl -X POST http://localhost:3000/identify -H "Content-Type: application/json" -d '{"email":"lorraine@hillvalley.edu","phoneNumber":"123456"}'
```

Run the built-in examples (executes flows and prints results):

```bash
npm run examples
```

Notes:
- Data is persisted in SQLite at `src/data/db.sqlite` (created automatically).
- The code implements the reconciliation rules described in the task: finds matching contacts, merges primaries when needed, creates secondaries when new info is supplied, and returns the consolidated contact structure.
