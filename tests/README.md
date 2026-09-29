# Tests

End-to-end suites that drive the real app (`app.py`) through Flask's test client against a
throwaway in-memory Postgres. **No suite ever talks to QuickBooks** — every QuickBooks call
(queries, CDC, token refresh, write-back) is replaced with a fake that records what the app
*would* have sent.

| Suite | Covers |
|---|---|
| `suite_reconciliation.py` | Statement import (date/amount formats, duplicate rows, running balances, OFX), the balance proof, carry-forward of outstanding items, footing and continuity checks, sign-off gating and override |
| `suite_sync_deletions.py` | Sync: CDC deletions, moved and voided transactions, restored transactions, full-resync safety guard, company change, stale watermarks, CDC cap |
| `suite_review_writeback.py` | Suggested matches waiting for review, books from QuickBooks, chart-of-accounts cache, write-back (single, bulk, retries, double-click protection), learned suggestions, the one-time migration |
| `suite_manual_match.py` | Matching by hand (1-1, many-1, 1-many), differences in the balance proof, undo, validation, learning from hand-made matches, the duplicate guard on write-back |
| `suite_ui.py` | The account page's own JavaScript run in a simulated browser (jsdom): manual-match totals, closest-amount sorting, search, "Match it instead", bulk-record confirmation |
| `suite_security.py` | CSRF tokens on every POST form, refusal of missing/forged/other-session tokens, session cookie flags |

## Running

One-time setup (needs Python 3.11+ and Node.js 20+; `npm install` fetches PGlite and jsdom):

```sh
pip install -r requirements.txt
cd tests && npm install && cd ..
```

Then:

```sh
python tests/run_all.py              # everything
python tests/run_all.py security     # suites whose name contains "security"
python tests/suite_sync_deletions.py # one suite on its own
```

`run_all.py` starts the test database (PGlite on port 54329, override with `TEST_PG_PORT`),
runs each suite in its own process, stops the database, and exits non-zero if anything failed.

## CI

`.github/workflows/tests.yml` runs `python tests/run_all.py` on GitHub Actions (Python 3.11,
Node 24) for every push to `main` and every pull request, and can be started by hand from the
Actions tab. It needs no secrets.

## Notes

- Each suite drops and recreates the `public` schema in the **test** database only. The
  harness always points `SUPABASE_DB_URL` at `127.0.0.1`, never at Supabase.
- The base schema in `harness.py` mirrors `docs/schema.sql`; the app's own startup code adds
  its extra columns and tables on import, the same way it does in production. If you add a
  table in Supabase directly, add it to `harness.SCHEMA` too.
- PGlite runs one session at a time, so the harness routes all of the app's connections
  through a single shared one (`harness.share_connection`).
- Test clients send the CSRF token like a browser would (`harness.browserlike`); use
  `client.raw_post` to send a POST without it.
