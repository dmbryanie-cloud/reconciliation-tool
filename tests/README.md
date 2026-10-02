# Tests

End-to-end suites that drive the real app (`app.py`) through Flask's test client against a
throwaway in-memory Postgres. **No suite ever talks to QuickBooks** — every QuickBooks call
(queries, CDC, token refresh, write-back) is replaced with a fake that records what the app
*would* have sent.

| Suite | Covers |
|---|---|
| `suite_reconciliation.py` | Statement import (date/amount formats, duplicate rows, running balances, OFX), the balance proof, carry-forward of outstanding items, footing and continuity checks, sign-off gating and override |
| `suite_sync_deletions.py` | Sync: CDC deletions, moved and voided transactions, restored transactions, full-resync safety guard, company change, stale watermarks, CDC cap, a due full re-pull never run inside an upload or balance fetch |
| `suite_background_sync.py` | Sync running in a background thread: the request returns mid-download, progress banner and status endpoint (its script run in Node), one sync at a time, uploads and balance fetches stay out of a running sync, a sync lost to a restart shown as stopped and restartable, failures reported |
| `suite_review_writeback.py` | Suggested matches waiting for review, books from QuickBooks, chart-of-accounts cache, write-back (single, bulk, retries, double-click protection), learned suggestions, the one-time migration |
| `suite_hedges_family.py` | Forward-deal hedges booked as by hand (USD leg: transfers via FX in Transit at the month's rate; UGX receipt: FX in Transit UGX cleared, gain or loss to Forex Gain), a parent's lump sum split into payments per child, batch suggestions trying a family's payments first (script run in jsdom) |
| `suite_manual_match.py` | Matching by hand (1-1, many-1, 1-many), differences in the balance proof, undo, validation, learning from hand-made matches, the duplicate guard on write-back |
| `suite_record_jobs.py` | An entry QuickBooks already has (same amount, dates within 3 days, similar payee) is matched instead of created; the message counts what was recorded ("Recorded 2 of 3 selected"); recorded lines leave the list; 4+ lines are recorded in the background with progress, one job at a time, a lost job reported; bank charges pair only on the exact date; the account switcher on the account page; identical same-day lines each keep the entry recorded for them, and a line whose entry an identical line took can be recorded again |
| `suite_sections.py` | Account page sections fold under their headings: those needing attention come first and open, finished ones go below folded; tiles, the side menu, links and #hash open a folded section; Expand/Collapse all; the record list puts lines still to record first |
| `suite_students_payables.py` | Receipts are paid to a student or family (their UGX or USD account), not Accounts Receivable in general; UGX for a student's USD account is paid in USD at a rate (typed, or QuickBooks'), exactly the UGX received; family lump sums split per child; a USD account only takes USD students; payables (Rent payable) need a supplier; tables sort by date and amount; the account switcher's Reconcile button says what's loading; a supplier suggested by name counts as picked; Enter never sends the form, and Record with an account typed but not picked is stopped |
| `suite_transfer_edit.py` | Possible transfers: Edit picks another counterpart (within 14 days) or just the other account; Not a transfer hides a suggestion (one, or several ticked at once) and Restore brings it back; a recorded transfer's Edit moves its other side to another line or account (updated in place in QuickBooks, old counterpart unpaired, new one matched); Undo deletes an app-recorded Transfer in QuickBooks (with its SyncToken), unmatches both lines and puts them back in the list; refused while a statement is signed off; a refused delete changes nothing |
| `suite_record_extras.py` | Recording: receipts against a customer's name (QuickBooks Payment, A/R in the bank's currency), a split line as one journal entry (an FX hedge and its loss; debits equal credits; the sync reads it back without a duplicate), typed rates and asking for one when QuickBooks has none, saving and discarding the selection, exact suggestions listed last, the selection count and the progress messages (script run in jsdom) |
| `suite_edit_match.py` | The Edit button on suggested matches: it opens Match manually with the suggestion's items ticked (its script run in jsdom), the edited group matched by hand rejects the old suggestion, matching it unchanged rejects nothing, editing a suggestion that was already confirmed |
| `suite_txn_type.py` | The record list's Type box (expense, customer/student payment, supplier payment, transfer) narrows the account picker; it starts on a guess (a transfer pair on another statement, a confident earlier posting, bank charges, words like transfer / fees / rent) and says why; a clear transfer pair suggests the other account, unticked |
| `suite_ui.py` | The account page's own JavaScript run in a simulated browser (jsdom): manual-match totals, closest-amount sorting, search, "Match it instead", bulk-record confirmation, the account search box (by any word, near-misses, Enter/click to pick, clear to leave a line unrecorded) |
| `suite_redesign.py` | ReconBook frame and pages: sidebar with company and account status, dashboard (month close, attention list, checklist, currency totals), per-user permission ticks enforced on every action, switch-off and expiry, the second-person sign-off rule, activity log, Settings (matching rules kept in range), Reports, Search, the sign-in page with its show-password eye, and the page dialog, menus and side panels |
| `suite_bulk.py` | Bulk actions the same in every section: tick rows or the header box, then Confirm / Reject selected suggested matches, Not a transfer / Record selected as transfers (one Transfer per pair, a line ticked twice recorded once), Restore selected hidden suggestions, Undo selected recorded transfers; only this account's items, review permission enforced; a successful upload closes the panel and shows a "Statement uploaded" message that stays |
| `suite_focus.py` | Focus on dates: narrow the account page to a week or month of the statement (quick picks, dates kept inside the period, kept while you work, cleared by Whole statement or a new statement); balances and sign-off still count the whole statement |
| `suite_transfer_charges.py` | Possible transfers leave bank charges out (on either side); Not a transfer / Restore selected with hundreds ticked are saved in one go, this account's lines only |
| `suite_search.py` | Search bars over the account page's long lists: words, amounts with or without commas, dates either way round; Tick only these / Untick all; the header box ticks only what's shown; the bulk bars count the ticks |
| `suite_progress.py` | Reconciled-to date (latest signed-off period end) on the dashboard, sidebar and account page; "cleared to" as an open statement's lines are matched; Save & finish later keeps the record table's choices, notes who and when, returns to the dashboard; book balance from QuickBooks filled in and kept current by each sync, a typed one never overwritten; a statement overlapping signed-off months is refused naming every one, and any month's sign-off can be undone from Reports; a big upload (a year's PDF) is read and matched in the background with progress shown, a second upload refused meanwhile, the result shown once; amount boxes show thousands separators and amounts typed with commas are saved |
| `suite_report.py` | The printable reconciliation statement (draft/signed, itemised outstanding and unrecorded items, differences, override notes), past periods after later ones are signed off, keeping UGX and USD separate in transfer checks and suggestions |
| `suite_transfers.py` | Recording money moved between your own accounts as one QuickBooks Transfer: from the record table, as a pair seen on two statements (both lines matched), card payments from a bank; same currency only; refusals (signed off, already matched, CSRF); the next sync brings the same rows back without duplicates; sync stores each side of a USD/UGX transfer in its own account's currency |
| `suite_pdf.py` | PDF bank statements generated by `pdfgen.py`: Debit/Credit/Balance, Amount+Balance newest-first, Money In/Out without balances, Withdrawals/Deposits with Cr/Dr balances; wrapped descriptions, two pages, period and balances read from the PDF; DFCU's layout (a day's lines out of posting order, descriptions starting above the date); the statement's account number refused on another account (PDF and OFX); refusals (balance doesn't add up, no direction, scanned, not a PDF); password-protected e-statements; an amount too wide for its column (its last digit wrapped onto the next line) is joined again; a refused PDF is never labelled as uploaded |
| `suite_security.py` | CSRF tokens on every POST form, refusal of missing/forged/other-session tokens, session cookie flags |

## Running

One-time setup (needs Python 3.11+ and Node.js 20+; `npm install` fetches PGlite and jsdom):

```sh
pip install -r requirements.txt -r tests/requirements.txt
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
