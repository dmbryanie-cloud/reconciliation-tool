"""When To review, To record and Transfers are all 0 but a difference is left, the account page refreshes the
books from QuickBooks by itself -- only if something was recorded since the last sync began.

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_auto_sync.py`, or all suites with `python tests/run_all.py`.
"""
import html, io, sys

import harness as H

ACCT = "00000000-0000-0000-0000-0000000000a1"
NAME = "KCB 09708"
A, c = H.setup(H.account_sql((ACCT, "35", NAME, "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
A.qbo_is_connected = lambda: False


def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
cl = H.login(A)
def page():
    return html.unescape(cl.get(f"/account/{NAME}").data.decode())

q("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency, description,
     last_modified) VALUES (%s,%s,'901','Purchase','2026-10-03',-500,'UGX','Umeme',now()) RETURNING 1""", (A.ORG_ID, ACCT))
body = "Date,Description,Amount\n2026-10-03,UMEME,-500\n"
cl.post(f"/account/{NAME}/upload", data={"replace": "1", "statement": (io.BytesIO(body.encode()), "s.csv"),
        "period_start": "2026-10-01", "period_end": "2026-10-31", "opening_balance": "1000", "closing_balance": "500"},
        content_type="multipart/form-data")
q("UPDATE statement SET book_balance=700, book_balance_source='user' RETURNING 1")    # out by 200
SYNCS = []
def fake_sync(full=False, progress=None):
    SYNCS.append(full); return 0, "none", "delta", "0s"
A.sync_from_quickbooks = fake_sync
A.refresh_book_balances = lambda: 0
A.qbo_is_connected = lambda: True

page()
check("nothing recorded since the last sync: no automatic refresh", SYNCS == [])
(lid,) = q("SELECT line_id FROM statement_line")[0]
q("INSERT INTO writeback_log (line_id, status, created_at) VALUES (%s,'done',now() - interval '1 second') RETURNING 1", (lid,))
p = page()
check("all settled, a difference left, something recorded since: it refreshes by itself", SYNCS == [False]
      and "refreshing the books from QuickBooks to bring in what was just recorded" in p)
check("…logged", q("SELECT count(*) FROM activity_log WHERE action LIKE 'started a QuickBooks refresh automatically%%'")[0][0] == 1)
page()
check("…once: the next visit doesn't sync again", SYNCS == [False])

q("UPDATE app_config SET value='{}' WHERE key='sync_job' RETURNING 1")
q("UPDATE statement SET book_balance=500 RETURNING 1")
page()
check("balanced: no refresh", SYNCS == [False])
q("UPDATE statement SET book_balance=700 RETURNING 1")
q("UPDATE match SET status='proposed', match_type='fuzzy', created_by='matcher' RETURNING 1")
page()
check("something still to do (a match to review): no refresh", SYNCS == [False])
# ---- clearing an account: its books are re-read in full, and it's logged ----------------------------------------------
q("UPDATE app_config SET value='{}' WHERE key='sync_job' RETURNING 1")
FULLS = []
A.sync_from_quickbooks = lambda full=False, progress=None: (FULLS.append(A._sync_plan()[0] is None), (0, "none", "full", "0s"))[1]
cl.post(f"/account/{NAME}/clear")
check("clearing removes the account's books", q("SELECT count(*) FROM book_txn WHERE account_id=%s", (ACCT,))[0][0] == 0)
check("…and starts a full re-read of them from QuickBooks", FULLS == [True])
check("…saying so", "The books are being re-read from QuickBooks" in page())
check("…logged", q("SELECT count(*) FROM activity_log WHERE action LIKE 'cleared the account%%'")[0][0] == 1)
sys.exit(T.summary())
