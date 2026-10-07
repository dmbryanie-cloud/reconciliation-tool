"""Reconcile in QuickBooks: once a reconciliation is signed off here, a page gives QuickBooks' Reconcile its
figures (ending date and balance) and the entries to tick, and checks how far QuickBooks got. Only people who
can sign off (admins, approvers) see it.

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_qbo_reconcile.py`, or all suites with `python tests/run_all.py`.
"""
import html, io, sys
from datetime import date

import harness as H

ACCT = "00000000-0000-0000-0000-0000000000a1"
NAME = "KCB 09708"
A, c = H.setup(H.account_sql((ACCT, "35", NAME, "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
A.qbo_is_connected = lambda: True
A.qbo_token = lambda: "tok"


def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
cl = H.login(A)
def text(url, client=None):
    return html.unescape((client or cl).get(url).data.decode())

for sid_, d, amt, who in (("901", "2026-10-03", -500, "Umeme"), ("902", "2026-10-10", 2500, "Customer A")):
    q("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency, description,
         last_modified) VALUES (%s,%s,%s,'Purchase',%s,%s,'UGX',%s,now()) RETURNING 1""", (A.ORG_ID, ACCT, sid_, d, amt, who))
body = "Date,Description,Amount\n2026-10-03,UMEME,-500\n2026-10-10,CUSTOMER A,2500\n"
cl.post(f"/account/{NAME}/upload", data={"replace": "1", "statement": (io.BytesIO(body.encode()), "s.csv"),
        "period_start": "2026-10-01", "period_end": "2026-10-31", "opening_balance": "1000", "closing_balance": "3000"},
        content_type="multipart/form-data")
URL = f"/account/{NAME}/qbo-reconcile"

p = text(f"/account/{NAME}")
check("before sign-off: the button shows but is disabled", 'disabled title="Sign off this reconciliation first">Reconcile in QuickBooks' in p)
r = cl.get(URL)
check("…and the page sends you back", r.status_code == 302)

q("UPDATE statement SET signed_off_at=now(), signed_off_by='Admin' RETURNING 1")
p = text(f"/account/{NAME}")
check("after sign-off: the button is a link", f'href="/account/{NAME}/qbo-reconcile"' in p.replace("%20", " "))
p = text(URL).replace("<b>", "").replace("</b>", "")
check("the page gives the ending date and balance", "Statement ending date: 31/10/2026" in p and "Ending balance: 3,000.00" in p)
check("…the entries to tick, with counts and totals", "Umeme" in p and "Customer A" in p
      and "1 deposit totalling 2,500.00" in p and "1 payment totalling -500.00" in p)
check("…and a link to QuickBooks' Reconcile", "/app/reconcile" in p)

ASKED = []
def fake_rec(token, acct_qbo, start, end, progress=None):
    ASKED.append((acct_qbo, start, end)); return {("901", date(2026, 10, 3))}
A.qbo_reconciled_lines = fake_rec
p = html.unescape(cl.post(URL, data={"action": "check"}).data.decode())
check("Check QuickBooks reads this account's reconciled entries over the period", ASKED == [("35", date(2026, 10, 3), date(2026, 10, 31))])
check("…and says which aren't reconciled yet", "1 of 2 entries are not reconciled in QuickBooks yet" in p
      and p.count("Not yet</span>") == 1)
A.qbo_reconciled_lines = lambda *a, **k: {("901", date(2026, 10, 3)), ("902", date(2026, 10, 10))}
p = html.unescape(cl.post(URL, data={"action": "check"}).data.decode())
check("…all done says so", "Done: all 2 entries are reconciled in QuickBooks, to 31/10/2026." in p)

sid = q("SELECT statement_id FROM statement")[0][0]
check("History links each signed-off reconciliation to it", f"qbo-reconcile?s={sid}" in text(f"/account/{NAME}/history"))

A.add_user("viewer", "Viewer", "secret1", False)
q("UPDATE app_users SET perms='upload,review,record' WHERE username='viewer' RETURNING 1")
vc = H.browserlike(A.app.test_client()); vc.post("/login", data={"username": "viewer", "password": "secret1"})
check("someone who can't sign off doesn't see the button", "Reconcile in QuickBooks" not in text(f"/account/{NAME}", vc))
check("…nor the page", vc.get(URL).status_code == 403)
A.add_user("appr", "Approver", "secret1", False)
q("UPDATE app_users SET perms='review,signoff,reopen' WHERE username='appr' RETURNING 1")
ac = H.browserlike(A.app.test_client()); ac.post("/login", data={"username": "appr", "password": "secret1"})
check("an approver does", "Reconcile in QuickBooks" in text(f"/account/{NAME}", ac) and ac.get(URL).status_code == 200)
sys.exit(T.summary())
