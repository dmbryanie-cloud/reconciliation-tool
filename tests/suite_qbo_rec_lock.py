"""Bank lines dated in a period QuickBooks has already reconciled are in QuickBooks already: they're
never suggested for recording, and recording them is refused. How far QuickBooks is reconciled is read
on each sync (its latest entry marked R in the open statement's period).

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_qbo_rec_lock.py`, or all suites with `python tests/run_all.py`.
"""
import html, io, re, sys
from datetime import date

import harness as H

ACCT = "00000000-0000-0000-0000-0000000000a1"
NAME = "Stanbic UGX 10202"
A, c = H.setup(H.account_sql((ACCT, "35", NAME, "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
A._store_coa([{"Id": "35", "Name": NAME, "AccountType": "Bank", "CurrencyRef": {"value": "UGX"}},
              {"Id": "83", "Name": "Bank charges UGX", "AccountType": "Expense", "CurrencyRef": {"value": "UGX"}}])
POSTS = []
def fake_post(token, entity, body):
    POSTS.append((entity, body)); return {entity: {"Id": str(54200 + len(POSTS))}}
A.qbo_post = fake_post
A.qbo_token = lambda: "tok"
A.qbo_realm = lambda: "REAL-1"
A.qbo_is_connected = lambda: False


def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
def lid(desc):
    return str(q("SELECT line_id FROM statement_line WHERE description=%s", (desc,))[0][0])
cl = H.login(A)
def page():
    return html.unescape(cl.get(f"/account/{NAME}").data.decode())
def msg():
    m = re.search(r"<div id=flash[^>]*>(.*?)</div>", page(), re.S)
    return re.sub(r"<[^>]+>", "", m.group(1)) if m else ""
def record_part(p):
    return p.split("id=sec-record")[1].split("</form>")[0] if "id=sec-record" in p else ""
def locked_part(p):
    return p.split("id=qlocked")[1].split("</details>")[0] if "id=qlocked" in p else ""

body = ("Date,Description,Amount\n2025-09-08,FEE ACH INWD CR,-2000\n2025-09-20,SERVICE CHARGE SEPT,-4600\n"
        "2025-10-15,SERVICE CHARGE OCT,-3000\n")
cl.post(f"/account/{NAME}/upload", data={"statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
        "period_start": "2025-09-01", "period_end": "2025-10-31"}, content_type="multipart/form-data")
SEP1, SEP2, OCT = lid("FEE ACH INWD CR"), lid("SERVICE CHARGE SEPT"), lid("SERVICE CHARGE OCT")

p = page()
check("before QuickBooks' reconciliation is known, every line can be recorded",
      all(f'name=sel value="{x}"' in p for x in (SEP1, SEP2, OCT)) and "id=qlocked" not in p)

# ---- the sync reads how far QuickBooks is reconciled ---------------------------------------------------------
ASKED = []
def fake_rec(token, acct_qbo, start, end, progress=None):
    ASKED.append((acct_qbo, start, end)); return {("50296", date(2025, 9, 25)), ("50100", date(2025, 9, 3))}
A.qbo_reconciled_lines = fake_rec
check("reading it: nothing fails", A.refresh_qbo_rec_points("tok") == [])
check("…asks QuickBooks only about the open statement's period", ASKED == [("35", date(2025, 9, 1), date(2025, 10, 31))])
check("…keeps its latest reconciled entry's date", q("SELECT as_of FROM qbo_rec_point")[0][0] == date(2025, 9, 25))

p = page()
check("lines up to then aren't offered for recording", not any(f'name=sel value="{x}"' in p for x in (SEP1, SEP2))
      and "FEE ACH INWD CR" not in record_part(p) and "SERVICE CHARGE SEPT" not in record_part(p))
check("…they're listed to match instead", "In QuickBooks' reconciled period — match, don't record (2)" in p
      and "QuickBooks is reconciled to 25/09/2025" in p and "FEE ACH INWD CR" in locked_part(p)
      and "Match by hand" in locked_part(p) and "Ignore — it's in QuickBooks" in locked_part(p))
check("the line after it can still be recorded", f'name=sel value="{OCT}"' in p)
check("…and only it counts as to record", 'data-note="1 to record"' in p)

# ---- recording one anyway is refused ------------------------------------------------------------------------
cl.post(f"/account/{NAME}/record", data={"bulk": "1", "sel": [SEP1, SEP2, OCT], **{f"acct_{x}": "83" for x in (SEP1, SEP2, OCT)}})
m = msg()
check("recording all three: only October's goes to QuickBooks", len(POSTS) == 1
      and q("SELECT count(*) FROM writeback_log WHERE line_id IN (%s::uuid, %s::uuid)", (SEP1, SEP2))[0][0] == 0)
check("…says why the other two were left out", "2 lines were left out: QuickBooks is reconciled to 25/09/2025" in m)

# ---- ignoring one ------------------------------------------------------------------------------------------
cl.post(f"/account/{NAME}/record_ignore", data={"ignore": SEP1})
p = page()
check("ignoring one moves it to the ignored list", "In QuickBooks' reconciled period — match, don't record (1)" in p
      and "FEE ACH INWD CR" in p.split("id=ignored")[1].split("</details>")[0])

# ---- a failed read is reported, and keeps what was known -----------------------------------------------------
def broken(*a, **k):
    raise RuntimeError("QuickBooks is down")
A.qbo_reconciled_lines = broken
check("a failed read names the account", A.refresh_qbo_rec_points("tok") == [NAME])
check("…and keeps the date read before", q("SELECT as_of FROM qbo_rec_point")[0][0] == date(2025, 9, 25))
sys.exit(T.summary())
