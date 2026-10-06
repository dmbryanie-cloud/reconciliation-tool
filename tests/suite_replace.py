"""An upload whose dates overlap an open reconciliation replaces it (one per period). When that
reconciliation has work in it, the upload is refused unless "Replace the open reconciliation" is ticked.

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_replace.py`, or all suites with `python tests/run_all.py`.
"""
import html, io, re, sys

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
def upload(body, **form):
    cl.post(f"/account/{NAME}/upload", data={"statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0", **form},
            content_type="multipart/form-data")
    m = re.search(r"<div id=flash[^>]*>(.*?)</div>", page(), re.S)
    return re.sub(r"<[^>]+>", "", m.group(1)) if m else ""
def periods():
    return [(str(a), str(b)) for a, b in q("SELECT period_start, period_end FROM statement ORDER BY period_start")]

FEB = "Date,Description,Amount\n2026-02-03,CHARGE,-3450\n2026-02-20,DEPOSIT,8460000\n"
FEB_MAR = FEB + "2026-03-05,CHARGE,-3450\n"
MAR = "Date,Description,Amount\n2026-03-05,CHARGE,-3450\n"

upload(FEB, period_start="2026-02-01", period_end="2026-02-28")
check("the upload box offers to replace the open reconciliation",
      "Replace the open reconciliation (01/02/2026 to 28/02/2026)" in page())
m = upload(FEB_MAR, period_start="2026-02-01", period_end="2026-03-31")
check("no work in it yet: an overlapping upload replaces it as before", periods() == [("2026-02-01", "2026-03-31")]
      and "replaces the earlier reconciliation" in m)

# Work: a line ignored.
lid = str(q("SELECT line_id FROM statement_line WHERE description='DEPOSIT'")[0][0])
cl.post(f"/account/{NAME}/record_ignore", data={"ignore": lid})
m = upload(FEB_MAR, period_start="2026-02-01", period_end="2026-03-31")
check("with work in it: refused, saying why and what to do", "Not uploaded" in m and "has work in it" in m
      and 'tick "Replace the open reconciliation"' in m and "Nothing was changed" in m)
check("…the reconciliation and its work are untouched", periods() == [("2026-02-01", "2026-03-31")]
      and q("SELECT status FROM writeback_log WHERE line_id=%s", (lid,)) == [("ignored",)])
m = upload(FEB_MAR, period_start="2026-02-01", period_end="2026-03-31", replace="1")
check("ticked: it's replaced", "replaces the earlier reconciliation" in m and periods() == [("2026-02-01", "2026-03-31")]
      and not q("SELECT 1 FROM statement_line WHERE line_id=%s", (lid,)))

# A statement after it never overlaps: kept alongside.
lid = str(q("SELECT line_id FROM statement_line WHERE description='DEPOSIT'")[0][0])
cl.post(f"/account/{NAME}/record_ignore", data={"ignore": lid})
upload(MAR.replace("2026-03-05", "2026-04-05"), period_start="2026-04-01", period_end="2026-04-30")
check("a later period with no overlap: uploaded, the earlier one kept",
      periods() == [("2026-02-01", "2026-03-31"), ("2026-04-01", "2026-04-30")])
sys.exit(T.summary())
