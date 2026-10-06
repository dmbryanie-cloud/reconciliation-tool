"""A statement whose dates overlap what's already here continues it instead of replacing it:
an open reconciliation takes the newer lines and runs on (its matches and work kept); after a signed-off
one, the new reconciliation starts the day after it. Replacing happens only when asked (replace=1).

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_replace.py`, or all suites with `python tests/run_all.py`.
"""
import html, io, re, sys
from decimal import Decimal as D

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
    cl.post(f"/account/{NAME}/upload", data={"statement": (io.BytesIO(body.encode()), "s.csv"), **form},
            content_type="multipart/form-data")
    m = re.search(r"<div id=flash[^>]*>(.*?)</div>", page(), re.S)
    return re.sub(r"<[^>]+>", "", m.group(1)) if m else ""
def periods():
    return [(str(a), str(b)) for a, b in q("SELECT period_start, period_end FROM statement ORDER BY period_start")]
def lines():
    return [(str(d), a) for d, a in q("""SELECT posted_date, amount FROM statement_line sl JOIN statement s USING (statement_id)
                                         ORDER BY posted_date, amount""")]

FEB = "Date,Description,Amount,Balance\n2026-02-03,CHARGE,-3450,996550\n2026-02-20,DEPOSIT,8460000,9456550\n"
FEB_MAR = FEB + "2026-03-05,CHARGE MAR,-3450,9453100\n"
TO_APR = FEB_MAR + "2026-04-07,CHARGE APR,-4000,9449100\n"

upload(FEB, period_start="2026-02-01", period_end="2026-02-28")
lid = str(q("SELECT line_id FROM statement_line WHERE description='DEPOSIT'")[0][0])
cl.post(f"/account/{NAME}/record_ignore", data={"ignore": lid})                # work on it
q("UPDATE statement SET book_balance=5, book_balance_source='qbo' RETURNING 1")

# ---- overlapping an open reconciliation: it continues -------------------------------------------------------------
m = upload(FEB_MAR, period_start="2026-02-01", period_end="2026-03-31")
check("an overlapping statement continues the open reconciliation (not replaced)",
      periods() == [("2026-02-01", "2026-03-31")] and "Added to the open reconciliation, which now runs 01/02/2026 to 31/03/2026" in m)
check("…only the newer line is added; the ones already there aren't doubled",
      lines() == [("2026-02-03", D("-3450")), ("2026-02-20", D("8460000")), ("2026-03-05", D("-3450"))]
      and "Loaded 1 statement lines" in m)
check("…the work on it is kept", q("SELECT status FROM writeback_log WHERE line_id=%s", (lid,)) == [("ignored",)]
      and q("SELECT 1 FROM statement_line WHERE line_id=%s", (lid,)))
check("…its closing balance is the file's at the new end, and the old book balance is cleared",
      q("SELECT closing_balance, closing_source, book_balance FROM statement") == [(D("9453100"), "file", None)])
m = upload(FEB, period_start="2026-02-01", period_end="2026-02-28")
check("a file with nothing newer: says so, nothing changed", "Nothing new" in m and periods() == [("2026-02-01", "2026-03-31")]
      and len(lines()) == 3)

# ---- after a signed-off one: the new reconciliation starts the day after it ----------------------------------------
q("UPDATE statement SET signed_off_at=now(), signed_off_by='x' RETURNING 1")
m = upload(TO_APR, period_start="2026-02-01", period_end="2026-04-30")
check("overlapping a signed-off reconciliation: uploaded from the day after it",
      periods() == [("2026-02-01", "2026-03-31"), ("2026-04-01", "2026-04-30")]
      and "are in the signed-off reconciliation for 01/02/2026 to 31/03/2026, so this one starts on 01/04/2026" in m)
check("…with only the lines after it; the signed-off one is untouched",
      q("""SELECT count(*) FROM statement_line sl JOIN statement s USING (statement_id) WHERE s.period_start='2026-04-01'""")[0][0] == 1
      and q("SELECT signed_off_at IS NOT NULL FROM statement WHERE period_start='2026-02-01'") == [(True,)] and len(lines()) == 4)
check("…its opening is the file's running balance before it", q("""SELECT opening_balance FROM statement
      WHERE period_start='2026-04-01'""") == [(D("9453100"),)])
q("UPDATE statement SET signed_off_at=now() WHERE period_start='2026-04-01' RETURNING 1")
m = upload(FEB_MAR, period_start="2026-02-01", period_end="2026-03-31")
check("all inside signed-off reconciliations: nothing new, nothing changed", "Nothing new" in m and len(periods()) == 2)

# ---- replacing only when asked ------------------------------------------------------------------------------------
q("UPDATE statement SET signed_off_at=NULL WHERE period_start='2026-04-01' RETURNING 1")
m = upload("Date,Description,Amount\n2026-04-09,OTHER,-1\n", period_start="2026-04-01", period_end="2026-04-30", replace="1")
check("asked to replace: the open one is replaced", "replaces the earlier reconciliation" in m
      and q("SELECT description FROM statement_line sl JOIN statement s USING (statement_id) WHERE s.period_start='2026-04-01'")
      == [("OTHER",)])
m = upload(FEB_MAR, period_start="2026-02-01", period_end="2026-03-31", replace="1")
check("…never a signed-off one", "never replaced" in m and len(periods()) == 2)
sys.exit(T.summary())
