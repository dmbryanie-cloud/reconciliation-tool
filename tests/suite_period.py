"""The reconciliation period can be part of the uploaded file: a statement covering several months is
reconciled for just the dates chosen. Only those lines are kept, and the opening and closing come
from the file's own running balance on those days.

QuickBooks isn't used. Run on its own with `python tests/suite_period.py`, or all suites with
`python tests/run_all.py`.
"""
import html, io, re, sys
from datetime import date
from decimal import Decimal as D

import harness as H

UGX = "00000000-0000-0000-0000-0000000000a1"
A, c = H.setup(H.account_sql((UGX, "35", "Stanbic UGX", "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
A.qbo_is_connected = lambda: False


def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r

cl = H.login(A)
def msg():
    m = re.search(r"<div id=flash[^>]*>(.*?)</div>", cl.get("/account/Stanbic UGX").data.decode(), re.S)
    return html.unescape(re.sub(r"<[^>]+>", "", m.group(1))) if m else ""
WITH_BAL = [("2026-05-10", "RENT MAY", -100000, 900000), ("2026-06-05", "FUEL", -50000, 850000),
            ("2026-06-20", "FEES BANKED", 200000, 1050000), ("2026-07-03", "WATER BILL", -25000, 1025000)]
def upload(rows, ps="", pe="", opening="", closing="", balance=True):
    if balance:
        body = "Date,Description,Amount,Balance\n" + "".join(f"{d},{t},{a},{b}\n" for d, t, a, b in rows)
    else:
        body = "Date,Description,Amount\n" + "".join(f"{d},{t},{a}\n" for d, t, a, *_ in rows)
    r = cl.post("/account/Stanbic UGX/upload", data={"replace": "1", "statement": (io.BytesIO(body.encode()), "s.csv"), "opening_balance": opening,
                "closing_balance": closing, "period_start": ps, "period_end": pe}, content_type="multipart/form-data")
    assert r.status_code == 302, r.data[:300]
    return msg()
def stmt():
    return q("""SELECT period_start, period_end, opening_balance, opening_source, closing_balance, closing_source,
                       (SELECT array_agg(description ORDER BY posted_date) FROM statement_line sl WHERE sl.statement_id=s.statement_id)
                FROM statement s ORDER BY created_at DESC LIMIT 1""")[0]

# ---- the panel ------------------------------------------------------------------------------------
p = cl.get("/account/Stanbic UGX").data.decode()
check("upload panel asks which dates to reconcile", "Reconcile from" in p and "Reconcile to (statement date)" in p
      and "reconciled one month at a time" in p)

# ---- one month out of three --------------------------------------------------------------------------
m = upload(WITH_BAL, "2026-06-01", "2026-06-30")
ps, pe, ob, osrc, cb, csrc, lines = stmt()
check("only June's lines kept", lines == ["FUEL", "FEES BANKED"] and (ps, pe) == (date(2026, 6, 1), date(2026, 6, 30)))
check("opening is the file's balance at the start of June", ob == D("900000") and osrc == "file")
check("closing is the file's balance at the end of June", cb == D("1050000") and csrc == "file")
check("message counts the lines kept and says what was left out", "Loaded 2 statement lines" in m
      and "Only 01/06/2026 to 30/06/2026 is reconciled: 1 before 01/06/2026 and 1 after 30/06/2026 in the file were left out" in m)
k = c.cursor(); rec = A.reconcile(k, UGX, A._latest_statement(k, UGX)); c.rollback()
check("the month adds up", rec["foot_diff"] == 0)

# ---- typed balances win ----------------------------------------------------------------------------
upload(WITH_BAL, "2026-06-01", "2026-06-30", opening="900,000", closing="1,050,000")
ps, pe, ob, osrc, cb, csrc, lines = stmt()
check("typed balances are kept as typed", (ob, osrc, cb, csrc) == (D("900000"), "user", D("1050000"), "user"))
check("…and the June statement replaced, not doubled", len(q("SELECT 1 FROM statement")) == 1)

# ---- only one end given ------------------------------------------------------------------------------
m = upload(WITH_BAL, "2026-07-01")
ps, pe, ob, osrc, cb, csrc, lines = stmt()
check("from a date to the end of the file", lines == ["WATER BILL"] and ps == date(2026, 7, 1) and pe == date(2026, 7, 3)
      and ob == D("1050000") and cb == D("1025000"))
check("…3 earlier lines left out", "3 before 01/07/2026" in m and "after" not in m.split("left out")[0].split("reconciled:")[1])
upload(WITH_BAL, "", "2026-05-31")
ps, pe, ob, osrc, cb, csrc, lines = stmt()
check("from the file's start to a date", lines == ["RENT MAY"] and pe == date(2026, 5, 31) and ob == D("1000000") and cb == D("900000"))

# ---- no running balance in the file ------------------------------------------------------------------
q("DELETE FROM statement RETURNING 1")
upload(WITH_BAL, "2026-06-01", "2026-06-30", closing="1,050,000", balance=False)
ps, pe, ob, osrc, cb, csrc, lines = stmt()
check("no balance column: June kept, typed closing used", lines == ["FUEL", "FEES BANKED"] and cb == D("1050000")
      and csrc == "user" and osrc == "derived" and ob == D("900000"))

# ---- refused -----------------------------------------------------------------------------------------
n = len(q("SELECT 1 FROM statement"))
m = upload(WITH_BAL, "2026-08-01", "2026-08-31")
check("a period with none of the file's lines: refused", "no transactions from 01/08/2026 to 31/08/2026" in m
      and "runs from 10/05/2026 to 03/07/2026" in m and len(q("SELECT 1 FROM statement")) == n)
m = upload(WITH_BAL, "2026-06-30", "2026-06-01")
check("a period that ends before it starts: refused", "starts (30/06/2026) after it ends (01/06/2026)" in m
      and len(q("SELECT 1 FROM statement")) == n)

# ---- the whole file, as before ------------------------------------------------------------------------
m = upload(WITH_BAL, "2026-05-01", "2026-07-31")
ps, pe, ob, osrc, cb, csrc, lines = stmt()
check("a period covering the whole file keeps every line, nothing said about leaving out", len(lines) == 4
      and "left out" not in m and ob == D("1000000") and cb == D("1025000"))
sys.exit(T.summary())
