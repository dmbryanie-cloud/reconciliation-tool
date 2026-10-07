"""The opening balance: straight after the last reconciliation it's that one's closing balance, whatever was
typed; a typed opening that isn't the statement's own balance on the start date is warned about, on upload
and on the account page, with a one-click fix.

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_opening.py`, or all suites with `python tests/run_all.py`.
"""
import html, io, re, sys
from datetime import date
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
def opening():
    return q("SELECT opening_balance, opening_source FROM statement ORDER BY period_start DESC LIMIT 1")[0]

# A statement with a running balance; the opening typed is the balance on a later day (as on KCB).
NOV = ("Date,Description,Amount,Balance\n2025-11-03,TRANSFER OUT,-14000000,16776925\n"
       "2025-11-28,OWN TRANSFER,-15000000,1776925\n2025-12-11,MASTERCARD,17831800,19608725\n")
q("INSERT INTO qbo_rec_point (account_id, as_of) VALUES (%s, '2025-12-11') RETURNING 1", (ACCT,))
m = upload(NOV, period_start="2025-11-06", period_end="2025-12-31", opening_balance="8,759,125")
check("a typed opening that isn't the statement's balance on the start date: warned on upload",
      "Check the opening balance: 8,759,125.00 was typed, but the statement's own balance going into 06/11/2025 is "
      "16,776,925.00 (a difference of -8,017,800.00)" in m)
check("…naming QuickBooks' reconciliation date when it's later", "QuickBooks is reconciled to 11/12/2025" in m
      and "starting 12/12/2025" in m)
check("…the typed one is kept until changed", opening() == (D("8759125"), "user"))
p = page()
check("…and the account page keeps warning, with a one-click fix", "id=opening-check" in p
      and "Use the statement's 16,776,925.00" in p)
form = re.search(r'<div class="recnote warn" id=opening-check>.*?</form>', p, re.S).group(0)
fields = dict(re.findall(r'name=(\w+) value="([^"]*)"', form))
cl.post(f"/account/{NAME}/balances", data={k: v for k, v in fields.items() if k != "_csrf"})
p = page()
check("the fix: the statement's opening, it adds up, the warning goes", opening() == (D("16776925"), "user")
      and "id=opening-check" not in p and "Statement adds up" in p)

# ---- straight after a signed-off reconciliation: its closing is the opening, whatever was typed ------------------------
q("UPDATE statement SET closing_balance=19608725, closing_source='file', signed_off_at=now() RETURNING 1")
JAN = "Date,Description,Amount,Balance\n2026-01-15,CHARGE,-3450,19605275\n"
m = upload(JAN, period_start="2026-01-01", period_end="2026-01-31", opening_balance="1,000")
check("the next reconciliation opens at the last one's closing balance, not the typed one",
      opening() == (D("19608725"), "carried") and "the last reconciliation's closing balance on 31/12/2025, 19,608,725.00"
      in m and "typed 1,000.00 wasn't used" in m)
p = page()
check("…the opening can't be typed over in Edit balances", re.search(r'name=opening [^>]*readonly', p) is not None)
cl.post(f"/account/{NAME}/balances", data={"period_start": "2026-01-01", "period_end": "2026-01-31", "opening": "5",
                                          "closing": "19605275", "book": ""})
check("…posting another one anyway keeps the carried balance, saying so", opening() == (D("19608725"), "carried")
      and "wasn't used" in page())
q("DELETE FROM statement WHERE period_start='2026-01-01' RETURNING 1")
m = upload(JAN.replace("2026-01-15", "2026-02-15"), period_start="2026-02-01", period_end="2026-02-28",
           opening_balance="19,608,725")
check("a gap after the last reconciliation: a typed opening is kept (the gap warning applies instead)",
      opening() == (D("19608725"), "user") and "wasn't used" not in m)
sys.exit(T.summary())
