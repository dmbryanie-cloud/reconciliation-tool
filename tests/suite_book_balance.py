"""The book balance from QuickBooks for a period older than the synced entries: read from QuickBooks'
Balance Sheet for that day, instead of refusing ("raise SYNC_MONTHS").

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_book_balance.py`, or all suites with `python tests/run_all.py`.
"""
import sys
from datetime import date
from decimal import Decimal as D

import harness as H

UGX = "00000000-0000-0000-0000-0000000000a1"
USD = "00000000-0000-0000-0000-0000000000a2"
A, c = H.setup(H.account_sql((UGX, "35", "DTB UGX 76001", "bank"), (USD, "36", "DTB USD 76002", "bank")))
cur = c.cursor()
cur.execute("UPDATE account SET currency='USD' WHERE account_id=%s", (USD,)); c.commit()
T = H.Checker()
check = T.check
A.qbo_home_currency = lambda cur: "UGX"
A._sync_since = lambda: "2025-10-01"

ASKED = []
def fake_report(token, report, params):
    ASKED.append((report, params.get("end_date")))
    sub = {"Header": {"ColData": [{"value": "Bank Accounts", "id": "20"}, {"value": ""}]},
           "Rows": {"Row": [{"ColData": [{"value": "DTB UGX 76001", "id": "35"}, {"value": "18615913.00"}], "type": "Data"},
                            {"ColData": [{"value": "Stanbic", "id": "40"}, {"value": "5.00"}], "type": "Data"}]},
           "Summary": {"ColData": [{"value": "Total Bank Accounts"}, {"value": "18615918.00"}]}, "type": "Section"}
    return {"Rows": {"Row": [{"Header": {"ColData": [{"value": "ASSETS"}, {"value": ""}]}, "Rows": {"Row": [sub]},
                              "type": "Section"}]}}
A.qbo_report = fake_report

bal = A.qbo_book_balance_at("tok", UGX, "35", date(2025, 4, 16))
check("a period before the synced entries: the balance from QuickBooks' Balance Sheet for that day",
      bal == D("18615913.00") and ASKED == [("BalanceSheet", "2025-04-16")])
check("…a parent account's total includes its sub-accounts",
      A.qbo_book_balance_at("tok", UGX, "20", date(2025, 4, 16)) == D("18615918.00"))
try:
    A.qbo_book_balance_at("tok", UGX, "99", date(2025, 4, 16)); msg = ""
except ValueError as e:
    msg = str(e)
check("an account the Balance Sheet doesn't list: says so, never a made-up 0", "doesn't list this account" in msg)
n = len(ASKED)
try:
    A.qbo_book_balance_at("tok", USD, "36", date(2025, 4, 16)); msg = ""
except ValueError as e:
    msg = str(e)
check("a foreign-currency account: asked to type it (the Balance Sheet is in UGX), QuickBooks not asked",
      "only gives the USD balance" in msg and len(ASKED) == n)
sys.exit(T.summary())
