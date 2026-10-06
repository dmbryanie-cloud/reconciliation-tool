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
A.set_config("sync_window_from", "2025-10-01")      # the books synced so far reach back to here

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
check("a foreign-currency account: asked to Sync (the Balance Sheet is in UGX), QuickBooks not asked",
      "Press Sync" in msg and len(ASKED) == n)

# ---- sync reaches back to cover every open reconciliation ----------------------------------------------------
A.SYNC_MONTHS = 12
env = A._env_since()
check("no open reconciliation: the window is SYNC_MONTHS", A._sync_since() == env)
cur.execute("""INSERT INTO statement (org_id, account_id, period_start, period_end, opening_balance, closing_balance, currency)
               VALUES (%s,%s,'2024-09-01','2025-02-01',0,0,'UGX')""", (A.ORG_ID, UGX)); c.commit()
check("an open reconciliation from 01/09/2024: sync reaches back to three months before it", A._sync_since() == "2024-06-01")
A.set_config("sync_entities", A._sync_plan(full=True)[1]); A.set_config("sync_force_full", "0")
A.set_config("last_sync_at", A.datetime.now(A.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S-00:00"))
A.set_config("sync_window_from", env)
cs, _, notes = A._sync_plan()
check("…the books only reach back to SYNC_MONTHS: the next sync is a full one, saying why",
      cs is None and any("reaching back to 2024-06-01" in x for x in notes))
A.set_config("sync_window_from", "2024-06-01")
check("…once a full sync has reached back, syncs are quick again", A._sync_plan()[0] is not None)
cur.execute("UPDATE statement SET signed_off_at=now()"); c.commit()
check("signed off: the window goes back to SYNC_MONTHS", A._sync_since() == env)
sys.exit(T.summary())
