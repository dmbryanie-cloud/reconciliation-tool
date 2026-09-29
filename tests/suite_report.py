"""Printable reconciliation report (current and past periods) and keeping currencies separate.

Run on its own with `python tests/suite_report.py`, or all suites with `python tests/run_all.py`.
"""
import io, re, sys
from decimal import Decimal as D

import harness as H

UGX, UGX2, USD = ("00000000-0000-0000-0000-0000000000a1", "00000000-0000-0000-0000-0000000000a2",
                  "00000000-0000-0000-0000-0000000000a4")
A, c = H.setup(H.account_sql((UGX, "35", "Stanbic UGX", "bank"), (UGX2, "36", "Centenary UGX", "bank"),
                             (USD, "37", "Stanbic USD", "bank")))
cur = c.cursor()
cur.execute("UPDATE account SET currency='USD' WHERE account_id=%s", (USD,))
c.commit()
T = H.Checker()
check = T.check
A.qbo_is_connected = lambda: False


def book(acct, tid, d, amt, who):
    cur.execute("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency,
                   description, counterparty, last_modified) VALUES (%s,%s,%s,'Purchase',%s,%s,'UGX',%s,%s,now())""",
                (A.ORG_ID, acct, tid, d, amt, who, who)); c.commit()

def q1(sql, args=()):
    cur.execute(sql, args); r = cur.fetchone(); c.rollback(); return r

def text(html):
    """Visible text of the report, whitespace-collapsed, for simple assertions."""
    t = re.sub(r"<style.*?</style>|<script.*?</script>", " ", html, flags=re.S)
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", t)).replace("&amp;", "&")

cl = H.login(A)
def upload(acct_name, body, **kw):
    r = cl.post(f"/account/{acct_name}/upload", data={"statement": (io.BytesIO(body.encode()), "s.csv"), **kw},
                content_type="multipart/form-data")
    assert r.status_code == 302, r.data[:300]

for tid, d, amt, who in (("b1", "2026-01-03", 500000, "Customer A"), ("b2", "2026-01-09", -200000, "Supplier cheque 1"),
                         ("b3", "2026-01-28", -150000, "Supplier cheque 2"), ("b4", "2026-01-30", 80000, "Customer B"),
                         ("b5", "2026-02-10", -30000, "Fuel")):
    book(UGX, tid, d, amt, who)
A.set_config("company_name", "Northgreen Test Co")

upload("Stanbic UGX", """Date,Description,Debit,Credit,Balance
10 Jan 2026,Supplier cheque 1,200000,,1295000
05 Jan 2026,Bank charge,2500,,1495000
05 Jan 2026,Bank charge,2500,,1497500
03 Jan 2026,Customer A,,500000,1500000
""", period_start="2026-01-01", period_end="2026-01-31")
jan_id = str(q1("SELECT statement_id FROM statement")[0])

# ---- currencies stay separate ----------------------------------------------------------------
book(UGX2, "c1", "2026-01-05", -2500, "Bank charge (Centenary)")
book(USD, "u1", "2026-01-05", -2500, "Bank charge (USD)")
k = c.cursor(); d = A.compute_detail(k, UGX, "bank", "35"); c.rollback()
flagged = {x["account"] for v in d["xfers"].values() for x in v}
check("transfer check still compares same-currency banks", "Centenary UGX" in flagged)
check("…but never a different currency (same number, different money)", "Stanbic USD" not in flagged)

cur.execute("""INSERT INTO payee_correction (org_id, payee, category, money_out, currency) VALUES
               (%s,'BANK CHARGES MONTHLY','Bank Charges:UGX',true,'UGX'),
               (%s,'BANK CHARGES MONTHLY','Bank Charges:USD',true,'USD')""", (A.ORG_ID, A.ORG_ID))
c.commit()
k = c.cursor()
s_ugx = A.PostingMemory(k, "UGX").suggest("BANK CHARGES MONTHLY FEB", True)
s_usd = A.PostingMemory(k, "USD").suggest("BANK CHARGES MONTHLY FEB", True)
c.rollback()
check("UGX line learns from UGX postings", s_ugx and s_ugx["cat"] == "Bank Charges:UGX")
check("USD line learns from USD postings", s_usd and s_usd["cat"] == "Bank Charges:USD")
cur.execute("""INSERT INTO qbo_coa (qbo_id, name, fqn, account_type, active) VALUES
               ('60','UGX','Bank Charges:UGX','Expense',true), ('61','USD','Bank Charges:USD','Expense',true)""")
c.commit()
upload("Stanbic USD", "Date,Description,Amount\n2026-01-31,BANK CHARGES MONTHLY JAN,-25\n",
       period_start="2026-01-01", period_end="2026-01-31", closing_balance="-25")
k = c.cursor(); dd = A.compute_detail(k, USD, "bank", "37"); c.rollback()
w = dd["writebacks"][0]
check("USD account page suggests the USD charges account", w["sug"]["cat"] == "Bank Charges:USD" and w["acct_id"] == "61")

# ---- the report: draft, then signed ------------------------------------------------------------
cl.post("/account/Stanbic UGX/balances", data={"opening": "1000000", "closing": "1295000", "book": "1230000"})
r = cl.get("/account/Stanbic UGX/report")
t = text(r.data.decode())
check("report renders", r.status_code == 200 and "Bank reconciliation statement" in t)
check("letterhead + account + currency + period", "Northgreen Test Co" in t and "Stanbic UGX" in t and "UGX" in t
      and "2026-01-01 to 2026-01-31" in t)
check("draft watermark before sign-off", "DRAFT" in t and "Draft — not signed off" in t)
check("statement balance", "Balance per bank statement at 2026-01-31" in t and "1,295,000.00" in t)
check("deposit in transit listed individually", re.search(r"Customer B 2026-01-30 80,000\.00", t) is not None)
check("outstanding cheque listed, in brackets", re.search(r"Supplier cheque 2 2026-01-28 \(150,000\.00\)", t) is not None)
check("adjusted bank balance", re.search(r"Adjusted bank balance 1,225,000\.00", t) is not None)
check("book balance + both unrecorded bank charges", "1,230,000.00" in t and len(re.findall(r"Bank charge 2026-01-05 \(2,500\.00\)", t)) == 2)
check("adjusted book balance", re.search(r"Adjusted book balance 1,225,000\.00", t) is not None)
check("reconciled result", "Reconciled — adjusted bank and book balances agree" in t and "Difference 0.00" in t)
check("statement check line", "opening balance 1,000,000.00 + movements 295,000.00 = closing balance 1,295,000.00" in t)
check("match counts", "4 statement lines: 2 matched automatically, 0 confirmed suggestions, 0 matched by hand, 2 not in the books" in t)
check("print button + print styles", "window.print()" in r.data.decode() and "@media print" in r.data.decode())

cl.post("/account/Stanbic UGX/signoff")
t = text(cl.get("/account/Stanbic UGX/report").data.decode())
check("signed-off report: no watermark, signer named", "DRAFT" not in t and re.search(r"Admin, \d\d \w{3} 2026", t) is not None)

page = cl.get("/account/Stanbic UGX").data.decode()
check("account page links to the report", "/account/Stanbic%20UGX/report" in page and "Print reconciliation report" in page)

# ---- February: out of balance, signed with override --------------------------------------
upload("Stanbic UGX", "Date,Description,Amount\n02/02/2026,Customer B,80000\n04/02/2026,Supplier cheque 2,-150000\n10/02/2026,Fuel,-30000\n",
       closing_balance="1195000", period_end="2026-02-28")
cl.post("/account/Stanbic UGX/review_all")
cl.post("/account/Stanbic UGX/balances", data={"opening": "", "closing": "1195000", "book": "1200000"})
t = text(cl.get("/account/Stanbic UGX/report").data.decode())
check("current report is February, out of balance", "2026-02-01 to 2026-02-28" in t and "Not reconciled — out of balance" in t
      and "Difference (5,000.00)" in t)
check("confirmed suggestion counted", "1 confirmed suggestion" in t)
cl.post("/account/Stanbic UGX/signoff", data={"override": "1", "note": "charges booked in March"})
t = text(cl.get("/account/Stanbic UGX/report").data.decode())
check("override reason printed", "Signed off while not reconciled. Reason given: charges booked in March" in t)

# ---- past period still shows what was outstanding at ITS end -------------------------------
t = text(cl.get(f"/account/Stanbic UGX/report?s={jan_id}").data.decode())
check("January report after February cleared its items: still lists them",
      "2026-01-01 to 2026-01-31" in t and "Customer B 2026-01-30" in t and "Supplier cheque 2 2026-01-28" in t)
check("…and still reconciles", re.search(r"Adjusted bank balance 1,225,000\.00", t) is not None and "Difference 0.00" in t)
k = c.cursor(); s_feb = A._latest_statement(k, UGX); r_feb = A.reconcile(k, UGX, s_feb); c.rollback()
check("February's pool unaffected (Jan-cleared items excluded, Jan outstanding items included)",
      {str(x[3]) for x in r_feb["pool"]} == {"Supplier cheque 2", "Customer B", "Fuel"})

cur.execute("UPDATE statement SET book_balance=1231000 WHERE statement_id=%s", (jan_id,)); c.commit()
t = text(cl.get(f"/account/Stanbic UGX/report?s={jan_id}").data.decode())
check("report notes when data changed since sign-off", "Recalculated from current data" in t and "difference was 0.00" in t)
cur.execute("UPDATE statement SET book_balance=1230000 WHERE statement_id=%s", (jan_id,)); c.commit()

hist = cl.get("/account/Stanbic UGX/history").data.decode()
check("history links each period's report", f"report?s={jan_id}" in hist and hist.count("report?s=") == 2)

# ---- differences on matched items are itemised ---------------------------------------------
cur.execute("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency,
               description, counterparty, last_modified) VALUES (%s,%s,'u9','Purchase','2026-01-30',-24,'USD','Fee','Fee',now())
               RETURNING txn_id""", (A.ORG_ID, USD))
tid = str(cur.fetchone()[0]); c.commit()
lid = str(q1("SELECT line_id FROM statement_line sl JOIN statement s USING (statement_id) WHERE s.account_id=%s", (USD,))[0])
cl.post("/account/Stanbic USD/match", data={"ml": [lid], "mb": [tid]})
t = text(cl.get("/account/Stanbic USD/report").data.decode())
check("USD report in USD with the match difference itemised", "Amounts in USD" in t
      and re.search(r"BANK CHARGES MONTHLY JAN — books: Fee \(24\.00\) 2026-01-31 \(1\.00\)", t) is not None
      and "1 matched by hand" in t)

# ---- access ----------------------------------------------------------------------------------
check("another account's statement id is refused", cl.get(f"/account/Stanbic USD/report?s={jan_id}").status_code == 404)
check("garbage id refused", cl.get("/account/Stanbic UGX/report?s=nope").status_code == 404)
check("unknown account", cl.get("/account/Nope/report").status_code == 404)
fresh = A.app.test_client()
check("login required", fresh.get("/account/Stanbic UGX/report").status_code == 302)
sys.exit(T.summary())
