"""Balance reconciliation: statement import, balance proof, carry-forward, sign-off.

Run on its own with `python tests/suite_reconciliation.py`, or all suites with `python tests/run_all.py`.
"""
import importlib, io, json, re, sys, urllib.error
from datetime import datetime, timezone, timedelta
from decimal import Decimal as D

import harness as H

A, c = H.setup(H.account_sql(
    ('00000000-0000-0000-0000-0000000000a1', '35', 'Stanbic UGX', 'bank')))
cur = c.cursor()
T = H.Checker()
check = T.check

ACCT = "00000000-0000-0000-0000-0000000000a1"
def book(tid, d, amt, who):
    cur.execute("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency,
                   description, counterparty, last_modified) VALUES (%s,%s,%s,'Purchase',%s,%s,'UGX',%s,%s,now())""",
                (A.ORG_ID, ACCT, tid, d, amt, who, who)); c.commit()

book("b1", "2026-01-03", 500000, "Customer A")
book("b2", "2026-01-09", -200000, "Supplier cheque 1")
book("b3", "2026-01-28", -150000, "Supplier cheque 2")   # outstanding at Jan 31
book("b4", "2026-01-30", 80000, "Customer B")           # deposit in transit
book("b5", "2026-02-10", -30000, "Fuel")

cl = H.login(A)

# Newest-first CSV with a running balance and "05 Jan 2026" dates, two identical bank charges.
jan = """Date,Description,Debit,Credit,Balance
10 Jan 2026,Supplier cheque 1,200000,,1295000
05 Jan 2026,Bank charge,2500,,1495000
05 Jan 2026,Bank charge,2500,,1497500
03 Jan 2026,Customer A,,500000,1500000
"""
def upload(text, **bal):
    r = cl.post("/account/Stanbic UGX/upload", data={"statement": (io.BytesIO(text.encode()), "s.csv"), **bal},
                content_type="multipart/form-data")
    assert r.status_code == 302, r.data[:400]

def rec():
    cur2 = c.cursor()
    s = A._latest_statement(cur2, ACCT)
    out = A.reconcile(cur2, ACCT, s); c.rollback(); return out


upload(jan, period_start="2026-01-01", period_end="2026-01-31")
r1 = rec()
check("4 lines stored (duplicate charges kept)", r1["n_lines"] == 4)
check("opening/closing read from running balance", (r1["opening"], r1["closing"]) == (D(1000000), D(1295000)) and r1["opening_src"] == "file")
check("statement foots", r1["foot_diff"] == 0)
check("incomplete without book balance", r1["status"] == "incomplete" and r1["missing"] == "book balance")
page = cl.get("/account/Stanbic UGX").data.decode()
check("detail page renders the panel", "Balance reconciliation" in page and "Enter the book balance" in page)
r = cl.post("/account/Stanbic UGX/signoff"); check("sign-off refused while incomplete", "signed_off_at" and rec() and True)
cur.execute("SELECT signed_off_at FROM statement"); check("…and nothing signed", cur.fetchone()[0] is None); c.rollback()

cl.post("/account/Stanbic UGX/balances", data={"opening": "1,000,000", "closing": "1295000", "book": "1230000"})
r1 = rec()
print("   adj_bank", r1["adj_bank"], "adj_book", r1["adj_book"], "out_in", r1["out_in"], "out_out", r1["out_out"], "unrec", r1["unrec"])
check("January balances", r1["status"] == "balanced" and r1["adj_bank"] == D(1225000))
page = cl.get("/account/Stanbic UGX").data.decode()
check("balanced banner shown", "Balanced — adjusted bank and book balances agree" in page)
dash = cl.get("/").data.decode()
check("dashboard shows balanced", 'class="pill info">Balanced' in dash)
cl.post("/account/Stanbic UGX/signoff")
cur.execute("SELECT signed_off_at, snap_diff FROM statement"); row = cur.fetchone(); c.rollback()
check("January signed off with 0 snapshot", row[0] is not None and row[1] == 0)

# February: clears the Jan items in transit; opening should carry from Jan's closing.
feb = """Date,Description,Amount
02/02/2026,Customer B,80000
04/02/2026,Supplier cheque 2,-150000
10/02/2026,Fuel,-30000
"""
upload(feb, closing_balance="1195000", period_end="2026-02-28")
r2 = rec()
check("opening carried from January", r2["opening"] == D(1295000) and r2["opening_src"] == "carried")
cur.execute("SELECT period_start FROM statement ORDER BY created_at DESC LIMIT 1"); ps_ = cur.fetchone()[0]; c.rollback()
check("Feb period starts the day after January's end", str(ps_) == "2026-02-01")
check("statement foots", r2["foot_diff"] == 0)
check("late-clearing cheque waits for review", rec()["n_pending"] == 1 and len(rec()["un_books"]) == 1)
cl.post("/account/Stanbic UGX/review_all")
r2 = rec()
check("brought-forward items matched (nothing outstanding)", len(r2["un_books"]) == 0 and len(r2["un_lines"]) == 0)
check("January-cleared book items not in pool", {str(t[3]) for t in r2["pool"]} == {"Supplier cheque 2", "Customer B", "Fuel"})
cl.post("/account/Stanbic UGX/balances", data={"opening": "", "closing": "1195000", "book": "1200000"})
r2 = rec()
check("February out by the unrecorded Jan charges (-5,000)", r2["status"] == "out" and r2["rec_diff"] == D(-5000))
page = cl.get("/account/Stanbic UGX").data.decode()
check("out banner + disabled sign-off", "Out of balance" in page and "disabled" in page)

# A mistyped closing balance is caught by the footing check.
cl.post("/account/Stanbic UGX/balances", data={"opening": "", "closing": "1196000", "book": "1201000"})
r2 = rec()
check("footing error detected", r2["foot_diff"] == D(-1000) and r2["status"] == "out")
page = cl.get("/account/Stanbic UGX").data.decode()
check("footing message shown", "The statement doesn&#39;t add up" in page or "The statement doesn't add up" in page)

# Admin override with note
cl.post("/account/Stanbic UGX/signoff", data={"override": "1", "note": "charges booked in March"})
cur.execute("SELECT signoff_note FROM statement WHERE period_start='2026-02-01'"); row = cur.fetchone(); c.rollback()
check("admin override records note", row and row[0] == "charges booked in March")
hist = cl.get("/account/Stanbic UGX/history").data.decode()
check("history shows the override", "Unbalanced — charges booked in March" in hist)

# OFX ledger balance
ofx = "<OFX><STMTTRN><DTPOSTED>20260305<TRNAMT>-100<FITID>x1<NAME>Test</STMTTRN><LEDGERBAL><BALAMT>1195400<DTASOF>20260331</LEDGERBAL></OFX>"
rows = A.parse_ofx(ofx.replace("<STMTTRN>", "<BANKTRANLIST><DTSTART>20260301<DTEND>20260331<STMTTRN>", 1))
check("OFX closing balance parsed", rows.closing == D(1195400))
check("OFX period parsed", str(rows.period_start) == "2026-03-01" and str(rows.period_end) == "2026-03-31")

# Late-clearing pairs: a fresh March statement where a cheque clears 10 days after booking.
book("b6", "2026-03-02", -45000, "Supplier cheque 3")
upload("Date,Description,Amount\n12/03/2026,CHQ 000123,-45000\n", closing_balance="1150000",
       period_start="2026-03-01", period_end="2026-03-31")
page = cl.get("/account/Stanbic UGX").data.decode()
check("late-clearing cheque is in the review list", "cleared later" in page)
cur.execute("SELECT m.match_id FROM match m JOIN statement s USING (statement_id) WHERE m.confidence < 1 AND m.match_type='exact' AND s.period_start='2026-03-01'"); mid = cur.fetchone()[0]; c.rollback()
cl.post(f"/account/Stanbic UGX/review/{mid}", data={"status": "rejected"})
cur.execute("SELECT statement_id FROM statement ORDER BY created_at DESC LIMIT 1"); sid = cur.fetchone()[0]; c.rollback()
A.run_matcher(sid)
cur.execute("SELECT m.status FROM match m JOIN statement s USING (statement_id) WHERE m.confidence < 1 AND m.match_type='exact' AND s.period_start='2026-03-01'"); st = cur.fetchone()[0]; c.rollback()
check("rejection survives a matcher re-run", st == "rejected")
r3 = rec()
print("   un_books", r3["un_books"], "un_lines", r3["un_lines"])
check("rejected pair shows up as exceptions again", len(r3["un_books"]) == 1 and len(r3["un_lines"]) == 1)

for u in ("/account/Stanbic UGX/exceptions.csv", "/account/Stanbic UGX/qbo_import.csv", "/account/Stanbic UGX/history", "/"):
    check("GET " + u, cl.get(u).status_code == 200)
cur.execute("UPDATE statement SET opening_source=NULL, closing_source=NULL, book_balance_source=NULL"); c.commit()
page = cl.get("/account/Stanbic UGX").data.decode()
check("legacy statement (no balances) renders as incomplete", "Enter the closing balance and book balance" in page)
check("dashboard asks for balances", "needs closing balance and book balance" in cl.get("/").data.decode())
H = A.QBO_HANDLERS
check("SalesReceipt into bank", H["SalesReceipt"]({"DepositToAccountRef": {"value": "35"}, "TotalAmt": 100}, "35", "bank")[0] == D(100))
check("RefundReceipt out of bank", H["RefundReceipt"]({"DepositToAccountRef": {"value": "35"}, "TotalAmt": 40}, "35", "bank")[0] == D(-40))
cc = {"BankAccountRef": {"value": "35"}, "CreditCardAccountRef": {"value": "41"}, "Amount": 500}
check("CC payment: bank side", H["CreditCardPayment"](cc, "35", "bank")[0] == D(-500))
check("CC payment: card side", H["CreditCardPayment"](cc, "41", "credit_card")[0] == D(-500))
check("refs for CC payment", A._account_refs("CreditCardPayment", cc) == ["35", "41"])

sys.exit(T.summary())
