"""Starting from QuickBooks' reconciliation: an account already reconciled in QuickBooks starts here
from that point -- its reconciled balance is the next statement's opening balance, its reconciled
entries never show as outstanding, and what QuickBooks hadn't reconciled is brought forward.

QuickBooks is fully mocked (its General Ledger report included) -- no network.

Run on its own with `python tests/suite_qbo_start.py`, or all suites with `python tests/run_all.py`.
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

A.qbo_token = lambda: "tok"
A.qbo_realm = lambda: "REAL-1"
A.qbo_is_connected = lambda: False
A.sync_full_due = lambda: False


def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
def book(tid, typ, d, amt, who):
    cur.execute("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency,
                   description, counterparty, last_modified) VALUES (%s,%s,%s,%s,%s,%s,'UGX',%s,%s,now())""",
                (A.ORG_ID, UGX, tid, typ, d, amt, who, who)); c.commit()

book("104", "Purchase", "2025-12-15", -10000, "Bank charges Dec")          # reconciled (last year)
book("101", "Deposit", "2026-05-02", 1000000, "School fees banked")         # reconciled
book("102", "Purchase", "2026-05-10", -200000, "Rent May")                  # reconciled
book("103", "Purchase", "2026-05-28", -50000, "Cheque 0451 Supplier Z")     # not reconciled: outstanding
book("105", "Purchase", "2026-06-03", -30000, "Fuel June")                  # after the starting point
RECONCILED = {"101", "102", "104"}
EXTRA = []          # reconciled rows QuickBooks has that the books here don't

CALLS = []
def fake_report(token, report, params):
    CALLS.append((report, dict(params)))
    s, e = date.fromisoformat(params["start_date"]), date.fromisoformat(params["end_date"])
    cur.execute("""SELECT source_txn_id, source_txn_type, posted_date FROM book_txn
                   WHERE account_id=%s AND posted_date BETWEEN %s AND %s ORDER BY posted_date""", (UGX, s, e))
    data = [{"ColData": [{"value": "Beginning Balance"}, {"value": ""}, {"value": ""}], "type": "Data"}]
    for tid, typ, d in cur.fetchall() + [x for x in EXTRA if s <= x[2] <= e]:
        data.append({"type": "Data", "ColData": [{"value": d.isoformat()}, {"value": typ, "id": tid},
                                                 {"value": "R" if tid in RECONCILED or tid in {x[0] for x in EXTRA} else ""}]})
    c.commit()
    col = lambda t, k: {"ColTitle": t, "ColType": "String", "MetaData": [{"Name": "ColKey", "Value": k}]}
    return {"Header": {"ReportName": "GeneralLedger"},
            "Columns": {"Column": [col("Date", "tx_date"), col("Transaction Type", "txn_type"), col("Cleared", "is_cleared")]},
            "Rows": {"Row": [{"type": "Section", "Header": {"ColData": [{"value": "Stanbic UGX"}]},
                              "Rows": {"Row": data}, "Summary": {"ColData": [{"value": "Total"}]}}]}}
A.qbo_report = fake_report

cl = H.login(A)
def page(path="/account/Stanbic UGX"):
    return cl.get(path).data.decode()
def msg():
    m = re.search(r"<div id=flash[^>]*>(.*?)</div>", page(), re.S)
    return html.unescape(re.sub(r"<[^>]+>", "", m.group(1))) if m else ""
def start(as_of, balance=""):
    return cl.post("/account/Stanbic UGX/qbo_start", data={"as_of": as_of, "balance": balance})
def upload(rows, ps, pe, opening=""):
    body = "Date,Description,Amount\n" + "".join(f"{d},{t},{a}\n" for d, t, a in rows)
    return cl.post("/account/Stanbic UGX/upload", data={"statement": (io.BytesIO(body.encode()), "s.csv"),
                   "opening_balance": opening, "closing_balance": "710000", "period_start": ps, "period_end": pe},
                   content_type="multipart/form-data")

# ---- the menu ----------------------------------------------------------------------------------------
check("no starting point menu while QuickBooks isn't connected", "QuickBooks starting point" not in page())
A.qbo_is_connected = lambda: True
p = page()
check("connected: admin menu offers it, with its side panel", "data-drawer=qbostart" in p and "id=dr-qbostart" in p
      and 'name=as_of' in p)
A.qbo_is_connected = lambda: False

# ---- refused --------------------------------------------------------------------------------------
start("2026-05-31", "800,000")
m = msg()
check("statement balance typed that doesn't agree: refused, with the difference",
      "add up to 790,000.00" in m and "800,000.00" in m and "10,000.00" in m)
check("…nothing saved", not q("SELECT 1 FROM qbo_baseline") and not q("SELECT 1 FROM qbo_reconciled"))
check("…QuickBooks' General Ledger read for this account, one year at a time, with the Cleared column",
      [c_[1]["start_date"] for c_ in CALLS[:2]] == ["2025-12-15", "2026-01-01"]
      and all(c_[0] == "GeneralLedger" and c_[1]["account"] == "35" and "is_cleared" in c_[1]["columns"] for c_ in CALLS))
A.sync_full_due = lambda: True
start("2026-05-31")
check("books due a full refresh: refused", "full refresh" in msg() and not q("SELECT 1 FROM qbo_baseline"))
A.sync_full_due = lambda: False
start("2030-01-01")
check("a future date: refused", "past date" in msg())

# ---- set ----------------------------------------------------------------------------------------------
start("2026-05-31", "790,000")
m = msg()
check("set: says the date, balance and reconciled count", "reconciled to 31/05/2026 at 790,000.00" in m and "3 reconciled" in m)
check("…and what's brought forward", "1 entry up to then, totalling -50,000.00" in m and "Upload statements from 01/06/2026" in m)
b = q("SELECT as_of, balance, n_rec, n_missing FROM qbo_baseline")
check("…saved", b == [(date(2026, 5, 31), D("790000"), 3, 0)])
check("…the reconciled entries listed", {r[0] for r in q("SELECT source_txn_id FROM qbo_reconciled")} == RECONCILED)
p = page()
check("header shows QuickBooks' figure beside ReconBook's", "QuickBooks reconciled to <b>31/05/2026</b> at <b>790,000.00</b>" in p
      and "Reconciled to <b>31/05/2026</b>" in p)
check("dashboard shows it too", "QuickBooks: to 31/05/2026 at 790,000.00" in page("/"))
check("activity log records who did it", q("SELECT 1 FROM activity_log WHERE action LIKE 'Started from QuickBooks%%'"))

# ---- statements after it -----------------------------------------------------------------------------
r = upload([("2026-05-20", "RENT", -200000)], "2026-05-01", "2026-05-31")
check("a statement on or before the starting point: refused", "starts from QuickBooks' reconciliation to 31/05/2026" in msg()
      and not q("SELECT 1 FROM statement"))
upload([("2026-06-04", "CHQ 0451 SUPPLIER Z", -50000), ("2026-06-05", "FUEL", -30000)], "2026-06-01", "2026-06-30")
st = q("SELECT opening_balance, opening_source FROM statement")
check("the next statement opens at QuickBooks' reconciled balance", st == [(D("790000"), "carried")])
k = c.cursor(); s_ = A._latest_statement(k, UGX); rec = A.reconcile(k, UGX, s_); c.rollback()
pool = {t[3] for t in rec["pool"]}
check("reconciled entries aren't outstanding", not pool & {"School fees banked", "Rent May", "Bank charges Dec"})
check("the unreconciled cheque is brought forward, with June's entries", {"Cheque 0451 Supplier Z", "Fuel June"} <= pool
      and rec["bf_count"] + sum(1 for t in rec["pool"] if t[3] == "Cheque 0451 Supplier Z" and t[0] in rec["mt"]) >= 1)
check("no 'opening doesn't match the last closing' warning", "doesn't match the last signed-off closing" not in page())
start("2026-06-30")
check("moving the starting point past a reconciliation here: refused", "already has a reconciliation" in msg()
      and q("SELECT as_of FROM qbo_baseline") == [(date(2026, 5, 31),)])

# ---- missing entries and removal ----------------------------------------------------------------------
q("DELETE FROM statement RETURNING 1")
EXTRA.append(("999", "Deposit", date(2026, 4, 1)))
start("2026-05-31")
check("an entry QuickBooks reconciled that isn't in the books here: counted and flagged",
      "1 entry QuickBooks reconciled" in msg() and q("SELECT n_missing FROM qbo_baseline") == [(1,)])
EXTRA.clear()
cl.post("/account/Stanbic UGX/qbo_start", data={"remove": "1"})
check("remove: starting point and its list gone", "Removed the starting point" in msg()
      and not q("SELECT 1 FROM qbo_baseline") and not q("SELECT 1 FROM qbo_reconciled"))
k = c.cursor(); check("…the account isn't reconciled to anything any more", A.reconciled_to(k, UGX) is None); c.rollback()
RECONCILED.clear()
start("2026-05-31")
check("nothing reconciled in QuickBooks: refused", "no reconciled entries" in msg() and not q("SELECT 1 FROM qbo_baseline"))

# ---- permissions -------------------------------------------------------------------------------------
check("only admins can set it", A.PERM_BY_ENDPOINT.get("qbo_start") == "users")
sys.exit(T.summary())
