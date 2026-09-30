"""Recording transfers between your own accounts: from the record table (one line), as one
transfer for a pair seen on two statements, card payments from a bank, same currency only.

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_transfers.py`, or all suites with `python tests/run_all.py`.
"""
import io, json, re, sys
from decimal import Decimal as D

import harness as H

UGX, CEN, USD, VISA = ("00000000-0000-0000-0000-0000000000a1", "00000000-0000-0000-0000-0000000000a2",
                       "00000000-0000-0000-0000-0000000000a4", "00000000-0000-0000-0000-0000000000a3")
A, c = H.setup(H.account_sql((UGX, "35", "Stanbic UGX", "bank"), (CEN, "36", "Centenary UGX", "bank"),
                             (USD, "37", "Stanbic USD", "bank"), (VISA, "41", "Visa", "credit_card")))
cur = c.cursor()
cur.execute("UPDATE account SET currency='USD' WHERE account_id=%s", (USD,)); c.commit()
T = H.Checker()
check = T.check

COA = [{"Id": "35", "Name": "Stanbic UGX", "AccountType": "Bank", "CurrencyRef": {"value": "UGX"}},
       {"Id": "36", "Name": "Centenary UGX", "AccountType": "Bank", "CurrencyRef": {"value": "UGX"}},
       {"Id": "37", "Name": "Stanbic USD", "AccountType": "Bank", "CurrencyRef": {"value": "USD"}},
       {"Id": "41", "Name": "Visa", "AccountType": "Credit Card", "CurrencyRef": {"value": "UGX"}},
       {"Id": "83", "Name": "Office Supplies", "AccountType": "Expense", "CurrencyRef": {"value": "UGX"}},
       {"Id": "90", "Name": "Sales", "AccountType": "Income", "CurrencyRef": {"value": "UGX"}},
       {"Id": "95", "Name": "USD Loan", "AccountType": "Other Current Liability", "CurrencyRef": {"value": "USD"}},
       {"Id": "96", "Name": "EUR Accruals", "AccountType": "Other Current Liability", "CurrencyRef": {"value": "EUR"}}]
A._store_coa(COA)
POSTS, QBO = [], []
def fake_post(token, entity, body):
    POSTS.append((entity, json.loads(json.dumps(body))))
    new = {**body, "Id": str(800 + len(POSTS))}
    if entity == "Transfer":
        QBO.append(new)
    return {entity: {"Id": new["Id"]}}
A.qbo_post = fake_post
A.qbo_token = lambda: "tok"
A.qbo_realm = lambda: "REAL-1"
A.qbo_is_connected = lambda: False

def q1(sql, args=()):
    cur.execute(sql, args); r = cur.fetchone(); c.commit(); return r
def lid(desc):
    return str(q1("SELECT line_id FROM statement_line WHERE description=%s", (desc,))[0])
def matched(line):
    return q1("""SELECT m.status, m.created_by FROM match m JOIN match_statement_line msl USING (match_id)
                 WHERE msl.line_id=%s AND m.status <> 'rejected'""", (line,))
def book(acct):
    cur.execute("SELECT source_txn_id, posted_date::text, amount, counterparty, category FROM book_txn "
                "WHERE account_id=%s AND source_txn_type='Transfer' ORDER BY posted_date, source_txn_id", (acct,))
    r = cur.fetchall(); c.commit(); return r
def coa_json(page):
    return json.loads(re.search(r"<script id=coa-data type=application/json>(.*?)</script>", page, re.S).group(1))

cl = H.login(A)
def upload(acct_name, rows):
    body = "Date,Description,Amount\n" + "".join(f"{d},{t},{a}\n" for d, t, a in rows)
    r = cl.post(f"/account/{acct_name}/upload", data={"statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
                "period_start": "2026-09-01", "period_end": "2026-09-30"}, content_type="multipart/form-data")
    assert r.status_code == 302, r.data[:300]

upload("Stanbic UGX", [("2026-09-10", "TRANSFER TO CENTENARY", -500000), ("2026-09-20", "INTERNAL TRF", -200000),
                       ("2026-09-25", "VISA PAYMENT", -150000)])
upload("Centenary UGX", [("2026-09-11", "FROM STANBIC", 500000), ("2026-09-22", "INTERNAL TRF IN", 200000)])
upload("Stanbic USD", [("2026-09-21", "USD TRF IN", 200000)])
upload("Visa", [("2026-09-15", "PAYMENT THANK YOU", -300000), ("2026-09-25", "PAYMENT RECEIVED", -150000)])

# ---- the picker ------------------------------------------------------------------------------
page = cl.get("/account/Stanbic UGX").data.decode()
xs = {a["n"] for a in coa_json(page) if a["x"]}
check("bank lines can be recorded as transfers to your other same-currency accounts", xs == {"Centenary UGX", "Visa"})
check("…never to itself or across currencies", "Stanbic UGX" not in xs and "Stanbic USD" not in xs)
check("transfer group labelled in the picker", "Transfer to your account" in page)
vpage = cl.get("/account/Visa").data.decode()
check("card: payments pick the bank they came from", "Card payment: choose the bank it was paid from" in vpage
      and 'data-dir="xfer"' in vpage)
check("card: only banks offered as the other side", {a["n"] for a in coa_json(vpage) if a["x"]} == {"Stanbic UGX", "Centenary UGX"})

# ---- suggestions: pairs seen on two statements -----------------------------------------------
k = c.cursor(); d = A.compute_detail(k, UGX, "bank", "35"); c.rollback()
fl = {(x["account"], str(x["amount"])) for v in d["xfers"].values() for x in v}
check("bank-to-bank pair flagged", ("Centenary UGX", "500000.00") in fl or ("Centenary UGX", "500000") in fl)
check("bank-to-card payment flagged (both statements show -150,000)", any(a == "Visa" for a, _ in fl))
k = c.cursor(); dc = A.compute_detail(k, CEN, "bank", "36"); c.rollback()
check("same number in another currency is not a transfer", all(x["account"] != "Stanbic USD" for v in dc["xfers"].values() for x in v))
check("pair button shown", "Record as one transfer" in page)
ids = re.findall(r"<h2 id=(sec-[a-z]+)", page)
check("each section heading has its own id (transfers and not-in-books apart)", len(ids) == len(set(ids))
      and "sec-transfers" in ids)

# ---- one line from the record table ------------------------------------------------------------
L1, C1 = lid("TRANSFER TO CENTENARY"), lid("FROM STANBIC")
n = len(POSTS)
cl.post("/account/Stanbic UGX/record", data={"only": L1, f"acct_{L1}": "37"})
check("refuses a transfer to another currency", len(POSTS) == n)
cl.post("/account/Stanbic UGX/record", data={"only": L1, f"acct_{L1}": "36"})
ent, body = POSTS[-1]
check("recorded as ONE QuickBooks Transfer, Stanbic -> Centenary",
      ent == "Transfer" and body["FromAccountRef"]["value"] == "35" and body["ToAccountRef"]["value"] == "36"
      and body["Amount"] == 500000.0 and body["TxnDate"] == "2026-09-10")
check("books of both accounts get their side", book(UGX)[0][2] == D(-500000) and book(CEN)[0][2] == D(500000)
      and book(UGX)[0][3] == "Transfer out" and book(CEN)[0][3] == "Transfer in")
check("this side matched straight away", matched(L1) == ("confirmed", "engine"))
check("…and the other account's line too", matched(C1) is not None)
check("the choice is remembered for next time",
      q1("SELECT category, currency FROM payee_correction WHERE payee='TRANSFER TO CENTENARY'") == ("Centenary UGX", "UGX"))
k = c.cursor(); mem = A.PostingMemory(k, "UGX"); c.rollback()
s = mem.suggest("TRANSFER TO CENTENARY", True)
check("…and suggested as the transfer target", s and A.resolve_coa(A.transfer_targets(c.cursor(), "35", "bank", "UGX"), s["cat"])["id"] == "36")
n = len(POSTS)
cl.post("/account/Stanbic UGX/record", data={"only": L1, f"acct_{L1}": "36"})
check("can't be recorded twice", len(POSTS) == n)

# ---- a pair: one transfer, both lines matched ------------------------------------------------
L2, C2, UL = lid("INTERNAL TRF"), lid("INTERNAL TRF IN"), lid("USD TRF IN")
n = len(POSTS)
cl.post("/account/Stanbic UGX/transfer", data={"line": L2, "other": UL})
check("pair refused across currencies", len(POSTS) == n)
cl.post("/account/Stanbic UGX/transfer", data={"line": L2, "other": lid("VISA PAYMENT")})
check("pair refused on the same account", len(POSTS) == n)
cl.post("/account/Stanbic UGX/transfer", data={"line": C2, "other": L2})
check("pair refused when the first line isn't this account's", len(POSTS) == n)
cur.execute("UPDATE statement SET signed_off_at=now() WHERE account_id=%s", (CEN,)); c.commit()
cl.post("/account/Stanbic UGX/transfer", data={"line": L2, "other": C2})
check("pair refused while the other statement is signed off", len(POSTS) == n
      and "signed off" in cl.get("/account/Stanbic UGX").data.decode())
cur.execute("UPDATE statement SET signed_off_at=NULL WHERE account_id=%s", (CEN,)); c.commit()
r = cl.raw_post("/account/Stanbic UGX/transfer", data={"line": L2, "other": C2})
check("CSRF: refused without token", r.status_code == 400 and len(POSTS) == n)

cl.post("/account/Stanbic UGX/transfer", data={"line": L2, "other": C2})
ent, body = POSTS[-1]
check("pair recorded as one Transfer, dated when the money left", len(POSTS) == n + 1 and ent == "Transfer"
      and body["FromAccountRef"]["value"] == "35" and body["ToAccountRef"]["value"] == "36"
      and body["Amount"] == 200000.0 and body["TxnDate"] == "2026-09-20")
check("both bank lines matched by you", matched(L2) == ("confirmed", "user") and matched(C2) == ("confirmed", "user"))
check("both marked recorded", q1("SELECT count(*) FROM writeback_log WHERE line_id = ANY(%s::uuid[]) AND status='done' AND qbo_type='Transfer'",
                                 ([L2, C2],))[0] == 2)
check("message says so", "matched both bank lines" in cl.get("/account/Stanbic UGX").data.decode())
cl.post("/account/Stanbic UGX/transfer", data={"line": L2, "other": C2})
check("pressing it again does nothing", len(POSTS) == n + 1)

# ---- cards ---------------------------------------------------------------------------------------
V1 = lid("PAYMENT THANK YOU")
n = len(POSTS)
cl.post("/account/Visa/record", data={"only": V1, f"acct_{V1}": "83"})
check("card payment can't be booked as an expense", len(POSTS) == n)
cl.post("/account/Visa/record", data={"only": V1, f"acct_{V1}": "35"})
ent, body = POSTS[-1]
check("card payment recorded as a Transfer from the bank to the card", ent == "Transfer"
      and body["FromAccountRef"]["value"] == "35" and body["ToAccountRef"]["value"] == "41")
check("card side lowers what's owed (-300,000), bank side is money out (-300,000)",
      [b[2] for b in book(VISA)] == [D(-300000)] and D(-300000) in [b[2] for b in book(UGX)])
check("card payment matched on the card statement", matched(V1) == ("confirmed", "engine"))

L3, V2 = lid("VISA PAYMENT"), lid("PAYMENT RECEIVED")
r = cl.post("/account/Visa/transfer", data={"line": V2, "other": L3})
check("recording a pair returns to the transfers section", r.headers["Location"].endswith("#sec-transfers"))
ent, body = POSTS[-1]
check("card/bank pair from the card's page: bank -> card", ent == "Transfer" and body["FromAccountRef"]["value"] == "35"
      and body["ToAccountRef"]["value"] == "41" and body["Amount"] == 150000.0)
check("…both lines matched", matched(L3) == ("confirmed", "user") and matched(V2) == ("confirmed", "user"))

# ---- the next sync brings the same transfers back without duplicating them ---------------------
check("sync reads card transfers the right way round", A._h_transfer(
    {"FromAccountRef": {"value": "35"}, "ToAccountRef": {"value": "41"}, "Amount": 10}, "41", "credit_card")[0] == D(-10)
    and A._h_transfer({"FromAccountRef": {"value": "41"}, "ToAccountRef": {"value": "35"}, "Amount": 10}, "41", "credit_card")[0] == D(10))
before = {a: book(a) for a in (UGX, CEN, VISA)}
META = {"LastUpdatedTime": "2026-09-28T10:00:00-07:00"}
def _q(entity, token, since=None, changed_since=None, each=None):
    recs = COA if entity == "Account" else [{**t, "MetaData": META} for t in QBO] if entity == "Transfer" else []
    return each(recs) if each else recs
A.qbo_query = _q
A.qbo_cdc_deleted = lambda t, e, cs: ({}, False)
A.sync_from_quickbooks(full=True)
after = {a: book(a) for a in (UGX, CEN, VISA)}
check("sync after recording: same rows, same amounts, no duplicates",
      {a: [(x[0], x[1], x[2]) for x in v] for a, v in before.items()} == {a: [(x[0], x[1], x[2]) for x in v] for a, v in after.items()})
check("matches survive the sync", matched(L2) == ("confirmed", "user") and matched(V1) is not None)

k = c.cursor(); r = A.reconcile(k, UGX, A._latest_statement(k, UGX)); c.rollback()
check("Stanbic statement fully matched", r["un_lines"] == [] or len(r["un_lines"]) == 0)

# ---- expenses and deposits on a USD bank are recorded in USD ----------------------------------
RATES = []
def fake_rate(token, ccy, d):
    RATES.append((ccy, str(d))); return 3712.5
A.qbo_exchange_rate = fake_rate
upload("Stanbic USD", [("2026-09-25", "ACCOUNT MAINTENANCE FEES", -10), ("2026-09-26", "USD CASH DEPOSIT", 50)])
F1, D1 = lid("ACCOUNT MAINTENANCE FEES"), lid("USD CASH DEPOSIT")
ids = [a["id"] for a in coa_json(cl.get("/account/Stanbic USD").data.decode())]
check("USD bank: offers home-currency and USD accounts, not EUR ones", "83" in ids and "95" in ids and "96" not in ids)
k = c.cursor(); ids = [a["id"] for a in A.load_coa(k, "UGX")]; c.rollback()
check("UGX bank: no foreign-currency accounts offered", "83" in ids and "95" not in ids and "96" not in ids)
cl.post("/account/Stanbic USD/record", data={"only": F1, f"acct_{F1}": "83"})
ent, body = POSTS[-1]
check("USD expense sent in USD at QuickBooks' rate for its date", ent == "Purchase" and body["CurrencyRef"] == {"value": "USD"}
      and body["ExchangeRate"] == 3712.5 and body["AccountRef"]["value"] == "37" and body["Line"][0]["Amount"] == 10.0
      and RATES == [("USD", "2026-09-25")])
cl.post("/account/Stanbic USD/record", data={"only": D1, f"acct_{D1}": "90"})
ent, body = POSTS[-1]
check("USD deposit too", ent == "Deposit" and body["CurrencyRef"] == {"value": "USD"} and body["ExchangeRate"] == 3712.5)
n = len(POSTS)
upload("Centenary UGX", [("2026-09-27", "STATIONERY", -5000)])
S1 = lid("STATIONERY")
cl.post("/account/Centenary UGX/record", data={"only": S1, f"acct_{S1}": "83"})
ent, body = POSTS[-1]
check("home-currency lines unchanged: no currency, no rate lookup", len(POSTS) == n + 1 and "CurrencyRef" not in body
      and "ExchangeRate" not in body and len(RATES) == 2)
def no_rate(token, ccy, d):
    raise ValueError(f"QuickBooks has no {ccy} exchange rate for {d}")
A.qbo_exchange_rate = no_rate
upload("Stanbic USD", [("2026-09-28", "LEDGER FEES", -3)])
F2 = lid("LEDGER FEES")
n = len(POSTS)
cl.post("/account/Stanbic USD/record", data={"only": F2, f"acct_{F2}": "83"})
page = cl.get("/account/Stanbic USD").data.decode()
check("no exchange rate: nothing sent, asks for the rate", len(POSTS) == n and "has no USD rate for 2026-09-28" in page
      and "Type the rate" in page)
# ---- sync: amounts in each account's own currency -------------------------------------------------
# QuickBooks gives a USD->UGX transfer as Amount 100 (USD) with ExchangeRate 3700; the UGX side is 370,000.
A.qbo_query = _q
QBO.append({"Id": "X1", "FromAccountRef": {"value": "37", "name": "Stanbic USD"}, "ToAccountRef": {"value": "35", "name": "Stanbic UGX"},
            "Amount": 100, "CurrencyRef": {"value": "USD"}, "ExchangeRate": 3700, "TxnDate": "2026-09-29"})
QBO.append({"Id": "X2", "FromAccountRef": {"value": "35", "name": "Stanbic UGX"}, "ToAccountRef": {"value": "36", "name": "Centenary UGX"},
            "Amount": 5000, "CurrencyRef": {"value": "UGX"}, "ExchangeRate": 1, "TxnDate": "2026-09-29"})
A.sync_from_quickbooks(full=True)
def row(acct, tid):
    return q1("SELECT amount, currency FROM book_txn WHERE account_id=%s AND source_txn_id=%s", (acct, tid))
check("sync: USD side of a USD->UGX transfer stays in USD", row(USD, "X1") == (D("-100"), "USD"))
check("sync: UGX side converted at the transfer's rate", row(UGX, "X1") == (D("370000.00"), "UGX"))
check("sync: same-currency transfers unchanged", row(UGX, "X2") == (D("-5000"), "UGX") and row(CEN, "X2") == (D("5000"), "UGX"))
check("sync: conversion helper leaves a record without a rate alone",
      A._in_account_ccy(D("10"), {"CurrencyRef": {"value": "USD"}}, "UGX", "UGX") == D("10"))
sys.exit(T.summary())

