"""Review-first matching, books from QuickBooks, write-back and learned suggestions.

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_review_writeback.py`, or all suites with `python tests/run_all.py`.
"""
import importlib, io, json, re, sys, urllib.error
from datetime import datetime, timezone, timedelta
from decimal import Decimal as D

import harness as H

A, c = H.setup(H.account_sql(
    ('00000000-0000-0000-0000-0000000000a1', '35', 'Stanbic', 'bank'),
    ('00000000-0000-0000-0000-0000000000a3', '41', 'Visa', 'credit_card')))
cur = c.cursor()
T = H.Checker()
check = T.check

META = {"LastUpdatedTime": "2026-09-20T10:00:00-07:00"}
COA = [{"Id": "35", "Name": "Stanbic", "AccountType": "Bank"},
       {"Id": "41", "Name": "Visa", "AccountType": "Credit Card"},
       {"Id": "80", "Name": "Fuel", "FullyQualifiedName": "Automobile:Fuel", "AccountType": "Expense"},
       {"Id": "81", "Name": "Electricity", "FullyQualifiedName": "Utilities:Electricity", "AccountType": "Expense"},
       {"Id": "82", "Name": "Telephone", "AccountType": "Expense"},
       {"Id": "83", "Name": "Office Supplies", "AccountType": "Expense"},
       {"Id": "90", "Name": "Sales", "AccountType": "Income"},
       {"Id": "95", "Name": "Accounts Payable (A/P)", "AccountType": "Accounts Payable"}]
def purchase(i, amt, d, who, ref, cat):
    return {"Id": i, "AccountRef": {"value": "35"}, "TotalAmt": amt, "TxnDate": d, "MetaData": META,
            "EntityRef": {"value": ref, "name": who, "type": "Vendor"},
            "Line": [{"AccountBasedExpenseLineDetail": {"AccountRef": {"value": "x", "name": cat}}}]}
QBO = {k: {} for k in A.QBO_HANDLERS}
for p in (purchase("501", 45000, "2026-08-10", "Shell Uganda", "56", "Automobile:Fuel"),
          purchase("502", 18000, "2026-07-01", "Umeme Ltd", "57", "Utilities:Electricity"),
          purchase("503", 60000, "2026-09-02", "Cheque 777", "58", "Office Supplies"),
          purchase("504", 12500, "2026-09-12", "Stationery World", "59", "Office Supplies")):
    QBO["Purchase"][p["Id"]] = p
POSTS, STATE = [], {"fail": None, "entity_400": False, "syncs": 0}

def fake_query(entity, token, since=None, changed_since=None, each=None):
    recs = COA if entity == "Account" else list(QBO.get(entity, {}).values())
    return each(recs) if each else recs
def fake_post(token, entity, body):
    POSTS.append((entity, json.loads(json.dumps(body))))
    if STATE["fail"]:
        raise urllib.error.HTTPError("u", STATE["fail"], "err", {}, io.BytesIO(b'{"Fault":"boom"}'))
    if STATE["entity_400"] and "EntityRef" in body:
        raise urllib.error.HTTPError("u", 400, "bad", {}, io.BytesIO(b'{"Fault":"Invalid Reference Id"}'))
    return {entity: {"Id": str(900 + len(POSTS))}}
real_sync = None
def patch():
    global real_sync
    H.share_connection(A, c)
    A.qbo_token = lambda: "tok"; A.qbo_query = fake_query; A.qbo_realm = lambda: "REAL-1"
    A.qbo_cdc_deleted = lambda t, e, cs: ({}, False); A.qbo_post = fake_post
    A.qbo_is_connected = lambda: True
    real_sync = A.sync_from_quickbooks
    def counting_sync(full=False, progress=None):
        STATE["syncs"] += 1; return real_sync(full, progress)
    A.sync_from_quickbooks = counting_sync
patch()

def q1(sql, args=()):
    cur.execute(sql, args); r = cur.fetchone(); c.rollback(); return r
def qa(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.rollback(); return r
ACCT = "00000000-0000-0000-0000-0000000000a1"
def rec():
    k = c.cursor(); s = A._latest_statement(k, ACCT); r = A.reconcile(k, ACCT, s); c.rollback(); return r
def detail():
    k = c.cursor(); d = A.compute_detail(k, ACCT, "bank", "35"); c.rollback(); return d
def lid(desc):
    return str(q1("""SELECT sl.line_id FROM statement_line sl JOIN statement s USING (statement_id)
                     WHERE sl.description=%s ORDER BY s.created_at DESC LIMIT 1""", (desc,))[0])

cl = H.login(A)
def upload(text, **kw):
    r = cl.post("/account/Stanbic/upload", data={"statement": (io.BytesIO(text.encode()), "s.csv"), **kw},
                content_type="multipart/form-data")
    assert r.status_code == 302, r.data[:300]

# ---- books straight from QBO -------------------------------------------------
page = cl.get("/account/Stanbic").data.decode()
check("books box offers QuickBooks refresh, CSV tucked away", "Refresh from QuickBooks" in page and "Offline? Import a QuickBooks CSV export instead" in page)
r = cl.post("/sync", data={"back": "Stanbic"})
check("refresh from account page returns to that page", r.status_code == 302 and r.headers["Location"].endswith("/account/Stanbic"))
check("chart of accounts cached (postable only)", {a["fqn"] for a in A.load_coa(c.cursor())} ==
      {"Automobile:Fuel", "Utilities:Electricity", "Telephone", "Office Supplies", "Sales"}); c.rollback()
check("payee refs stored from QBO", q1("SELECT counterparty_ref FROM book_txn WHERE source_txn_id='501'")[0] == "Vendor:56")

# August: a Shell card purchase that matches QBO exactly -> teaches bank wording -> Fuel / Shell Uganda
upload("Date,Description,Amount\n2026-08-10,POS PURCHASE SHELL KAMPALA 123456,-45000\n",
       period_start="2026-08-01", period_end="2026-08-31", closing_balance="0")
syncs = STATE["syncs"]
check("upload refreshed books from QuickBooks first", syncs >= 1)
check("August exact match auto-confirmed", q1("SELECT status FROM match")[0] == "confirmed")

# September
sep = """Date,Description,Amount
2026-09-05,POS PURCHASE SHELL NTINDA 998877,-30000
2026-09-06,UMEME PREPAID TOKEN 0412,-20000
2026-09-07,AIRTEL DATA BUNDLE,-10000
2026-09-08,CUSTOMER XYZ DEPOSIT,150000
2026-09-20,CHQ 000777,-60000
2026-09-12,Stationery World,-12000
"""
upload(sep, period_start="2026-09-01", period_end="2026-09-30", closing_balance="0")
check("upload refreshed books again", STATE["syncs"] == syncs + 1)

# ---- review-first matching -------------------------------------------------
r1 = rec()
st = dict(qa("SELECT match_type || ':' || confidence, status FROM match m JOIN statement s USING (statement_id) WHERE s.period_start='2026-09-01'"))
check("cleared-later + same-payee suggestions are proposed, not confirmed", st.get("exact:0.9") == "proposed" and st.get("fuzzy:0.6") == "proposed")
check("proposals don't count: both sides still unmatched", r1["n_pending"] == 2 and len(r1["un_books"]) == 2)
page = cl.get("/account/Stanbic").data.decode()
check("review list shows Confirm/Reject", "to review" in page and ">Confirm<" in page and ">Reject<" in page)
cl.post("/account/Stanbic/balances", data={"opening": "0", "closing": "-(0)".replace("-(0)", "0"), "book": "0"})
cl.post("/account/Stanbic/signoff")
check("sign-off blocked while suggestions pending", q1("SELECT signed_off_at FROM statement WHERE period_start='2026-09-01'")[0] is None)

mid_fuzzy = q1("SELECT match_id FROM match WHERE match_type='fuzzy'")[0]
mid_late = q1("SELECT match_id FROM match WHERE match_type='exact' AND confidence<1")[0]
cl.post(f"/account/Stanbic/review/{mid_late}", data={"status": "confirmed"})
check("confirmed suggestion now counts", rec()["n_pending"] == 1 and len(rec()["un_books"]) == 1)
check("confirmed_by recorded", q1("SELECT confirmed_by FROM match WHERE match_type='exact' AND confidence<1")[0] == "Admin")
cl.post("/sync", data={})   # re-matches open statements
check("confirmed suggestion survives a sync re-match", q1("SELECT status FROM match WHERE match_type='exact' AND confidence<1")[0] == "confirmed")
cl.post(f"/account/Stanbic/review/{q1('SELECT match_id FROM match WHERE match_type=%s', ('fuzzy',))[0]}", data={"status": "rejected"})
check("rejected suggestion stays rejected and frees both sides",
      q1("SELECT status FROM match WHERE match_type='fuzzy'")[0] == "rejected" and rec()["n_pending"] == 0)
mid = q1("SELECT match_id FROM match WHERE match_type='fuzzy'")[0]
cl.post(f"/account/Stanbic/review/{mid}", data={"status": "proposed"})
check("undo puts it back up for review", rec()["n_pending"] == 1)
cl.post("/account/Stanbic/review_all")
check("confirm all", rec()["n_pending"] == 0 and q1("SELECT status FROM match WHERE match_type='fuzzy'")[0] == "confirmed")
check("confirmed amount difference feeds the balance proof", rec()["match_adj"] == D(500))

# ---- learning ---------------------------------------------------------------
d = detail()
items = {w["who"]: w for w in d["writebacks"] + d["deposits"]}
sh = items["POS PURCHASE SHELL NTINDA 998877"]
print("   shell:", sh["sug"])
check("tier 2: bank wording learned from matched line -> Fuel", sh["sug"] and sh["sug"]["cat"] == "Automobile:Fuel" and sh["sug"]["tier"] == 2)
check("tier 2: payee + vendor ID learned too", sh["payee"] == "Shell Uganda" and sh["payee_ref"] == "Vendor:56")
check("suggested account resolved to its QBO id", sh["acct_id"] == "80")
um = items["UMEME PREPAID TOKEN 0412"]
print("   umeme:", um["sug"])
check("tier 3: payee history -> Electricity", um["sug"] and um["sug"]["cat"] == "Utilities:Electricity" and um["sug"]["tier"] == 3)
check("no suggestion for a first-time payee", items["AIRTEL DATA BUNDLE"]["sug"] is None)
check("money-in line doesn't get an expense suggestion", items["CUSTOMER XYZ DEPOSIT"]["sug"] is None and items["CUSTOMER XYZ DEPOSIT"]["out"] is False)
page = cl.get("/account/Stanbic").data.decode()
check("record table renders with suggestion reason", "Not in QuickBooks yet" in page and "similar bank lines were posted to Automobile:Fuel" in page)
check("chart of accounts embedded safely as JSON", '"n": "Automobile:Fuel"' in page and "Accounts Payable" not in page.split('id=coa-data')[1][:2000])

# ---- write-back ---------------------------------------------------------------
L = lid("POS PURCHASE SHELL NTINDA 998877")
form = {"only": L, f"acct_{L}": "80", f"payee_{L}": "Shell Uganda", f"psug_{L}": "Shell Uganda", f"pref_{L}": "Vendor:56"}
cl.post("/account/Stanbic/record", data=form)
ent, body = POSTS[-1]
check("Purchase posted with the learned vendor", ent == "Purchase" and body["EntityRef"] == {"value": "56", "type": "Vendor"})
check("…from the bank account, dated as the statement, to Fuel",
      body["AccountRef"]["value"] == "35" and body["TxnDate"] == "2026-09-05" and body["PaymentType"] == "Cash"
      and body["Line"][0]["Amount"] == 30000.0 and body["Line"][0]["AccountBasedExpenseLineDetail"]["AccountRef"]["value"] == "80")
check("write-back logged as done", q1("SELECT status, qbo_type FROM writeback_log WHERE line_id=%s", (L,)) == ("done", "Purchase"))
check("line now matched (exact, confirmed)", q1("""SELECT m.status FROM match m JOIN match_statement_line msl USING (match_id)
      WHERE msl.line_id=%s""", (L,))[0] == "confirmed")
n = len(POSTS)
cl.post("/account/Stanbic/record", data=form)
check("second click posts nothing", len(POSTS) == n)

# user edits the payee -> learned vendor ID is not reused
L = lid("AIRTEL DATA BUNDLE")
cl.post("/account/Stanbic/record", data={"only": L, f"acct_{L}": "82", f"payee_{L}": "Airtel Uganda", f"psug_{L}": "", f"pref_{L}": ""})
check("typed payee goes in the memo, no vendor ref", "EntityRef" not in POSTS[-1][1] and POSTS[-1][1]["PrivateNote"].startswith("Airtel Uganda"))

# vendor rejected by QBO (inactive) -> recorded without it
STATE["entity_400"] = True
L = lid("UMEME PREPAID TOKEN 0412")
cl.post("/account/Stanbic/record", data={"only": L, f"acct_{L}": "81", f"payee_{L}": "Umeme Ltd", f"psug_{L}": "Umeme Ltd", f"pref_{L}": "Vendor:57"})
STATE["entity_400"] = False
check("stale vendor: retried without EntityRef and recorded", "EntityRef" in POSTS[-2][1] and "EntityRef" not in POSTS[-1][1]
      and q1("SELECT status FROM writeback_log WHERE line_id=%s", (L,))[0] == "done")

# deposit + a QBO failure
L = lid("CUSTOMER XYZ DEPOSIT")
STATE["fail"] = 500
cl.post("/account/Stanbic/record", data={"only": L, f"acct_{L}": "90"})
STATE["fail"] = None
check("QBO error logged as failed, message shown", q1("SELECT status FROM writeback_log WHERE line_id=%s", (L,))[0] == "failed"
      and "QuickBooks said HTTP 500" in cl.get("/account/Stanbic").data.decode())
cl.post("/account/Stanbic/record", data={"only": L, f"acct_{L}": "90"})
ent, body = POSTS[-1]
check("failed line can be retried; posts a Deposit to Sales", ent == "Deposit" and body["DepositToAccountRef"]["value"] == "35"
      and body["Line"][0]["DepositLineDetail"]["AccountRef"]["value"] == "90")
check("deposit book row is positive", q1("SELECT amount FROM book_txn WHERE source_txn_type='Deposit'")[0] == D(150000))

# interrupted attempt blocks until cleared
upload(sep + "2026-09-25,KCB TRANSFER FEE,-3000\n2026-09-26,NEW LINE,-4000\n", period_start="2026-09-01", period_end="2026-09-30", closing_balance="0")
La, Lb = lid("KCB TRANSFER FEE"), lid("NEW LINE")
cur.execute("INSERT INTO writeback_log (line_id, status) VALUES (%s,'pending')", (La,)); c.commit()
n = len(POSTS)
cl.post("/account/Stanbic/record", data={"sel": [La, Lb], "bulk": "1", f"acct_{La}": "83", f"acct_{Lb}": ""})
msg = cl.get("/account/Stanbic").data.decode()
check("bulk: pending line skipped, line without account left for later, nothing posted",
      len(POSTS) == n and "Skipped 1 already recorded or in progress" in msg
      and "1 line has no account, so it was left for later" in msg)
check("page offers 'not in QuickBooks' reset for the interrupted line", "I checked — it" in msg)
cl.post("/account/Stanbic/record_reset", data={"reset": La})
cl.post("/account/Stanbic/record", data={"sel": [La, Lb], "bulk": "1", f"acct_{La}": "83", f"acct_{Lb}": "83"})
check("after reset, bulk records both", len(POSTS) == n + 2)

# tier 1: the user's own choice wins next time
k = c.cursor(); mem = A.PostingMemory(k); c.rollback()
s1 = mem.suggest("AIRTEL DATA BUNDLE 55", True)
check("recorded choices remember the account's currency",
      q1("SELECT count(*) FROM payee_correction WHERE currency='UGX'")[0] >= 1
      and q1("SELECT count(*) FROM payee_correction WHERE currency IS NULL")[0] == 0)
check("tier 1: your recorded choice is suggested next time", s1 and s1["cat"] == "Telephone" and s1["tier"] == 1 and s1["payee"] == "Airtel Uganda")

# credit card: charges recordable (as CreditCard purchase), payments only as a transfer from a bank
cl.post("/account/Visa/upload", data={"statement": (io.BytesIO(b"Date,Description,Amount\n2026-09-03,AMAZON WEB SERVICES,55000\n2026-09-10,PAYMENT THANK YOU,-100000\n"), "v.csv"),
        "period_start": "2026-09-01", "period_end": "2026-09-30"}, content_type="multipart/form-data")
page = cl.get("/account/Visa").data.decode()
check("card payment offered as a transfer from a bank", "Card payment: choose the bank it was paid from" in page and "Card payment or refund" not in page)
Lc = lid("AMAZON WEB SERVICES")
cl.post("/account/Visa/record", data={"only": Lc, f"acct_{Lc}": "82"})
ent, body = POSTS[-1]
check("card charge posted as CreditCard purchase from the card account", ent == "Purchase" and body["PaymentType"] == "CreditCard" and body["AccountRef"]["value"] == "41")
check("card book row positive (charge)", q1("SELECT amount FROM book_txn bt JOIN account a USING (account_id) WHERE a.name='Visa'")[0] == D(55000))

# ---- one-time migration of old auto-confirmed suggestions ----------------------
sid = q1("SELECT statement_id FROM statement WHERE period_start='2026-09-01' ORDER BY created_at DESC LIMIT 1")[0]
cur.execute("UPDATE match SET status='confirmed' WHERE statement_id=%s AND match_type='manual'", (sid,))
cur.execute("""INSERT INTO match (org_id, statement_id, status, match_type, confidence) VALUES (%s,%s,'confirmed','fuzzy',0.6)""", (A.ORG_ID, sid))
cur.execute("DELETE FROM app_config WHERE key='migr_proposed_v1'"); c.commit()
importlib.reload(A); patch()
check("migration put old auto-confirmed suggestions back up for review",
      q1("SELECT count(*) FROM match WHERE statement_id=%s AND match_type='fuzzy' AND status='proposed'", (sid,))[0] >= 1)
cur.execute("UPDATE match SET status='confirmed' WHERE statement_id=%s AND match_type='fuzzy'", (sid,)); c.commit()
importlib.reload(A); patch()
check("migration runs only once", q1("SELECT count(*) FROM match WHERE statement_id=%s AND match_type='fuzzy' AND status='proposed'", (sid,))[0] == 0)

for u in ("/", "/account/Stanbic", "/account/Stanbic/exceptions.csv", "/account/Stanbic/qbo_import.csv", "/account/Stanbic/history"):
    check("GET " + u, cl.get(u).status_code == 200)
sys.exit(T.summary())
