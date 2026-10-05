"""Manual matching (any shape), undo, validation, and the duplicate guard on write-back.

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_manual_match.py`, or all suites with `python tests/run_all.py`.
"""
import io, json, sys
from decimal import Decimal as D

import harness as H

ACCT = "00000000-0000-0000-0000-0000000000a1"
A, c = H.setup(H.account_sql((ACCT, "35", "Stanbic", "bank")))
A.set_config("rule_clear_days", "31"); A.set_config("rule_group_days", "31")   # these checks use wide windows (Settings); the 3-day defaults are in suite_match_windows
cur = c.cursor()
T = H.Checker()
check = T.check

POSTS = []
A.qbo_is_connected = lambda: False          # uploads don't try to sync
A.qbo_token = lambda: "tok"
A.qbo_realm = lambda: "REAL-1"
def fake_post(token, entity, body):
    POSTS.append((entity, json.loads(json.dumps(body))))
    return {entity: {"Id": str(700 + len(POSTS))}}
A.qbo_post = fake_post
cur.execute("INSERT INTO qbo_coa (qbo_id, name, fqn, account_type, active) VALUES ('83','Office Supplies','Office Supplies','Expense',true)")
c.commit()

def book(tid, d, amt, who, cat=None):
    cur.execute("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency,
                   description, counterparty, category, last_modified) VALUES (%s,%s,%s,'Purchase',%s,%s,'UGX',%s,%s,%s,now())
                   RETURNING txn_id""", (A.ORG_ID, ACCT, tid, d, amt, who, who, cat))
    t = str(cur.fetchone()[0]); c.commit(); return t

def q1(sql, args=()):
    cur.execute(sql, args); r = cur.fetchone(); c.rollback(); return r

def lid(desc):
    return str(q1("SELECT line_id FROM statement_line WHERE description=%s", (desc,))[0])

def rec():
    k = c.cursor(); s = A._latest_statement(k, ACCT); r = A.reconcile(k, ACCT, s); c.rollback(); return r

def detail():
    k = c.cursor(); d = A.compute_detail(k, ACCT, "bank", "35"); c.rollback(); return d

# Books the matcher can't pair on its own:
B1 = book("b1", "2026-09-04", 100000, "Cust A")                     # 100,000 + 199,000 banked as one 300,000 deposit
B2 = book("b2", "2026-09-04", 199000, "Cust B")                     #   (typo: should be 200,000)
B3 = book("b3", "2026-09-09", -50000, "Savings transfer", "Office Supplies")   # typo: bank says 500,000
B4 = book("b4", "2026-10-05", -80000, "Supplier X")                 # same money, booked after the period end
B5 = book("b5", "2026-09-01", -70000, "Supplier Y")                 # cleared 21 days later -> only a suggestion

cl = H.login(A)
stmt = """Date,Description,Amount
2026-09-05,DEPOSIT CASH,300000
2026-09-10,TRANSFER TO SAVINGS,-500000
2026-09-20,SUPPLIER X,-80000
2026-09-22,SUPPLIER Y,-70000
2026-09-25,NEW EXPENSE,-10000
"""
r = cl.post("/account/Stanbic/upload", data={"statement": (io.BytesIO(stmt.encode()), "s.csv"), "closing_balance": "0",
            "period_start": "2026-09-01", "period_end": "2026-09-30"}, content_type="multipart/form-data")
assert r.status_code == 302
sid = str(q1("SELECT statement_id FROM statement")[0])
L1, L2, L3, L4, L5 = (lid(x) for x in ("DEPOSIT CASH", "TRANSFER TO SAVINGS", "SUPPLIER X", "SUPPLIER Y", "NEW EXPENSE"))

page = cl.get("/account/Stanbic").data.decode()
check("manual-match panel lists bank lines and QuickBooks entries", "Match manually" in page and f'name=ml value="{L1}"' in page
      and f'name=mb value="{B1}"' in page)
check("entries dated after the period aren't offered", f'name=mb value="{B4}"' not in page)
check("engine proposed only the late-clearing pair", rec()["n_pending"] == 1)

# ---- one bank line to two book entries, with a difference ---------------------------------
cl.post("/account/Stanbic/match", data={"ml": [L1], "mb": [B1, B2]})
m = q1("SELECT match_id, status, match_type, created_by, confirmed_by, amount_delta FROM match WHERE created_by='user'")
check("manual match saved as confirmed, by the user", m and m[1:5] == ("confirmed", "manual", "user", "Admin"))
check("difference recorded (300,000 - 299,000)", m and m[5] == D(1000))
r1 = rec()
check("counts straight away: line and both entries cleared", L1 not in {str(l[0]) for l in r1["un_lines"]}
      and not ({B1, B2} & {str(t[0]) for t in r1["un_books"]}))
check("difference flows into the balance proof", r1["match_adj"] == D(1000))
page = cl.get("/account/Stanbic").data.decode()
check("listed under 'Matched by you' with Undo", "Matched by you" in page and f"/unmatch/{m[0]}" in page)
check("not in the suggestions review list", f"/review/{m[0]}" not in page)

A.run_matcher(sid)
m2 = q1("SELECT created_by, confirmed_by, status FROM match m JOIN match_statement_line msl USING (match_id) WHERE msl.line_id=%s", (L1,))
check("survives a re-match with author intact", m2 == ("user", "Admin", "confirmed"))

# ---- undo -----------------------------------------------------------------------------------
mid = q1("SELECT match_id FROM match WHERE created_by='user'")[0]
cl.post(f"/account/Stanbic/unmatch/{mid}")
check("undo removes it and frees both sides", q1("SELECT count(*) FROM match WHERE created_by='user'")[0] == 0
      and L1 in {str(l[0]) for l in rec()["un_lines"]})
cl.post(f"/account/Stanbic/unmatch/{mid}")
check("undo twice is harmless", q1("SELECT count(*) FROM match WHERE statement_id=%s", (sid,))[0] >= 1)

# ---- two bank lines to one entry ------------------------------------------------------------
cl.post("/account/Stanbic/match", data={"ml": [L2, L5], "mb": [B3]})
m = q1("SELECT match_id, amount_delta FROM match WHERE created_by='user'")
check("many-to-one match works (delta -460,000)", m and m[1] == D(-460000)
      and q1("SELECT count(*) FROM match_statement_line WHERE match_id=%s", (m[0],))[0] == 2)
cl.post(f"/account/Stanbic/unmatch/{m[0]}")

# ---- validation -------------------------------------------------------------------------------
n = q1("SELECT count(*) FROM match")[0]
cl.post("/account/Stanbic/match", data={"ml": [L3], "mb": [B4]})
check("refuses a book entry outside the statement period", q1("SELECT count(*) FROM match")[0] == n)
cl.post("/account/Stanbic/match", data={"ml": [L2], "mb": []})
check("refuses an empty side", q1("SELECT count(*) FROM match")[0] == n)
cl.post("/account/Stanbic/match", data={"ml": [L2], "mb": [B3]})
cl.post("/account/Stanbic/match", data={"ml": [L2], "mb": [B1]})
check("refuses a line that's already matched", q1("SELECT count(*) FROM match WHERE created_by='user'")[0] == 1)
r = cl.raw_post("/account/Stanbic/match", data={"ml": [L3], "mb": [B1]})
check("CSRF: manual match without token refused", r.status_code == 400)

# ---- learning from a hand-made match ---------------------------------------------------------
k = c.cursor(); mem = A.PostingMemory(k); c.rollback()
sg = mem.suggest("TRANSFER TO SAVINGS ACCT", True)
check("hand-made matches teach the suggestions", sg and sg["cat"] == "Office Supplies" and sg["tier"] == 2)

# ---- duplicate guard -------------------------------------------------------------------------
# SUPPLIER Y cleared 21 days after its entry: a suggestion, so it waits there rather than in the list to record.
check("a line in a suggestion isn't offered for recording", L4 not in {str(w["line_id"]) for w in detail()["record_rows"]}
      and detail()["n_waiting"] == 1)
mid = q1("""SELECT m.match_id FROM match m JOIN match_statement_line x USING (match_id)
            WHERE x.line_id=%s AND m.status='proposed'""", (L4,))[0]
cl.post(f"/account/Stanbic/review/{mid}", data={"status": "rejected"})     # rejected: now it's listed, with the warning
d = detail()
items = {str(w["line_id"]): w for w in d["writebacks"] + d["deposits"]}
check("line with a later-dated twin is flagged, not matchable", items[L3]["dups"] and items[L3]["dups"][0]["txn_id"] == B4
      and items[L3]["dup_matchable"] is None)
check("line with an in-period twin offers 'Match it instead'", items[L4]["dup_matchable"] == B5)
check("clean line not flagged", items[L5]["dups"] == [])
page = cl.get("/account/Stanbic").data.decode()
check("warnings rendered", "QuickBooks may already have this" in page and "Match it instead" in page
      and "correct its date in QuickBooks" in page)
row4 = page.split(f'value="{L4}" class=rsel')[1][:120]
check("flagged line not ticked for bulk recording", "checked" not in row4.split(">")[0])

n = len(POSTS)
cl.post("/account/Stanbic/record", data={"only": L3, f"acct_{L3}": "83"})
check("record refused for a likely duplicate", len(POSTS) == n and "QuickBooks may already have it" in cl.get("/account/Stanbic").data.decode())
cl.post("/account/Stanbic/record", data={"only": L5, f"acct_{L5}": "83"})
check("clean line records normally", len(POSTS) == n + 1)
cl.post("/account/Stanbic/record", data={"only": L3, f"acct_{L3}": "83", f"dupok_{L3}": "1"})
check("'Not a duplicate' lets it through", len(POSTS) == n + 2 and POSTS[-1][1]["Line"][0]["Amount"] == 80000.0)

n = len(POSTS)
cl.post("/account/Stanbic/record", data={"bulk": "1", "sel": [L4], f"acct_{L4}": ""})
check("bulk record: a line left without an account isn't recorded, and says so", len(POSTS) == n
      and "1 line has no account, so it was left for later" in cl.get("/account/Stanbic").data.decode())

# ---- manual match replaces the engine's suggestion -------------------------------------------
cl.post("/account/Stanbic/match", data={"ml": [L4], "mb": [B5]})
check("matching the suggested pair by hand clears the pending suggestion", rec()["n_pending"] == 0
      and q1("SELECT count(*) FROM match WHERE status='proposed'")[0] == 0)

for u in ("/", "/account/Stanbic", "/account/Stanbic/exceptions.csv"):
    check("GET " + u, cl.get(u).status_code == 200)
sys.exit(T.summary())
