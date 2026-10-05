"""Recorded twice. A bank line waiting in a suggested match isn't offered for recording (if the
suggestion is right, the money is already in QuickBooks). Lines recorded anyway, whose amounts add up
to an entry QuickBooks already had, are listed so the copies can be deleted and the lines matched to
the original.

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_recorded_twice.py`, or all suites with `python tests/run_all.py`.
"""
import html, io, json, re, sys
from decimal import Decimal as D

import harness as H

ACCT = "00000000-0000-0000-0000-0000000000a1"
NAME = "Stanbic UGX 10202"
A, c = H.setup(H.account_sql((ACCT, "35", NAME, "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
CH = "Operational Expenses:Finance Expenses:Charges & Fees:Bank charges UGX"
A._store_coa([{"Id": "35", "Name": NAME, "AccountType": "Bank", "CurrencyRef": {"value": "UGX"}},
              {"Id": "83", "Name": "Bank charges UGX", "AccountType": "Expense", "CurrencyRef": {"value": "UGX"}}])
POSTS, DELETES, READS = [], [], []
def fake_post(token, entity, body):
    POSTS.append((entity, body))
    return {entity: {"Id": str(54200 + len(POSTS))}}
FAIL_ON = set()
def fake_read(token, entity, i):
    READS.append((entity, i)); return {"Id": i, "SyncToken": "3"}
def fake_delete(token, entity, i, st):
    if i in FAIL_ON:
        raise RuntimeError("Object Not Found")
    DELETES.append((entity, i, st)); return {}
A.qbo_post, A.qbo_read, A.qbo_delete = fake_post, fake_read, fake_delete
A.qbo_token = lambda: "tok"
A.qbo_realm = lambda: "REAL-1"
A.qbo_is_connected = lambda: False


def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
def lid(desc):
    return str(q("SELECT line_id FROM statement_line WHERE description=%s", (desc,))[0][0])
def book(i, d, amt, text):
    q("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency, description,
         category, last_modified) VALUES (%s,%s,%s,'Purchase',%s,%s,'UGX',%s,%s,now()) RETURNING 1""", (A.ORG_ID, ACCT, i, d, amt, text, CH))
cl = H.login(A)
def page():
    return cl.get(f"/account/{NAME}").data.decode()
def msg():
    m = re.search(r"<div id=flash[^>]*>(.*?)</div>", page(), re.S)
    return html.unescape(re.sub(r"<[^>]+>", "", m.group(1))) if m else ""
def in_list(p, line):
    return f'value="{line}" class=rsel' in p

book("50309", "2025-09-16", -2300, "EFT BOL FEES INST ID 82220009 Fee Collection")     # fee + excise, booked together
book("50332", "2025-09-18", -4600, "FEE ACH INWD CR")
EXTRA = [("50401", "2025-09-22", -6000, -900), ("50402", "2025-09-24", -2000, -300), ("50403", "2025-09-26", -4000, -600)]
for i, d, fee, ex in EXTRA:
    book(i, d, fee + ex, f"FEE ACH INWD CR {d}")
body = ("Date,Description,Amount\n2025-09-16,EFT BOL FEES INST ID 82220009 Fee Collection,-2000\n"
        "2025-09-16,Excise Duty EFT BOL FEES 82220009 Fee Collection,-300\n"
        "2025-09-18,FEE ACH INWD CR,-4000\n2025-09-18,GOVERNMENT EXCISE DUTY CHARGE,-600\n2025-09-20,STATIONERY,-15000\n"
        + "".join(f"{d},FEE {d},{fee}\n{d},EXCISE {d},{ex}\n" for _, d, fee, ex in EXTRA))
cl.post(f"/account/{NAME}/upload", data={"statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
        "period_start": "2025-09-01", "period_end": "2025-09-30"}, content_type="multipart/form-data")
F1, E1 = lid("EFT BOL FEES INST ID 82220009 Fee Collection"), lid("Excise Duty EFT BOL FEES 82220009 Fee Collection")
F2, E2, ST = lid("FEE ACH INWD CR"), lid("GOVERNMENT EXCISE DUTY CHARGE"), lid("STATIONERY")
MORE = [lid(f"{w} {d}") for _, d, _f, _e in EXTRA for w in ("FEE", "EXCISE")]
pend = q("""SELECT count(*) FROM match m JOIN match_statement_line x USING (match_id) WHERE m.status='proposed'
            AND x.line_id IN (%s::uuid,%s::uuid,%s::uuid,%s::uuid)""", (F1, E1, F2, E2))[0][0]
assert pend == 4, pend

# ---- 1. waiting in a suggestion: not offered for recording ---------------------------------------------
p = page()
check("lines in a suggested match aren't in the list to record", not any(in_list(p, x) for x in (F1, E1, F2, E2)))
check("…a line with no suggestion still is", in_list(p, ST))
check("…a note says how many wait, and where", "10 more lines aren't listed here: a suggested match is waiting for them" in html.unescape(p))
n = len(POSTS)
cl.post(f"/account/{NAME}/record", data={"bulk": "1", "sel": [F1, E1, ST], f"acct_{F1}": "83", f"acct_{E1}": "83", f"acct_{ST}": "83"})
m = msg()
check("a stale page recording them anyway: they're left out, the rest recorded", len(POSTS) == n + 1
      and "2 lines were left out: a suggested match is waiting for them" in m)

# ---- 2. recorded twice: found -----------------------------------------------------------------------------
# Reject the suggestions and record the lines: QuickBooks now has each charge twice.
for _ in range(6):          # each review re-matches (new ids), so take the next one each time
    left = q("SELECT match_id FROM match WHERE status='proposed' LIMIT 1")
    if not left:
        break
    cl.post(f"/account/{NAME}/review/{left[0][0]}", data={"status": "rejected"})
n = len(POSTS)
ALL = [F1, E1, F2, E2] + MORE
cl.post(f"/account/{NAME}/record", data={"bulk": "1", "sel": ALL, **{f"acct_{x}": "83" for x in ALL}})
check("(rejected, so they could be recorded: 10 entries made)", len(POSTS) == n + 10)
p = html.unescape(page())
check("a 'Recorded twice?' section lists all five groups", "Recorded twice? — 5 to check" in p)
k = c.cursor(); s = A._latest_statement(k, ACCT); groups = A.recorded_twice(k, s[0], A.reconcile(k, ACCT, s)["un_books"]); c.rollback()
check("…the 2,300 entry with the fee and its excise", groups[0]["amount"] == D("-2300") and groups[0]["qbo"] == "Purchase #50309"
      and sorted(l["amount"] for l in groups[0]["lines"]) == [D("-2000"), D("-300")])
check("…the 4,600 entry with its two lines", groups[1]["amount"] == D("-4600") and len(groups[1]["lines"]) == 2)
check("…the stationery line recorded once isn't listed", not any(l["line_id"] == ST for g in groups for l in g["lines"]))
picks = [g["txn_id"] + "|" + ",".join(l["line_id"] for l in g["lines"]) for g in groups]

# ---- 3. put right ------------------------------------------------------------------------------------------
cl.post(f"/account/{NAME}/recorded_twice", data={"grp": ["nonsense|x"]})
check("a pick that isn't on the page does nothing", not DELETES and "Nothing ticked" in msg())
FAIL_ON.add(groups[1]["lines"][1]["qbo_id"])
cl.post(f"/account/{NAME}/recorded_twice", data={"grp": picks[:2]})      # two groups: done in the request
m = msg()
first = {l["qbo_id"] for l in groups[0]["lines"]}
check("deletes the copies in QuickBooks (with their SyncToken)", {d[1] for d in DELETES} >= first and all(d[2] == "3" for d in DELETES))
check("…the first group's lines matched to the entry QuickBooks already had",
      q("""SELECT count(*) FROM match m JOIN match_statement_line x USING (match_id) JOIN match_book_txn y USING (match_id)
           WHERE m.status='confirmed' AND y.txn_id=%s::uuid AND x.line_id IN (%s::uuid,%s::uuid)""", (groups[0]["txn_id"], F1, E1))[0][0] == 2)
check("…the copies are gone from the books here and the log says why",
      not q("SELECT 1 FROM book_txn WHERE source_txn_id = ANY(%s) AND NOT is_deleted", (list(first),))
      and q("SELECT error FROM writeback_log WHERE line_id=%s", (F1,))[0][0].startswith("deleted by Admin: QuickBooks already had it as Purchase #50309"))
check("QuickBooks refusing one: stops there and says so", "Put right 1 recorded-twice group" in m and "Stopped:" in m
      and "Object Not Found" in m)
p = page()
check("…the line whose copy was deleted is back in the list; the one QuickBooks refused stays recorded",
      in_list(p, groups[1]["lines"][0]["line_id"]) and not in_list(p, groups[1]["lines"][1]["line_id"]))
check("activity log records each group as it's done (so a cut-off run still leaves a record)",
      [r[0] for r in q("SELECT action FROM activity_log WHERE action LIKE 'removed %%recorded twice%%' ORDER BY id")]
      == ["removed 2 entries recorded twice (duplicate of Purchase #50309)", "removed 1 entry recorded twice (duplicate of Purchase #50332)"])

# ---- 3b. many groups: in the background, with progress ------------------------------------------------------
import json, threading
A.SYNC_IN_BACKGROUND = True
gate = threading.Event()
slow_read = A.qbo_read
def held_read(*a):
    gate.wait(10); return slow_read(*a)
A.qbo_read = held_read
n = len(DELETES)
cl.post(f"/account/{NAME}/recorded_twice", data={"grp": picks[2:]})
job = json.loads(A.get_config(f"record_job:{NAME}"))
check("three groups or more: the request returns straight away, the work runs in the background",
      job["state"] == "running" and job.get("kind") == "twice" and job["total"] == 3 and len(DELETES) == n)
gate.set()
for t_ in [t_ for t_ in threading.enumerate() if t_.name == "qbo-twice"]:
    t_.join(20)
A.qbo_read = slow_read
A.SYNC_IN_BACKGROUND = False
job = json.loads(A.get_config(f"record_job:{NAME}"))
check("…finishes: six copies deleted, three groups matched to the originals", job["state"] == "done"
      and len(DELETES) == n + 6 and job["msg"].startswith("Put right 3 recorded-twice groups: deleted 6 entries"))
check("…the lines are matched to the entries QuickBooks already had", all(
      q("""SELECT 1 FROM match m JOIN match_statement_line x USING (match_id) WHERE x.line_id=%s AND m.status='confirmed'
           AND m.created_by='user'""", (x,)) for x in MORE))
check("…the result is shown once on the page", "Put right 3 recorded-twice groups" in msg() and "Put right 3" not in msg())

# ---- 4. limits ---------------------------------------------------------------------------------------------
q("UPDATE statement SET signed_off_at=now() RETURNING 1")
n = len(DELETES)
cl.post(f"/account/{NAME}/recorded_twice", data={"grp": picks})
check("a signed-off statement: nothing deleted", len(DELETES) == n and "signed off" in msg())
check("only users allowed to undo can do it", A.PERM_BY_ENDPOINT.get("recorded_twice_fix") == "undo")
sys.exit(T.summary())
