"""Recording, round three: entries QuickBooks already has are matched instead of created; the message
counts what was recorded; recorded lines leave the list; long batches run in the background with
progress, one at a time, and a lost job is reported.

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_record_jobs.py`, or all suites with `python tests/run_all.py`.
"""
import io, json, re, sys, threading, time

import harness as H

ACCT = "00000000-0000-0000-0000-0000000000a1"
A, c = H.setup(H.account_sql((ACCT, "35", "Stanbic", "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
A._store_coa([{"Id": "35", "Name": "Stanbic", "AccountType": "Bank", "CurrencyRef": {"value": "UGX"}},
              {"Id": "83", "Name": "Office Supplies", "AccountType": "Expense", "CurrencyRef": {"value": "UGX"}}])
POSTS = []
def fake_post(token, entity, body):
    POSTS.append((entity, body))
    return {entity: {"Id": str(600 + len(POSTS))}}
A.qbo_post = fake_post
A.qbo_token = lambda: "tok"
A.qbo_realm = lambda: "REAL-1"
A.qbo_is_connected = lambda: False

def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
def lid(desc):
    return str(q("""SELECT sl.line_id FROM statement_line sl JOIN statement s USING (statement_id)
                    WHERE sl.description=%s ORDER BY s.created_at DESC LIMIT 1""", (desc,))[0][0])
def book(tid, d, amt, who):
    q("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency,
         description, counterparty, last_modified) VALUES (%s,%s,%s,'Purchase',%s,%s,'UGX',%s,%s,now()) RETURNING txn_id""",
      (A.ORG_ID, ACCT, tid, d, amt, who, who))
def matched(line):
    return q("""SELECT m.match_type, m.created_by FROM match m JOIN match_statement_line msl USING (match_id)
                WHERE msl.line_id=%s AND m.status='confirmed'""", (line,))
cl = H.login(A)
def upload(rows):
    body = "Date,Description,Amount\n" + "".join(f"{d},{t},{a}\n" for d, t, a in rows)
    r = cl.post("/account/Stanbic/upload", data={"statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
                "period_start": "2026-09-01", "period_end": "2026-09-30"}, content_type="multipart/form-data")
    assert r.status_code == 302
def page():
    return cl.get("/account/Stanbic").data.decode()
def msg():
    m = re.search(r"<div id=flash[^>]*>(.*?)</div>", page(), re.S)
    return m.group(1) if m else ""
def in_list(p, line):
    return f'value="{line}" class=rsel' in p

# ---- an entry QuickBooks already has: matched, not created ------------------------------------------
upload([("2026-09-10", "SUPPLIER ZETA KAMPALA", -12000), ("2026-09-11", "STATIONERY", -5000), ("2026-09-12", "GENERATOR FUEL", -7000)])
Z, S, G = lid("SUPPLIER ZETA KAMPALA"), lid("STATIONERY"), lid("GENERATOR FUEL")
# Booked in QuickBooks after the statement was matched (so the matcher hasn't paired them yet):
book("z1", "2026-09-12", -12000, "Supplier Zeta")          # same money, 2 days apart, same payee
book("s1", "2026-09-11", -5000, "Airtime top-up")          # same amount and day, different payee: a different transaction
book("g1", "2026-09-05", -7000, "Generator fuel")          # same payee, 7 days apart: beyond 3 days
n = len(POSTS)
cl.post("/account/Stanbic/record", data={"only": Z, f"acct_{Z}": "83"})
check("already in QuickBooks (same amount, 2 days, same payee): matched, nothing created", len(POSTS) == n
      and matched(Z) == [("manual", "user")])
check("…and the message says so", "already in QuickBooks, so it was matched to the existing entry" in msg())
check("…and the line leaves the list", not in_list(page(), Z))
cl.post("/account/Stanbic/record", data={"only": S, f"acct_{S}": "83", f"dupok_{S}": "1"})
check("same amount and day but a different payee: recorded as its own", len(POSTS) == n + 1)
cl.post("/account/Stanbic/record", data={"only": G, f"acct_{G}": "83", f"dupok_{G}": "1"})
check("same payee but 7 days apart: not taken as the same (recorded)", len(POSTS) == n + 2)
check("same_text: bank wording vs QuickBooks payee", A.same_text("POS PURCHASE SHELL KAMPALA 123", "Shell Kampala")
      and not A.same_text("STATIONERY", "Airtime top-up") and not A.same_text("", ""))

# ---- the message counts what was recorded; recorded lines leave the list -------------------------------
upload([("2026-09-15", "PAPER", -1000), ("2026-09-16", "INK", -2000), ("2026-09-17", "PENS", -3000)])
P1, P2, P3 = lid("PAPER"), lid("INK"), lid("PENS")
n = len(POSTS)
cl.post("/account/Stanbic/record", data={"bulk": "1", "sel": [P1, P2, P3], f"acct_{P1}": "83", f"acct_{P2}": "83", f"acct_{P3}": ""})
m = msg()
check("bulk: 'Recorded 2 of 3 selected transactions'", "Recorded 2 of 3 selected transactions in QuickBooks." in m
      and "1 line has no account" in m and len(POSTS) == n + 2)
p = page()
check("…recorded lines leave the list, the one without an account stays", not in_list(p, P1) and not in_list(p, P2) and in_list(p, P3))
check("…because they're matched to what was recorded", matched(P1) and matched(P2))

# ---- long batches run in the background ---------------------------------------------------------------
upload([(f"2026-09-{d:02d}", f"ITEM {d}", -1111 * d) for d in range(18, 23)])
L = [lid(f"ITEM {d}") for d in range(18, 23)]
A.SYNC_IN_BACKGROUND = True
gate, entered = threading.Event(), threading.Event()
real_post = A.qbo_post
def slow_post(token, entity, body):
    entered.set(); gate.wait(10)
    return real_post(token, entity, body)
A.qbo_post = slow_post
n = len(POSTS)
r = cl.post("/account/Stanbic/record", data={"bulk": "1", "sel": L, **{f"acct_{x}": "83" for x in L}})
check("5 lines: the request returns straight away", r.status_code == 302 and entered.wait(10) and len(POSTS) == n)
job = json.loads(A.get_config("record_job:Stanbic"))
check("…a recording job is running", job["state"] == "running" and job["total"] == 5)
# (the shared test database can't serve two threads at once, so the page is checked after the job)
gate.set()
for t in [t for t in threading.enumerate() if t.name == "qbo-record"]:
    t.join(20)
A.SYNC_IN_BACKGROUND = False
A.qbo_post = real_post
job = json.loads(A.get_config("record_job:Stanbic"))
check("…finishes with the count", job["state"] == "done" and job["msg"].startswith("Recorded 5 of 5 selected transactions")
      and len(POSTS) == n + 5)
st = cl.get("/account/Stanbic/record_status").get_json()
check("status endpoint reports it", st["state"] == "done" and st["total"] == 5)
p = page()
check("the result is shown once on the page", "Recorded 5 of 5 selected transactions" in p)
check("…and not again on the next visit", "Recorded 5 of 5 selected transactions" not in page())
check("…the recorded lines are gone from the list", not any(in_list(p, x) for x in L))

# ---- one at a time; progress shown; a lost job reported ------------------------------------------------
A.set_config("record_job:Stanbic", json.dumps({"state": "running", "total": 10, "n": 3, "done": 3, "beat": time.time(),
                                               "started": time.time()}))
upload([("2026-09-25", "LATE ITEM", -900)])
LI = lid("LATE ITEM")
p = page()
check("while recording: progress shown", "id=recjob" in p and "<b class=rj-n>3</b> of 10 lines done" in p)
n = len(POSTS)
cl.post("/account/Stanbic/record", data={"only": LI, f"acct_{LI}": "83"})
check("…and a second recording waits for it", len(POSTS) == n and "Still recording 10 lines" in msg())
A.set_config("record_job:Stanbic", json.dumps({"state": "running", "total": 10, "n": 4, "done": 4,
                                               "beat": time.time() - 3600, "started": time.time() - 3700}))
m = msg()
check("a job lost to a restart: reported as stopped with how far it got", "Recording stopped before finishing (4 of 10 lines done)" in m)
cl.post("/account/Stanbic/record", data={"only": LI, f"acct_{LI}": "83"})
check("…and recording works again", len(POSTS) == n + 1)
sys.exit(T.summary())
