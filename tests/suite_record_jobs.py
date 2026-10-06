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
    r = cl.post("/account/Stanbic/upload", data={"replace": "1", "statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
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
book("g1", "2026-09-19", -7000, "Generator fuel")          # same payee, 7 days apart: beyond 3 days
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
upload([("2026-09-15", "PAPER", -1100), ("2026-09-16", "INK", -2200), ("2026-09-17", "PENS", -3300)])
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
check("while recording: a progress bar with the percentage done", "id=recjob" in p and "<b class=rj-pct>30%</b>" in p
      and 'aria-valuenow="30"><i style="width:30%">' in p and "lines done" not in p)
import os, shutil, subprocess, tempfile
node = shutil.which("node")
if node and os.path.isdir(os.path.join(H.HERE, "node_modules", "jsdom")):
    JS = r"""
const { JSDOM } = require("jsdom");
const dom = new JSDOM(require("fs").readFileSync(process.argv[2], "utf8"), { runScripts: "dangerously", pretendToBeVisual: true,
  beforeParse(w) { w.fetch = () => Promise.resolve({ json: () => ({ state: "running", n: 7, total: 10 }) });
                   const st = w.setTimeout; w.setTimeout = (f, ms) => st(f, ms > 100 ? 5 : ms); } });
setTimeout(() => { const d = dom.window.document;
  console.log(JSON.stringify({ pct: d.querySelector(".rj-pct").textContent, w: d.querySelector(".rj-bar i").style.width,
                               aria: d.querySelector(".rj-bar").getAttribute("aria-valuenow") })); process.exit(0); }, 300);
"""
    f = tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8"); f.write(p); f.close()
    g = tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, dir=H.HERE, encoding="utf-8"); g.write(JS); g.close()
    try:
        res = subprocess.run([node, g.name, f.name], capture_output=True, text=True, encoding="utf-8", cwd=H.HERE)
    finally:
        os.unlink(f.name); os.unlink(g.name)
    o = json.loads([l for l in res.stdout.splitlines() if l.startswith("{")][-1]) if "{" in res.stdout else {}
    check("…and the bar moves as lines are recorded (70% after 7 of 10)", o == {"pct": "70%", "w": "70%", "aria": "70"})
else:
    print("SKIP browser check (no node/jsdom)")
n = len(POSTS)
cl.post("/account/Stanbic/record", data={"only": LI, f"acct_{LI}": "83"})
check("…and a second recording waits for it", len(POSTS) == n and "Still recording 10 lines" in msg())
A.set_config("record_job:Stanbic", json.dumps({"state": "running", "total": 10, "n": 4, "done": 4,
                                               "beat": time.time() - 3600, "started": time.time() - 3700}))
m = msg()
check("a job lost to a restart: reported as stopped with how far it got", "Recording stopped before finishing (4 of 10 lines done)" in m)
cl.post("/account/Stanbic/record", data={"only": LI, f"acct_{LI}": "83"})
check("…and recording works again", len(POSTS) == n + 1)
# ---- bank charges pair on the exact date only ------------------------------------------------------------
book("c0", "2026-09-26", -300, "Excise duty")          # a day before the bank's charge
book("n0", "2026-09-26", -333, "Textbooks")            # an ordinary payment, a day before
upload([("2026-09-27", "GOVERNMENT EXCISE DUTY CHARGE", -300), ("2026-09-27", "TEXTBOOKS", -333),
        ("2026-09-28", "CASH WITHDRAWAL FEE", -3500), ("2026-09-29", "INWARD EFT FEE", -2300)])
C1, N1, C2, C3 = lid("GOVERNMENT EXCISE DUTY CHARGE"), lid("TEXTBOOKS"), lid("CASH WITHDRAWAL FEE"), lid("INWARD EFT FEE")
check("a charge isn't matched to the same amount a day earlier: only suggested, as charges with different dates",
      not matched(C1) and q("""SELECT m.status, m.confidence FROM match_statement_line msl JOIN match m USING (match_id)
      WHERE msl.line_id=%s""", (C1,)) == [("proposed", A.Decimal(str(A.CHARGE_GROUP_CONF)))])
check("…an ordinary payment still is", matched(N1) == [("exact", "engine")])
check("is_bank_charge: fees and duty going out; not school fees coming in, cheques or EFT payments",
      A.is_bank_charge("FEE  ACH INWD CR", -4000) and A.is_bank_charge("MONTHLY MANAGEMENT FEE", -36000)
      and not A.is_bank_charge("SCHOOL FEES TERM 3", 500000) and not A.is_bank_charge("CHQW FEES REFUND", -9000)
      and not A.is_bank_charge("EFT: 12:PAULA:EDUCATIONAL FEES", -2088) and not A.is_bank_charge("LOAN RECOVERY", -5000))
book("c2", "2026-09-27", -3500, "Cash withdrawal fee")  # already in QuickBooks, but a day off
book("c3", "2026-09-29", -2300, "Inward EFT fee")       # already in QuickBooks, same day
n = len(POSTS)
cl.post("/account/Stanbic/record", data={"only": C2, f"acct_{C2}": "83"})
check("recording a charge: an entry a day off isn't taken as it (recorded, no duplicate warning)",
      len(POSTS) == n + 1 and "already in QuickBooks" not in msg())
cl.post("/account/Stanbic/record", data={"only": C3, f"acct_{C3}": "83"})
check("…one on the same day is paired with it; nothing created", len(POSTS) == n + 1 and matched(C3))

# ---- two identical charges on one day: each recorded entry stays with its own line --------------------------
upload([("2026-09-30", "CASH DEPOSIT CHARGES", -925), ("2026-09-30", "CASH DEPOSIT CHARGES", -925)])
D1, D2 = [str(r[0]) for r in q("""SELECT sl.line_id FROM statement_line sl JOIN statement s USING (statement_id)
                                   WHERE sl.amount=-925 ORDER BY s.created_at DESC, sl.line_id LIMIT 2""")]
sid = q("SELECT statement_id FROM statement ORDER BY created_at DESC LIMIT 1")[0][0]
for x in (D2, D1):
    cl.post("/account/Stanbic/record", data={"only": x, f"acct_{x}": "83"})
    A.run_matcher(str(sid))
own = lambda x: q("""SELECT bt.source_txn_id FROM match_statement_line msl JOIN match m USING (match_id)
                     JOIN match_book_txn USING (match_id) JOIN book_txn bt USING (txn_id)
                     WHERE msl.line_id=%s AND m.status='confirmed'""", (x,))
wl = lambda x: q("SELECT qbo_id FROM writeback_log WHERE line_id=%s", (x,))
check("identical charges: each line keeps the entry recorded for it through a re-match",
      own(D1) == wl(D1) and own(D2) == wl(D2) and own(D1) != own(D2))
# The old way (before the fix): a line logged as recorded whose entry an identical line took.
q("DELETE FROM match WHERE match_id IN (SELECT match_id FROM match_statement_line WHERE line_id IN (%s,%s)) RETURNING 1", (D1, D2))
gone = wl(D2)[0][0]                                   # QuickBooks has only one of the two
q("UPDATE writeback_log SET qbo_id=%s WHERE line_id=%s RETURNING 1", (wl(D1)[0][0], D2))
q("DELETE FROM book_txn WHERE source_txn_id=%s RETURNING 1", (gone,))
A.run_matcher(str(sid))
taken = [x for x in (D1, D2) if not own(x)]
check("an entry taken by an identical line is flagged, with a way to record the line again",
      len(taken) == 1 and "was paired with another identical line" in page())
n = len(POSTS)
cl.post("/account/Stanbic/record_reset", data={"reset": taken[0]})
cl.post("/account/Stanbic/record", data={"only": taken[0], f"acct_{taken[0]}": "83"})
check("…and it can be recorded again", len(POSTS) == n + 1 and own(taken[0]))

# ---- switching accounts from the account page --------------------------------------------------------
q(H.account_sql(("00000000-0000-0000-0000-0000000000a2", "36", "DFCU Idle", "bank")) + " SELECT 1")
p = page()
side = re.search(r"<aside class=side.*?</aside>", p, re.S)
check("sidebar lists the accounts, this one highlighted", side and re.search(r'<a href="/account/Stanbic" class="on"', side.group(0)))
check("…each with its status: one with no statement is marked so", side and re.search(r'sdot none"></span><span class=nm>DFCU Idle', side.group(0)))
r = cl.get("/switch?name=DFCU Idle")
check("…and opens the chosen account", r.status_code == 302 and r.headers["Location"].endswith("/account/DFCU%20Idle"))
sys.exit(T.summary())
