"""Speed at real size. A year's statement (1,500 lines) must not make a page ask the database more
questions than a short one -- every question is a round trip to a database far from the server --
and bulk actions on hundreds of rows must be done in a handful of queries, not one per row.
Also: connections are kept and reused, every page reports its database time (Server-Timing), and
pages that fail or run slow are written to the problems log shown under Settings.

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_scale.py`, or all suites with `python tests/run_all.py`.
"""
import io, os, random, sys, time
from datetime import date, timedelta

import psycopg2.extensions as E
import harness as H

STB, DF, SMALL = ("00000000-0000-0000-0000-0000000000a1", "00000000-0000-0000-0000-0000000000a2",
                  "00000000-0000-0000-0000-0000000000a3")
A, c = H.setup(H.account_sql((STB, "35", "Stanbic UGX", "bank"), (DF, "36", "DFCU UGX", "bank"),
                             (SMALL, "37", "Centenary UGX", "bank")))
T = H.Checker()
check = T.check
A.qbo_is_connected = lambda: False
A._store_coa([{"Id": i, "Name": n, "AccountType": "Bank", "CurrencyRef": {"value": "UGX"}}
              for i, n in (("35", "Stanbic UGX"), ("36", "DFCU UGX"), ("37", "Centenary UGX"))]
             + [{"Id": "83", "Name": "Office Supplies", "AccountType": "Expense", "CurrencyRef": {"value": "UGX"}}])

QN = {"n": 0, "log": []}
class Counting(E.cursor):
    def execute(self, sql, args=None):
        QN["n"] += 1
        if os.environ.get("DUMP"):
            QN["log"].append(" ".join(str(sql).split())[:120])
        return super().execute(sql, args)
c.cursor_factory = Counting
cur = c.cursor()
def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r

@A.app.route("/_boom")
def _boom():
    raise ValueError("kaboom on purpose")

random.seed(7)
def statement(acct, name, n, start=date(2026, 1, 1)):
    """n bank lines over six months; most fees have a book entry a little off (suggestions to review)."""
    rows = []
    for i in range(n):
        d = start + timedelta(days=random.randint(0, 180))
        k = random.random()
        if k < 0.3:
            rows.append((d, "EXCISE DUTY", -150))
        elif k < 0.75:
            rows.append((d, f"SCHOOL FEES STUDENT {i}", 250000 + i))
        else:
            rows.append((d, f"PAYMENT SUPPLIER {i % 40}", -(100000 + i * 7)))
    for i, (d, t, a) in enumerate(rows):
        if "STUDENT" in t and i % 3:
            q("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency,
                 description, counterparty, last_modified) VALUES (%s,%s,%s,'Deposit',%s,%s,'UGX',%s,%s,now()) RETURNING 1""",
              (A.ORG_ID, acct, f"{name}{i}", d + timedelta(days=1), a + (100 if i % 2 else 0), t.title(), t.title()))
    body = "Date,Description,Amount\n" + "".join(f"{d.isoformat()},{t},{a}\n" for d, t, a in rows)
    cl.post(f"/account/{name}/upload", data={"statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
            "period_start": "2026-01-01", "period_end": "2026-06-30"}, content_type="multipart/form-data")
    return rows

def measure(fn):
    QN["n"] = 0; QN["log"] = []
    t = time.perf_counter(); r = fn()
    if os.environ.get("DUMP"):
        import collections
        for k, v in collections.Counter(QN["log"]).most_common(25): print(f"      x{v:3d} {k}")
    return r, QN["n"], time.perf_counter() - t

cl = H.login(A)
statement(SMALL, "Centenary UGX", 150)
statement(STB, "Stanbic UGX", 1500)
statement(DF, "DFCU UGX", 600)
n_lines = q("SELECT count(*) FROM statement_line sl JOIN statement s USING (statement_id) WHERE s.account_id=%s", (STB,))[0][0]
check("the big statement loaded (1,500 lines)", n_lines == 1500)

# ---- pages: the number of database questions doesn't grow with the statement ----
cl.get("/account/Centenary UGX"); cl.get("/account/Stanbic UGX")      # first visit refreshes the matching
r_s, n_s, t_s = measure(lambda: cl.get("/account/Centenary UGX"))
r_b, n_b, t_b = measure(lambda: cl.get("/account/Stanbic UGX"))
print(f"    account page: 150 lines {n_s} queries {t_s:.2f}s; 1,500 lines {n_b} queries {t_b:.2f}s, {len(r_b.data) // 1024} KB")
check("account page: 1,500 lines ask no more than 150 do", r_b.status_code == 200 and n_b <= n_s + 3)
check("…and under 70 questions in all", n_b < 70)
check("…built in a reasonable time on the test database (under 15 s)", t_b < 15)
r_d, n_d, t_d = measure(lambda: cl.get("/"))
print(f"    dashboard: {n_d} queries {t_d:.2f}s")
check("dashboard: a dozen or so questions per account with a statement, not per line", r_d.status_code == 200 and n_d < 50 and t_d < 10)
import re
cur.execute(H.account_sql(*[(f"00000000-0000-0000-0000-0000000001{i:02d}", str(60 + i), f"Spare {i}", "bank") for i in range(10)])); c.commit()
r_d2, n_d2, _ = measure(lambda: cl.get("/"))
check("…and accounts with no statement this month cost no extra questions", n_d2 == n_d and "Spare 9" in r_d2.data.decode())
strip = lambda b: re.sub(r"updated [^<]*", "", b.decode())
A.DASH_WORKERS = 1
one_by_one = strip(cl.get("/").data)
A.DASH_WORKERS = 6
check("…the accounts summarised side by side come out exactly as one at a time", strip(cl.get("/").data) == one_by_one)
r_r, n_r, t_r = measure(lambda: cl.get("/account/Stanbic UGX/report"))
check("printable report: not per line either", r_r.status_code == 200 and n_r < 40)

# ---- bulk actions on hundreds of rows ----
sid = q("SELECT statement_id FROM statement WHERE account_id=%s", (STB,))[0][0]
mids = [m for m, in q("SELECT match_id::text FROM match WHERE statement_id=%s AND status='proposed'", (sid,))]
_, n_c, t_c = measure(lambda: cl.post("/account/Stanbic UGX/review_bulk", data={"status": "confirmed", "mid": mids}))
print(f"    confirm {len(mids)} suggestions: {n_c} queries {t_c:.2f}s")
check("confirming hundreds of suggestions at once: a few dozen queries, not one per suggestion",
      len(mids) >= 200 and n_c < 40 and q("SELECT count(*) FROM match WHERE statement_id=%s AND status='proposed'", (sid,))[0][0] == 0)
mine = [l for l, in q("SELECT line_id::text FROM statement_line WHERE statement_id=%s LIMIT 1000", (sid,))]
other = [l for l, in q("SELECT sl.line_id::text FROM statement_line sl JOIN statement s USING (statement_id) WHERE s.account_id=%s LIMIT 1000", (DF,))]
picks = [f"{a}|line:{b}" for a, b in zip(mine, other * 2)]
_, n_x, t_x = measure(lambda: cl.post("/account/Stanbic UGX/transfer_dismiss", data={"pick": picks}))
print(f"    'Not a transfer' on {len(picks)}: {n_x} queries {t_x:.2f}s")
check("'Not a transfer' on 1,000 suggestions: a handful of queries",
      n_x < 15 and q("SELECT count(*) FROM transfer_dismissal")[0][0] == len(picks))
_, n_y, _ = measure(lambda: cl.post("/account/Stanbic UGX/transfer_restore", data={"pick": picks}))
check("…and Restore too", n_y < 15 and q("SELECT count(*) FROM transfer_dismissal")[0][0] == 0)

# ---- every page reports its database time ----
st = r_b.headers.get("Server-Timing", "")
check("pages carry Server-Timing: database time, query count and total",
      st.startswith("db;dur=") and "queries" in st and "total;dur=" in st)

# ---- the problems log ----
r = cl.get("/_boom")
row = q("SELECT kind, path, detail, username FROM problem_log ORDER BY id DESC LIMIT 1")
check("a failing page: a plain message with its problem number, not a bare error",
      r.status_code == 500 and b"Something went wrong" in r.data and b"problem #" in r.data)
check("…logged with the page and what went wrong",
      row and row[0][0] == "error" and row[0][1] == "/_boom" and row[0][2].startswith("ValueError: kaboom on purpose"))
cl.get("/no-such-page")
check("a missing page (404) isn't logged as a problem", q("SELECT count(*) FROM problem_log WHERE path='/no-such-page'")[0][0] == 0)
A.SLOW_MS = 0
cl.get("/reports")
A.SLOW_MS = 5000
slow = q("SELECT kind, ms, queries, detail FROM problem_log WHERE path='/reports'")
check("a slow page is logged with its time and how much was the database",
      slow and slow[0][0] == "slow" and slow[0][1] is not None and "queries took" in slow[0][3])
h = cl.get("/settings").data.decode()
check("admins see the problems log under Settings", "Problems log" in h and "Error #" in h and "kaboom on purpose" in h and "/reports" in h)
q("INSERT INTO problem_log (kind, path) SELECT 'slow', '/x' FROM generate_series(1, 1005) RETURNING 1")
A.SLOW_MS = 0; cl.get("/reports"); A.SLOW_MS = 5000
check("the log keeps only its last 1,000 rows", q("SELECT count(*) FROM problem_log")[0][0] <= 1000)

# ---- kept connections ----
class Fake:
    made = []
    def __init__(self): self.closed, self.autocommit, self.rolled, self.pinged, self.dead = 0, False, 0, 0, False; Fake.made.append(self)
    def rollback(self): self.rolled += 1
    def close(self): self.closed = 1
    def cursor(self):
        me = self
        class K:
            def execute(self, sql, args=None):
                if me.dead: raise A.psycopg2.OperationalError("server closed the connection")
                me.pinged += 1
            def close(self): pass
        return K()
real_connect, get = A._connect, A._pooled_get_conn
A._connect = Fake
A._idle.clear()
a = get(); a_raw = Fake.made[-1]; a.close()
b = get()
check("a closed connection is handed to the next caller, not reopened", len(Fake.made) == 1 and b._c is a_raw and a_raw.rolled >= 1)
b.close(); b.close()
check("closing twice hands it back once", len(A._idle) == 1)
x, y = get(), get()
check("two at once: the second is a new one", len(Fake.made) == 2)
x.close(); y.close()
A._idle[:] = [(cn, t - A.POOL_CHECK_AFTER - 1) for cn, t in A._idle]
z = get()
check("one idle a while is checked with SELECT 1 before use", z._c.pinged == 1 and len(Fake.made) == 2)
z.close()
for cn, _ in A._idle: cn.dead = True
A._idle[:] = [(cn, t - A.POOL_CHECK_AFTER - 1) for cn, t in A._idle]
w = get()
check("…and one the server dropped is replaced, not handed out", w._c is Fake.made[-1] and len(Fake.made) == 3)
w.close(); A._idle.clear()
A._idle.append((Fake(), time.time() - A.POOL_MAX_IDLE - 1))
v = get()
check("one idle too long is hung up and a fresh one opened", Fake.made[-2].closed and v._c is Fake.made[-1])
v.close(); A._idle.clear()
many = [get() for _ in range(A.POOL_KEEP + 3)]
for m in many: m.close()
check("only a few are kept; the rest are closed", len(A._idle) == A.POOL_KEEP and sum(f.closed for f in Fake.made[-(A.POOL_KEEP + 3):]) == 3)
dropped = get(); del dropped
check("a connection never closed is simply dropped (nothing breaks)", len(A._idle) == A.POOL_KEEP - 1)
keep = list(A._idle)
A._idle_pid = -1
u = get()
check("in a new server process the parent's connections are left alone", u._c not in [cn for cn, _ in keep]
      and not any(cn.closed for cn, _ in keep))
u.close()
A._connect = real_connect; A._idle.clear()
sys.exit(T.summary())
