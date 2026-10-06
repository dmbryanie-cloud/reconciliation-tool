"""A recording cut off by a server restart (a deploy) carries on by itself: the next server finds the
job silent, claims it, and records what's left of the same selection. Lines already recorded aren't
sent again, and the line being sent when it stopped stays blocked until someone checks QuickBooks.

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_record_resume.py`, or all suites with `python tests/run_all.py`.
"""
import io, json, re, sys, threading, time

import harness as H

ACCT = "00000000-0000-0000-0000-0000000000a1"
A, c = H.setup(H.account_sql((ACCT, "35", "Stanbic UGX 10202", "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
A._store_coa([{"Id": "35", "Name": "Stanbic UGX 10202", "AccountType": "Bank", "CurrencyRef": {"value": "UGX"}},
              {"Id": "83", "Name": "Bank charges UGX", "AccountType": "Expense", "CurrencyRef": {"value": "UGX"}}])
POSTS = []
def fake_post(token, entity, body):
    POSTS.append((entity, body))
    return {entity: {"Id": str(700 + len(POSTS))}}
A.qbo_post = fake_post
A.qbo_token = lambda: "tok"
A.qbo_realm = lambda: "REAL-1"
A.qbo_is_connected = lambda: False
NAME = "Stanbic UGX 10202"
KEY, ARGS = f"record_job:{NAME}", f"record_args:{NAME}"


def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
def lid(desc):
    return str(q("SELECT line_id FROM statement_line WHERE description=%s", (desc,))[0][0])
def wb(line):
    r = q("SELECT status FROM writeback_log WHERE line_id=%s", (line,))
    return r[0][0] if r else None
def join_jobs():
    for t in [t for t in threading.enumerate() if t.name == "qbo-record"]:
        t.join(20)

cl = H.login(A)
body = "Date,Description,Amount\n" + "".join(f"2025-11-{d:02d},EXCISE DUTY {d},-{d}00\n" for d in range(1, 9))
cl.post(f"/account/{NAME}/upload", data={"replace": "1", "statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
        "period_start": "2025-11-01", "period_end": "2025-11-30"}, content_type="multipart/form-data")
L = [lid(f"EXCISE DUTY {d}") for d in range(1, 9)]
form = {"bulk": "1", **{f"acct_{x}": "83" for x in L}, f"acct_{'0' * 8}-0000-0000-0000-{'9' * 12}": "83"}   # + a stray line's choice

# ---- the selection is kept with the job --------------------------------------------------------------
A.SYNC_IN_BACKGROUND = True
gate = threading.Event()
real_run = A._record_run
def held_run(*a, **k):
    gate.wait(10)
    return real_run(*a, **k)
A._record_run = held_run
cl.post(f"/account/{NAME}/record", data={**form, "sel": L})
args = json.loads(A.get_config(ARGS) or "null")
check("a background recording keeps its selection: the lines and their choices", args and args["ids"] == L
      and all(args["form"].get(f"acct_{x}") == "83" for x in L))
check("…but not choices for lines it isn't recording", not any("9999" in k for k in args["form"]))
gate.set(); join_jobs()
A._record_run = real_run
A.SYNC_IN_BACKGROUND = False
check("…and forgets it once finished", not A.get_config(ARGS) and json.loads(A.get_config(KEY))["state"] == "done")

# ---- a recording cut off by a restart ------------------------------------------------------------------
# Start again from scratch: the first 3 recorded, the 4th was being sent when the server stopped.
q("DELETE FROM match RETURNING 1"); q("DELETE FROM writeback_log RETURNING 1"); q("DELETE FROM book_txn RETURNING 1")
A.run_matcher(q("SELECT statement_id FROM statement")[0][0])
cl.post(f"/account/{NAME}/record", data={**form, "sel": L[:3]})
assert all(wb(x) == "done" for x in L[:3])
A._claim_writeback(L[3], "SUPER ADMIN")                      # in flight at the restart: may or may not be in QuickBooks
A.set_config(ARGS, json.dumps({"form": form, "ids": L, "user": "SUPER ADMIN"}))
dead = {"state": "running", "by": "SUPER ADMIN", "started": time.time() - 400, "beat": time.time() - 60,
        "total": 8, "n": 4, "done": 3, "msg": ""}
A.set_config(KEY, json.dumps(dead))
n = len(POSTS)
check("a job that went quiet a minute ago isn't taken over (its server may just be slow)", A.resume_record_jobs() == [])
dead["beat"] = time.time() - A.RECORD_RESUME_SECS - 5
A.set_config(KEY, json.dumps(dead))
check("silent for 3 minutes: resumed", A.resume_record_jobs() == [NAME])
check("…claimed, so another server checking now leaves it alone", A.resume_record_jobs() == [])
join_jobs()
check("…only the lines never sent are recorded (4 of them)", len(POSTS) == n + 4 and all(wb(x) == "done" for x in L[4:]))
check("…lines recorded before the restart aren't sent again", all(wb(x) == "done" for x in L[:3]))
check("…the line in flight at the restart stays blocked for checking", wb(L[3]) == "pending")
job = json.loads(A.get_config(KEY))
check("…the job finishes, saying it carried on and what happened", job["state"] == "done" and job.get("resumes") == 1
      and job["msg"].startswith("Recording carried on after the server restarted (3 lines recorded before it).")
      and "Recorded 4 of" in job["msg"] and "Skipped 1 already recorded or in progress" in job["msg"])
p = cl.get(f"/account/{NAME}").data.decode()
check("…shown once on the account page", "Recording carried on after the server restarted" in p)
check("…with the interrupted line flagged for checking", "Recording was interrupted — check QuickBooks before trying again" in p)

# ---- limits -------------------------------------------------------------------------------------------
A.set_config(ARGS, "")
A.set_config(KEY, json.dumps(dict(dead, beat=time.time() - 999)))
check("no saved selection (a job from before this version): not resumed", A.resume_record_jobs() == [])
A.set_config(ARGS, json.dumps({"form": form, "ids": L, "user": "x"}))
A.set_config(KEY, json.dumps(dict(dead, beat=time.time() - 999, resumes=A.RECORD_MAX_RESUMES)))
check(f"resumed {A.RECORD_MAX_RESUMES} times already: left to stop (reported as stopped)", A.resume_record_jobs() == []
      and A.record_job(NAME)["state"] == "stalled")
A.set_config(KEY, json.dumps(dict(dead, state="done", beat=time.time() - 999)))
check("a finished job is never resumed", A.resume_record_jobs() == [])
check("the watcher doesn't start in the tests (no background threads on the shared connection)",
      not any(t.name == "resume-watch" for t in threading.enumerate()))
sys.exit(T.summary())
