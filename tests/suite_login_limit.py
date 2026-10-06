"""Wrong sign-ins are limited (a username is paused after 5 wrong passwords in 15 minutes, an internet address
after 20); recovery-password sign-ins are logged; the admins' improvements list in Settings.

Run on its own with `python tests/suite_login_limit.py`, or all suites with `python tests/run_all.py`.
"""
import html, sys

import harness as H

A, c = H.setup(H.account_sql(("00000000-0000-0000-0000-0000000000a1", "35", "Stanbic", "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
A.qbo_is_connected = lambda: False


def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
A.add_user("grace", "Grace", "secret1", False)
def attempt(pw, un="grace", ip="10.0.0.1"):
    cl = H.browserlike(A.app.test_client())
    r = cl.post("/login", data={"username": un, "password": pw}, headers={"X-Forwarded-For": ip})
    return r.status_code, html.unescape(r.data.decode())

for _ in range(4):
    attempt("wrong")
check("4 wrong passwords: the right one still works", attempt("secret1")[0] == 302)
check("…and a success clears the count", not q("SELECT 1 FROM login_fail WHERE key='user:grace'"))
for _ in range(5):
    attempt("wrong")
st, body = attempt("secret1")
check("5 wrong: paused, even with the right password, saying how long", st == 429 and "Too many wrong passwords. Wait 15 minutes" in body)
check("…another username from the same address isn't paused", attempt("x", un="peter")[0] == 200)
q("UPDATE login_fail SET at = at - interval '16 minutes' WHERE key='user:grace' RETURNING 1")
check("after 15 minutes: signs in again", attempt("secret1")[0] == 302)

for i in range(20):
    attempt("wrong", un=f"guess{i}", ip="10.9.9.9")
check("20 wrong from one address, any usernames: that address is paused",
      attempt("secret1", ip="10.9.9.9")[0] == 429 and attempt("secret1", ip="10.0.0.2")[0] == 302)

st, _ = attempt(H.PASSWORD, un="", ip="10.0.0.3")
check("the recovery password signs in, and it's in the activity log", st == 302 and
      q("SELECT 1 FROM activity_log WHERE action LIKE 'signed in with the recovery password (from 10.0.0.3)'"))

gc = H.browserlike(A.app.test_client()); gc.post("/login", data={"username": "grace", "password": "secret1"})
for _ in range(5):
    gc.post("/change-password", data={"current": "nope", "new": "secret2", "confirm": "secret2"})
r = gc.post("/change-password", data={"current": "secret1", "new": "secret2", "confirm": "secret2"})
check("changing a password: the current-password check is limited too", "Too many wrong passwords" in html.unescape(r.data.decode()))

# ---- the improvements list ------------------------------------------------------------------------------------------
admin = H.login(A)
p = html.unescape(admin.get("/settings").data.decode())
check("Settings lists the improvements to do, sign-in limits already done",
      "Improvements to do" in p and "1 of 8 done" in p and "3. Limit wrong sign-ins" in p and "Done " in p
      and "4. Change the recovery password" in p and "1. A test copy of the app" in p)
admin.post("/settings", data={"action": "todo", "key": "backups", "on": "1"})
p = html.unescape(admin.get("/settings").data.decode())
check("ticking one off records who and when", "2 of 8 done" in p and q("SELECT done_by FROM admin_todo WHERE key='backups'")
      == [("Admin",)] and q("SELECT 1 FROM activity_log WHERE action LIKE 'ticked off the improvement%%'"))
admin.post("/settings", data={"action": "todo", "key": "backups", "on": "0"})
check("…and unticking reopens it", "1 of 8 done" in html.unescape(admin.get("/settings").data.decode()))
check("only admins see Settings", gc.get("/settings").status_code in (302, 403))
sys.exit(T.summary())
