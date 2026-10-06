"""ReconBook redesign: the sidebar frame, dashboard, users & permissions (per-user ticks, enforcement,
switch-off and expiry, the second-person sign-off rule, the activity log), Settings (matching rules),
Reports, Search, the sign-in page, and the page's own dialog, menus and side panels (in jsdom).

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_redesign.py`, or all suites with `python tests/run_all.py`.
"""
import io, json, os, re, shutil, subprocess, sys, tempfile

import harness as H

STB, DF = "00000000-0000-0000-0000-0000000000a1", "00000000-0000-0000-0000-0000000000a2"
A, c = H.setup(H.account_sql((STB, "35", "Stanbic UGX", "bank"), (DF, "36", "DFCU UGX", "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
A.qbo_is_connected = lambda: False
A.set_config("company_name", "THE NORTH GREEN SCHOOL")
cur.execute("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency,
               description, counterparty, last_modified) VALUES (%s,%s,'b1','Purchase','2026-09-04',-80000,'UGX','Supplier X','Supplier X',now())""",
            (A.ORG_ID, STB))
c.commit()

def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
def msg(page):
    m = re.search(r"<div id=flash[^>]*>(.*?)</div>", page, re.S)
    return m.group(1) if m else ""
def upload(cl, name, rows):
    body = "Date,Description,Amount\n" + "".join(f"{d},{t},{a}\n" for d, t, a in rows)
    return cl.post(f"/account/{name}/upload", data={"replace": "1", "statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
                   "period_start": "2026-09-01", "period_end": "2026-09-30"}, content_type="multipart/form-data")

admin = H.login(A)
upload(admin, "Stanbic UGX", [("2026-09-04", "SUPPLIER X", -80000), ("2026-09-06", "MYSTERY", -1000)])

# ---- the frame and the dashboard ------------------------------------------------------------------------------------
dash = admin.get("/").data.decode()
check("sidebar: product, company from QuickBooks, accounts with status", "<b>ReconBook</b><small>THE NORTH GREEN SCHOOL</small>" in dash
      and 'sdot attn"></span><span class=nm>Stanbic UGX<small class=upto>not reconciled yet</small></span><span class=cnt>1</span>' in dash
      and 'sdot none"></span><span class=nm>DFCU UGX' in dash)
side = dash[dash.index("<aside class=side"):dash.index("</aside>")]
check("…accounts with no reconciliation first, then work in progress", side.index(">DFCU UGX<") < side.index(">Stanbic UGX<"))
tbl = dash[dash.index("<h2>Bank accounts</h2>"):]
check("…the dashboard table in the same order", tbl.index("<b>DFCU UGX</b>") < tbl.index("<b>Stanbic UGX</b>"))
check("…admins see Users and Settings", "Users &amp; permissions</a>" in dash and ">Settings</a>" in dash)
check("dashboard: month close, attention list, checklist, per-currency totals", "September 2026 close" in dash
      and "Needs your attention" in dash and "Month-end checklist" in dash and '<div class=code>UGX</div>' in dash)
check("…an account with no statement is flagged with an Upload button", "has no September 2026 statement yet" in dash
      and "/account/DFCU%20UGX?upload=1" in dash)
check("…progress per account", "1/2</span>" in dash)
check("the account page opens the upload panel when asked", "<aside class=drawer id=dr-upload  aria-label" in
      admin.get("/account/DFCU UGX?upload=1").data.decode().replace("hidden ", ""))
acct = admin.get("/account/Stanbic UGX").data.decode()
check("account page: header actions, summary strip with Sign off", "data-drawer=upload" in acct and "class=sumstrip id=dtiles" in acct
      and ">Sign off</button>" in acct and "Prepared by Admin" in acct)
check("…no browser confirm() boxes left anywhere", 'onsubmit="return confirm' not in acct)

# ---- users and permissions ------------------------------------------------------------------------------------------
r = admin.post("/users", data={"action": "save", "new": "1", "name": "Peter Ssali", "username": "peter.ssali",
                               "password": "secret1", "preset": "assistant", "title": "Accounts assistant"})
check("add a user from a preset", r.status_code == 302 and
      q("SELECT perms, title, is_admin FROM app_users WHERE username='peter.ssali'") == [("upload,review,record", "Accounts assistant", False)])
r = admin.post("/users", data={"action": "save", "new": "1", "name": "X", "username": "peter.ssali", "password": "secret1"})
check("…a username can't be taken twice", "already a user called peter.ssali" in r.data.decode())
peter = H.browserlike(A.app.test_client())
r = peter.post("/login", data={"username": "peter.ssali", "password": "secret1"})
check("the user signs in", r.status_code == 302)
pd = peter.get("/").data.decode()
check("…their sidebar has no Users or Settings", "Users &amp; permissions</a>" not in pd and ">Settings</a>" not in pd
      and "<small>Accounts assistant</small>" in pd)
check("…and those pages are refused", peter.get("/users").status_code == 403 and peter.get("/settings").status_code == 403)
peter.post("/account/Stanbic UGX/signoff")
check("signing off without the tick is refused, saying why", "allow ‘sign off’" in msg(peter.get("/account/Stanbic UGX").data.decode()))
admin.post("/users", data={"action": "perm", "username": "peter.ssali", "perm": "signoff", "on": "1"})
check("an admin ticks Sign off for them", "signoff" in q("SELECT perms FROM app_users WHERE username='peter.ssali'")[0][0])
upload(peter, "DFCU UGX", [("2026-09-10", "FEES", 5000)])
check("a statement records who prepared it", q("SELECT prepared_by FROM statement WHERE account_id=%s", (DF,)) == [("Peter Ssali",)])
peter.post("/account/DFCU UGX/signoff")
check("…and its preparer can't sign it off (second person)", "second person" in msg(peter.get("/account/DFCU UGX").data.decode()))
pg = peter.get("/account/DFCU UGX").data.decode()
admin.post("/account/DFCU UGX/signoff")
check("…an admin isn't held by that rule", "second person" not in msg(admin.get("/account/DFCU UGX").data.decode()))
admin.post("/settings", data={"action": "signoff", "close_day": "10"})       # two_person unticked
check("the rule can be switched off in Settings", A.rule("two_person") == 0)
peter.post("/account/DFCU UGX/signoff")
check("…then the preparer meets only the usual checks", "second person" not in msg(peter.get("/account/DFCU UGX").data.decode()))
admin.post("/settings", data={"action": "signoff", "two_person": "1", "close_day": "10"})
q("UPDATE statement SET signed_off_at=now() WHERE account_id=%s RETURNING 1", (DF,))
q("UPDATE statement SET signed_off_at=now() - interval '1 day' WHERE account_id=%s RETURNING 1", (STB,))
side = admin.get("/").data.decode(); side = side[side.index("<aside class=side"):side.index("</aside>")]
check("signed off last, the most recent sign-off at the bottom", side.index(">Stanbic UGX<") < side.index(">DFCU UGX<"))
q("UPDATE statement SET signed_off_at=NULL RETURNING 1")
admin.post("/users", data={"action": "perm", "username": "peter.ssali", "perm": "record", "on": "0"})
peter.post("/account/Stanbic UGX/record", data={"only": q("SELECT line_id::text FROM statement_line WHERE description='MYSTERY'")[0][0]})
check("taking a tick away applies at once", "allow ‘record in quickbooks’" in msg(peter.get("/account/Stanbic UGX").data.decode()))
admin.post("/users", data={"action": "active", "username": "peter.ssali", "on": "0"})
check("switching a user off signs them out", peter.get("/").status_code == 302)
check("…and they can't sign in", "switched off or has expired" in peter.post("/login", data={"username": "peter.ssali", "password": "secret1"}).data.decode())
admin.post("/users", data={"action": "active", "username": "peter.ssali", "on": "1"})
admin.post("/users", data={"action": "save", "username": "peter.ssali", "name": "Peter Ssali", "expires": "2020-01-01"})
check("an expired user can't sign in", "switched off or has expired" in peter.post("/login", data={"username": "peter.ssali", "password": "secret1"}).data.decode())
admin.post("/users", data={"action": "admin", "username": "peter.ssali", "on": "1"})
check("make admin", q("SELECT is_admin FROM app_users WHERE username='peter.ssali'") == [(True,)])
admin.post("/users", data={"action": "delete", "username": "peter.ssali"})
check("remove a user", q("SELECT count(*) FROM app_users WHERE username='peter.ssali'") == [(0,)])
up = admin.get("/users").data.decode()
check("activity log: uploads, permission changes, sign-in switched off", "uploaded a statement (2 lines)" in up
      and "allowed ‘sign off’ for Peter Ssali" in up and "switched off the sign-in of Peter Ssali" in up)

# ---- settings -------------------------------------------------------------------------------------------------------
admin.post("/settings", data={"action": "rules", "date_days": "5", "clear_days": "999", "transfer_days": "x"})
check("matching rules saved, kept in range; an unticked box is off", A.rule("date_days") == 5 and A.rule("clear_days") == 120
      and A.rule("transfer_days") == 4 and A.rule("charges_exact") == 0)
st = admin.get("/settings").data.decode()
check("settings page: QuickBooks card, rules, sign-off, backup", "QuickBooks Online" in st and 'name=date_days value="5"' in st
      and "Sign-off and month end" in st and "Download a backup now" in st)
admin.post("/settings", data={"action": "rules", "date_days": "3", "clear_days": "31", "transfer_days": "4", "charges_exact": "1"})
check("settings lists the bank accounts with Show ticks", "<h2>Bank accounts <span class=info" in st and st.count("class=acc name=active") == 2)
r = admin.post("/accounts", data={"to": "settings", "active": [STB]})
check("hiding one from Settings returns there and hides it", r.headers["Location"].endswith("/settings#banks")
      and q("SELECT is_active FROM account WHERE account_id=%s", (DF,)) == [(False,)]
      and ">DFCU UGX<" not in admin.get("/").data.decode().split("</aside>")[0])
admin.post("/accounts", data={"to": "settings", "active": [STB, DF]})
check("…and ticking it again brings it back", ">DFCU UGX<" in admin.get("/").data.decode().split("</aside>")[0])

# ---- reports, search, sign-in ---------------------------------------------------------------------------------------
rp = admin.get("/reports").data.decode()
check("reports list every reconciliation with who prepared it", "Stanbic UGX" in rp and "Peter Ssali" in rp and "Open report" in rp)
sr = admin.get("/search?q=80,000").data.decode()
check("search by amount finds the bank line and the QuickBooks entry", "SUPPLIER X" in sr and "Supplier X" in sr)
check("search by text", "MYSTERY" in admin.get("/search?q=myst").data.decode())
li = A.app.test_client().get("/login").data.decode()
check("sign-in page: ReconBook, the company, a show-password eye", "<b>ReconBook</b>" in li and "THE NORTH GREEN SCHOOL" in li
      and "class=pw-toggle" in li and 'aria-label="Show password"' in li)

# ---- one reconciliation per account per period; Reports actions ------------------------------------------------------
n0 = q("SELECT count(*) FROM statement WHERE account_id=%s", (STB,))[0][0]
r = upload(admin, "Stanbic UGX", [("2026-09-10", "SUPPLIER X", -80000), ("2026-09-12", "MYSTERY", -1000)])
m = msg(admin.get("/account/Stanbic UGX").data.decode())
check("re-uploading overlapping dates replaces the open reconciliation (one per period)",
      q("SELECT count(*) FROM statement WHERE account_id=%s", (STB,))[0][0] == n0 and "replaces the earlier reconciliation" in m)
sid = q("SELECT statement_id::text FROM statement WHERE account_id=%s", (STB,))[0][0]
q("UPDATE statement SET signed_off_at=now(), signed_off_by='Jane' WHERE statement_id=%s RETURNING 1", (sid,))
upload(admin, "Stanbic UGX", [("2026-09-20", "LATE", -5)])
check("…but never over a signed-off one", "already has a signed-off reconciliation" in msg(admin.get("/account/Stanbic UGX").data.decode())
      and q("SELECT statement_id::text FROM statement WHERE account_id=%s", (STB,)) == [(sid,)])
rp = admin.get("/reports").data.decode()
check("reports: each row has Open report and a menu", rp.count("Open report</a>") >= 2 and "History of this account" in rp)
check("…a signed-off one offers Undo sign-off, not Delete", "Undo sign-off</button>" in rp and "undo the sign-off to delete it" in rp)
admin.post("/reports/delete", data={"s": sid})
check("…deleting a signed-off one is refused", q("SELECT count(*) FROM statement WHERE statement_id=%s", (sid,))[0][0] == 1)
r = admin.post(f"/account/Stanbic UGX/reopen", headers={"Referer": "http://localhost/reports"})
check("undo sign-off from Reports returns to Reports", r.headers["Location"].endswith("/reports")
      and q("SELECT signed_off_at FROM statement WHERE statement_id=%s", (sid,)) == [(None,)])
# an old duplicate (from before the rule) is flagged so it can be deleted
dup = q("""INSERT INTO statement (org_id, account_id, period_start, period_end, opening_balance, closing_balance, currency, created_at)
           VALUES (%s,%s,'2026-09-05','2026-09-25',0,0,'UGX', now() - interval '1 day') RETURNING statement_id::text""", (A.ORG_ID, STB))[0][0]
rp = admin.get("/reports").data.decode()
check("overlapping reconciliations are flagged on Reports", "Overlaps another" in rp and "overlap another for the same account" in rp.replace("overlaps another", "overlap another"))
admin.post("/reports/delete", data={"s": dup})
check("delete one: gone from the app, the other kept", q("SELECT count(*) FROM statement WHERE statement_id=%s", (dup,))[0][0] == 0
      and q("SELECT count(*) FROM statement WHERE statement_id=%s", (sid,))[0][0] == 1 and "Overlaps another" not in admin.get("/reports").data.decode())

# ---- the page's dialog, menus and panels in a browser ---------------------------------------------------------------
node = shutil.which("node")
if not node or not os.path.isdir(os.path.join(H.HERE, "node_modules", "jsdom")):
    check("jsdom available (run: cd tests && npm install)", False)
else:
    JS = r"""
const { JSDOM, VirtualConsole } = require("jsdom");
const errors = []; const vc = new VirtualConsole(); vc.on("jsdomError", e => { const m = String(e.message || e); if (!/Not implemented/.test(m)) errors.push(m); });
const dom = new JSDOM(require("fs").readFileSync(process.argv[2], "utf8"), { runScripts: "dangerously", virtualConsole: vc, pretendToBeVisual: true, url: "http://app.test/account/Stanbic%20UGX" });
const w = dom.window, d = w.document; w.HTMLElement.prototype.scrollIntoView = function () {};
const out = {};
// a form with data-confirm asks in the page dialog; Cancel stops it, Confirm sends it
const f = d.querySelector('form[action$="/clear"]');
let sent = 0; f.addEventListener("submit", e => { if (!e.defaultPrevented) sent++; e.preventDefault(); });
const ev = new w.SubmitEvent("submit", { cancelable: true, bubbles: true, submitter: f.querySelector("button") }); f.dispatchEvent(ev);
const dlg = d.getElementById("rb-dlg");
out.asked = !dlg.hidden; out.title = d.getElementById("rb-dlg-t").textContent; out.yes = d.getElementById("rb-yes").textContent;
out.danger = d.getElementById("rb-yes").classList.contains("danger"); out.stopped = ev.defaultPrevented && sent === 0;
d.getElementById("rb-no").click(); out.cancelled = dlg.hidden && sent === 0;
f.dispatchEvent(new w.SubmitEvent("submit", { cancelable: true, bubbles: true, submitter: f.querySelector("button") }));
d.getElementById("rb-yes").click(); out.sentAfterYes = sent;
// the account menu opens and closes
const kb = d.querySelector('.ph .kebab [data-dd]'), dd = kb.parentNode.querySelector(".dd");
kb.click(); out.ddOpen = !dd.hidden; d.body.click(); out.ddClosed = dd.hidden;
// the upload panel opens from the header and closes with Escape
d.querySelector('[data-drawer=upload]').click(); out.drOpen = !d.getElementById("dr-upload").hidden;
d.dispatchEvent(new w.KeyboardEvent("keydown", { key: "Escape", bubbles: true })); out.drClosed = d.getElementById("dr-upload").hidden;
// the result notice: a success fades, a problem stays with a close button
const fl = d.getElementById("flash"); out.flash = fl ? fl.textContent : null; out.flashErr = fl ? fl.classList.contains("err") : null;
out.errors = errors; console.log(JSON.stringify(out));
"""
    mystery = q("SELECT line_id::text FROM statement_line WHERE description='MYSTERY'")[0][0]
    admin.post("/account/Stanbic UGX/record", data={"only": mystery})     # no account chosen: a problem notice
    html = admin.get("/account/Stanbic UGX").data.decode()
    f = tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8"); f.write(html); f.close()
    g = tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, dir=H.HERE, encoding="utf-8"); g.write(JS); g.close()
    try:
        res = subprocess.run([node, g.name, f.name], capture_output=True, text=True, encoding="utf-8", cwd=H.HERE)
    finally:
        os.unlink(f.name); os.unlink(g.name)
    if res.stderr.strip():
        print(res.stderr[-1500:])
    o = json.loads([l for l in res.stdout.splitlines() if l.startswith("{")][-1]) if "{" in res.stdout else {}
    print("   ", o)
    check("browser: a confirm-first form asks in the page dialog, not the browser's box", o.get("asked") and o.get("stopped")
          and (o.get("title") or "").startswith("Clear this account's data?"))
    check("browser: …a destructive action gets a red button named for it", o.get("danger") and o.get("yes") == "Clear")
    check("browser: …Cancel sends nothing; Confirm sends it once", o.get("cancelled") and o.get("sentAfterYes") == 1)
    check("browser: the account menu opens and closes", o.get("ddOpen") and o.get("ddClosed"))
    check("browser: the upload side panel opens and closes", o.get("drOpen") and o.get("drClosed"))
    check("browser: a problem notice stays (styled as a problem)", o.get("flashErr") is True and "Not recorded" in (o.get("flash") or ""))
    check("browser: no script errors", o.get("errors") == [])
# ---- one way to upload on each page ----------------------------------------------------------------
c.cursor().execute(H.account_sql(("00000000-0000-0000-0000-0000000000e9", "99", "Empty Bank", "bank"))); c.commit()
q_ = c.cursor(); q_.execute("SELECT a.name, count(s.statement_id) FROM account a LEFT JOIN statement s USING (account_id) GROUP BY 1"); c.rollback()
for nm, n_st in q_.fetchall():
    pg = admin.get(f"/account/{nm}").data.decode()
    check(f"{nm} ({'a statement' if n_st else 'no statement yet'}): one Upload statement button, not two",
          pg.count("data-drawer=upload") == 1)
sys.exit(T.summary())
