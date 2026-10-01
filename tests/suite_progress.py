"""Following progress: each account shows the date it's reconciled up to (the end of its latest signed-off
reconciliation) on the dashboard, in the sidebar and on its page; an open one shows how far its bank lines
are cleared; and Save & finish later keeps the work (with the record table's choices) for another time.
Also: a book balance from QuickBooks is filled in / kept current by each sync.

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_progress.py`, or all suites with `python tests/run_all.py`.
"""
import io, re, sys
from decimal import Decimal as D

import harness as H

STB, DF = "00000000-0000-0000-0000-0000000000a1", "00000000-0000-0000-0000-0000000000a2"
A, c = H.setup(H.account_sql((STB, "35", "Stanbic UGX", "bank"), (DF, "36", "DFCU UGX", "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
A.qbo_is_connected = lambda: False
A.qbo_token = lambda: "tok"

def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
cl = H.login(A)
def upload(acct_name, rows, ps, pe):
    body = "Date,Description,Amount\n" + "".join(f"{d},{t},{a}\n" for d, t, a in rows)
    r = cl.post(f"/account/{acct_name}/upload", data={"statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
                "period_start": ps, "period_end": pe}, content_type="multipart/form-data")
    assert r.status_code == 302, r.data[:300]
def text(path):
    return re.sub(r"\s+", " ", cl.get(path).data.decode())

# August signed off; September in progress
upload("Stanbic UGX", [("2026-08-05", "AUG FEES", 1000)], "2026-08-01", "2026-08-31")
q("UPDATE statement SET signed_off_at=now(), signed_off_by='Jane' RETURNING 1")
upload("Stanbic UGX", [("2026-09-03", "UMEME", -500), ("2026-09-10", "WATER", -200), ("2026-09-20", "FEES", 3000)],
       "2026-09-01", "2026-09-30")

p = text("/account/Stanbic UGX")
check("the account page says how far it's reconciled", "Reconciled to <b>31/08/2026</b>" in p)
check("…and how far this statement is cleared (to the day before its first open line)", "Cleared to <b>02/09/2026</b>" in p)
d = text("/")
check("the dashboard shows each account's reconciled-to date under its name", "Reconciled to <b>31/08/2026</b>" in d)
check("…an account never signed off says so", "Not reconciled yet</div>" in d)
check("the sidebar shows each account's date", "to 31/08/2026</small>" in d and "not reconciled yet</small>" in d)

# match the first line by hand: cleared to the day before the next open line
sid = q("SELECT statement_id FROM statement WHERE signed_off_at IS NULL")[0][0]
(l1,) = q("SELECT line_id FROM statement_line WHERE description='UMEME'")[0]
q("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency, description,
     last_modified) VALUES (%s,%s,'b1','Purchase','2026-09-03',-500,'UGX','Umeme',now()) RETURNING 1""", (A.ORG_ID, STB))
(tid,) = q("SELECT txn_id FROM book_txn WHERE source_txn_id='b1'")[0]
cl.post("/account/Stanbic UGX/match", data={"ml": [str(l1)], "mb": [str(tid)]})
check("as lines are matched, it moves on: cleared to the day before the next open line",
      "Cleared to <b>09/09/2026</b>" in text("/account/Stanbic UGX"))
check("…shown on the dashboard too", "Cleared to 09/09/2026" in text("/"))

# Save & finish later
p = text("/account/Stanbic UGX")
check("an open reconciliation has Save & finish later", "Save &amp; finish later" in p and "id=laterform" in p)
(l2,) = q("SELECT line_id FROM statement_line WHERE description='WATER'")[0]
r = cl.post("/account/Stanbic UGX/later", data={"rowid": [str(l2)], "sel": [str(l2)], f"acct_{l2}": "83"})
check("it goes back to the dashboard", r.status_code == 302 and r.headers["Location"].rstrip("/").endswith(""))
d = text("/")
check("…saying what was saved and how far it got", "Saved Stanbic UGX (01/09/2026 to 30/09/2026) to finish later: cleared to 09/09/2026" in d
      and "selection to record (1 of 1 line ticked)" in d)
check("…the record table's choices are kept", q("SELECT data::json->>'acct', (data::json->>'sel')::boolean FROM record_draft WHERE line_id=%s", (l2,))
      == [("83", True)])
check("…who saved it and when shows on the dashboard", re.search(r'title="Saved for later">Saved \d\d/\d\d \d\d:\d\d · ', d) is not None)
check("…and on the account page", "Saved for later" in text("/account/Stanbic UGX"))
check("…logged", q("SELECT count(*) FROM activity_log WHERE action='saved the reconciliation for later'")[0][0] == 1)
q("UPDATE statement SET signed_off_at=now() WHERE statement_id=%s RETURNING 1", (sid,))
p = text("/account/Stanbic UGX")
check("once signed off: reconciled to the new date, no Save for later", "Reconciled to <b>30/09/2026</b>" in p
      and "id=laterform" not in p and "Saved for later" not in p)
q("UPDATE statement SET signed_off_at=NULL WHERE statement_id=%s RETURNING 1", (sid,))

# Book balance: filled in by the sync, kept current, a typed one left alone
A.qbo_book_balance_at = lambda token, acct_uuid, acct_qbo, as_of: D("777.00")
A.sync_from_quickbooks = lambda full=False, progress=None: (0, "", "changes", "1s")
A.start_sync(False, "t")
check("after a sync, an open reconciliation's empty book balance is filled from QuickBooks",
      q("SELECT book_balance, book_balance_source FROM statement WHERE statement_id=%s", (sid,)) == [(D("777.00"), "qbo")])
check("…and the sync says so", "Book balance updated on" in A.sync_job().get("msg", ""))
A.qbo_book_balance_at = lambda token, acct_uuid, acct_qbo, as_of: D("888.00")
A.start_sync(False, "t")
check("…a later sync keeps it current", q("SELECT book_balance FROM statement WHERE statement_id=%s", (sid,)) == [(D("888.00"),)])
cl.post("/account/Stanbic UGX/balances", data={"opening": "0", "closing": "0", "book": "888.00"})
check("saving Edit balances unchanged keeps it from QuickBooks", q("SELECT book_balance_source FROM statement WHERE statement_id=%s", (sid,)) == [("qbo",)])
cl.post("/account/Stanbic UGX/balances", data={"opening": "0", "closing": "0", "book": "900"})
A.start_sync(False, "t")
check("a book balance typed in by hand is never overwritten by a sync",
      q("SELECT book_balance, book_balance_source FROM statement WHERE statement_id=%s", (sid,)) == [(D("900"), "user")])

# A big statement (a year's PDF) is read and matched in the background, so the server's time limit
# can't cut it off: the page shows progress, refuses a second upload meanwhile, then shows the result.
import threading
gate, real_run = threading.Event(), A._upload_run
def slow_run(progress=None, **kw):
    if progress:
        progress("Reading the PDF: page 10 of 120")
    gate.wait(10)
    return real_run(progress=progress, **kw)
A._upload_run, A.SYNC_IN_BACKGROUND = slow_run, True
A.start_sync = lambda full=False, by=None: False
body = "Date,Description,Amount\n2026-10-03,OCT LINE,-50\n"
r = cl.post("/account/DFCU UGX/upload", data={"statement": (io.BytesIO(body.encode()), "year.csv"), "closing_balance": "0",
            "period_start": "2026-10-01", "period_end": "2026-10-31"}, content_type="multipart/form-data")
check("a big upload returns at once (the work goes on in the background)", r.status_code == 302)
p = text("/account/DFCU UGX")
check("…the page shows it's being read and matched, with the step", "id=upjob" in p and "Reading the PDF: page 10 of 120" in p
      and "year.csv" in p)
st = cl.get("/account/DFCU UGX/upload_status").get_json()
check("…and reports progress to the page", st == {"state": "running", "step": "Reading the PDF: page 10 of 120"})
r = cl.post("/account/DFCU UGX/upload", data={"statement": (io.BytesIO(body.encode()), "again.csv")}, content_type="multipart/form-data")
check("…a second upload meanwhile is refused", "still being read and matched" in text("/account/DFCU UGX"))
gate.set()
for th in threading.enumerate():
    if th.name == "upload":
        th.join(20)
p = text("/account/DFCU UGX")
check("when done: the result shows once, as uploaded", "Statement uploaded." in p and "Loaded 1 statement lines" in p
      and "id=upjob" not in p)
check("…the statement is saved and matched", q("SELECT count(*) FROM statement_line WHERE description='OCT LINE'")[0][0] == 1)
check("…shown once", "Statement uploaded." not in text("/account/DFCU UGX"))
def bad_run(progress=None, **kw):
    return False, "Not uploaded: this account already has a signed-off reconciliation for 2026-10-01 to 2026-10-31."
A._upload_run = bad_run
cl.post("/account/DFCU UGX/upload", data={"statement": (io.BytesIO(body.encode()), "x.csv")}, content_type="multipart/form-data")
for th in threading.enumerate():
    if th.name == "upload":
        th.join(20)
p = text("/account/DFCU UGX")
check("a refused upload says why (not as uploaded)", "already has a signed-off reconciliation" in p and "Statement uploaded." not in p)
A._upload_run, A.SYNC_IN_BACKGROUND = real_run, False

# Amount boxes show thousands separators (Edit balances, the upload panel, split amounts...)
import json, os, shutil, subprocess, tempfile
cl.post("/account/Stanbic UGX/balances", data={"opening": "1234567.5", "closing": "-2500000", "book": "178221410.00"})
node = shutil.which("node")
if not node or not os.path.isdir(os.path.join(H.HERE, "node_modules", "jsdom")):
    print("SKIP browser checks (no node/jsdom)")
else:
    html = cl.get("/account/Stanbic UGX").data.decode()
    JS = r"""
const { JSDOM, VirtualConsole } = require("jsdom");
const errors = []; const vc = new VirtualConsole(); vc.on("jsdomError", e => errors.push(String(e.message || e)));
const dom = new JSDOM(require("fs").readFileSync(process.argv[2], "utf8"), { runScripts: "dangerously", virtualConsole: vc, pretendToBeVisual: true });
const w = dom.window, d = w.document, out = {};
const v = n => d.querySelector(`input[name=${n}]`).value;
out.opening = v("opening"); out.closing = v("closing"); out.book = v("book");
const ob = d.getElementById("up-ob"); ob.value = "45000000"; ob.dispatchEvent(new w.FocusEvent("focusout", { bubbles: true })); out.typed = ob.value;
ob.value = "1000DR"; ob.dispatchEvent(new w.FocusEvent("focusout", { bubbles: true })); out.odd = ob.value;
const x = d.createElement("input"); x.setAttribute("inputmode", "decimal"); x.value = "9876543.21"; d.body.appendChild(x);
const hov = el => { el.dispatchEvent(new w.MouseEvent("mouseover", { bubbles: true })); return el.getAttribute("title"); };
out.hoverBook = hov(d.querySelector("input[name=book]"));
const hinted = d.createElement("input"); hinted.title = "Pick it from the list"; hinted.value = "Office Supplies:Stationery"; d.body.appendChild(hinted);
out.hoverHinted = hov(hinted);
hinted.title = "Not in the list: pick one from it"; out.hoverChanged = hov(hinted);
hinted.value = ""; out.hoverEmpty = hov(hinted);
setTimeout(() => { out.added = x.value; out.errors = errors; console.log(JSON.stringify(out)); }, 30);
"""
    f = tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8"); f.write(html); f.close()
    g = tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, dir=H.HERE, encoding="utf-8"); g.write(JS); g.close()
    try:
        res = subprocess.run([node, g.name, f.name], capture_output=True, text=True, encoding="utf-8", cwd=H.HERE)
    finally:
        os.unlink(f.name); os.unlink(g.name)
    o = json.loads([l for l in res.stdout.splitlines() if l.startswith("{")][-1]) if "{" in res.stdout else {}
    print("   ", o, res.stderr[-500:])
    check("browser: Edit balances shows amounts with commas", o.get("opening") == "1,234,567.50" and o.get("closing") == "-2,500,000.00"
          and o.get("book") == "178,221,410.00")
    check("browser: a typed amount gets its commas when you leave the box", o.get("typed") == "45,000,000")
    check("browser: …anything that isn't a plain number is left as typed", o.get("odd") == "1000DR")
    check("browser: amount boxes added later (split lines) are formatted too", o.get("added") == "9,876,543.21")
    check("browser: hovering over a box shows what's in it", o.get("hoverBook") == "178,221,410.00")
    check("browser: …above the box's own hint", o.get("hoverHinted") == "Office Supplies:Stationery\nPick it from the list")
    check("browser: …keeping a hint a script changed meanwhile", o.get("hoverChanged") == "Office Supplies:Stationery\nNot in the list: pick one from it")
    check("browser: …an empty box shows just its hint", o.get("hoverEmpty") == "Not in the list: pick one from it")
    check("browser: no script errors", o.get("errors") == [])
r = cl.post("/account/Stanbic UGX/balances", data={"opening": "1,234,567.50", "closing": "-2,500,000.00", "book": "178,221,410.00"})
check("amounts with commas are saved as numbers", q("SELECT opening_balance, closing_balance, book_balance FROM statement WHERE statement_id=%s", (sid,))
      == [(D("1234567.50"), D("-2500000.00"), D("178221410.00"))])

sys.exit(T.summary())
