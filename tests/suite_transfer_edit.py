"""Possible transfers: Edit (a different counterpart, or just the other account), Not a transfer /
Restore, and Undo of a transfer the app recorded (deleted in QuickBooks, bank lines back in the list).

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_transfer_edit.py`, or all suites with `python tests/run_all.py`.
"""
import io, json, os, re, shutil, subprocess, sys, tempfile, urllib.error

import harness as H

STB, DF, CEN = ("00000000-0000-0000-0000-0000000000a1", "00000000-0000-0000-0000-0000000000a2",
                "00000000-0000-0000-0000-0000000000a3")
A, c = H.setup(H.account_sql((STB, "35", "Stanbic UGX", "bank"), (DF, "36", "DFCU UGX", "bank"),
                             (CEN, "38", "Centenary UGX", "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
A._store_coa([{"Id": i, "Name": n, "AccountType": "Bank", "CurrencyRef": {"value": "UGX"}}
              for i, n in (("35", "Stanbic UGX"), ("36", "DFCU UGX"), ("38", "Centenary UGX"))]
             + [{"Id": "83", "Name": "Office Supplies", "AccountType": "Expense", "CurrencyRef": {"value": "UGX"}}])
POSTS, QBO, DELETED = [], {}, []
def fake_post(token, entity, body):
    POSTS.append((entity, json.loads(json.dumps(body))))
    i = str(800 + len(POSTS))
    QBO[i] = {**body, "Id": i, "SyncToken": "3"}
    return {entity: {"Id": i}}
def fake_read(token, entity, i):
    return QBO.get(str(i))
def fake_delete(token, entity, i, sync):
    DELETED.append((entity, str(i), str(sync))); QBO.pop(str(i), None); return {}
A.qbo_post, A.qbo_read, A.qbo_delete = fake_post, fake_read, fake_delete
A.qbo_token = lambda: "tok"
A.qbo_realm = lambda: "REAL-1"
A.qbo_is_connected = lambda: False

def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
def lid(desc):
    return str(q("SELECT line_id FROM statement_line WHERE description=%s", (desc,))[0][0])
def matched(line):
    return q("""SELECT 1 FROM match m JOIN match_statement_line msl USING (match_id)
                WHERE msl.line_id=%s AND m.status='confirmed'""", (line,)) != []
cl = H.login(A)
def upload(acct_name, rows):
    body = "Date,Description,Amount\n" + "".join(f"{d},{t},{a}\n" for d, t, a in rows)
    r = cl.post(f"/account/{acct_name}/upload", data={"statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
                "period_start": "2026-07-01", "period_end": "2026-07-31"}, content_type="multipart/form-data")
    assert r.status_code == 302, r.data[:300]
def page(n="Stanbic UGX"):
    return cl.get(f"/account/{n}").data.decode()
def msg(n="Stanbic UGX"):
    m = re.search(r"<div id=flash[^>]*>(.*?)</div>", page(n), re.S)
    return m.group(1) if m else ""
def section(p):
    m = re.search(r"<h2 id=sec-transfers.*?<h2 id=sec-exceptions", p, re.S)
    return m.group(0) if m else ""

upload("DFCU UGX", [("2026-07-02", "FROM STANBIC", 500000), ("2026-07-12", "FROM STANBIC AGAIN", 500000)])
upload("Stanbic UGX", [("2026-07-01", "TRANSFER TO DFCU", -500000), ("2026-07-05", "TO CENTENARY", -300000)])
S1, S2, D1, D2 = lid("TRANSFER TO DFCU"), lid("TO CENTENARY"), lid("FROM STANBIC"), lid("FROM STANBIC AGAIN")

# ---- the suggestion, with Edit and Not a transfer ----------------------------------------------------------------
sec = section(page())
check("the suggestion offers Record, Edit and Not a transfer", f'name=other value="{D1}"' in sec and "xfer-edit" in sec
      and "Not a transfer" in sec)
edit = re.search(rf'<tr class=xferedit id="xe-{S1}".*?</tr>', sec, re.S).group(0)
check("Edit lists the counterparts within 14 days (the one 11 days later too)", f'value="{D1}"' in edit and f'value="{D2}"' in edit)
check("…and your other accounts, for when their statement isn't uploaded", '<option value="38">Centenary UGX</option>' in edit
      and '<option value="36">DFCU UGX</option>' in edit and 'value="35"' not in edit)

# ---- Not a transfer / Restore ------------------------------------------------------------------------------------
cl.post("/account/Stanbic UGX/transfer_dismiss", data={"line": S1, "other": f"line:{D1}"})
sec = section(page())
check("Not a transfer: the suggestion goes, listed under the hidden ones", f'name=other value="{D1}"><button type=submit class=btn-sm>Record'
      not in sec and "marked ‘Not a transfer’" in sec and "Restore" in sec)
cl.post("/account/Stanbic UGX/transfer_dismiss", data={"line": D1, "other": "line:x"})   # not this account's line
check("…only this account's lines can be hidden from here", q("SELECT count(*) FROM transfer_dismissal")[0][0] == 1)
cl.post("/account/Stanbic UGX/transfer_restore", data={"line": S1, "other": f"line:{D1}"})
check("Restore brings it back", f'name=other value="{D1}"' in section(page()) and "Not a transfer’" not in section(page()))

# ---- bulk: tick several and mark them all 'Not a transfer' ---------------------------------------------------------
sec = section(page())
check("each suggestion has a tick box for the bulk 'Not a transfer'", f'name=pick value="{S1}|line:{D1}" form=xferbulk' in sec
      and "id=xferbulk" in sec and "xb-all" in sec)
cl.post("/account/Stanbic UGX/transfer_dismiss", data={"pick": [f"{S1}|line:{D1}", f"{D1}|line:x", "junk"]})
bm = msg()
check("bulk hides the ticked ones (only this account's lines)", q("SELECT line_id::text, other FROM transfer_dismissal") == [(S1, f"line:{D1}")]
      and "Hidden" in bm)
cl.post("/account/Stanbic UGX/transfer_dismiss", data={})
check("…nothing ticked: nothing hidden, and it says so", "Nothing hidden" in msg())
cl.post("/account/Stanbic UGX/transfer_restore", data={"line": S1, "other": f"line:{D1}"})
check("…and they restore one by one", q("SELECT count(*) FROM transfer_dismissal")[0][0] == 0)

# ---- Edit: a different counterpart ---------------------------------------------------------------------------------
n = len(POSTS)
cl.post("/account/Stanbic UGX/transfer", data={"line": S1, "other": D2})
ent, body = POSTS[-1]
check("Edit with another line: one Transfer from Stanbic to DFCU, both lines matched", len(POSTS) == n + 1 and ent == "Transfer"
      and body["FromAccountRef"]["value"] == "35" and body["ToAccountRef"]["value"] == "36" and matched(S1) and matched(D2)
      and not matched(D1))
XID = str(800 + len(POSTS))
sec = section(page())
check("recorded transfers are listed with Undo", f"QuickBooks #{XID}" in sec and f'name=qbo_id value="{XID}"' in sec)

# ---- Edit: just the account (its statement isn't uploaded) --------------------------------------------------------
n = len(POSTS)
cl.post("/account/Stanbic UGX/transfer", data={"line": S2, "other": "", "other_acct": "83"})
check("only your own accounts can be the other side", len(POSTS) == n and "same currency" in msg())
cl.post("/account/Stanbic UGX/transfer", data={"line": S2, "other": "", "other_acct": "38"})
ent, body = POSTS[-1]
check("only the account: a Transfer to Centenary, this line matched", len(POSTS) == n + 1 and ent == "Transfer"
      and body["ToAccountRef"]["value"] == "38" and body["Amount"] == 300000.0 and matched(S2))
YID = str(800 + len(POSTS))

# ---- Undo ---------------------------------------------------------------------------------------------------------
sid = q("SELECT statement_id FROM statement WHERE account_id=%s", (DF,))[0][0]
q("UPDATE statement SET signed_off_at=now() WHERE statement_id=%s RETURNING 1", (sid,))
cl.post("/account/Stanbic UGX/transfer_undo", data={"qbo_id": XID})
check("Undo refused while the other side's statement is signed off", DELETED == [] and "signed off" in msg() and matched(S1))
q("UPDATE statement SET signed_off_at=NULL WHERE statement_id=%s RETURNING 1", (sid,))
cl.post("/account/Centenary UGX/transfer_undo", data={"qbo_id": XID})
check("…and from an account it wasn't recorded on", DELETED == [])
cl.post("/account/Stanbic UGX/transfer_undo", data={"qbo_id": XID})
um = msg()
check("Undo deletes the Transfer in QuickBooks (with its SyncToken)", DELETED == [("Transfer", XID, "3")])
check("…both bank lines are unmatched and back in the list", not matched(S1) and not matched(D2)
      and f'value="{S1}" class=rsel' in page() and f'value="{D2}" class=rsel' in page("DFCU UGX"))
check("…its entries leave the books here", q("""SELECT count(*) FROM book_txn WHERE source_txn_type='Transfer'
                                               AND source_txn_id=%s AND NOT is_deleted""", (XID,))[0][0] == 0)
check("…and it says so", f"transfer #{XID} was deleted in QuickBooks" in um and "back in the list" in um)
check("…the lines can be recorded again", q("SELECT status FROM writeback_log WHERE line_id=%s", (S1,)) == [("failed",)])
n = len(POSTS)
cl.post("/account/Stanbic UGX/transfer", data={"line": S1, "other": D1})
check("…e.g. with the right counterpart this time", len(POSTS) == n + 1 and matched(S1) and matched(D1))

QBO.pop(YID)   # already deleted in QuickBooks by hand
cl.post("/account/Stanbic UGX/transfer_undo", data={"qbo_id": YID})
check("Undo of one already deleted in QuickBooks: just tidies up here", not matched(S2) and len(DELETED) == 1)

def boom(*a):
    raise urllib.error.HTTPError("u", 400, "Stale object", {}, None)
ZID = str(800 + len(POSTS))
A.qbo_delete = boom
cl.post("/account/Stanbic UGX/transfer_undo", data={"qbo_id": ZID})
check("QuickBooks refuses the delete: nothing changes here", matched(S1) and matched(D1) and "Not undone" in msg())
# ---- the Edit panel in the browser ---------------------------------------------------------------------------------
A.qbo_delete = fake_delete

# ---- Edit a recorded transfer: another line, then just another account ---------------------------------------------
upload("Stanbic UGX", [("2026-07-25", "TO DFCU X", -900000)])
upload("DFCU UGX", [("2026-07-25", "IN X1", 900000), ("2026-07-27", "IN X2", 900000)])
S4, X1, X2 = lid("TO DFCU X"), lid("IN X1"), lid("IN X2")
cl.post("/account/Stanbic UGX/transfer", data={"line": S4, "other": X1})
WID = str(800 + len(POSTS))
check("(set up: S4 and X1 recorded as one transfer)", matched(S4) and matched(X1))
sec = section(page())
check("recorded transfers fold into their own list", '<details class=xferrec id=xferrec' in sec)
check("…each with Edit and Undo", f'data-line="r{WID}"' in sec and f'id="xe-r{WID}"' in sec
      and f'name=qbo_id value="{WID}"' in sec)
edit = re.search(rf'<tr class=xferedit id="xe-r{WID}".*?</tr>', sec, re.S).group(0)
check("Edit offers the other unmatched line, and your other accounts", f'value="{X2}"' in edit and f'value="{X1}"' not in edit
      and '<option value="38">Centenary UGX</option>' in edit and "transfer_change" in edit)
n = len(POSTS)
cl.post("/account/Stanbic UGX/transfer_change", data={"qbo_id": WID, "other": X2})
cm = msg()
check("Edit to another line on the same account: nothing to change in QuickBooks", len(POSTS) == n)
check("…X2 now matched, X1 back in its list, this line still matched", matched(X2) and not matched(X1) and matched(S4)
      and f'value="{X1}" class=rsel' in page("DFCU UGX"))
check("…and it says so", f"transfer #{WID}" in cm and "matched to that bank line" in cm)
cl.post("/account/Stanbic UGX/transfer_change", data={"qbo_id": WID, "other": "", "other_acct": "83"})
check("only your own accounts can be the other side", len(POSTS) == n and "same currency" in msg())
sid = q("SELECT statement_id FROM statement WHERE account_id=%s", (DF,))[0][0]
q("UPDATE statement SET signed_off_at=now() WHERE statement_id=%s RETURNING 1", (sid,))
cl.post("/account/Stanbic UGX/transfer_change", data={"qbo_id": WID, "other": "", "other_acct": "38"})
check("refused while the other side's statement is signed off", len(POSTS) == n and "signed off" in msg())
q("UPDATE statement SET signed_off_at=NULL WHERE statement_id=%s RETURNING 1", (sid,))
cl.post("/account/Stanbic UGX/transfer_change", data={"qbo_id": WID, "other": "", "other_acct": "38"})
ent, body = POSTS[-1]
check("Edit to just Centenary: the same transfer updated in QuickBooks", len(POSTS) == n + 1 and ent == "Transfer"
      and body.get("Id") == WID and body.get("SyncToken") == "3" and body["ToAccountRef"]["value"] == "38"
      and body["FromAccountRef"]["value"] == "35")
check("…X2 unpaired, and its DFCU side gone from the books here", not matched(X2) and matched(S4)
      and q("""SELECT count(*) FROM book_txn bt WHERE source_txn_type='Transfer' AND source_txn_id=%s
               AND account_id=%s AND NOT is_deleted""", (WID, DF))[0][0] == 0)
check("…Centenary's side is in the books", q("""SELECT count(*) FROM book_txn WHERE source_txn_type='Transfer'
                                              AND source_txn_id=%s AND account_id=%s AND NOT is_deleted""", (WID, CEN))[0][0] == 1)
check("…and the list says Centenary", "Centenary UGX<br><span class=hint>QuickBooks #" + WID in section(page()))
QBO.pop(WID)
cl.post("/account/Stanbic UGX/transfer_change", data={"qbo_id": WID, "other": "", "other_acct": "36"})
check("deleted in QuickBooks meanwhile: says to use Undo, nothing changes", "Use Undo" in msg() and len(POSTS) == n + 1)
upload("Stanbic UGX", [("2026-07-20", "TO DFCU LATER", -700000)])
upload("DFCU UGX", [("2026-07-21", "IN FROM STANBIC", 700000)])
S3 = lid("TO DFCU LATER")
html = page()
node = shutil.which("node")
if not node or not os.path.isdir(os.path.join(H.HERE, "node_modules", "jsdom")):
    check("jsdom available (run: cd tests && npm install)", False)
else:
    JS = r"""
const { JSDOM, VirtualConsole } = require("jsdom");
const errors = []; const vc = new VirtualConsole(); vc.on("jsdomError", e => errors.push(String(e.message || e)));
const dom = new JSDOM(require("fs").readFileSync(process.argv[2], "utf8"), { runScripts: "dangerously", virtualConsole: vc, pretendToBeVisual: true });
const w = dom.window, d = w.document; let asked = null; w.confirm = m => { asked = m; return false; }; w.rbAsk = m => { asked = m; };
const out = {}, lid = process.argv[3];
const btn = d.querySelector(`.xfer-edit[data-line="${lid}"]`), row = d.getElementById("xe-" + lid);
out.closed0 = row.hidden; btn.click(); out.open = !row.hidden;
const f = row.querySelector(".xe-form");
const s1 = new w.SubmitEvent("submit", { cancelable: true, bubbles: true }); f.dispatchEvent(s1);
out.noPickStopped = s1.defaultPrevented; out.hint = (f.querySelector(".xe-err") || {}).textContent;
const sel = row.querySelector(".xe-acct"); sel.value = "38"; sel.dispatchEvent(new w.Event("change", { bubbles: true }));
out.acctRadio = row.querySelector(".xe-acct-r").checked;
const s2 = new w.SubmitEvent("submit", { cancelable: true, bubbles: true }); f.dispatchEvent(s2);
out.asked = asked;
row.querySelector(".xe-cancel").click(); out.closedAgain = row.hidden;
const picks = [...d.querySelectorAll(".xb-pick")], all = d.querySelector(".xb-all"), bf = d.getElementById("xferbulk"), bb = bf.querySelector("button");
out.bulkOff = bb.disabled; all.click(); out.allTicked = picks.length > 0 && picks.every(p => p.checked); out.bulkLabel = bb.textContent;
out.n = picks.length; out.inForm = [...new w.FormData(bf).getAll("pick")].length; asked = null;
const s3 = new w.SubmitEvent("submit", { cancelable: true, bubbles: true }); bf.dispatchEvent(s3);
out.bulkAsked = asked; out.bulkStopped = s3.defaultPrevented;
picks[0].click(); out.offAgain = bb.disabled && !all.checked;
out.errors = errors; console.log(JSON.stringify(out));
"""
    f = tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8"); f.write(html); f.close()
    g = tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, dir=H.HERE, encoding="utf-8"); g.write(JS); g.close()
    try:
        res = subprocess.run([node, g.name, f.name, S3], capture_output=True, text=True, encoding="utf-8", cwd=H.HERE)
    finally:
        os.unlink(f.name); os.unlink(g.name)
    if res.stderr.strip():
        print(res.stderr[-1500:])
    o = json.loads([l for l in res.stdout.splitlines() if l.startswith("{")][-1]) if "{" in res.stdout else {}
    print("   ", o)
    check("browser: Edit opens the chooser under the line; Cancel closes it", o.get("closed0") and o.get("open") and o.get("closedAgain"))
    check("browser: recording from it needs a line or an account picked", o.get("noPickStopped") and "Pick the matching line" in (o.get("hint") or ""))
    check("browser: choosing an account picks 'Only the account', and it asks before recording",
          o.get("acctRadio") and "Centenary UGX" in (o.get("asked") or "") and "700,000.00" in (o.get("asked") or ""))
    n = o.get("n")
    check("browser: bulk button is off until something is ticked; the header box ticks all",
          o.get("bulkOff") is True and o.get("allTicked") and f"({n})" in (o.get("bulkLabel") or ""))
    check("browser: the ticked boxes are sent with the bulk form", n and o.get("inForm") == n)
    check("browser: bulk asks first, with the count", f"Mark {n} suggestion" in (o.get("bulkAsked") or "") and o.get("bulkStopped") is True)
    check("browser: unticking turns the bulk button off again", o.get("offAgain") is True)
    check("browser: no script errors", o.get("errors") == [])
sys.exit(T.summary())
