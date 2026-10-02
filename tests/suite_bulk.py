"""Bulk actions, the same in every section of the account page: tick rows (or the header box), then
act on them all at once -- Suggested matches (Confirm / Reject selected), Possible transfers (Not a
transfer / Record selected as transfers), hidden suggestions (Restore selected) and recorded
transfers (Undo selected). Also: a successful upload closes the panel and says so plainly.

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_bulk.py`, or all suites with `python tests/run_all.py`.
"""
import io, json, os, re, shutil, subprocess, sys, tempfile

import harness as H

STB, DF, CEN = ("00000000-0000-0000-0000-0000000000a1", "00000000-0000-0000-0000-0000000000a2",
                "00000000-0000-0000-0000-0000000000a3")
A, c = H.setup(H.account_sql((STB, "35", "Stanbic UGX", "bank"), (DF, "36", "DFCU UGX", "bank"),
                             (CEN, "38", "Centenary UGX", "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
A._store_coa([{"Id": i, "Name": n, "AccountType": "Bank", "CurrencyRef": {"value": "UGX"}}
              for i, n in (("35", "Stanbic UGX"), ("36", "DFCU UGX"), ("38", "Centenary UGX"))])
POSTS, QBO, DELETED = [], {}, []
def fake_post(token, entity, body):
    POSTS.append((entity, json.loads(json.dumps(body))))
    i = str(900 + len(POSTS))
    QBO[i] = {**body, "Id": i, "SyncToken": "1"}
    return {entity: {"Id": i}}
def fake_read(token, entity, i):
    return QBO.get(str(i))
def fake_delete(token, entity, i, sync):
    DELETED.append(str(i)); QBO.pop(str(i), None); return {}
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
    return cl.post(f"/account/{acct_name}/upload", data={"statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
                   "period_start": "2026-07-01", "period_end": "2026-07-31"}, content_type="multipart/form-data")
def page(n="Stanbic UGX"):
    return cl.get(f"/account/{n}").data.decode()
def flash(p):
    m = re.search(r"<div id=flash[^>]*>(.*?)</div>", p, re.S)
    return m.group(0) if m else ""
def sec(p, a, b):
    m = re.search(rf"<h2 id={a}.*?<h2 id={b}", p, re.S)
    return m.group(0) if m else ""

# Book entries close to the bank lines (same payee, amount a little off): suggested matches to review.
for i, (d, amt, who) in enumerate([("2026-07-03", -80500, "Kampala Stationers"), ("2026-07-04", -120300, "Umeme Ltd"),
                                    ("2026-07-06", -45200, "Nwsc Water")]):
    q("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency,
         description, counterparty, last_modified) VALUES (%s,%s,%s,'Purchase',%s,%s,'UGX',%s,%s,now()) RETURNING 1""",
      (A.ORG_ID, STB, f"b{i}", d, amt, who, who))

upload("DFCU UGX", [("2026-07-02", "FROM STANBIC", 500000), ("2026-07-08", "FROM STANBIC TWO", 250000)])
r = upload("Stanbic UGX", [("2026-07-03", "KAMPALA STATIONERS", -80000), ("2026-07-04", "UMEME LTD", -120000),
                           ("2026-07-06", "NWSC WATER", -45000), ("2026-07-01", "TRANSFER TO DFCU", -500000),
                           ("2026-07-07", "TRANSFER TO DFCU TWO", -250000)])
S1, S2, D1, D2 = lid("TRANSFER TO DFCU"), lid("TRANSFER TO DFCU TWO"), lid("FROM STANBIC"), lid("FROM STANBIC TWO")

# ---- the upload itself: panel closed, a plain 'uploaded' message that stays -----------------------------------------
p = page()
check("after uploading, the upload panel is closed", re.search(r"<aside class=drawer id=dr-upload hidden", p) is not None)
check("…and it says the statement was uploaded, in a message that stays until closed",
      "Statement uploaded" in flash(p) and "data-stay" in flash(p) and "class=ok" in flash(p))
check("…shown once", "Statement uploaded" not in flash(page()))

def proposed():
    return [str(m) for (m,) in q("""SELECT match_id FROM match WHERE status='proposed' AND statement_id IN
                                    (SELECT statement_id FROM statement WHERE account_id=%s) ORDER BY match_id""", (STB,))]
P = proposed()
check("three suggested matches to review", len(P) == 3)
def line_of(m):
    return q("SELECT sl.description FROM match_statement_line msl JOIN statement_line sl USING (line_id) WHERE msl.match_id=%s", (m,))[0][0]
L = [line_of(m) for m in P]
def st_of():   # the review status of each suggestion, by its bank line (re-matching gives proposals new ids)
    return dict(q("""SELECT sl.description, m.status FROM match m JOIN match_statement_line msl USING (match_id)
                     JOIN statement_line sl USING (line_id) WHERE sl.description = ANY(%s)""", (L,)))

# ---- Suggested matches -----------------------------------------------------------------------------------------------
rv = sec(page(), "sec-review", "sec-matched")
check("Suggested matches has the bulk bar with Confirm and Reject selected", "id=revbulk" in rv and "Confirm selected" in rv
      and "Reject selected" in rv and 'data-for=revbulk' in rv)
check("…and a tick box on each suggestion to review", all(f'name=mid value="{m}" form=revbulk' in rv for m in P))
cl.post("/account/Stanbic UGX/review_bulk", data={"status": "confirmed", "mid": P[:2] + ["junk", "00000000-0000-0000-0000-000000000000"]})
p = page()
st = st_of()
check("Confirm selected confirms only the ticked ones", st[L[0]] == st[L[1]] == "confirmed" and st[L[2]] == "proposed"
      and "Confirmed 2 suggested matches" in flash(p))
check("…logged in the activity", q("SELECT count(*) FROM activity_log WHERE action LIKE 'confirmed 2 suggested%%'")[0][0] == 1)
P = proposed()
cl.post("/account/Stanbic UGX/review_bulk", data={"status": "rejected", "mid": P + [m for m, in q("SELECT match_id::text FROM match WHERE status='confirmed'")]})
st = st_of()
check("Reject selected rejects the one still to review, leaving confirmed ones alone",
      st[L[2]] == "rejected" and st[L[0]] == "confirmed" and "Rejected 1 suggested match." in flash(page()))
cl.post("/account/Stanbic UGX/review_bulk", data={"status": "confirmed"})
check("nothing ticked: nothing changes, and it says so", "Nothing changed: tick the suggestions" in flash(page()))
cl.post("/account/Stanbic UGX/review_bulk", data={"status": "confirmed", "mid": ["00000000-0000-0000-0000-000000000000"]})
check("ticked ones that were reviewed or refreshed meanwhile: it says why, not 'tick first'",
      "already reviewed, or a QuickBooks sync refreshed them" in flash(page()))
cl.post("/account/Stanbic UGX/review/" + q("SELECT match_id::text FROM match WHERE status='rejected'")[0][0], data={"status": "proposed"})     # back to review (Undo)
P = proposed()
cl.post("/account/DFCU UGX/review_bulk", data={"status": "confirmed", "mid": P})
check("…another account's suggestion can't be changed from here", st_of()[L[2]] == "proposed")
cl.post("/account/Stanbic UGX/review_bulk", data={"status": "deleted", "mid": P})
check("…only confirm or reject", st_of()[L[2]] == "proposed")

# ---- Possible transfers: Record selected as transfers -------------------------------------------------------------------
xs = sec(page(), "sec-transfers", "sec-exceptions")
check("transfers have the same bulk bar: Not a transfer and Record selected as transfers",
      "id=xferbulk class=bulkbar" in xs and "Record selected as transfers" in xs and "Not a transfer" in xs)
check("…the recordable ones are marked as such", f'value="{S1}|line:{D1}" form=xferbulk class="xb-pick bk-pick" data-rec' in xs)
n = len(POSTS)
cl.post("/account/Stanbic UGX/transfer", data={"pick": [f"{S1}|line:{D1}", f"{S2}|line:{D2}", f"{S1}|line:{D2}", f"{S1}|book:x"]})
p = page()
check("Record selected records one Transfer per ticked pair, and matches both lines",
      len(POSTS) == n + 2 and all(e == "Transfer" for e, _ in POSTS[n:]) and matched(S1) and matched(D1) and matched(S2) and matched(D2))
check("…a line ticked twice is recorded once; ones already in QuickBooks are left for Edit",
      "Recorded 2 transfers" in flash(p) and "be recorded from here" in flash(p))
cl.post("/account/Stanbic UGX/transfer", data={"pick": [f"{S1}|line:{D1}"]})
check("…recording the same pair again does nothing", len(POSTS) == n + 2 and "Nothing recorded" in flash(page()))
X1, X2 = str(900 + n + 1), str(900 + n + 2)

# ---- Recorded transfers: Undo selected --------------------------------------------------------------------------------
xs = sec(page(), "sec-transfers", "sec-exceptions")
check("recorded transfers have a tick box each and Undo selected", "id=xferrecbulk" in xs and "Undo selected" in xs
      and f'name=qbo_ids value="{X1}" form=xferrecbulk' in xs and f'name=qbo_ids value="{X2}" form=xferrecbulk' in xs)
cl.post("/account/Stanbic UGX/transfer_undo", data={"qbo_ids": [X1, X2, "12345"]})
p = page()
check("Undo selected deletes each in QuickBooks and puts the lines back", sorted(DELETED) == sorted([X1, X2])
      and not matched(S1) and not matched(S2) and not matched(D1))
check("…and says which couldn't be undone", "Undone 2 transfers" in flash(p) and "#12345" in flash(p))

# ---- Hidden suggestions: Restore selected ------------------------------------------------------------------------------
cl.post("/account/Stanbic UGX/transfer_dismiss", data={"pick": [f"{S1}|line:{D1}", f"{S2}|line:{D2}"]})
xs = sec(page(), "sec-transfers", "sec-exceptions")
check("hidden suggestions have tick boxes and Restore selected", "id=xferhidbulk" in xs and "Restore selected" in xs
      and f'name=pick value="{S1}|line:{D1}" form=xferhidbulk' in xs)
cl.post("/account/Stanbic UGX/transfer_restore", data={"pick": [f"{S1}|line:{D1}", f"{S2}|line:{D2}", f"{D1}|line:x"]})
check("Restore selected brings them all back", q("SELECT count(*) FROM transfer_dismissal")[0][0] == 0
      and "Restored 2 suggestions" in flash(page()))

# ---- permissions ---------------------------------------------------------------------------------------------------------
A.add_user("viewer.only", "Viewer", "pw-123456", False)
q("UPDATE app_users SET perms='' WHERE username='viewer.only' RETURNING 1")
v = A.app.test_client()
v.post("/login", data={"username": "viewer.only", "password": "pw-123456"})
P2 = proposed()
v.post("/account/Stanbic UGX/review_bulk", data={"status": "confirmed", "mid": P2})
check("bulk confirm needs the review permission", proposed() == P2)

# ---- in the browser: the bar counts ticks, asks first, and closes the upload panel on submit ---------------------------
node = shutil.which("node")
if not node or not os.path.isdir(os.path.join(H.HERE, "node_modules", "jsdom")):
    print("SKIP browser checks (no node/jsdom)")
else:
    q("UPDATE match SET status='proposed' WHERE statement_id IN (SELECT statement_id FROM statement WHERE account_id=%s) RETURNING 1", (STB,))
    cl.post("/account/Stanbic UGX/review_bulk", data={"status": "confirmed"})    # a "nothing ticked" hint on the page
    html = page()
    JS = r"""
const { JSDOM, VirtualConsole } = require("jsdom");
const errors = []; const vc = new VirtualConsole(); vc.on("jsdomError", e => errors.push(String(e.message || e)));
const dom = new JSDOM(require("fs").readFileSync(process.argv[2], "utf8"), { runScripts: "dangerously", virtualConsole: vc, pretendToBeVisual: true });
const w = dom.window, d = w.document; let asked = null, yes = null; w.rbAsk = (m, cb, o) => { asked = m; yes = o && o.yes; };
const out = {};
const bf = d.getElementById("revbulk"), [conf, rej] = bf.querySelectorAll("button"), all = d.querySelector('.bk-all[data-for=revbulk]');
const picks = [...d.querySelectorAll('input.bk-pick[form=revbulk]')];
out.off = conf.disabled && rej.disabled; out.n = picks.length;
picks[0].click(); out.one = conf.textContent; out.lab = bf.querySelector(".bk-n").textContent; out.part = all.indeterminate;
all.click(); out.all = picks.every(p => p.checked) && conf.textContent; out.on = bf.classList.contains("on");
const s = new w.SubmitEvent("submit", { cancelable: true, bubbles: true, submitter: rej }); bf.dispatchEvent(s);
out.asked = asked; out.yes = yes; out.stopped = s.defaultPrevented;
all.click(); out.offAgain = conf.disabled;
const up = d.getElementById("dr-upload"); d.querySelector("[data-drawer=upload]").click(); out.upOpen = !up.hidden;
const uf = up.querySelector("form");
uf.dispatchEvent(new w.SubmitEvent("submit", { cancelable: true, bubbles: true }));
const fl = d.getElementById("flash"); out.flashCls = fl ? fl.className : null;
setTimeout(() => { out.upClosed = up.hidden; out.errors = errors; console.log(JSON.stringify(out)); }, 50);
"""
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
    n = o.get("n")
    check("browser: Confirm and Reject selected are off until something is ticked", o.get("off") is True and n == 3)
    check("browser: ticking one counts it (and half-ticks the header box)", o.get("one") == "Confirm selected (1)"
          and o.get("lab") == "1 suggested match ticked" and o.get("part") is True)
    check("browser: the header box ticks them all", o.get("all") == "Confirm selected (3)" and o.get("on"))
    check("browser: it asks first, with the count", o.get("stopped") and (o.get("asked") or "").startswith("Reject 3 suggested matches?")
          and o.get("yes") == "Reject")
    check("browser: unticking all switches the buttons off again", o.get("offAgain") is True)
    check("browser: sending the upload closes its panel", o.get("upOpen") and o.get("upClosed") is True)
    check("browser: a 'nothing ticked' hint is shown quietly, not as an error", o.get("flashCls") == "note")
    check("browser: no script errors", o.get("errors") == [])

sys.exit(T.summary())
