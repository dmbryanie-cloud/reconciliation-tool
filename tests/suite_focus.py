"""Focus on dates: narrow the account page to part of the statement (a week of June, a month of a
year-long PDF) while the balances, the difference and sign-off still cover the whole statement.

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_focus.py`, or all suites with `python tests/run_all.py`.
"""
import io, json, os, re, shutil, subprocess, sys, tempfile

import harness as H

STB = "00000000-0000-0000-0000-0000000000a1"
CEN = "00000000-0000-0000-0000-0000000000c1"
A, c = H.setup(H.account_sql((STB, "35", "Stanbic UGX", "bank"), (CEN, "39", "Centenary UGX", "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
A.qbo_is_connected = lambda: False

def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
cl = H.login(A)
def upload(rows, ps, pe):
    body = "Date,Description,Amount\n" + "".join(f"{d},{t},{a}\n" for d, t, a in rows)
    return cl.post("/account/Stanbic UGX/upload", data={"replace": "1", "statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
                   "period_start": ps, "period_end": pe}, content_type="multipart/form-data")
def page(qs=""):
    return cl.get("/account/Stanbic UGX" + qs).data.decode()
def sec(p, a, b):
    m = re.search(rf"<h2 id={a}.*?<h2 id={b}", p, re.S)
    return m.group(0) if m else ""
def tile(p, label):
    m = re.search(rf"<span class=t-label>{label}</span><span class=\"t-val[^\"]*\">(.*?)</span>", p, re.S)
    return m.group(1).strip() if m else None
def proposed_by_line():
    return {d: (str(m), st) for d, m, st in q("""SELECT sl.description, m.match_id, m.status FROM match m
              JOIN match_statement_line msl USING (match_id) JOIN statement_line sl USING (line_id)
              WHERE m.match_type='fuzzy'""")}

# Book entries a little off the bank amounts (same payee): suggested matches, one in each of two weeks.
for i, (d, amt, who) in enumerate([("2026-06-03", -80500, "Kampala Stationers"), ("2026-06-10", -120300, "Umeme Ltd")]):
    q("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency,
         description, counterparty, last_modified) VALUES (%s,%s,%s,'Purchase',%s,%s,'UGX',%s,%s,now()) RETURNING 1""",
      (A.ORG_ID, STB, f"b{i}", d, amt, who, who))
upload([("2026-06-03", "KAMPALA STATIONERS", -80000), ("2026-06-10", "UMEME LTD", -120000),
        ("2026-06-02", "WEEK ONE FEES", 300000), ("2026-06-09", "WEEK TWO FEES", 410000),
        ("2026-06-12", "WEEK TWO CHARGES", -2500), ("2026-06-24", "WEEK FOUR FEES", 520000)], "2026-06-01", "2026-06-30")

p = page()
check("the account page offers a date focus over the statement's period",
      'id=focusbar' in p and 'name=from value="2026-06-01" min="2026-06-01" max="2026-06-30"' in p
      and 'name=to value="2026-06-30"' in p and "Whole statement" not in p)
check("…with each week of it as a quick pick (Monday to Sunday), no months for a one-month statement",
      '<option value="2026-06-08|2026-06-14">08/06–14/06/2026</option>' in p
      and '<option value="2026-06-29|2026-06-30">29/06–30/06/2026</option>' in p and 'label="Months"' not in p)
check("whole statement: everything is listed", all(x in p for x in ("WEEK ONE FEES", "WEEK TWO FEES", "WEEK FOUR FEES")))
check("…and both suggestions are to review", tile(p, "To review") == "2")

p = page("?from=2026-06-08&to=2026-06-14")
rec = sec(p, "sec-record", "sec-")
check("focused on a week: only that week's lines are listed to record",
      "WEEK TWO FEES" in p and "WEEK TWO CHARGES" in p and "WEEK ONE FEES" not in p and "WEEK FOUR FEES" not in p)
check("…only that week's suggested match", "UMEME" in p.upper() and "KAMPALA STATIONERS" not in p)
check("…the tiles count that week (Umeme waits in its suggestion, not to record)", tile(p, "To review") == "1"
      and tile(p, "To record") == "2"
      and "of 3 lines these days" in p)
check("…it says what's narrowed and that balances and sign-off cover the whole statement",
      "Showing only" in p and "narrowed to 08/06/2026 – 14/06/2026" in p
      and "still cover the whole statement (01/06/2026 – 30/06/2026); 2 suggested matches to review in all" in p)
check("…sign-off still waits for every suggestion, not just this week's", "Review the 2 suggested matches first" in p)
check("…and there's a way back to the whole statement", "focus=off" in p and "Whole statement" in p)
check("the focus stays while you work on the account", "WEEK FOUR FEES" not in page() and "Showing only" in page())

m = proposed_by_line()
cl.post("/account/Stanbic UGX/review_bulk", data={"status": "confirmed", "mid": [m["UMEME LTD"][0]]})
p = page()
check("acting on the week (confirm a suggestion) comes back to the same week",
      proposed_by_line()["UMEME LTD"][1] == "confirmed" and "Showing only" in p and "WEEK FOUR FEES" not in p)
check("…the result is shown as a success", re.search(r"<div id=flash[^>]*>Confirmed 1 suggested match", p) is not None)
check("…the other week's suggestion is untouched, and still holds up sign-off",
      proposed_by_line()["KAMPALA STATIONERS"][1] == "proposed" and "Review the 1 suggested match first" in p)

p = page("?focus=off")
check("Whole statement clears the focus", "Showing only" not in p and "WEEK FOUR FEES" in p and "WEEK FOUR FEES" in page())
p = page("?from=2026-05-20&to=2026-06-05")
check("dates outside the statement are brought inside it", "narrowed to 01/06/2026 – 05/06/2026" in p and "WEEK TWO FEES" not in p)
p = page("?from=2026-06-20&to=2026-06-10")
check("…dates the wrong way round are swapped", "narrowed to 10/06/2026 – 20/06/2026" in p)
p = page("?from=2026-06-01&to=2026-06-30")
check("…choosing the whole period is the same as no focus", "Showing only" not in p and "WEEK ONE FEES" in p)
p = page("?from=junk&to=2026-06-10")
check("…a bad date changes nothing", "Showing only" not in p)

page("?from=2026-06-08&to=2026-06-14")
upload([("2025-10-03", "OCT FEES", 100000), ("2026-02-11", "FEB FEES", 200000), ("2026-06-10", "JUN FEES", 300000)],
       "2025-09-01", "2026-09-30")
p = page()
check("a new statement starts on the whole period (the old focus doesn't carry over)",
      "Showing only" not in p and all(x in p for x in ("OCT FEES", "FEB FEES", "JUN FEES")))
check("a long statement also offers each month", 'label="Months"' in p
      and '<option value="2026-02-01|2026-02-28">February 2026</option>' in p)
p = page("?from=2026-02-01&to=2026-02-28")
check("focusing on one month of a year's statement lists just that month",
      "FEB FEES" in p and "OCT FEES" not in p and "JUN FEES" not in p and "of 1 lines these days" in p)

# ---- the balance as at the focus end date ---------------------------------------------------------------------------
for i, (d, amt, who) in enumerate([("2026-06-02", 300000, "Fees A"), ("2026-06-10", -50000, "Cheque 101"),
                                    ("2026-06-25", -30000, "Rent"), ("2026-06-20", 100000, "Fees B")]):
    q("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency,
         description, counterparty, last_modified) VALUES (%s,%s,%s,'Deposit',%s,%s,'UGX',%s,%s,now()) RETURNING 1""",
      (A.ORG_ID, CEN, f"cb{i}", d, amt, who, who))
body = "Date,Description,Amount\n" + "".join(f"{d},{x},{a}\n" for d, x, a in [
    ("2026-06-02", "FEES A", 300000), ("2026-06-05", "LEDGER FEES", -2000), ("2026-06-15", "CHEQUE 101", -50000),
    ("2026-06-20", "FEES B", 100000)])
cl.post("/account/Centenary UGX/upload", data={"replace": "1", "statement": (io.BytesIO(body.encode()), "c.csv"), "opening_balance": "1,000,000",
        "closing_balance": "1,348,000", "period_start": "2026-06-01", "period_end": "2026-06-30"}, content_type="multipart/form-data")
def cpage(qs=""):
    return cl.get("/account/Centenary UGX" + qs).data.decode()
p = cpage("?from=2026-06-08&to=2026-06-12")
check("before a book balance is entered, the as-at panel says what it needs",
      "As at 12/06/2026" in p and "Enter the book balance under Edit balances" in p)
cl.post("/account/Centenary UGX/balances", data={"opening": "1,000,000", "closing": "1,348,000", "book": "1,320,000",
        "period_start": "2026-06-01", "period_end": "2026-06-30"})
p = cpage()
fb = re.search(r"<div class=fbal id=fbal>.*?</div>\s*</div>", p, re.S)
fb = fb.group(0) if fb else ""
# bank at 12/06 = 1,000,000 + 300,000 - 2,000; books at 12/06 = 1,320,000 less Rent (25/06) and Fees B (20/06);
# the cheque booked 10/06 clears the bank on 15/06, so it's outstanding at 12/06; the ledger fee isn't in the books.
check("bank balance at the focus end: opening + the lines up to it", "1,298,000.00" in fb)
check("book balance at the focus end: the balance at period end less later entries", "1,250,000.00" in fb)
check("a match whose bank side clears after the date is outstanding at it", "payments in books by then, not yet on statement (1)" in fb
      and "-50,000.00" in fb and "on statement by then, not in books (1)" in fb and "-2,000.00" in fb)
check("…and the week balances", "Balanced at 12/06/2026" in fb and "1,248,000.00" in fb)
check("the Difference tile shows the focus date's difference", "Difference at 12/06</span><span class=\"t-val \">0.00" in p)
check("the whole statement's reconciliation is still shown below", "Balanced — adjusted bank and book balances agree" in p)
check("Edit balances says it's the whole statement, and the period can't be changed while focused",
      "These are the whole statement's balances (01/06/2026 – 30/06/2026)" in p
      and re.search(r'name=period_end value="2026-06-30" readonly', p) is not None and "Opening balance (whole statement)" in p)
cl.post("/account/Centenary UGX/balances", data={"opening": "1,000,000", "closing": "1,348,000", "book": "1,321,000",
        "period_start": "2026-06-01", "period_end": "2026-06-30"})
p = cpage()
check("saving balances while focused keeps the focus and saves them", "Balances saved." in p and "Showing only" in p
      and q("SELECT book_balance FROM statement WHERE account_id=%s", (CEN,))[0][0] == 1321000)
check("…a wrong book balance shows out of balance at the date", "Out of balance at 12/06/2026" in p and "-1,000.00" in p)
p = cpage("?from=2026-06-03&to=2026-06-30")
fb = re.search(r"<div class=fbal id=fbal>.*?</div>\s*</div>", p, re.S).group(0)
check("at the statement's last day the as-at figures are the statement's own",
      "Out of balance at 30/06/2026" in fb and "1,348,000.00" in fb and "-1,000.00" in fb)
cpage("?focus=off")

# ---- in the browser: picking a week fills the dates and shows it -------------------------------------------------------
node = shutil.which("node")
if not node or not os.path.isdir(os.path.join(H.HERE, "node_modules", "jsdom")):
    print("SKIP browser checks (no node/jsdom)")
else:
    html = page("?focus=off")
    JS = r"""
const { JSDOM, VirtualConsole } = require("jsdom");
const errors = []; const vc = new VirtualConsole(); vc.on("jsdomError", e => errors.push(String(e.message || e)));
const dom = new JSDOM(require("fs").readFileSync(process.argv[2], "utf8"), { runScripts: "dangerously", virtualConsole: vc, pretendToBeVisual: true });
const w = dom.window, d = w.document, out = {};
const f = d.getElementById("focusbar"), pick = f.querySelector("[data-pick]");
let sent = null; f.addEventListener("submit", e => { e.preventDefault(); sent = f.elements["from"].value + "|" + f.elements["to"].value; });
pick.value = "2026-03-01|2026-03-31"; pick.dispatchEvent(new w.Event("change", { bubbles: true }));
out.sent = sent; out.errors = errors; console.log(JSON.stringify(out));
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
    check("browser: picking a month fills both dates and shows it", o.get("sent") == "2026-03-01|2026-03-31")
    check("browser: no script errors", o.get("errors") == [])

    # The calendar on date fields: month and year chosen at the bottom (as in QuickBooks), then a day.
    JS = r"""
const { JSDOM, VirtualConsole } = require("jsdom");
const errors = []; const vc = new VirtualConsole(); vc.on("jsdomError", e => errors.push(String(e.message || e)));
const dom = new JSDOM(require("fs").readFileSync(process.argv[2], "utf8"), { runScripts: "dangerously", virtualConsole: vc, pretendToBeVisual: true });
const w = dom.window, d = w.document, out = {};
const inp = d.getElementById("up-ps"); let changed = 0; inp.addEventListener("change", () => changed++);
inp.value = "2026-03-15";
inp.dispatchEvent(new w.MouseEvent("click", { bubbles: true, cancelable: true }));
const box = d.querySelector(".dp");
out.open = !!box; out.title = box && box.querySelector(".dp-h b").textContent;
out.years = box && [...box.querySelectorAll(".dp-y option")].map(o => +o.value);
const y = box.querySelector(".dp-y"); y.value = "2019"; y.dispatchEvent(new w.Event("change", { bubbles: true }));
const mo = d.querySelector(".dp-m"); mo.value = "6"; mo.dispatchEvent(new w.Event("change", { bubbles: true }));
out.after = d.querySelector(".dp-h b").textContent;
d.querySelector('.dp-g button[data-v="2019-07-04"]').click();
out.value = inp.value; out.changed = changed; out.closed = !d.querySelector(".dp");
inp.dispatchEvent(new w.MouseEvent("click", { bubbles: true, cancelable: true }));
d.body.dispatchEvent(new w.MouseEvent("click", { bubbles: true }));
out.outside = !d.querySelector(".dp");
out.errors = errors; console.log(JSON.stringify(out));
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
    check("calendar: opens on the field's date", o.get("open") and o.get("title") == "March 2026")
    check("…with a year list at the bottom, several years back", 2011 in (o.get("years") or []) and 2026 in (o.get("years") or []))
    check("…choosing a year and month there moves the calendar", o.get("after") == "July 2019")
    check("…a day fills the field (and tells the page) and closes it",
          o.get("value") == "2019-07-04" and o.get("changed") == 1 and o.get("closed"))
    check("…a click elsewhere closes it", o.get("outside"))
    check("calendar: no script errors", o.get("errors") == [])

sys.exit(T.summary())
