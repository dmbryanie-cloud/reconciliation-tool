"""Focus on dates: narrow the account page to part of the statement (a week of June, a month of a
year-long PDF) while the balances, the difference and sign-off still cover the whole statement.

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_focus.py`, or all suites with `python tests/run_all.py`.
"""
import io, json, os, re, shutil, subprocess, sys, tempfile

import harness as H

STB = "00000000-0000-0000-0000-0000000000a1"
A, c = H.setup(H.account_sql((STB, "35", "Stanbic UGX", "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
A.qbo_is_connected = lambda: False

def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
cl = H.login(A)
def upload(rows, ps, pe):
    body = "Date,Description,Amount\n" + "".join(f"{d},{t},{a}\n" for d, t, a in rows)
    return cl.post("/account/Stanbic UGX/upload", data={"statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
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
check("…the tiles count that week", tile(p, "To review") == "1" and tile(p, "To record") == "3"
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

sys.exit(T.summary())
