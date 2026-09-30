"""UI: the account page's own JavaScript, run in a simulated browser (jsdom) against a real render.

Seeds a statement with things for the manual-match panel and the record table to show, renders
/account/<name> through the app, then runs tests/ui/account_page.test.js on that HTML.
QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_ui.py`, or all suites with `python tests/run_all.py`.
"""
import io, os, re, shutil, subprocess, sys, tempfile

import harness as H

ACCT = "00000000-0000-0000-0000-0000000000a1"
A, c = H.setup(H.account_sql((ACCT, "35", "Stanbic", "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check

A.qbo_is_connected = lambda: False
cur.execute("""INSERT INTO qbo_coa (qbo_id, name, fqn, account_type, active)
               VALUES ('83','Office Supplies','Office Supplies','Expense',true), ('90','Sales','Sales','Income',true),
                      ('36','Centenary','Centenary','Bank',true)""")
for tid, d, amt, who in (("b1", "2026-09-04", 100000, "Cust A"), ("b2", "2026-09-04", 199000, "Cust B"),
                         ("b3", "2026-09-09", -50000, "Savings transfer"), ("b4", "2026-10-05", -80000, "Supplier X"),
                         ("b5", "2026-09-01", -70000, "Supplier Y"), ("b6", "2026-09-14", -12000, "Airtel"),
                         ("b7", "2026-09-18", 300000, "Deposit (unmatched)")):
    cur.execute("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency,
                   description, counterparty, last_modified) VALUES (%s,%s,%s,'Purchase',%s,%s,'UGX',%s,%s,now())""",
                (A.ORG_ID, ACCT, tid, d, amt, who, who))
c.commit()

cl = H.login(A)
stmt = ("Date,Description,Amount\n2026-09-05,DEPOSIT CASH,300000\n2026-09-10,TRANSFER TO SAVINGS,-500000\n"
        "2026-09-20,SUPPLIER X,-80000\n2026-09-22,SUPPLIER Y,-70000\n2026-09-25,NEW EXPENSE,-10000\n")
r = cl.post("/account/Stanbic/upload", data={"statement": (io.BytesIO(stmt.encode()), "s.csv"), "closing_balance": "0",
            "period_start": "2026-09-01", "period_end": "2026-09-30"}, content_type="multipart/form-data")
assert r.status_code == 302
page = cl.get("/account/Stanbic")
check("account page renders", page.status_code == 200 and b"id=mmform" in page.data)

node = shutil.which("node")
js = os.path.join(H.HERE, "ui", "account_page.test.js")
if not node or not os.path.isdir(os.path.join(H.HERE, "node_modules", "jsdom")):
    check("jsdom available (run: cd tests && npm install)", False)
else:
    with tempfile.NamedTemporaryFile("wb", suffix=".html", delete=False) as f:
        f.write(page.data)
    try:
        out = subprocess.run([node, js, f.name], capture_output=True, text=True, encoding="utf-8", cwd=H.HERE)
    finally:
        os.unlink(f.name)
    # Re-report each browser check so it shows up (and counts) like the others.
    lines = [l for l in out.stdout.splitlines() if l.startswith(("PASS ", "FAIL "))]
    for l in lines:
        check("browser: " + l[5:], l.startswith("PASS "))
    for l in out.stdout.splitlines():
        if l.startswith("   "):
            print(l)
    check("browser test ran to the end", out.returncode in (0, 1) and len(lines) > 10 and "failure(s)" in out.stdout)
    if out.stderr.strip():
        print(out.stderr[-2000:])

    # After an action the page opens at its section; the result must be shown there, not off-screen.
    cur = c.cursor()
    cur.execute("SELECT line_id FROM statement_line WHERE description='NEW EXPENSE'"); lid = str(cur.fetchone()[0]); c.rollback()
    r = cl.post("/account/Stanbic/record", data={"only": lid})   # no account chosen
    check("record without an account redirects to the record section", r.headers["Location"].endswith("#sec-record"))
    after = cl.get("/account/Stanbic").data
    FLASH = """
const { JSDOM } = require("jsdom");
const html = require("fs").readFileSync(process.argv[2], "utf8");
for (const hash of ["#sec-record", ""]) {
  const dom = new JSDOM(html, { runScripts: "dangerously", url: "http://app.test/account/Stanbic" + hash });
  dom.window.document.dispatchEvent(new dom.window.Event("DOMContentLoaded"));
  const f = dom.window.document.getElementById("flash");
  const prev = f && f.previousElementSibling;
  console.log(JSON.stringify({hash, text: f ? f.textContent : null, after: prev ? prev.id : null}));
}"""
    with tempfile.NamedTemporaryFile("wb", suffix=".html", delete=False) as f:
        f.write(after)
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, dir=H.HERE, encoding="utf-8") as g:
        g.write(FLASH)
    try:
        res = subprocess.run([node, g.name, f.name], capture_output=True, text=True, encoding="utf-8", cwd=H.HERE)
    finally:
        os.unlink(f.name); os.unlink(g.name)
    import json
    got = [json.loads(l) for l in res.stdout.splitlines() if l.startswith("{")]
    if res.stderr.strip():
        print(res.stderr[-1500:])
    check("result message says what's wrong", len(got) == 2 and "choose an account" in (got[0]["text"] or ""))
    check("…shown right under the record section heading", len(got) == 2 and got[0]["after"] == "sec-record")
    check("…and at the top when the page opens without a section", len(got) == 2 and got[1]["after"] != "sec-record")
    # Section menu: built from the h2[id^=sec-] headings actually on the page, with their counts.
    MENU = """
const { JSDOM } = require("jsdom");
const src = require("fs").readFileSync(process.argv[2], "utf8");
for (const drop of [false, true]) {
  // drop=true: the Suggested section isn't rendered (conditional sections must simply be left out).
  const html = drop ? src.replace("id=sec-review", "id=x-review") : src;
  const dom = new JSDOM(html, { runScripts: "dangerously", url: "http://app.test/account/Stanbic" });
  const w = dom.window, d = w.document;
  let scrolled = null; w.HTMLElement.prototype.scrollIntoView = function () { scrolled = this.id; };
  const menu = d.getElementById("secnav");
  const items = [...menu.querySelectorAll("a")];
  const heads = [...d.querySelectorAll("h2[id^=sec-]")].map(h => ({ id: h.id, text: h.textContent }));
  const out = { hidden: menu.hidden, heads, items: items.map(a => ({ href: a.getAttribute("href"), text: a.textContent })),
                on: (menu.querySelector("a.on") || {}).textContent || null };
  const rec = items.find(a => a.getAttribute("href") === "#sec-record");
  if (rec) { rec.click(); out.hash = w.location.hash; out.scrolled = scrolled; out.onAfter = (menu.querySelector("a.on") || {}).textContent || null; }
  out.overlay = d.getElementById("loadingov").classList.contains("on");
  console.log(JSON.stringify(out));
}"""
    with tempfile.NamedTemporaryFile("wb", suffix=".html", delete=False) as f:
        f.write(page.data)
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, dir=H.HERE, encoding="utf-8") as g:
        g.write(MENU)
    try:
        res = subprocess.run([node, g.name, f.name], capture_output=True, text=True, encoding="utf-8", cwd=H.HERE)
    finally:
        os.unlink(f.name); os.unlink(g.name)
    got = [json.loads(l) for l in res.stdout.splitlines() if l.startswith("{")]
    if res.stderr.strip():
        print(res.stderr[-1500:])
    check("section menu script ran without errors", len(got) == 2 and "Error" not in res.stderr)
    html_ = page.data.decode()
    css = re.search(r"\.secnav\{.*?@media print", html_, re.S).group(0)
    check("side menu has its own column beside the content (it can't overlap it)",
          "<div class=pagecols><nav id=secnav" in html_ and "</div></div><div class=appfoot>" in html_
          and "grid-template-columns:132px minmax(0,1000px)" in css)
    check("…and is never pinned over the page", "position:fixed" not in css)
    full, dropped = (got + [{}, {}])[:2]
    import re
    def count(text):
        m = re.search(r"\(([^)]*)\)", text)
        return sum(int(x) for x in re.findall(r"\d+", m.group(1))) if m else None
    heads = full.get("heads", [])
    items = full.get("items", [])
    print("   menu:", " | ".join(i["text"] for i in items))
    check("section menu is shown", full.get("hidden") is False)
    check("…one item per section heading on the page, in page order",
          len(heads) >= 4 and [i["href"] for i in items] == ["#" + h["id"] for h in heads])
    rec_h = next((h for h in heads if h["id"] == "sec-record"), None)
    rec_i = next((i for i in items if i["href"] == "#sec-record"), None)
    check("…Record item carries the section's count",
          rec_h and rec_i and rec_i["text"] == f"Record ({count(rec_h['text'])})")
    man_i = next((i for i in items if i["href"] == "#sec-manual"), None)
    check("…Match manually has no count", man_i and man_i["text"] == "Match manually")
    mat_h = next((h for h in heads if h["id"] == "sec-matched"), None)
    mat_i = next((i for i in items if i["href"] == "#sec-matched"), None)
    check("…Matched shows its count", mat_h and mat_i and mat_i["text"] == f"Matched ({count(mat_h['text'])})")
    check("…labels are short", all(len(i["text"]) <= 24 for i in items))
    check("one item is highlighted on load", bool(full.get("on")))
    check("clicking an item sets the hash", full.get("hash") == "#sec-record")
    check("…scrolls to that heading", full.get("scrolled") == "sec-record")
    check("…and highlights it", (full.get("onAfter") or "").startswith("Record"))
    check("…without triggering the loading overlay", full.get("overlay") is False)
    check("a section that isn't rendered isn't listed",
          "#sec-review" in [i["href"] for i in items]
          and "#sec-review" not in [i["href"] for i in dropped.get("items", [])]
          and len(dropped.get("items", [])) == len(items) - 1)
sys.exit(T.summary())
