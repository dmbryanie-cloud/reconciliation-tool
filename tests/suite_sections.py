"""Account page sections: each folds under its heading, sections that need attention come first and
start open, finished ones go to the bottom folded. Jumps (tiles, side menu, links, #hash) open a
folded section. The 'record them' list puts lines still to record first.

Run on its own with `python tests/suite_sections.py`, or all suites with `python tests/run_all.py`.
QuickBooks is fully mocked -- no network.
"""
import io, json, os, shutil, subprocess, sys, tempfile

import harness as H

ACCT = "00000000-0000-0000-0000-0000000000a1"
A, c = H.setup(H.account_sql((ACCT, "35", "Stanbic", "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check

A.qbo_is_connected = lambda: False
for tid, d, amt, who in (("b1", "2026-09-20", -80000, "Supplier X"), ("b2", "2026-09-22", -70000, "Supplier Y"),
                         ("b3", "2026-09-18", 300000, "Deposit (unmatched)")):
    cur.execute("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency,
                   description, counterparty, last_modified) VALUES (%s,%s,%s,'Purchase',%s,%s,'UGX',%s,%s,now())""",
                (A.ORG_ID, ACCT, tid, d, amt, who, who))
c.commit()

cl = H.login(A)
stmt = ("Date,Description,Amount\n2026-09-20,SUPPLIER X,-80000\n2026-09-22,SUPPLIER Y,-70000\n"
        "2026-09-25,NEW EXPENSE,-10000\n")
r = cl.post("/account/Stanbic/upload", data={"statement": (io.BytesIO(stmt.encode()), "s.csv"), "closing_balance": "0",
            "period_start": "2026-09-01", "period_end": "2026-09-30"}, content_type="multipart/form-data")
assert r.status_code == 302
page = cl.get("/account/Stanbic")
html = page.data.decode()
check("account page renders", page.status_code == 200)
check("headings carry their state", 'id=sec-matched style="font-size:15px" data-sec data-state=done' in html
      and 'id=sec-record style="font-size:15px" data-sec data-state="attn"' in html)
check("'In books, not on statement' is a section too", "id=sec-inbooks" in html)
check("an end marker stops the last section", "<div id=sec-end></div>" in html)

# record list order: interrupted/taken first, then lines to record, then unrecordable, recorded last
rows = [{"wb": "done", "recordable": True, "n": 1}, {"wb": None, "recordable": False, "n": 2},
        {"wb": None, "recordable": True, "n": 3}, {"wb": "taken", "recordable": True, "n": 4},
        {"wb": None, "recordable": True, "n": 5}, {"wb": "pending", "recordable": True, "n": 6}]
check("record list: problems, to record, can't record, recorded",
      [w["n"] for w in sorted(rows, key=A._record_rank)] == [4, 6, 3, 5, 2, 1])

JS = r"""
const { JSDOM, VirtualConsole } = require("jsdom");
const src = require("fs").readFileSync(process.argv[2], "utf8");
function run(html, hash) {
  const errors = []; const vc = new VirtualConsole(); vc.on("jsdomError", e => errors.push(String(e.message || e)));
  const dom = new JSDOM(html, { runScripts: "dangerously", url: "http://app.test/account/Stanbic" + (hash || ""), virtualConsole: vc });
  const w = dom.window, d = w.document;
  w.HTMLElement.prototype.scrollIntoView = function () {};
  const secs = () => [...d.querySelectorAll("section.dsec")].map(s => ({
    id: s.querySelector("h2").id, st: s.className.replace("dsec ", "").replace(" closed", ""),
    open: !s.classList.contains("closed"), hidden: s.querySelector(".dsec-body").hidden,
    badge: (s.querySelector(".dsec-badge") || {}).textContent, title: s.querySelector("h2").getAttribute("data-title"),
    n: s.querySelector(".dsec-body").children.length }));
  return { w, d, secs, errors };
}
const out = {};
let p = run(src);
out.first = p.secs();
out.bar = (p.d.querySelector(".dsecbar") || {}).textContent || "";
out.barBeforeFirst = !!p.d.querySelector(".dsecbar + section.dsec");
out.menu = [...p.d.querySelectorAll("#secnav a")].map(a => ({ href: a.getAttribute("href"), text: a.textContent, attn: a.classList.contains("attn") }));
out.tablesInside = [...p.d.querySelectorAll("table")].filter(t => !t.closest(".dsec") && !t.classList.contains("rec")).length;
// click a folded heading: opens and is remembered for the tab
const mh = p.d.getElementById("sec-matched"); mh.click();
out.afterClick = p.secs().find(s => s.id === "sec-matched");
out.stored = p.w.sessionStorage.getItem("sec:/account/Stanbic:sec-matched:done");
mh.click(); out.afterClick2 = p.secs().find(s => s.id === "sec-matched");
// a tile jumping into a folded section opens it
const tile = [...p.d.querySelectorAll("#dtiles .tile")].find(t => t.getAttribute("data-target") === "sec-matched");
tile.click(); out.afterTile = p.secs().find(s => s.id === "sec-matched");
// expand / collapse all
const [ex, co] = [...p.d.querySelectorAll(".dsecbar button")];
co.click(); out.allClosed = p.secs().every(s => !s.open && s.hidden);
ex.click(); out.allOpen = p.secs().every(s => s.open && !s.hidden);
// side-menu item opens its (folded) section
co.click(); const mi = p.d.querySelector('#secnav a[href="#sec-matched"]'); mi.click();
out.afterMenu = p.secs().find(s => s.id === "sec-matched");
out.errors = p.errors;
// opening the page on a #hash inside a folded section opens it
out.hash = run(src, "#sec-matched").secs().find(s => s.id === "sec-matched");
// nothing needs attention: the bar says so
const calm = run(src.replace(/data-state="attn"/g, 'data-state="done"').replace(/data-state="ready"/g, 'data-state="done"'));
out.calmBar = (calm.d.querySelector(".dsecbar") || {}).textContent || "";
out.calmAllClosed = calm.secs().every(s => !s.open);
console.log(JSON.stringify(out));
"""
node = shutil.which("node")
if not node or not os.path.isdir(os.path.join(H.HERE, "node_modules", "jsdom")):
    check("jsdom available (run: cd tests && npm install)", False)
else:
    with tempfile.NamedTemporaryFile("wb", suffix=".html", delete=False) as f:
        f.write(page.data)
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, dir=H.HERE, encoding="utf-8") as g:
        g.write(JS)
    try:
        res = subprocess.run([node, g.name, f.name], capture_output=True, text=True, encoding="utf-8", cwd=H.HERE)
    finally:
        os.unlink(f.name); os.unlink(g.name)
    got = next((json.loads(l) for l in res.stdout.splitlines() if l.startswith("{")), {})
    if res.stderr.strip():
        print(res.stderr[-1500:])
    first = got.get("first", [])
    print("   order:", " | ".join(f"{s['id']}:{s['st']}{'' if s['open'] else ' (folded)'}" for s in first))
    check("script ran without errors", bool(first) and not got.get("errors"))
    ids = [s["id"] for s in first]
    check("every section is wrapped", {"sec-balance", "sec-matched", "sec-record", "sec-exceptions", "sec-inbooks"} <= set(ids))
    ranks = [{"attn": 0, "ready": 1, "done": 2}[s["st"]] for s in first]
    check("sections needing attention come first", ranks == sorted(ranks) and ranks[0] == 0)
    check("…in their usual order among themselves",
          [s["id"] for s in first if s["st"] == "attn"] == ["sec-balance", "sec-record", "sec-manual", "sec-inbooks"])
    check("a finished section moves below them even if it came earlier on the page",
          ids.index("sec-matched") > ids.index("sec-inbooks"))
    check("attention sections start open", all(s["open"] and not s["hidden"] for s in first if s["st"] != "done"))
    check("finished sections start folded", all(not s["open"] and s["hidden"] for s in first if s["st"] == "done")
          and any(s["st"] == "done" for s in first))
    check("each section kept its content", all(s["n"] > 0 for s in first))
    check("no table was left outside a section", got.get("tablesInside") == 0)
    rec = next((s for s in first if s["id"] == "sec-record"), {})
    check("badge says what's left", rec.get("badge") == "1 to record")
    mat = next((s for s in first if s["id"] == "sec-matched"), {})
    check("…and a tick on finished ones", (mat.get("badge") or "").startswith("✓"))
    check("heading title is kept without the badge", (mat.get("title") or "").startswith("Matched (") and "✓" not in mat.get("title", ""))
    check("summary bar counts sections needing attention", "need attention" in got.get("bar", "") and got.get("barBeforeFirst"))
    menu = got.get("menu", [])
    check("side menu follows the new order", [m["href"] for m in menu] == ["#" + i for i in ids])
    check("…labels don't include the badge", all("✓" not in m["text"] and "to record" not in m["text"] for m in menu))
    check("…and marks sections needing attention",
          [m["attn"] for m in menu] == [s["st"] == "attn" for s in first])
    check("clicking a folded heading opens it", (got.get("afterClick") or {}).get("open") is True)
    check("…and remembers that for the tab", got.get("stored") == "1")
    check("clicking again folds it", (got.get("afterClick2") or {}).get("open") is False)
    check("a tile jumping into a folded section opens it", (got.get("afterTile") or {}).get("open") is True)
    check("Collapse all folds everything", got.get("allClosed") is True)
    check("Expand all opens everything", got.get("allOpen") is True)
    check("side-menu item opens its folded section", (got.get("afterMenu") or {}).get("open") is True)
    check("opening the page on a #section opens it", (got.get("hash") or {}).get("open") is True)
    check("nothing to do: the bar says so and all is folded",
          "Nothing needs attention" in got.get("calmBar", "") and got.get("calmAllClosed") is True)
sys.exit(T.summary())
