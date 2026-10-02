"""Search bars over the account page's long lists, to find lines and tick them together: every word
must appear (words, amount with or without commas, date either way round, the chosen account);
"Tick only these" ticks what's found and unticks the rest; the header box ticks only what's shown.

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_search.py`, or all suites with `python tests/run_all.py`.
"""
import io, json, os, re, shutil, subprocess, sys, tempfile

import harness as H

STB = "00000000-0000-0000-0000-0000000000a1"
A, c = H.setup(H.account_sql((STB, "35", "Stanbic UGX", "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
A.qbo_is_connected = lambda: False
A._store_coa([{"Id": "35", "Name": "Stanbic UGX", "AccountType": "Bank", "CurrencyRef": {"value": "UGX"}},
              {"Id": "83", "Name": "Bank Charges", "AccountType": "Expense", "CurrencyRef": {"value": "UGX"}},
              {"Id": "90", "Name": "Tuition Income", "AccountType": "Income", "CurrencyRef": {"value": "UGX"}}])

def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
cl = H.login(A)
rows = ([(f"2026-06-{d:02d}", "EXCISE DUTY", -150) for d in (3, 10, 17)]
        + [("2026-06-10", "LEDGER FEES", -1500)]
        + [(f"2026-06-{d:02d}", f"SCHOOL FEES STUDENT {d}", 250000 + d) for d in (4, 5, 6, 7, 8, 11, 12, 13)])
body = "Date,Description,Amount\n" + "".join(f"{d},{t},{a}\n" for d, t, a in rows)
# Book entries a little off two of the fees: suggested matches to review (and a long enough list with more)
for i, d in enumerate((4, 5, 6, 7, 8, 11)):
    q("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency,
         description, counterparty, last_modified) VALUES (%s,%s,%s,'Deposit',%s,%s,'UGX',%s,%s,now()) RETURNING 1""",
      (A.ORG_ID, STB, f"b{i}", f"2026-06-{d:02d}", 250000 + d + 100, f"School fees student {d}", f"School fees student {d}"))
cl.post("/account/Stanbic UGX/upload", data={"statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
        "period_start": "2026-06-01", "period_end": "2026-06-30"}, content_type="multipart/form-data")
html = cl.get("/account/Stanbic UGX").data.decode()
check("the list to record has a search bar with Tick only these / Untick all",
      re.search(r'<div class=tsearch data-table=rectbl data-pick="\.rsel"><input type=search', html) is not None
      and "Tick only these" in html and "Untick all" in html)
n_rev = len(re.findall(r'name=mid value=', html))
check("…and so do the suggested matches (when there are more than five)", n_rev > 5 and 'data-table=revtbl' in html)
check("short lists have none", 'data-table=excstmt' not in html and 'data-table=xfertbl' not in html)

node = shutil.which("node")
if not node or not os.path.isdir(os.path.join(H.HERE, "node_modules", "jsdom")):
    print("SKIP browser checks (no node/jsdom)")
else:
    JS = r"""
const { JSDOM, VirtualConsole } = require("jsdom");
const errors = []; const vc = new VirtualConsole(); vc.on("jsdomError", e => errors.push(String(e.message || e)));
const dom = new JSDOM(require("fs").readFileSync(process.argv[2], "utf8"), { runScripts: "dangerously", virtualConsole: vc, pretendToBeVisual: true });
const w = dom.window, d = w.document, out = {};
const ts = d.querySelector('.tsearch[data-table=rectbl]'), inp = ts.querySelector("input"), n = ts.querySelector(".ts-n");
const find = v => { inp.value = v; inp.dispatchEvent(new w.KeyboardEvent("keydown", { key: "Enter", bubbles: true })); };
const shown = () => [...d.querySelectorAll("#rectbl tr[data-amt]")].filter(r => !r.classList.contains("tsx"))
                      .map(r => r.querySelector(".desc").firstChild.nodeValue.trim());
const ticked = () => [...d.querySelectorAll(".rsel:checked")].length;
find("excise"); out.excise = shown(); out.exciseN = n.textContent;
ts.querySelector("[data-only]").click(); out.onlyTicked = ticked(); out.onlyN = n.textContent;
out.onlyRight = [...d.querySelectorAll(".rsel:checked")].every(c => c.closest("tr").textContent.includes("EXCISE"));
out.selcount = d.getElementById("selcount").textContent;
find("1,500"); out.commas = shown(); find("1500"); out.plain = shown();
find("10/06/2026"); out.dmy = shown(); find("2026-06-10 excise"); out.both = shown();
find("student"); d.getElementById("selall").click(); out.allShown = ticked(); out.allN = n.textContent;
ts.querySelector("[data-none]").click(); out.none = ticked();
inp.dispatchEvent(new w.KeyboardEvent("keydown", { key: "Escape", bubbles: true })); out.cleared = shown().length; out.clearedN = n.textContent;
// suggested matches: the same search, and the bulk bar counts the ticks
const rs = d.querySelector('.tsearch[data-table=revtbl]'), ri = rs.querySelector("input");
ri.value = "student 4"; ri.dispatchEvent(new w.KeyboardEvent("keydown", { key: "Enter", bubbles: true }));
rs.querySelector("[data-only]").click();
out.revBtn = d.querySelector("#revbulk button").textContent; out.revN = rs.querySelector(".ts-n").textContent;
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
    check("browser: searching a word shows just those lines", o.get("excise") == ["EXCISE DUTY"] * 3
          and o.get("exciseN", "").startswith("Showing 3 of 12"))
    check("browser: Tick only these ticks the lines found and unticks every other",
          o.get("onlyTicked") == 3 and o.get("onlyRight") and o.get("selcount", "").startswith("Selected 3 of 12"))
    check("browser: amounts are found with or without commas", o.get("commas") == o.get("plain") == ["LEDGER FEES"])
    check("browser: dates are found either way round, combined with words",
          set(o.get("dmy") or []) == {"EXCISE DUTY", "LEDGER FEES"} and o.get("both") == ["EXCISE DUTY"])
    check("browser: the header box ticks only what's shown", o.get("allShown") == 8 + 3
          and "(3 not shown)" in o.get("allN", ""))
    check("browser: Untick all", o.get("none") == 0)
    check("browser: Escape clears the search and shows everything", o.get("cleared") == 12 and "Showing" not in o.get("clearedN", "x"))
    check("browser: suggested matches search too, and the bulk bar counts what's ticked",
          o.get("revBtn") == "Confirm selected (1)" and "Showing 1 of" in o.get("revN", ""))
    check("browser: no script errors", o.get("errors") == [])

sys.exit(T.summary())
