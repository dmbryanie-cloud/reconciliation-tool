"""The record list's Type box (Expense / Customer or student payment / Supplier payment / Transfer):
it narrows the account picker, and starts on a guess from the description and how well the line
matches earlier postings, saying why. A clear transfer pair also suggests the other account.

QuickBooks is fully mocked -- no network. The page's script is run in jsdom.

Run on its own with `python tests/suite_txn_type.py`, or all suites with `python tests/run_all.py`.
"""
import io, json, os, re, shutil, subprocess, sys, tempfile
from datetime import date

import harness as H

STB, DF = "00000000-0000-0000-0000-0000000000a1", "00000000-0000-0000-0000-0000000000a2"
A, c = H.setup(H.account_sql((STB, "35", "Stanbic UGX", "bank"), (DF, "36", "DFCU UGX", "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check

def acct(i, n, t, fqn=None):
    return {"Id": i, "Name": n, "FullyQualifiedName": fqn or n, "AccountType": t, "CurrencyRef": {"value": "UGX"}}
A._store_coa([acct("35", "Stanbic UGX", "Bank"), acct("36", "DFCU UGX", "Bank"),
              acct("83", "Office Supplies", "Expense"), acct("84", "Bank Charges", "Expense"),
              acct("90", "Tuition", "Income"), acct("60", "Accounts Receivable", "Accounts Receivable"),
              acct("222", "Rent payable UGX", "Accounts Payable")])
A._store_customers([{"Id": "12", "DisplayName": "2021001 Amy Okello (UGX)", "FullyQualifiedName": "2021001 Amy Okello (UGX)",
                     "CurrencyRef": {"value": "UGX"}}])
A._store_vendors([{"Id": "70", "DisplayName": "Regina Muwonge", "CurrencyRef": {"value": "UGX"}}])
A.qbo_is_connected = lambda: False

def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
def lid(desc):
    return str(q("SELECT line_id FROM statement_line WHERE description=%s", (desc,))[0][0])
cl = H.login(A)
def upload(name, rows):
    body = "Date,Description,Amount\n" + "".join(f"{d},{t},{a}\n" for d, t, a in rows)
    r = cl.post(f"/account/{name}/upload", data={"statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
                "period_start": "2026-07-01", "period_end": "2026-07-31"}, content_type="multipart/form-data")
    assert r.status_code == 302, r.data[:300]

# ---- the guess on its own -------------------------------------------------------------------------------------------
g = A.guess_type
pair = {"account": "DFCU UGX", "date": date(2026, 7, 2), "d": date(2026, 7, 1)}
check("a transfer pair on another statement: Transfer, saying where and how far apart",
      g("ANYTHING", True, -5, "gl", 0.9, pair) == ("xfer", "Transfer: the same amount moves the other way on DFCU UGX's statement, 1 day apart"))
check("a confident earlier posting wins over the description", g("TRF RENT", True, -5, "ap", 0.85)[0] == "ap"
      and "85% match" in g("TRF RENT", True, -5, "ap", 0.85)[1])
check("bank charges: Expense", g("EXCISE DUTY", True, -500)[0] == "gl")
check("'transfer' in the description: Transfer", g("TRF TO SAVINGS", True, -5)[0] == "xfer")
check("fees coming in: a student payment", g("SCHOOL FEES AMY", False, 5)[0] == "cust")
check("…but 'fees' going out is a charge, not a student", g("LEDGER FEES", True, -5)[0] == "gl")
check("rent going out: a supplier payment", g("RENT AUG", True, -5)[0] == "ap")
check("a weak earlier posting is used, and called weak", g("XYZ", True, -5, "gl", 0.3) == ("gl", "From similar lines posted before (30% match, a weak match)"))
check("nothing to go on: money out an expense, money in a payment", g("XYZ", True, -5)[0] == "gl" and g("XYZ", False, 5)[0] == "cust")

# ---- on the page ----------------------------------------------------------------------------------------------------
upload("DFCU UGX", [("2026-07-02", "FROM STANBIC", 500000)])
upload("Stanbic UGX", [("2026-07-01", "TO DFCU", -500000), ("2026-07-04", "SCHOOL FEES AMY", 731000),
                       ("2026-07-05", "RENT AUG", -400000), ("2026-07-06", "EXCISE DUTY", -500),
                       ("2026-07-07", "RANDOM SHOP", -12000)])
TR, FE, RE_, CH, RS = (lid(x) for x in ("TO DFCU", "SCHOOL FEES AMY", "RENT AUG", "EXCISE DUTY", "RANDOM SHOP"))
html = cl.get("/account/Stanbic UGX").data.decode()
def row(l):
    m = re.search(rf'<tr data-amt="[^"]*"[^>]*>\s*<td><input type=checkbox name=sel value="{l}".*?</tr>', html, re.S)
    return m.group(0) if m else ""
check("each line has a Type box", html.count("<select class=ttype") >= 5 and "<th>Type and account</th>" in html)
check("the transfer pair is guessed Transfer, with why", 'data-guess="xfer"' in row(TR) and "moves the other way on DFCU UGX" in row(TR))
check("…and DFCU is suggested as the account, but not ticked", 'data-sel="36"' in row(TR) and " checked>" not in row(TR).split("</td>")[0])
check("fees in: student payment", 'data-guess="cust"' in row(FE))
check("rent out: supplier payment", 'data-guess="ap"' in row(RE_))
check("excise duty: expense (bank charges)", 'data-guess="gl"' in row(CH) and "Bank charges" in row(CH))

node = shutil.which("node")
if not node or not os.path.isdir(os.path.join(H.HERE, "node_modules", "jsdom")):
    check("jsdom available (run: cd tests && npm install)", False)
else:
    JS = r"""
const { JSDOM, VirtualConsole } = require("jsdom");
const errors = []; const vc = new VirtualConsole(); vc.on("jsdomError", e => errors.push(String(e.message || e)));
const dom = new JSDOM(require("fs").readFileSync(process.argv[2], "utf8"), { runScripts: "dangerously", virtualConsole: vc, pretendToBeVisual: true });
const w = dom.window, d = w.document; w.HTMLElement.prototype.scrollIntoView = function () {};
const fire = (el, type, init = {}) => el.dispatchEvent(new w[type === "keydown" ? "KeyboardEvent" : "Event"](type, { bubbles: true, cancelable: true, ...init }));
const rowOf = id => d.querySelector(`input.rsel[value="${id}"]`).closest("tr");
const [TR, FE, RE, CH, RS] = process.argv.slice(3);
const listed = r => { const q = r.querySelector(".acctbox.main .acct-q"); fire(q, "focus");
  const n = [...r.querySelectorAll(".acctbox.main .acct-list .ao span:first-child")].map(s => s.textContent); fire(q, "blur"); return n; };
const out = {};
const tr = rowOf(TR), ts = tr.querySelector(".ttype");
out.outOpts = [...ts.options].map(o => o.value);
out.inOpts = [...rowOf(FE).querySelector(".ttype").options].map(o => o.value);
out.trType = ts.value; out.trAcct = tr.querySelector(".acctbox.main .acct-v").value;
out.feType = rowOf(FE).querySelector(".ttype").value; out.feList = listed(rowOf(FE));
const rs = rowOf(RS), rts = rs.querySelector(".ttype");
out.rsType = rts.value; out.rsList = listed(rs);
rts.value = "xfer"; fire(rts, "change"); out.xferList = listed(rs);
rts.value = ""; fire(rts, "change"); out.anyList = listed(rs);
// picking an account shows its type; changing to a type it isn't clears it
const q = rs.querySelector(".acctbox.main .acct-q"); fire(q, "focus"); q.value = "office supplies"; fire(q, "input"); fire(q, "keydown", { key: "Enter" }); fire(q, "blur");
out.pickedType = rts.value; out.ticked = rs.querySelector(".rsel").checked;
rts.value = "xfer"; fire(rts, "change");
out.clearedAcct = rs.querySelector(".acctbox.main .acct-v").value; out.unticked = !rs.querySelector(".rsel").checked;
out.whyHidden = (rs.querySelector(".ttype-why") || {}).hidden;
out.errors = errors; console.log(JSON.stringify(out));
"""
    f = tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8"); f.write(html); f.close()
    g_ = tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, dir=H.HERE, encoding="utf-8"); g_.write(JS); g_.close()
    try:
        res = subprocess.run([node, g_.name, f.name, TR, FE, RE_, CH, RS], capture_output=True, text=True, encoding="utf-8", cwd=H.HERE)
    finally:
        os.unlink(f.name); os.unlink(g_.name)
    if res.stderr.strip():
        print(res.stderr[-1500:])
    o = json.loads([l for l in res.stdout.splitlines() if l.startswith("{")][-1]) if "{" in res.stdout else {}
    print("   ", {k: v for k, v in o.items() if k != "errors"})
    check("browser: money out offers Expense, Supplier payment, Transfer", o.get("outOpts") == ["", "gl", "ap", "xfer"])
    check("browser: money in offers Customer/student payment, Deposit, Transfer", o.get("inOpts") == ["", "cust", "gl", "xfer"])
    check("browser: the transfer line starts on Transfer with DFCU picked", o.get("trType") == "xfer" and o.get("trAcct") == "36")
    check("browser: the fees line starts on student payment, listing students only",
          o.get("feType") == "cust" and o.get("feList") == ["2021001 Amy Okello (UGX)"])
    check("browser: an unknown payment starts on Expense, listing expense accounts",
          o.get("rsType") == "gl" and "Office Supplies" in o.get("rsList", []) and "DFCU UGX" not in o.get("rsList", [])
          and "Rent payable UGX" not in o.get("rsList", []))
    check("browser: Transfer lists only your own accounts", o.get("xferList") == ["DFCU UGX"])
    check("browser: Any type lists everything", {"Office Supplies", "Rent payable UGX", "DFCU UGX"} <= set(o.get("anyList", [])))
    check("browser: picking an account sets its type", o.get("pickedType") == "gl" and o.get("ticked"))
    check("browser: switching to a type the account isn't clears (and unticks) it", o.get("clearedAcct") == "" and o.get("unticked"))
    check("browser: …and the guess's reason is hidden once you choose", o.get("whyHidden") is True)
    check("browser: no script errors", o.get("errors") == [])
sys.exit(T.summary())
