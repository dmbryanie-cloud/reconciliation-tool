"""Forward-deal hedges and family payments.

Hedges are booked the way the company always has: the USD leg as transfers USD bank -> FX in Transit
-> FX in Transit UGX at the month's transaction rate; the UGX receipt as a deposit that clears FX in
Transit UGX (USD x that rate) with the difference, gain or loss, on Forex Gain. A parent's lump sum
can be split into payments for their children, and batch suggestions try a family's payments first.
QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_hedges_family.py`, or all suites with `python tests/run_all.py`.
"""
import io, json, os, re, shutil, subprocess, sys, tempfile
from decimal import Decimal as D

import harness as H

USD, UGX, FXU, FXH = ("00000000-0000-0000-0000-0000000000a1", "00000000-0000-0000-0000-0000000000a2",
                      "00000000-0000-0000-0000-0000000000a3", "00000000-0000-0000-0000-0000000000a4")
A, c = H.setup(H.account_sql((USD, "37", "DFCU USD 12477", "bank"), (UGX, "35", "DFCU UGX 04353", "bank"),
                             (FXU, "39", "FX in Transit", "bank"), (FXH, "38", "FX in Transit UGX", "bank")))
cur = c.cursor()
cur.execute("UPDATE account SET currency='USD' WHERE account_id IN (%s,%s)", (USD, FXU)); c.commit()
T = H.Checker()
check = T.check

def acct(i, n, t, ccy, fqn=None):
    return {"Id": i, "Name": n, "FullyQualifiedName": fqn or n, "AccountType": t, "CurrencyRef": {"value": ccy}}
A._store_coa([acct("37", "DFCU USD 12477", "Bank", "USD"), acct("35", "DFCU UGX 04353", "Bank", "UGX"),
              acct("39", "FX in Transit", "Bank", "USD", "Bank Accounts USD:FX in Transit"),
              acct("38", "FX in Transit UGX", "Bank", "UGX", "Bank Accounts:FX in Transit UGX"),
              acct("70", "Forex Gain", "Other Income", "UGX", "Other Income:Forex Gain"),
              acct("90", "Tuition", "Income", "UGX"), acct("83", "Office Supplies", "Expense", "UGX"),
              acct("60", "Accounts Receivable (A/R)", "Accounts Receivable", "UGX")])
A._store_customers([{"Id": "P1", "DisplayName": "Okello Family", "CurrencyRef": {"value": "UGX"}},
                    {"Id": "K1", "DisplayName": "Okello Anna", "CurrencyRef": {"value": "UGX"}, "ParentRef": {"value": "P1"}},
                    {"Id": "K2", "DisplayName": "Okello Ben", "CurrencyRef": {"value": "UGX"}, "ParentRef": {"value": "P1"}},
                    {"Id": "C9", "DisplayName": "Someone Else", "CurrencyRef": {"value": "UGX"}}])
POSTS = []
def fake_post(token, entity, body):
    POSTS.append((entity, json.loads(json.dumps(body))))
    return {entity: {"Id": str(700 + len(POSTS))}}
A.qbo_post = fake_post
A.qbo_token = lambda: "tok"
A.qbo_realm = lambda: "REAL-1"
A.qbo_is_connected = lambda: False

def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
def lid(desc):
    return str(q("""SELECT sl.line_id FROM statement_line sl JOIN statement s USING (statement_id)
                    WHERE sl.description=%s ORDER BY s.created_at DESC LIMIT 1""", (desc,))[0][0])
def bk(acct_, tid):
    r = q("SELECT amount, currency FROM book_txn WHERE account_id=%s AND source_txn_id=%s", (acct_, tid))
    return r[0] if r else None
cl = H.login(A)
def upload(name, rows):
    body = "Date,Description,Amount\n" + "".join(f"{d},{t},{a}\n" for d, t, a in rows)
    r = cl.post(f"/account/{name}/upload", data={"statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
                "period_start": "2026-06-01", "period_end": "2026-06-30"}, content_type="multipart/form-data")
    assert r.status_code == 302, r.data[:300]
def page(name):
    return cl.get(f"/account/{name}").data.decode()
def msg(name):
    m = re.search(r"<div id=flash[^>]*>(.*?)</div>", page(name), re.S)
    return m.group(1) if m else ""

DEAL = "FXPLOU~1110179~FWD~BUY~USD/UGX~3.840.0000".replace("3.840", "3840")
upload("DFCU USD 12477", [("2026-06-15", DEAL, -50000), ("2026-06-16", "FXPLSP~1160575~SPOT~SELL~USD/UGX~3720.00", 1000)])
L_OUT = lid(DEAL)
p = page("DFCU USD 12477")
check("forward deal line offers Hedge", "hedge-btn" in p and 'data-leg="out"' in p and "Forward deal 1110179" in p)
check("…spot deals don't (left out)", p.count('class="btn-sm hedge-btn"') == 1)
check("…names the accounts it will use", "Bank Accounts USD:FX in Transit" in p and "Bank Accounts:FX in Transit UGX" in p)

# ---- the USD leg ----------------------------------------------------------------------------------
n = len(POSTS)
cl.post("/account/DFCU USD 12477/record", data={"only": L_OUT, f"hedge_{L_OUT}": "1", f"hedge_rate_{L_OUT}": ""})
check("hedge without the month's rate: asks for it", len(POSTS) == n and "type this month&#39;s transaction rate" in msg("DFCU USD 12477"))
cl.post("/account/DFCU USD 12477/record", data={"only": L_OUT, f"hedge_{L_OUT}": "1", f"hedge_rate_{L_OUT}": "3,720"})
(e1, b1), (e2, b2) = POSTS[-2:]
check("USD leg: two transfers, as booked by hand", len(POSTS) == n + 2 and e1 == e2 == "Transfer")
check("…USD bank -> FX in Transit", b1["FromAccountRef"]["value"] == "37" and b1["ToAccountRef"]["value"] == "39" and b1["Amount"] == 50000.0)
check("…FX in Transit -> FX in Transit UGX", b2["FromAccountRef"]["value"] == "39" and b2["ToAccountRef"]["value"] == "38")
check("…in USD at the month's rate", all(b["CurrencyRef"] == {"value": "USD"} and b["ExchangeRate"] == 3720.0 for b in (b1, b2)))
t1, t2 = str(n + 701), str(n + 702)
check("…books: USD bank out 50,000 USD", bk(USD, t1) == (D("-50000"), "USD"))
check("…FX in Transit in and out", bk(FXU, t1) == (D("50000"), "USD") and bk(FXU, t2) == (D("-50000"), "USD"))
check("…FX in Transit UGX receives 186,000,000 UGX (converted)", bk(FXH, t2) == (D("186000000.00"), "UGX"))
check("…the month's rate is remembered", q("SELECT value FROM app_config WHERE key='fx_rate:USD:2026-06'") == [("3720",)])
check("…and the deal's leg kept for its receipt", q("SELECT deal, usd, rate FROM hedge_leg") == [("1110179", D("50000"), D("3720"))])

# ---- the UGX receipt: principal clears FX in Transit UGX, the difference is the gain ----------------
upload("DFCU UGX 04353", [("2026-06-15", DEAL, 192000000), ("2026-06-18", "FXPLOU~949893~FWD~BUY~USD/UGX~3660.0000", 36600000),
                          ("2026-06-20", "OKELLO FAMILY FEES", 900000)])
L_IN, L_LOSS, L_FAM = lid(DEAL), lid("FXPLOU~949893~FWD~BUY~USD/UGX~3660.0000"), lid("OKELLO FAMILY FEES")
p = page("DFCU UGX 04353")
check("receipt: amount and rate filled in from the USD leg", f'name="hedge_usd_{L_IN}" class=hedge-usd inputmode=decimal value="50000' in p
      and f'name="hedge_rate_{L_IN}" class=hedge-rate inputmode=decimal value="3720' in p)
check("…another deal: USD worked out from its forward rate, rate from the month", f'name="hedge_usd_{L_LOSS}" class=hedge-usd inputmode=decimal value="10000.00"' in p
      and f'name="hedge_rate_{L_LOSS}" class=hedge-rate inputmode=decimal value="3720"' in p)
n = len(POSTS)
cl.post("/account/DFCU UGX 04353/record", data={"only": L_IN, f"hedge_{L_IN}": "1", f"hedge_usd_{L_IN}": "50,000", f"hedge_rate_{L_IN}": "3720"})
e, b = POSTS[-1]
ln = [(x["DepositLineDetail"]["AccountRef"]["value"], x["Amount"]) for x in b["Line"]]
check("receipt: one deposit into the UGX bank", e == "Deposit" and b["DepositToAccountRef"]["value"] == "35" and len(POSTS) == n + 1)
check("…186,000,000 clears FX in Transit UGX, 6,000,000 gain to Forex Gain", ln == [("38", 186000000.0), ("70", 6000000.0)])
check("…adds up to what the bank received", sum(a for _, a in ln) == 192000000.0 and "CurrencyRef" not in b)
cl.post("/account/DFCU UGX 04353/record", data={"only": L_LOSS, f"hedge_{L_LOSS}": "1", f"hedge_usd_{L_LOSS}": "10000", f"hedge_rate_{L_LOSS}": "3700"})
e, b = POSTS[-1]
ln = [(x["DepositLineDetail"]["AccountRef"]["value"], x["Amount"]) for x in b["Line"]]
check("a loss: the Forex Gain line is negative", ln == [("38", 37000000.0), ("70", -400000.0)])

# ---- a parent's lump sum split between children ---------------------------------------------------
n = len(POSTS)
base = {"only": L_FAM, f"acct_{L_FAM}": "60", f"cust_{L_FAM}": "P1"}
cl.post("/account/DFCU UGX 04353/record", data={**base, f"kids_{L_FAM}": json.dumps([{"c": "K1", "v": "500000"}, {"c": "K2", "v": "300000"}])})
check("children's amounts must add up", len(POSTS) == n and "add up to 800,000.00, not 900,000.00" in msg("DFCU UGX 04353"))
cl.post("/account/DFCU UGX 04353/record", data={**base, f"kids_{L_FAM}": json.dumps([{"c": "K1", "v": "500000"}, {"c": "C9", "v": "400000"}])})
check("only the parent and their children", len(POSTS) == n and "only between Okello Family and their children" in msg("DFCU UGX 04353"))
cl.post("/account/DFCU UGX 04353/record", data={**base, f"kids_{L_FAM}": json.dumps([{"c": "K1", "v": "500,000"}, {"c": "K2", "v": "400000"}])})
got = [(e_, b_["CustomerRef"]["value"], b_["TotalAmt"]) for e_, b_ in POSTS[n:]]
check("one payment per child", got == [("Payment", "K1", 500000.0), ("Payment", "K2", 400000.0)])
m = q("""SELECT m.status, count(mbt.txn_id) FROM match m JOIN match_statement_line msl USING (match_id)
         JOIN match_book_txn mbt USING (match_id) WHERE msl.line_id=%s GROUP BY m.status""", (L_FAM,))
check("…and the bank line matched to both", m == [("confirmed", 2)])

# ---- batch suggestions: a family's payments first ------------------------------------------------
for tid, d, amt, ref, who in (("f1", "2026-06-05", 220000, "Customer:K1", "Okello Anna"), ("f2", "2026-06-06", 290000, "Customer:K2", "Okello Ben"),
                              ("d1", "2026-06-24", 220000, "Customer:C9", "Someone Else"), ("d2", "2026-06-24", 290000, None, "Random")):
    cur.execute("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency,
                   description, counterparty, counterparty_ref, last_modified) VALUES (%s,%s,%s,'Payment',%s,%s,'UGX',%s,%s,%s,now())""",
                (A.ORG_ID, UGX, tid, d, amt, who, who, ref))
c.commit()
upload("DFCU UGX 04353", [("2026-06-25", "OKELLO LUMP SUM", 510000)])
r = q("""SELECT array(SELECT bt.source_txn_id FROM match_book_txn mbt JOIN book_txn bt USING (txn_id) WHERE mbt.match_id=m.match_id
                      ORDER BY 1), m.match_type, m.status
         FROM match m JOIN match_statement_line msl USING (match_id) WHERE msl.line_id=%s""", (lid("OKELLO LUMP SUM"),))
check("lump sum: the children's payments suggested together, not closer-dated strangers",
      r == [(["f1", "f2"], "many_to_one", "proposed")])

# ---- the page's script ------------------------------------------------------------------------------
upload("DFCU UGX 04353", [("2026-06-15", "FXPLOU~555555~FWD~BUY~USD/UGX~3800.0000", 38000000), ("2026-06-26", "OKELLO TERM 2", 700000)])
L_H, L_K = lid("FXPLOU~555555~FWD~BUY~USD/UGX~3800.0000"), lid("OKELLO TERM 2")
html = page("DFCU UGX 04353")
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
const out = {};
const h = rowOf(process.argv[3]);
h.querySelector(".hedge-btn").click();
let hr = h.nextElementSibling; while (!hr.classList.contains("hedgerow")) hr = hr.nextElementSibling;
out.hedgeOpen = !hr.hidden && h.querySelector(".hedge-on").value === "1";
const rate = hr.querySelector(".hedge-rate"); rate.value = "3,720"; fire(rate, "input");
out.calc = hr.querySelector(".hedgecalc").textContent;
out.ticked = h.querySelector(".rsel").checked;
hr.querySelector(".hedge-cancel").click();
out.closed = hr.hidden && h.querySelector(".hedge-on").value === "";
const k = rowOf(process.argv[4]);
const aq = k.querySelector(".acctbox.main .acct-q"); fire(aq, "focus"); aq.value = "receivable"; fire(aq, "input"); fire(aq, "keydown", { key: "Enter" }); fire(aq, "blur");
const cq = k.querySelector(".custbox .acct-q"); fire(cq, "focus"); cq.value = "okello family"; fire(cq, "input"); fire(cq, "keydown", { key: "Enter" }); fire(cq, "blur");
let kr = k.nextElementSibling; while (!kr.classList.contains("kidsrow")) kr = kr.nextElementSibling;
out.kidsOpen = !kr.hidden; out.kidNames = [...kr.querySelectorAll(".kidline span")].map(s => s.textContent);
out.remNone = kr.querySelector(".splitrem").textContent;
const ins = kr.querySelectorAll(".kidline input"); ins[1].value = "400000"; fire(ins[1], "input");
out.remPart = kr.querySelector(".splitrem").textContent;
ins[2].value = "300,000"; fire(ins[2], "input");
out.remDone = kr.querySelector(".splitrem").textContent;
out.kidsJson = JSON.parse(k.querySelector(".kids-v").value);
out.errors = errors;
console.log(JSON.stringify(out));
"""
    f = tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8"); f.write(html); f.close()
    g = tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, dir=H.HERE, encoding="utf-8"); g.write(JS); g.close()
    try:
        res = subprocess.run([node, g.name, f.name, L_H, L_K], capture_output=True, text=True, encoding="utf-8", cwd=H.HERE)
    finally:
        os.unlink(f.name); os.unlink(g.name)
    if res.stderr.strip():
        print(res.stderr[-2000:])
    o = json.loads([l for l in res.stdout.splitlines() if l.startswith("{")][-1]) if "{" in res.stdout else {}
    print("   ", {k_: o.get(k_) for k_ in ("calc", "kidNames", "remNone", "remPart", "remDone")})
    check("browser: Hedge opens its panel", o.get("hedgeOpen"))
    check("browser: …shows principal and gain as you type the rate",
          o.get("calc") == "FX in Transit 37,200,000.00  ·  gain 800,000.00 to Forex Gain")
    check("browser: …ticks the line, and 'Not a hedge' closes it", o.get("ticked") and o.get("closed"))
    check("browser: a parent with children offers the split", o.get("kidsOpen")
          and o.get("kidNames") == ["Okello Family (the parent)", "Okello Anna", "Okello Ben"])
    check("browser: …nothing split means all to the parent", "whole amount goes to Okello Family" in (o.get("remNone") or ""))
    check("browser: …shows what's left, then that it adds up", o.get("remPart") == "Left to allocate: 300,000.00"
          and o.get("remDone") == "Adds up to 700,000.00")
    check("browser: …only the children given an amount are sent", o.get("kidsJson") == [{"c": "K1", "v": "400000"}, {"c": "K2", "v": "300000"}])
    check("browser: no script errors", o.get("errors") == [])
sys.exit(T.summary())
