"""Transfers between banks in two currencies (a UGX bank and a USD one), at an exchange rate:
offered in the picker with a rate box, recorded as one QuickBooks Transfer in the foreign currency,
each account's books in its own currency, and pairs seen on both statements at the rate they imply.
Two foreign currencies (USD and EUR) can't be transferred between in QuickBooks, so they aren't offered.

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_transfer_fx.py`, or all suites with `python tests/run_all.py`.
"""
import html, io, json, os, re, shutil, subprocess, sys, tempfile
from decimal import Decimal as D

import harness as H

UGX, USD, EUR, CEN = ("00000000-0000-0000-0000-0000000000a1", "00000000-0000-0000-0000-0000000000a2",
                      "00000000-0000-0000-0000-0000000000a3", "00000000-0000-0000-0000-0000000000a4")
A, c = H.setup(H.account_sql((UGX, "35", "Stanbic UGX", "bank"), (USD, "37", "Stanbic USD", "bank"),
                             (EUR, "39", "Stanbic EUR", "bank"), (CEN, "36", "Centenary UGX", "bank")))
cur = c.cursor()
cur.execute("UPDATE account SET currency='USD' WHERE account_id=%s", (USD,))
cur.execute("UPDATE account SET currency='EUR' WHERE account_id=%s", (EUR,)); c.commit()
T = H.Checker()
check = T.check
A._store_coa([{"Id": "35", "Name": "Stanbic UGX", "AccountType": "Bank", "CurrencyRef": {"value": "UGX"}},
              {"Id": "36", "Name": "Centenary UGX", "AccountType": "Bank", "CurrencyRef": {"value": "UGX"}},
              {"Id": "37", "Name": "Stanbic USD", "AccountType": "Bank", "CurrencyRef": {"value": "USD"}},
              {"Id": "39", "Name": "Stanbic EUR", "AccountType": "Bank", "CurrencyRef": {"value": "EUR"}},
              {"Id": "83", "Name": "Office Supplies", "AccountType": "Expense", "CurrencyRef": {"value": "UGX"}},
              {"Id": "90", "Name": "Sales", "AccountType": "Income", "CurrencyRef": {"value": "UGX"}}])
POSTS, QBO, RATES = [], {}, []
def fake_post(token, entity, body):
    POSTS.append((entity, json.loads(json.dumps(body))))
    i = str(900 + len(POSTS))
    QBO[i] = {**body, "Id": i, "SyncToken": "0"}
    return {entity: {"Id": i}}
def fake_rate(token, ccy, d):
    RATES.append((ccy, str(d)))
    if str(d) == "2026-09-29":
        raise LookupError("no rate")
    return 3650.0
A.qbo_post, A.qbo_read = fake_post, lambda token, entity, i: QBO.get(str(i))
A.qbo_exchange_rate = fake_rate
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
def book(acct, tid):
    r = q("SELECT amount, currency FROM book_txn WHERE account_id=%s AND source_txn_type='Transfer' AND source_txn_id=%s",
          (acct, tid))
    return r[0] if r else None
def coa_json(page):
    return json.loads(re.search(r"<script id=coa-data type=application/json>(.*?)</script>", page, re.S).group(1))
cl = H.login(A)
def page(n="Stanbic UGX"):
    return cl.get(f"/account/{n}").data.decode()
def msg(n="Stanbic UGX"):
    m = re.search(r"<div id=flash[^>]*>(.*?)</div>", page(n), re.S)
    return html.unescape(m.group(1)) if m else ""
def upload(acct_name, rows):
    body = "Date,Description,Amount\n" + "".join(f"{d},{t},{a}\n" for d, t, a in rows)
    r = cl.post(f"/account/{acct_name}/upload", data={"replace": "1", "statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
                "period_start": "2026-09-01", "period_end": "2026-09-30"}, content_type="multipart/form-data")
    assert r.status_code == 302, r.data[:300]

upload("Stanbic UGX", [("2026-09-02", "TO USD ACCOUNT", -3700000), ("2026-09-03", "TO USD ODD RATE", -1000000),
                       ("2026-09-04", "TO USD QBO RATE", -730000), ("2026-09-29", "TO USD NO RATE", -500000),
                       ("2026-09-05", "FROM USD ACCOUNT", 1850000), ("2026-09-10", "FX TRF OUT", -7400000),
                       ("2026-09-12", "TO USD EDIT", -365000), ("2026-09-14", "PRINTER INK", -45000),
                       ("2026-09-16", "TO CENTENARY", -80000)])
upload("Centenary UGX", [("2026-09-16", "FROM STANBIC", 80000)])
upload("Stanbic USD", [("2026-09-06", "TO UGX ACCOUNT", -600), ("2026-09-10", "FX TRF IN", 2000),
                       ("2026-09-11", "USD SAME WAY", -2000), ("2026-09-15", "USD TO EUR", -100)])
upload("Stanbic EUR", [("2026-09-15", "EUR FROM USD", 90)])

# ---- the picker -----------------------------------------------------------------------------------------
p = page()
xs = {a["n"]: a.get("c") for a in coa_json(p) if a["x"] == 1}
check("UGX bank: the USD and EUR banks are offered, marked as another currency",
      xs.get("Stanbic USD") == "USD" and xs.get("Stanbic EUR") == "EUR" and "Centenary UGX" in xs and xs["Centenary UGX"] is None)
check("…only transfer accounts carry a currency mark (an expense doesn't)",
      all("c" not in a for a in coa_json(p) if a["x"] != 1))
check("…same-currency banks listed before the others",
      [a["n"] for a in coa_json(p) if a["x"] == 1][0] == "Centenary UGX")
xs_usd = {a["n"]: a.get("c") for a in coa_json(page("Stanbic USD")) if a["x"] == 1}
check("USD bank: the UGX banks are offered, not the EUR one (QuickBooks can't move USD to EUR)",
      xs_usd.get("Stanbic UGX") == "UGX" and "Stanbic EUR" not in xs_usd)
L = lid("TO USD ACCOUNT")
check("a rate box for every line on the UGX bank, out and in", f'name="rate_{L}"' in p
      and f'name="rate_{lid("FROM USD ACCOUNT")}" value="" class="rate xrate"' in p)
check("the help says how it's recorded", "in the foreign currency, at the rate you type" in p)

# ---- from the record list, UGX bank -> USD bank ---------------------------------------------------------
cl.post("/account/Stanbic UGX/record", data={"only": L, f"acct_{L}": "37", f"rate_{L}": "3,700"})
ent, body = POSTS[-1]
TID = str(900 + len(POSTS))
check("recorded as one Transfer in USD: 3,700,000 at 3,700 = USD 1,000",
      ent == "Transfer" and body["FromAccountRef"]["value"] == "35" and body["ToAccountRef"]["value"] == "37"
      and body["Amount"] == 1000.0 and body["CurrencyRef"] == {"value": "USD"} and body["ExchangeRate"] == 3700.0)
check("…each account's books in its own currency", book(UGX, TID) == (D("-3700000.00"), "UGX")
      and book(USD, TID) == (D("1000.00"), "USD"))
check("…and the bank line is matched", matched(L))
check("…no QuickBooks rate looked up when one is typed", RATES == [])

L = lid("TO USD ODD RATE")
cl.post("/account/Stanbic UGX/record", data={"only": L, f"acct_{L}": "37", f"rate_{L}": "3712.5"})
ent, body = POSTS[-1]
TID = str(900 + len(POSTS))
check("an uneven rate: USD rounded to the cent (269.36), rate exact so UGX ties to the shilling",
      body["Amount"] == 269.36 and abs(body["ExchangeRate"] - 1000000 / 269.36) < 1e-6
      and book(UGX, TID) == (D("-1000000.00"), "UGX") and book(USD, TID) == (D("269.36"), "USD") and matched(L))

L = lid("TO USD QBO RATE")
cl.post("/account/Stanbic UGX/record", data={"only": L, f"acct_{L}": "37"})
ent, body = POSTS[-1]
check("no rate typed: QuickBooks' USD rate for the date (730,000 at 3,650 = USD 200)",
      RATES == [("USD", "2026-09-04")] and body["Amount"] == 200.0 and body["ExchangeRate"] == 3650.0)

L = lid("TO USD NO RATE")
n = len(POSTS)
cl.post("/account/Stanbic UGX/record", data={"only": L, f"acct_{L}": "37"})
check("QuickBooks has no rate: not recorded, asks for one", len(POSTS) == n and "type the rate (UGX per USD)" in msg())
cl.post("/account/Stanbic UGX/record", data={"only": L, f"acct_{L}": "37", f"rate_{L}": "0"})
check("…a zero rate is refused", len(POSTS) == n and "isn't a usable number" in msg())

L = lid("FROM USD ACCOUNT")
cl.post("/account/Stanbic UGX/record", data={"only": L, f"acct_{L}": "37", f"rate_{L}": "3700"})
ent, body = POSTS[-1]
TID = str(900 + len(POSTS))
check("money in from the USD bank: USD 500 from Stanbic USD into Stanbic UGX",
      body["FromAccountRef"]["value"] == "37" and body["ToAccountRef"]["value"] == "35" and body["Amount"] == 500.0
      and body["CurrencyRef"] == {"value": "USD"} and book(UGX, TID) == (D("1850000.00"), "UGX")
      and book(USD, TID) == (D("-500.00"), "USD"))

# ---- from the USD bank's list, USD -> UGX: the USD rate box it always has ----------------------------------
L = lid("TO UGX ACCOUNT")
cl.post("/account/Stanbic USD/record", data={"only": L, f"acct_{L}": "35", f"rate_{L}": "3650"})
ent, body = POSTS[-1]
TID = str(900 + len(POSTS))
check("USD bank -> UGX bank: USD 600 at 3,650", body["FromAccountRef"]["value"] == "37" and body["ToAccountRef"]["value"] == "35"
      and body["Amount"] == 600.0 and body["ExchangeRate"] == 3650.0 and book(UGX, TID) == (D("2190000.00"), "UGX")
      and book(USD, TID) == (D("-600.00"), "USD") and matched(L))
L = lid("USD TO EUR")
n = len(POSTS)
cl.post("/account/Stanbic USD/record", data={"only": L, f"acct_{L}": "39", f"rate_{L}": "3650"})
check("USD -> EUR isn't possible", len(POSTS) == n)

# ---- a pair seen on both statements: the rate is what the two amounts say --------------------------------
U, S = lid("FX TRF OUT"), lid("FX TRF IN")
n = len(POSTS)
cl.post("/account/Stanbic USD/transfer", data={"line": lid("USD SAME WAY"), "other": U})
check("pair refused when both lines move the same way", len(POSTS) == n and "opposite directions" in msg("Stanbic USD"))
cl.post("/account/Stanbic USD/transfer", data={"line": lid("USD TO EUR"), "other": lid("EUR FROM USD")})
check("pair refused between two foreign currencies", len(POSTS) == n and "one of them in UGX" in msg("Stanbic USD"))
cl.post("/account/Stanbic UGX/transfer", data={"line": U, "other": S})
ent, body = POSTS[-1]
TID = str(900 + len(POSTS))
check("UGX 7,400,000 out, USD 2,000 in: one Transfer of USD 2,000 at 3,700",
      ent == "Transfer" and body["FromAccountRef"]["value"] == "35" and body["ToAccountRef"]["value"] == "37"
      and body["Amount"] == 2000.0 and body["CurrencyRef"] == {"value": "USD"} and body["ExchangeRate"] == 3700.0)
check("…both books in their own currency, both lines matched", book(UGX, TID) == (D("-7400000.00"), "UGX")
      and book(USD, TID) == (D("2000.00"), "USD") and matched(U) and matched(S))
check("…and it says so", "USD 2,000.00 at 3,700.00" in msg())

# ---- Edit -> Only the account, with a rate ----------------------------------------------------------------
L = lid("TO USD EDIT")
p = page()
check("Possible transfers -> Edit offers the USD bank too, labelled, with a rate box",
      'data-ccy="USD">Stanbic USD (USD, at a rate)' in p and "class=xe-rate hidden" in p)
cl.post("/account/Stanbic UGX/transfer", data={"line": L, "other": "", "other_acct": "37", "rate": "3650"})
ent, body = POSTS[-1]
TID = str(900 + len(POSTS))
check("…recorded at the rate typed there: 365,000 at 3,650 = USD 100",
      body["Amount"] == 100.0 and body["ExchangeRate"] == 3650.0 and book(UGX, TID) == (D("-365000.00"), "UGX") and matched(L))

# ---- changing a recorded one: only within its currency --------------------------------------------------
n = len(POSTS)
cl.post("/account/Stanbic UGX/transfer_change", data={"qbo_id": TID, "other": "", "other_acct": "36"})
check("a transfer between two currencies can't be changed in place (Undo and record again)",
      len(POSTS) == n and "can't be changed here" in msg())
check("…and the change form only offers same-currency banks",
      re.search(r'transfer_change.*?</form>', page(), re.S) and "Stanbic USD (USD" not in
      re.search(r'transfer_change.*?</form>', page(), re.S).group(0))

# ---- in a browser: the rate box appears for a USD bank, and goes for anything else ------------------------
node = shutil.which("node")
if node and os.path.isdir(os.path.join(H.HERE, "node_modules", "jsdom")):
    L = lid("PRINTER INK")
    JS = r"""
const { JSDOM } = require("jsdom");
const dom = new JSDOM(require("fs").readFileSync(process.argv[2], "utf8"), { runScripts: "dangerously", pretendToBeVisual: true });
const d = dom.window.document, id = process.argv[3];
const tr = d.querySelector('input[name="rate_' + id + '"]').closest('tr'), q = tr.querySelector('.acctbox.main .acct-q');
const xr = tr.querySelector('.xrate'), ts = tr.querySelector('.ttype');
function pick(name) { if (ts) ts.value = '';   // any type (picking one sets it)
  q.value = name; q.dispatchEvent(new dom.window.Event('input'));
  const o = [...tr.querySelectorAll('.acct-list .ao')].find(o => o.firstChild.textContent === name);
  o.dispatchEvent(new dom.window.MouseEvent('mousedown', { bubbles: true, cancelable: true })); }
const before = xr.hidden;
pick('Stanbic USD');
const usd = { hidden: xr.hidden, ph: xr.placeholder };
pick('Office Supplies');
console.log(JSON.stringify({ before, usd, after: xr.hidden }));
"""
    f = tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8"); f.write(page()); f.close()
    g = tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, dir=H.HERE, encoding="utf-8"); g.write(JS); g.close()
    try:
        res = subprocess.run([node, g.name, f.name, L], capture_output=True, text=True, encoding="utf-8", cwd=H.HERE)
    finally:
        os.unlink(f.name); os.unlink(g.name)
    o = json.loads([l for l in res.stdout.splitlines() if l.startswith("{")][-1]) if "{" in res.stdout else {}
    check("browser: the rate box shows for the USD bank (UGX per USD) and hides for an expense",
          o == {"before": True, "usd": {"hidden": False, "ph": "UGX per USD"}, "after": True} or print(res.stdout, res.stderr))
else:
    print("SKIP browser check (no node/jsdom)")
# ---- the other bank's statement decides the foreign amount (QuickBooks' rate for the day isn't the bank's) --
upload("Stanbic UGX", [("2026-09-24", "TO DFCU USD", -333000000), ("2026-09-25", '"FXPLSP~1206232~SPOT~BUY~USD/UGX~3,733.000"', 559950000),
                       ("2026-09-23", "FROM USD SIDE", 3690000)])
upload("Stanbic USD", [("2026-09-24", "IN FROM UGX", 90000), ("2026-09-23", "OUT TO UGX", -1000)])
RATES.clear()
L = lid("TO DFCU USD")
cl.post("/account/Stanbic UGX/record", data={"only": L, f"acct_{L}": "37"})
ent, body = POSTS[-1]
TID = str(900 + len(POSTS))
check("UGX -> USD, the USD statement shows 90,000: recorded as USD 90,000 at 3,700 (not 91,232.88 at QuickBooks' 3,650)",
      body["Amount"] == 90000.0 and body["ExchangeRate"] == 3700.0 and book(USD, TID) == (D("90000.00"), "USD")
      and book(UGX, TID) == (D("-333000000.00"), "UGX"))
check("…so the USD bank's line is matched too", matched(L) and matched(lid("IN FROM UGX")))
L = lid("FXPLSP~1206232~SPOT~BUY~USD/UGX~3,733.000")
n_rates = len(RATES)
cl.post("/account/Stanbic UGX/record", data={"only": L, f"acct_{L}": "37"})
ent, body = POSTS[-1]
check("no line on the USD statement: the bank's spot rate in the text (3,733) gives USD 150,000, to the shilling",
      body["Amount"] == 150000.0 and body["ExchangeRate"] == 3733.0 and len(RATES) == n_rates
      and body["FromAccountRef"]["value"] == "37")
L = lid("OUT TO UGX")
cl.post("/account/Stanbic USD/record", data={"only": L, f"acct_{L}": "35"})
ent, body = POSTS[-1]
TID = str(900 + len(POSTS))
check("USD -> UGX, the UGX statement shows 3,690,000: the rate is 3,690, not QuickBooks' 3,650",
      body["Amount"] == 1000.0 and body["ExchangeRate"] == 3690.0 and book(UGX, TID) == (D("3690000.00"), "UGX")
      and matched(L) and matched(lid("FROM USD SIDE")))
check("spot_rate reads the other way round too", A.spot_rate("FXPLSP~1206233~SPOT~SELL~UGX/USD~0.0002", "USD", "UGX") == 5000
      and A.spot_rate("FXPLOU~1110179~FWD~BUY~USD/UGX~3,840", "USD", "UGX") is None)
sys.exit(T.summary())
