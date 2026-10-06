"""Recording, round two: customer receipts, split lines (FX hedges), typed rates, saved selections,
exact suggestions listed last, the selection count and the progress messages.

QuickBooks is fully mocked -- no network. The page's script is run in jsdom.

Run on its own with `python tests/suite_record_extras.py`, or all suites with `python tests/run_all.py`.
"""
import io, json, os, re, shutil, subprocess, sys, tempfile
from decimal import Decimal as D

import harness as H

UGX, USD = "00000000-0000-0000-0000-0000000000a1", "00000000-0000-0000-0000-0000000000a2"
A, c = H.setup(H.account_sql((UGX, "35", "Stanbic UGX", "bank"), (USD, "37", "DFCU USD", "bank")))
A.set_config("rule_clear_days", "31"); A.set_config("rule_group_days", "31")   # these checks use wide windows (Settings); the 3-day defaults are in suite_match_windows
cur = c.cursor()
cur.execute("UPDATE account SET currency='USD' WHERE account_id=%s", (USD,)); c.commit()
T = H.Checker()
check = T.check

def acct(i, n, t, ccy):
    return {"Id": i, "Name": n, "AccountType": t, "CurrencyRef": {"value": ccy}}
A._store_coa([acct("35", "Stanbic UGX", "Bank", "UGX"), acct("37", "DFCU USD", "Bank", "USD"),
              acct("38", "FX in Transit UGX", "Bank", "UGX"), acct("39", "FX in Transit", "Bank", "USD"),
              acct("83", "Office Supplies", "Expense", "UGX"), acct("90", "Tuition", "Income", "UGX"),
              acct("92", "Realised FX loss", "Other Expense", "UGX"),
              acct("60", "Accounts Receivable (A/R)", "Accounts Receivable", "UGX"),
              acct("61", "Accounts Receivable (A/R) - USD", "Accounts Receivable", "USD"),
              acct("96", "EUR Accruals", "Other Current Liability", "EUR")])
A._store_customers([{"Id": "501", "DisplayName": "20218580 Liam Migisha (UGX)", "CurrencyRef": {"value": "UGX"}},
                    {"Id": "502", "DisplayName": "Jane Doe (USD)", "CurrencyRef": {"value": "USD"}},
                    {"Id": "503", "DisplayName": "Old Pupil", "CurrencyRef": {"value": "USD"}, "Active": False}])
POSTS, RATES = [], []
def fake_post(token, entity, body):
    POSTS.append((entity, json.loads(json.dumps(body))))
    return {entity: {"Id": str(900 + len(POSTS))}}
def fake_rate(token, ccy, d):
    RATES.append(str(d)); return 3700.0
A.qbo_post = fake_post
A.qbo_exchange_rate = fake_rate
A.qbo_token = lambda: "tok"
A.qbo_realm = lambda: "REAL-1"
A.qbo_is_connected = lambda: False

def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
def lid(desc):
    return str(q("""SELECT sl.line_id FROM statement_line sl JOIN statement s USING (statement_id)
                    WHERE sl.description=%s ORDER BY s.created_at DESC LIMIT 1""", (desc,))[0][0])
def book(tid):
    cur.execute("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency,
                   description, counterparty, last_modified) VALUES (%s,%s,%s,'Purchase',%s,%s,'UGX',%s,%s,now())""",
                (A.ORG_ID, UGX, *tid)); c.commit()
def coa_ids(page, x=None):
    data = json.loads(re.search(r"<script id=coa-data type=application/json>(.*?)</script>", page, re.S).group(1))
    return {a["id"] for a in data if x is None or a["x"] == x}

cl = H.login(A)
def upload(acct_name, rows):
    body = "Date,Description,Amount\n" + "".join(f"{d},{t},{a}\n" for d, t, a in rows)
    r = cl.post(f"/account/{acct_name}/upload", data={"replace": "1", "statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
                "period_start": "2026-06-01", "period_end": "2026-06-30"}, content_type="multipart/form-data")
    assert r.status_code == 302, r.data[:300]
def page(acct_name):
    return cl.get(f"/account/{acct_name}").data.decode()
def last_msg(acct_name):
    m = re.search(r"<div id=flash[^>]*>(.*?)</div>", page(acct_name), re.S)
    return m.group(1) if m else ""

upload("DFCU USD", [("2026-06-15", "FXPLSP SPOT SELL USD-UGX 3720", 400000), ("2026-06-16", "ACCOUNT MAINTENANCE FEES", -10),
                    ("2026-06-17", "JANE DOE FEES", 500), ("2026-06-18", "LEDGER FEES", -3)])
FX, FEE, RCPT, LEDG = lid("FXPLSP SPOT SELL USD-UGX 3720"), lid("ACCOUNT MAINTENANCE FEES"), lid("JANE DOE FEES"), lid("LEDGER FEES")
usd = page("DFCU USD")

# ---- what each account offers --------------------------------------------------------------------
check("USD bank: its own A/R offered for receipts, not the UGX one", "61" in coa_ids(usd) and "60" not in coa_ids(usd))
check("USD bank: split lines can use UGX and USD banks (FX in Transit), not EUR", {"38", "39", "35"} <= coa_ids(usd, 2)
      and "96" not in coa_ids(usd))
cj = json.loads(re.search(r"<script id=cust-data type=application/json>(.*?)</script>", usd, re.S).group(1))
check("USD bank: only active USD customers offered", [x["id"] for x in cj] == ["502"])
check("rate box on each line of a foreign-currency account", f'name="rate_{FEE}"' in usd and "UGX per USD" in usd)

# ---- customer receipts -------------------------------------------------------------------------
cl.post("/account/DFCU USD/record", data={"only": RCPT, f"acct_{RCPT}": "61", f"cust_{RCPT}": "502", f"rate_{RCPT}": "3,705.5"})
ent, body = POSTS[-1]
check("receipt against a customer: a QuickBooks Payment in their name", ent == "Payment" and body["CustomerRef"] == {"value": "502"}
      and body["TotalAmt"] == 500.0 and body["DepositToAccountRef"] == {"value": "37"})
check("…in USD at the rate typed (no lookup)", body["CurrencyRef"] == {"value": "USD"} and body["ExchangeRate"] == 3705.5 and RATES == [])
bk = q("SELECT source_txn_type, amount, counterparty FROM book_txn WHERE source_txn_id=%s", (str(900 + len(POSTS)),))
check("…and in the books straight away", bk == [("Payment", D("500"), "Jane Doe (USD)")])
n = len(POSTS)
upload("DFCU USD", [("2026-06-15", "FXPLSP SPOT SELL USD-UGX 3720", 400000), ("2026-06-16", "ACCOUNT MAINTENANCE FEES", -10),
                    ("2026-06-19", "ANOTHER RECEIPT", 250), ("2026-06-18", "LEDGER FEES", -3)])
FX, FEE, R2, LEDG = lid("FXPLSP SPOT SELL USD-UGX 3720"), lid("ACCOUNT MAINTENANCE FEES"), lid("ANOTHER RECEIPT"), lid("LEDGER FEES")
cl.post("/account/DFCU USD/record", data={"only": R2, f"acct_{R2}": "61", f"rate_{R2}": "3700"})
check("A/R without a customer: refused, asks for one", len(POSTS) == n and "choose the customer" in last_msg("DFCU USD"))
cl.post("/account/DFCU USD/record", data={"only": R2, f"acct_{R2}": "61", f"cust_{R2}": "501", f"rate_{R2}": "3700"})
check("a customer in another currency: refused", len(POSTS) == n and "choose the customer" in last_msg("DFCU USD"))
cl.post("/account/DFCU USD/record", data={"only": FEE, f"acct_{FEE}": "61", f"rate_{FEE}": "3700"})
check("money out can't be a customer payment", len(POSTS) == n and "can&#39;t be a customer payment" in last_msg("DFCU USD"))

# ---- rates -------------------------------------------------------------------------------------
cl.post("/account/DFCU USD/record", data={"only": FEE, f"acct_{FEE}": "83", f"rate_{FEE}": "abc"})
check("a rate that isn't a number: refused", len(POSTS) == n and "isn&#39;t a number" in last_msg("DFCU USD"))
def no_rate(token, ccy, d):
    raise ValueError("none")
A.qbo_exchange_rate = no_rate
cl.post("/account/DFCU USD/record", data={"only": FEE, f"acct_{FEE}": "83"})
m = last_msg("DFCU USD")
check("no QuickBooks rate: asks for one", len(POSTS) == n and "has no USD rate for 2026-06-16" in m and "Type the rate (UGX per USD)" in m)
check("…and nothing is left half-recorded", not q("SELECT 1 FROM writeback_log WHERE line_id=%s", (FEE,)))
cl.post("/account/DFCU USD/record", data={"only": FEE, f"acct_{FEE}": "83", f"rate_{FEE}": "3690"})
check("…typing it records the line", len(POSTS) == n + 1 and POSTS[-1][1]["ExchangeRate"] == 3690.0)
A.qbo_exchange_rate = fake_rate

# ---- split (an FX hedge and its loss) ----------------------------------------------------------
n = len(POSTS)
bad = json.dumps([{"a": "38", "v": "401000"}, {"a": "92", "v": "-2000"}])
cl.post("/account/DFCU USD/record", data={"only": FX, f"split_{FX}": bad, f"rate_{FX}": "3720"})
check("split that doesn't add up: refused with the totals", len(POSTS) == n and "add up to 399,000.00, not 400,000.00" in last_msg("DFCU USD"))
cl.post("/account/DFCU USD/record", data={"only": FX, f"split_{FX}": json.dumps([{"a": "96", "v": "400000"}]), f"rate_{FX}": "3720"})
check("split to an account in a third currency: refused", len(POSTS) == n and "account from the list" in last_msg("DFCU USD"))
cl.post("/account/DFCU USD/record", data={"only": FX, f"split_{FX}": json.dumps([{"a": "61", "v": "400000"}]), f"rate_{FX}": "3720"})
check("split to A/R (needs a customer): refused", len(POSTS) == n and "account from the list" in last_msg("DFCU USD"))
good = json.dumps([{"a": "38", "v": "401,000"}, {"a": "92", "v": "-1000"}])
cl.post("/account/DFCU USD/record", data={"only": FX, f"split_{FX}": good, f"rate_{FX}": "3720", f"acct_{FX}": "83"})
ent, body = POSTS[-1]
L = [(l["JournalEntryLineDetail"]["AccountRef"]["value"], l["JournalEntryLineDetail"]["PostingType"], l["Amount"]) for l in body["Line"]]
check("split recorded as one journal entry", ent == "JournalEntry" and len(POSTS) == n + 1)
check("…bank debited the full amount, the lines credited, the loss debited", L == [("37", "Debit", 400000.0),
      ("38", "Credit", 401000.0), ("92", "Debit", 1000.0)])
check("…debits equal credits", sum(a for _, t, a in L if t == "Debit") == sum(a for _, t, a in L if t == "Credit"))
check("…in USD at the contract rate typed", body["CurrencyRef"] == {"value": "USD"} and body["ExchangeRate"] == 3720.0)
jid = str(900 + len(POSTS))
bk = q("SELECT amount, counterparty, description FROM book_txn WHERE source_txn_type='JournalEntry' AND source_txn_id=%s", (jid,))
back = A._h_journalentry({**body, "Id": jid}, "37", "bank")
check("…in the books as the next sync will read it (no duplicate)", bk and bk[0][0] == back[0] and bk[0][1] == back[1]
      and bk[0][2] == back[2])
check("…and the split's account isn't learned as a suggestion", not q("SELECT 1 FROM payee_correction WHERE payee LIKE 'FXPLSP%%'"))

# ---- saved selection ---------------------------------------------------------------------------
cl.post("/account/DFCU USD/record_save", data={"rowid": [LEDG, R2], "sel": [LEDG], f"acct_{LEDG}": "83", f"payee_{LEDG}": "DFCU Bank",
                                               f"rate_{LEDG}": "3650", f"acct_{R2}": "", f"payee_{R2}": ""})
m = last_msg("DFCU USD")
check("save selection: says what was kept", "Saved your selection: 1 of 2 lines ticked" in m)
p = page("DFCU USD")
row = p.split(f'value="{LEDG}" class=rsel')[1][:300]
check("…a refresh shows it again: ticked, account, payee and rate", "checked" in row.split(">")[0]
      and re.search(rf'data-sel="83"[^>]*>.*?name="acct_{LEDG}"', p, re.S) is not None
      and f'name="payee_{LEDG}" value="DFCU Bank"' in p and f'name="rate_{LEDG}" value="3650"' in p)
check("…with a note and a way to discard it", "Showing the selection saved by" in p and "Discard it" in p)
row2 = p.split(f'value="{R2}" class=rsel')[1][:200]
check("…an unticked line stays unticked", "checked" not in row2.split(">")[0])
cl.post("/account/DFCU USD/record_discard", data={})
p = page("DFCU USD")
check("discard: suggestions are back", "Showing the selection saved by" not in p and f'name="rate_{LEDG}" value=""' in p)
cl.post("/account/DFCU USD/record_save", data={"rowid": [LEDG], "sel": [LEDG], f"acct_{LEDG}": "83", f"rate_{LEDG}": "3650"})
cl.post("/account/DFCU USD/record", data={"only": LEDG, f"acct_{LEDG}": "83", f"rate_{LEDG}": "3650"})
check("recording a saved line clears its saved choices", not q("SELECT 1 FROM record_draft WHERE line_id=%s", (LEDG,)))

# ---- suggested matches: exact ones last ----------------------------------------------------------
book(("y1", "2026-06-01", -70000, "Supplier Y", "Supplier Y"))                                   # cleared 21 days later
book(("c1", "2026-06-10", -4000, "Bank charge", "Stanbic")); book(("c2", "2026-06-10", -600, "Excise", "Stanbic"))
upload("Stanbic UGX", [("2026-06-22", "SUPPLIER Y", -70000), ("2026-06-10", "CHARGES", -4600),
                       ("2026-06-20", "LIAM MIGISHA ATM CLDEP", 50000)])
k = c.cursor(); d = A.compute_detail(k, UGX, "bank", "35"); c.rollback()
types = [r["type"] for r in d["reviewable"]]
check("suggested matches: exact ones listed last", "exact" in types and "many_to_one" in types and types[-1] == "exact"
      and types.index("many_to_one") < types.index("exact"))

# ---- customers come from QuickBooks with the accounts --------------------------------------------
def fake_query(entity, token, since=None, changed_since=None, each=None):
    recs = {"Customer": [{"Id": "777", "DisplayName": "New Parent", "CurrencyRef": {"value": "UGX"}}]}.get(entity, [])
    return each(recs) if each else recs
real_query = A.qbo_query
A.qbo_query = fake_query
A._store_customers(fake_query("Customer", "tok"))
check("customers cached from QuickBooks", q("SELECT qbo_id, name FROM qbo_customer") == [("777", "New Parent")])
A.qbo_query = real_query
A._store_customers([{"Id": "501", "DisplayName": "20218580 Liam Migisha (UGX)", "CurrencyRef": {"value": "UGX"}},
                    {"Id": "502", "DisplayName": "Jane Doe (USD)", "CurrencyRef": {"value": "USD"}}])

# ---- the page's script -----------------------------------------------------------------------------
upload("DFCU USD", [("2026-06-25", "FXPLSP SPOT SELL USD-UGX 3750", 250000), ("2026-06-26", "JANE DOE TERM 1", 520),
                    ("2026-06-27", "BANK FEE", -7)])
FX, RC, BF = lid("FXPLSP SPOT SELL USD-UGX 3750"), lid("JANE DOE TERM 1"), lid("BANK FEE")
usd = page("DFCU USD")
ugx = page("Stanbic UGX")
node = shutil.which("node")
if not node or not os.path.isdir(os.path.join(H.HERE, "node_modules", "jsdom")):
    check("jsdom available (run: cd tests && npm install)", False)
else:
    JS = r"""
const { JSDOM, VirtualConsole } = require("jsdom");
const fs = require("fs");
const errors = []; const vc = new VirtualConsole(); vc.on("jsdomError", e => errors.push(String(e.message || e)));
function load(p) { const dom = new JSDOM(fs.readFileSync(p, "utf8"), { runScripts: "dangerously", virtualConsole: vc, pretendToBeVisual: true });
  dom.window.HTMLElement.prototype.scrollIntoView = function () {}; dom.window.confirm = () => false; return dom.window; }
const out = {};
const w = load(process.argv[2]), d = w.document;
const fire = (el, type, init = {}) => el.dispatchEvent(new w[type === "keydown" ? "KeyboardEvent" : "Event"](type, { bubbles: true, cancelable: true, ...init }));
const type = (q, t) => { q.value = t; fire(q, "input"); };
const rowOf = id => d.querySelector(`input.rsel[value="${id}"]`).closest("tr");
out.count0 = d.getElementById("selcount").textContent;
// split the FX line
const fx = rowOf(process.argv[4]);
fx.querySelector(".split-btn").click();
const srow = fx.nextElementSibling;
out.splitOpen = !srow.hidden;
let lines = srow.querySelectorAll(".splitline");
out.nLines = lines.length; out.firstAmt = lines[0].querySelector(".splitamt").value;
const amt0 = lines[0].querySelector(".splitamt"); amt0.value = "251000"; fire(amt0, "input");
let q0 = lines[0].querySelector(".acct-q"); fire(q0, "focus"); type(q0, "fx in transit ugx"); fire(q0, "keydown", { key: "Enter" }); fire(q0, "blur");
let q1 = lines[1].querySelector(".acct-q"); fire(q1, "focus"); type(q1, "loss"); fire(q1, "keydown", { key: "Enter" }); fire(q1, "blur");
out.remMid = srow.querySelector(".splitrem").textContent;
const amt1 = lines[1].querySelector(".splitamt"); amt1.value = "-1000"; fire(amt1, "input");
out.rem = srow.querySelector(".splitrem").textContent;
out.json = JSON.parse(fx.querySelector(".split-v").value);
out.fxTicked = fx.querySelector(".rsel").checked;
out.mainOff = fx.querySelector(".acctbox.main").classList.contains("off");
out.count1 = d.getElementById("selcount").textContent;
// customer receipt
const rc = rowOf(process.argv[5]);
const fee = rowOf(process.argv[6]), fq = fee.querySelector(".custbox .acct-q");
fire(fq, "focus"); type(fq, "jane"); out.custBefore = fee.querySelector(".custbox .acct-list").hidden; fire(fq, "blur");
out.feeNotAr = !fee.querySelector(".custbox").classList.contains("ar");
const aq = rc.querySelector(".acctbox.main .acct-q"); fire(aq, "focus"); type(aq, "");
out.groups = [...rc.querySelectorAll(".acctbox.main .acct-list .ag")].map(g => g.textContent);
type(aq, "receivable"); out.arHits = [...rc.querySelectorAll(".acctbox.main .acct-list .ao span:first-child")].map(s => s.textContent);
type(aq, "jane"); out.arTop = rc.querySelector(".acctbox.main .acct-list .ao span").textContent;
fire(aq, "keydown", { key: "Enter" }); fire(aq, "blur");
out.cust = rc.querySelector(".acctbox.main .acct-v").value; out.custName = aq.value;
out.payeeOff = rc.querySelector(".custbox .acct-q").disabled;
// progress message says what's being done (once the page has loaded, as in a browser)
const bulkBtn = d.querySelector("button[name=bulk]");
out.bulkBusy = bulkBtn.getAttribute("data-busy");
const later = (ms) => new Promise(r => setTimeout(r, ms));
const press = (win, btn) => btn.closest("form").dispatchEvent(new win.SubmitEvent("submit", { cancelable: true, bubbles: true, submitter: btn }));
(async () => {
  await later(100);
  press(w, d.querySelector('button[formaction$="/record_save"]')); await later(300);
  out.busySave = d.getElementById("loadingmsg").textContent;
  const w2 = load(process.argv[3]), d2 = w2.document; await later(100);
  press(w2, d2.querySelector("form.btnrow button[value=confirmed]")); await later(300);
  out.busyConfirm = d2.getElementById("loadingmsg").textContent;
  press(w2, d2.querySelector("form.btnrow button[value=rejected]")); await later(300);
  out.busyReject = d2.getElementById("loadingmsg").textContent;
  out.errors = errors;
  console.log(JSON.stringify(out));
})();
"""
    paths = []
    for html in (usd, ugx):
        f = tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8"); f.write(html); f.close(); paths.append(f.name)
    g = tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, dir=H.HERE, encoding="utf-8"); g.write(JS); g.close()
    try:
        res = subprocess.run([node, g.name, *paths, FX, RC, BF], capture_output=True, text=True, encoding="utf-8", cwd=H.HERE)
    finally:
        for p_ in paths + [g.name]:
            os.unlink(p_)
    if res.stderr.strip():
        print(res.stderr[-2000:])
    o = json.loads([l for l in res.stdout.splitlines() if l.startswith("{")][-1]) if "{" in res.stdout else {}
    print("   ", {k: o.get(k) for k in ("groups", "arHits", "arTop", "cust", "count0", "count1", "rem", "busySave", "busyConfirm", "busyReject", "bulkBusy")})
    check("browser: count of selected transactions shown", re.match(r"^Selected \d+ of 3 transactions", o.get("count0", "")) is not None)
    check("browser: Split opens with the whole amount on the first line", o.get("splitOpen") and o.get("nLines") == 2
          and o.get("firstAmt") == "250000")
    check("browser: …says what's left while it doesn't add up", "Left to allocate" in o.get("remMid", ""))
    check("browser: …and when it does", o.get("rem") == "Adds up to 250,000.00")
    check("browser: …the lines go to the server", o.get("json") == [{"a": "38", "v": "251000"}, {"a": "92", "v": "-1000"}])
    check("browser: …line ticked, single account set aside", o.get("fxTicked") and o.get("mainOff"))
    check("browser: …and the count follows", o.get("count1", "").startswith("Selected ") and o.get("count1") != o.get("count0"))
    check("browser: payee stays plain text without A/R (money out)", o.get("custBefore") is True and o.get("feeNotAr") is True)
    check("browser: money in offers students first, not Accounts Receivable in general",
          (o.get("groups") or [""])[0].startswith("Students and families (USD)")
          and not any("Receivable" in x for x in o.get("arHits") or []))
    check("browser: …typing a name finds the student, who is the payee", o.get("arTop") == "Jane Doe (USD)"
          and o.get("cust") == "cust:502" and o.get("custName") == "Jane Doe (USD)" and o.get("payeeOff"))
    check("browser: progress says 'Saving your selection...'", o.get("busySave") == "Saving your selection...")
    check("browser: …'Confirming...' and 'Rejecting...' on suggestions", o.get("busyConfirm") == "Confirming..."
          and o.get("busyReject") == "Rejecting...")
    check("browser: record button's message counts the lines", re.match(r"^Recording \d+ transactions? in QuickBooks\.\.\.$",
          o.get("bulkBusy") or "") is not None)
    check("browser: no script errors", o.get("errors") == [])
sys.exit(T.summary())
