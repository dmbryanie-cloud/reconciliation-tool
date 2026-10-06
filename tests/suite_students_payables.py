"""Receipts to students, payables, sorting and the account switcher.

- A receipt is paid to a student or family (their UGX or USD account), not to Accounts Receivable in
  general. UGX received for a student's USD account is paid in USD at a rate.
- Payables (Rent payable, say) can be used on money out, against a supplier QuickBooks knows.
- Tables sort by date and amount; the account switcher has a Reconcile button and says what's loading.

QuickBooks is fully mocked -- no network. The page's script is run in jsdom.

Run on its own with `python tests/suite_students_payables.py`, or all suites with `python tests/run_all.py`.
"""
import io, json, os, re, shutil, subprocess, sys, tempfile
from decimal import Decimal as D

import harness as H

UGX, USD = "00000000-0000-0000-0000-0000000000a1", "00000000-0000-0000-0000-0000000000a2"
A, c = H.setup(H.account_sql((UGX, "35", "Stanbic UGX", "bank"), (USD, "37", "DFCU USD", "bank")))
cur = c.cursor()
cur.execute("UPDATE account SET currency='USD' WHERE account_id=%s", (USD,)); c.commit()
T = H.Checker()
check = T.check

def acct(i, n, t, ccy, fqn=None):
    return {"Id": i, "Name": n, "FullyQualifiedName": fqn or n, "AccountType": t, "CurrencyRef": {"value": ccy}}
A._store_coa([acct("35", "Stanbic UGX", "Bank", "UGX"), acct("37", "DFCU USD", "Bank", "USD"),
              acct("83", "Office Supplies", "Expense", "UGX"), acct("90", "Tuition", "Income", "UGX"),
              acct("60", "Accounts Receivable", "Accounts Receivable", "UGX"),
              acct("61", "Accounts Receivable (A/R) - USD", "Accounts Receivable", "USD"),
              acct("221", "Accounts Payable (A/P)  UGX", "Accounts Payable", "UGX"),
              acct("222", "Rent payable UGX", "Accounts Payable", "UGX", "Accounts Payable (A/P)  UGX:Rent payable UGX"),
              acct("253", "Rent Payable USD", "Accounts Payable", "USD")])
cust = lambda i, n, ccy, p=None: {"Id": i, "DisplayName": n, "FullyQualifiedName": n, "CurrencyRef": {"value": ccy},
                                  **({"ParentRef": {"value": p}} if p else {})}
A._store_customers([cust("11", "Mr. & Mrs. Okello (UGX)", "UGX"), cust("12", "2021001 Amy Okello (UGX)", "UGX", "11"),
                    cust("21", "Mr. & Mrs. Okello (USD)", "USD"), cust("22", "2021001 Amy Okello (USD)", "USD", "21"),
                    cust("23", "2021002 Ben Okello (USD)", "USD", "21")])
A._store_vendors([{"Id": "70", "DisplayName": "Regina Muwonge", "CurrencyRef": {"value": "UGX"}},
                  {"Id": "71", "DisplayName": "Regina Muwonge USD", "CurrencyRef": {"value": "USD"}},
                  {"Id": "72", "DisplayName": "Gone Landlord", "CurrencyRef": {"value": "UGX"}, "Active": False}])
POSTS, RATES = [], []
def fake_post(token, entity, body):
    POSTS.append((entity, json.loads(json.dumps(body))))
    return {entity: {"Id": str(700 + len(POSTS))}}
def fake_rate(token, ccy, d):
    RATES.append((ccy, str(d))); return 3650.0
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
def data(page, name):
    return json.loads(re.search(rf"<script id={name} type=application/json>(.*?)</script>", page, re.S).group(1))
cl = H.login(A)
def upload(acct_name, rows):
    body = "Date,Description,Amount\n" + "".join(f"{d},{t},{a}\n" for d, t, a in rows)
    r = cl.post(f"/account/{acct_name}/upload", data={"replace": "1", "statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
                "period_start": "2026-07-01", "period_end": "2026-07-31"}, content_type="multipart/form-data")
    assert r.status_code == 302, r.data[:300]
def page(acct_name):
    return cl.get(f"/account/{acct_name}").data.decode()
def msg(acct_name):
    m = re.search(r"<div id=flash[^>]*>(.*?)</div>", page(acct_name), re.S)
    return m.group(1) if m else ""

upload("Stanbic UGX", [("2026-07-01", "RENT JULY", -5000000), ("2026-07-02", "OKELLO SCHOOL FEES", 3650000),
                       ("2026-07-03", "OKELLO USD FEES", 1000000), ("2026-07-04", "OKELLO FAMILY LUMP", 7300000),
                       ("2026-07-05", "AMY OKELLO UGX FEES", 250000), ("2026-07-06", "NO RATE FEES", 730000)])
RENT, F1, F2, FAM, F3, NR = (lid(x) for x in ("RENT JULY", "OKELLO SCHOOL FEES", "OKELLO USD FEES", "OKELLO FAMILY LUMP",
                                              "AMY OKELLO UGX FEES", "NO RATE FEES"))
ugx = page("Stanbic UGX")

# ---- what the UGX account offers --------------------------------------------------------------------------
coa = {a["id"]: a for a in data(ugx, "coa-data")}
check("payables in the bank's currency are offered (Rent payable UGX), not the USD one", "222" in coa and "253" not in coa)
cj = data(ugx, "cust-data")
check("students and families: the UGX ones first, then the USD ones (paid at a rate)",
      [x["c"] for x in cj] == ["UGX"] * 2 + ["USD"] * 3)
check("…with their families (sub-customers keep their parent)", {x["id"]: x.get("p") for x in cj}["22"] == "21")
check("suppliers offered for payables: active, in this currency", [v["id"] for v in data(ugx, "vend-data")] == ["70"])
check("a rate box for paying a student's USD account", f'name="rate_{F2}"' in ugx and "UGX per USD" in ugx)

# ---- payables ---------------------------------------------------------------------------------------------
n = len(POSTS)
cl.post("/account/Stanbic UGX/record", data={"only": RENT, f"acct_{RENT}": "222"})
check("a payable without a supplier: refused, asks for one", len(POSTS) == n and "choose the supplier" in msg("Stanbic UGX"))
cl.post("/account/Stanbic UGX/record", data={"only": RENT, f"acct_{RENT}": "222", f"cust_{RENT}": "70"})
ent, body = POSTS[-1]
line = body["Line"][0]["AccountBasedExpenseLineDetail"]
check("with the supplier: an expense to Rent payable UGX against them", ent == "Purchase" and line["AccountRef"] == {"value": "222"}
      and body["EntityRef"] == {"value": "70", "type": "Vendor"} and body["Line"][0]["Amount"] == 5000000.0)

# a supplier suggested by name only (older entries carry no supplier ID) counts as picked
q("""INSERT INTO payee_correction (org_id, payee, category, money_out, vendor, vendor_ref, currency)
     VALUES (%s, 'LANDLORD SEPT RENT', 'Accounts Payable (A/P)  UGX:Rent payable UGX', true, 'Regina Muwonge', NULL, 'UGX') RETURNING 1""",
  (A.ORG_ID,))
upload("Stanbic UGX", [("2026-07-01", "RENT JULY", -5000000), ("2026-07-02", "OKELLO SCHOOL FEES", 3650000),
                       ("2026-07-03", "OKELLO USD FEES", 1000000), ("2026-07-04", "OKELLO FAMILY LUMP", 7300000),
                       ("2026-07-05", "AMY OKELLO UGX FEES", 250000), ("2026-07-06", "NO RATE FEES", 730000),
                       ("2026-07-07", "LANDLORD SEPT RENT", -700000)])
RENT, F1, F2, FAM, F3, NR, LS = (lid(x) for x in ("RENT JULY", "OKELLO SCHOOL FEES", "OKELLO USD FEES", "OKELLO FAMILY LUMP",
                                                  "AMY OKELLO UGX FEES", "NO RATE FEES", "LANDLORD SEPT RENT"))
p_ = page("Stanbic UGX")
row = re.search(rf'value="{LS}" class=rsel.*?</tr>', p_, re.S).group(0)
check("a supplier suggested by name only is preselected", 'class=custbox data-sel="70"' in row)
n = len(POSTS)
cl.post("/account/Stanbic UGX/record", data={"only": LS, f"acct_{LS}": "222", f"payee_{LS}": "Regina Muwonge",
                                              f"psug_{LS}": "Regina Muwonge"})
check("…and the name alone is enough to record it against them", len(POSTS) == n + 1
      and POSTS[-1][1].get("EntityRef") == {"value": "70", "type": "Vendor"})

# ---- students ----------------------------------------------------------------------------------------------
cl.post("/account/Stanbic UGX/record", data={"only": F3, f"acct_{F3}": "cust:12"})
ent, body = POSTS[-1]
check("UGX received for a student's UGX account: a Payment in their name, in UGX", ent == "Payment"
      and body["CustomerRef"] == {"value": "12"} and body["TotalAmt"] == 250000.0 and "CurrencyRef" not in body
      and body["DepositToAccountRef"] == {"value": "35"})
check("…matched straight away, the student as the payee",
      q("""SELECT bt.counterparty, bt.amount FROM match_statement_line msl JOIN match m USING (match_id)
           JOIN match_book_txn USING (match_id) JOIN book_txn bt USING (txn_id) WHERE msl.line_id=%s""", (F3,))
      == [("2021001 Amy Okello (UGX)", D("250000"))])
cl.post("/account/Stanbic UGX/record", data={"only": F1, f"acct_{F1}": "cust:22", f"rate_{F1}": "3,650"})
ent, body = POSTS[-1]
check("UGX for a student's USD account: paid in USD at the rate (3,650,000 at 3,650 = 1,000.00)", ent == "Payment"
      and body["CustomerRef"] == {"value": "22"} and body["TotalAmt"] == 1000.0 and body["CurrencyRef"] == {"value": "USD"}
      and body["ExchangeRate"] == 3650.0 and body["DepositToAccountRef"] == {"value": "35"})
check("…the books keep the UGX the bank received", q("SELECT amount, currency FROM book_txn WHERE source_txn_id=%s",
                                                     (str(700 + len(POSTS)),)) == [(D("3650000"), "UGX")])
cl.post("/account/Stanbic UGX/record", data={"only": F2, f"acct_{F2}": "cust:22", f"rate_{F2}": "3712.5"})
ent, body = POSTS[-1]
check("an amount that doesn't divide evenly: USD to the cent, rate adjusted so it's exactly the UGX received",
      body["TotalAmt"] == 269.36 and abs(body["TotalAmt"] * body["ExchangeRate"] - 1000000) < 0.001)
n, r0 = len(POSTS), len(RATES)
cl.post("/account/Stanbic UGX/record", data={"only": NR, f"acct_{NR}": "cust:23"})
ent, body = POSTS[-1]
check("no rate typed: QuickBooks' rate for the day (730,000 at 3,650 = 200.00)", len(POSTS) == n + 1 and len(RATES) == r0 + 1
      and body["TotalAmt"] == 200.0 and body["CurrencyRef"] == {"value": "USD"})
kids = json.dumps([{"c": "22", "v": "3650000"}, {"c": "23", "v": "3650000"}])
n = len(POSTS)
cl.post("/account/Stanbic UGX/record", data={"only": FAM, f"acct_{FAM}": "cust:21", f"kids_{FAM}": kids, f"rate_{FAM}": "3650"})
check("a USD family's UGX lump sum split between children: a USD payment each", len(POSTS) == n + 2
      and [(b["CustomerRef"]["value"], b["TotalAmt"], b["CurrencyRef"]["value"]) for _, b in POSTS[-2:]]
      == [("22", 1000.0, "USD"), ("23", 1000.0, "USD")])

upload("DFCU USD", [("2026-07-10", "USD FEES", 500)])
UF = lid("USD FEES")
usd = page("DFCU USD")
check("USD account: only USD students (UGX ones can't be paid from it)", {x["c"] for x in data(usd, "cust-data")} == {"USD"})
check("…and only its own payables", "253" in {a["id"] for a in data(usd, "coa-data")}
      and "222" not in {a["id"] for a in data(usd, "coa-data")})
n = len(POSTS)
cl.post("/account/DFCU USD/record", data={"only": UF, f"acct_{UF}": "cust:12", f"rate_{UF}": "3650"})
check("…a UGX student picked anyway: refused", len(POSTS) == n and "choose the customer" in msg("DFCU USD"))
cl.post("/account/DFCU USD/record", data={"only": UF, f"acct_{UF}": "cust:22", f"rate_{UF}": "3650"})
ent, body = POSTS[-1]
check("…a USD student: paid in USD at the account's rate", body["CurrencyRef"] == {"value": "USD"} and body["TotalAmt"] == 500.0)

# ---- a saved selection from before (Accounts Receivable + a customer) shows as the student ------------------
upload("Stanbic UGX", [("2026-07-20", "SAVED BEFORE", 100000)])
SB = lid("SAVED BEFORE")
q("INSERT INTO record_draft (line_id, data, saved_by) VALUES (%s, %s, 'x') RETURNING 1",
  (SB, json.dumps({"sel": True, "acct": "60", "cust": "12"})))
check("an old saved choice (A/R + customer) opens as that student", 'data-sel="cust:12"' in page("Stanbic UGX"))

# ---- the page's script: students, suppliers, rate box, sorting, switcher -----------------------------------
upload("Stanbic UGX", [("2026-07-03", "LINE B", 300), ("2026-07-01", "LINE A", -100), ("2026-07-02", "LINE C", 200),
                       ("2026-07-04", "FEES FOR BEN", 731000), ("2026-07-05", "RENT AUG", -400)])
B_, A_, C_, BEN, RA = (lid(x) for x in ("LINE B", "LINE A", "LINE C", "FEES FOR BEN", "RENT AUG"))
ugx = page("Stanbic UGX")
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
const pick = (q, t) => { fire(q, "focus"); q.value = t; fire(q, "input"); fire(q, "keydown", { key: "Enter" }); fire(q, "blur"); };
const rowOf = id => d.querySelector(`input.rsel[value="${id}"]`).closest("tr");
const out = {};
const ben = rowOf(process.argv[3]), xr = ben.querySelector(".xrate");
out.rateHidden0 = xr.hidden;
pick(ben.querySelector(".acctbox.main .acct-q"), "ben okello (usd)");
out.benPicked = ben.querySelector(".acctbox.main .acct-v").value; out.rateShown = !xr.hidden;
pick(ben.querySelector(".acctbox.main .acct-q"), "2021001 amy okello (ugx)");
out.rateHiddenAgain = xr.hidden;
const rent = rowOf(process.argv[4]);
out.vendLearned = rent.querySelector(".custbox .acct-v").value;   // learned from July's rent
rent.querySelector(".custbox .acct-x").click();
pick(rent.querySelector(".acctbox.main .acct-q"), "rent payable ugx");
out.rentTicked = rent.querySelector(".rsel").checked;
const pq = rent.querySelector(".custbox .acct-q"); fire(pq, "focus"); pq.value = "regina"; fire(pq, "input");
out.vendOffered = [...rent.querySelectorAll(".custbox .acct-list .ao span:first-child")].map(s => s.textContent);
fire(pq, "keydown", { key: "Enter" }); fire(pq, "blur");
out.vend = rent.querySelector(".custbox .acct-v").value; out.rentTicked2 = rent.querySelector(".rsel").checked;
// sorting the record table by amount, then date
const tbl = d.querySelector("table.rectbl"), th = [...tbl.rows[0].cells];
const amounts = () => [...tbl.querySelectorAll("input.rsel")].map(c => parseFloat(c.getAttribute("data-amt")));
const dates = () => [...tbl.querySelectorAll("input.rsel")].map(c => c.closest("tr").cells[1].textContent.trim());
th.find(t => /^Amount/.test(t.textContent)).click(); out.amtUp = amounts();
th.find(t => /^Amount/.test(t.textContent)).click(); out.amtDown = amounts();
th.find(t => /^Date/.test(t.textContent)).click(); out.dateUp = dates();
out.panelsFollow = [...tbl.querySelectorAll("tr.splitrow")].length > 3 && [...tbl.querySelectorAll("tr.splitrow")].every(r => { let p = r.previousElementSibling; while (p && !p.hasAttribute("data-amt")) p = p.previousElementSibling; return p && p.nextElementSibling === r; });
// Enter never sends the form; a line's Record with a typed-but-unpicked account is stopped
const lc = rowOf(process.argv[5]), lq = lc.querySelector(".acctbox.main .acct-q");
fire(lq, "focus"); lq.value = "office"; fire(lq, "input");
const e1 = new w.KeyboardEvent("keydown", { key: "Enter", bubbles: true, cancelable: true }); lq.dispatchEvent(e1);
out.pickedByEnter = lc.querySelector(".acctbox.main .acct-v").value;
const e2 = new w.KeyboardEvent("keydown", { key: "Enter", bubbles: true, cancelable: true }); lq.dispatchEvent(e2);
out.enterBlocked = e2.defaultPrevented;
lq.value = "offic"; fire(lq, "input"); fire(lq, "blur");
const rb = lc.querySelector("button[name=only]");
const sev = new w.SubmitEvent("submit", { cancelable: true, bubbles: true, submitter: rb }); d.getElementById("recform").dispatchEvent(sev);
out.unpickedStopped = sev.defaultPrevented; out.pickHint = (lc.querySelector(".pickhint") || {}).textContent;
fire(lq, "focus"); lq.value = "office supplies"; fire(lq, "input"); fire(lq, "keydown", { key: "Enter" });
const sev2 = new w.SubmitEvent("submit", { cancelable: true, bubbles: true, submitter: rb }); d.getElementById("recform").dispatchEvent(sev2);
out.pickedGoes = !sev2.defaultPrevented && !lc.querySelector(".pickhint");
out.errors = errors;
console.log(JSON.stringify(out));
"""
    f = tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8"); f.write(ugx); f.close()
    g = tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, dir=H.HERE, encoding="utf-8"); g.write(JS); g.close()
    try:
        res = subprocess.run([node, g.name, f.name, BEN, RA, A_], capture_output=True, text=True, encoding="utf-8", cwd=H.HERE)
    finally:
        os.unlink(f.name); os.unlink(g.name)
    if res.stderr.strip():
        print(res.stderr[-2000:])
    o = json.loads([l for l in res.stdout.splitlines() if l.startswith("{")][-1]) if "{" in res.stdout else {}
    print("   ", o)
    check("browser: a student's USD account asks for the rate; their UGX one doesn't",
          o.get("rateHidden0") and o.get("benPicked") == "cust:23" and o.get("rateShown") and o.get("rateHiddenAgain"))
    check("browser: the supplier is suggested from the last rent paid", o.get("vendLearned") == "70")
    check("browser: a payable isn't ticked until its supplier is chosen", o.get("rentTicked") is False)
    check("browser: …the payee box then searches suppliers", o.get("vendOffered") == ["Regina Muwonge"]
          and o.get("vend") == "70" and o.get("rentTicked2"))
    am = o.get("amtUp") or []
    check("browser: clicking Amount sorts by amount, again reverses it", am == sorted(am) and o.get("amtDown") == sorted(am, reverse=True)
          and len(am) > 3)
    dt = o.get("dateUp") or []
    check("browser: clicking Date sorts by date", dt == sorted(dt) and len(dt) > 3)
    check("browser: a line's panels move with it", o.get("panelsFollow"))
    check("browser: Enter picks from the list but never sends the form", o.get("pickedByEnter") == "83" and o.get("enterBlocked"))
    check("browser: Record with an account typed but not picked is stopped, saying why", o.get("unpickedStopped")
          and "Pick it from the list" in (o.get("pickHint") or ""))
    check("browser: …once picked, Record goes ahead", o.get("pickedGoes"))
    check("browser: no script errors", o.get("errors") == [])
sys.exit(T.summary())
