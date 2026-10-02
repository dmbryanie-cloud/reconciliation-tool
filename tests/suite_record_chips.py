"""Shortcuts above the list to record: Bank charges, Own transfers, Student payments and the accounts
most lines are suggested for (from how the same payees were recorded in QuickBooks before). Clicking
one shows every line in it; clicking it again, Clear search or Escape shows them all.

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_record_chips.py`, or all suites with `python tests/run_all.py`.
"""
import html, io, json, os, re, shutil, subprocess, sys, tempfile

import harness as H

STB, CEN = "00000000-0000-0000-0000-0000000000a1", "00000000-0000-0000-0000-0000000000a2"
A, c = H.setup(H.account_sql((STB, "35", "Stanbic UGX", "bank"), (CEN, "36", "Centenary UGX", "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
A.qbo_is_connected = lambda: False
A.qbo_token = lambda: "tok"
A.qbo_post = lambda token, entity, body: {entity: {"Id": "1"}}
A._store_coa([{"Id": "35", "Name": "Stanbic UGX", "AccountType": "Bank", "CurrencyRef": {"value": "UGX"}},
              {"Id": "36", "Name": "Centenary UGX", "AccountType": "Bank", "CurrencyRef": {"value": "UGX"}},
              {"Id": "83", "Name": "Office Supplies", "AccountType": "Expense", "CurrencyRef": {"value": "UGX"}},
              {"Id": "84", "Name": "Bank Charges", "AccountType": "Expense", "CurrencyRef": {"value": "UGX"}},
              {"Id": "85", "Name": "Fuel", "AccountType": "Expense", "CurrencyRef": {"value": "UGX"}},
              {"Id": "90", "Name": "Sales", "AccountType": "Income", "CurrencyRef": {"value": "UGX"}}])

def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
# How these payees were recorded in QuickBooks before (the suggestion engine's strongest memory).
for payee, cat in (("PENS LTD", "Office Supplies"), ("PAPER WORLD", "Office Supplies"), ("SHELL KAMPALA", "Fuel"),
                   ("EXCISE DUTY", "Bank Charges"), ("LEDGER FEES", "Bank Charges")):
    q("""INSERT INTO payee_correction (org_id, payee, category, money_out, currency) VALUES (%s,%s,%s,true,'UGX') RETURNING 1""",
      (A.ORG_ID, payee, cat))
cl = H.login(A)
body = ("Date,Description,Amount\n2026-09-02,PENS LTD,-77000\n2026-09-03,PAPER WORLD,-31000\n2026-09-04,SHELL KAMPALA,-120000\n"
        "2026-09-05,EXCISE DUTY,-500\n2026-09-05,LEDGER FEES,-2500\n2026-09-06,TRANSFER TO CENTENARY,-400000\n"
        "2026-09-07,MYSTERY ITEM,-9999\n2026-09-08,PENS LTD,-15000\n")
cl.post("/account/Stanbic UGX/upload", data={"statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
        "period_start": "2026-09-01", "period_end": "2026-09-30"}, content_type="multipart/form-data")
p = cl.get("/account/Stanbic UGX").data.decode()
lid = lambda who: str(q("SELECT line_id FROM statement_line WHERE description=%s LIMIT 1", (who,))[0][0])

chips = re.search(r"<div class=tschips data-table=rectbl.*?</div>", p, re.S)
labels = re.findall(r'data-cat="([^"]+)"[^>]*>([^<]+?) <span class=n>(\d+)</span>', chips.group(0)) if chips else []
check("shortcuts sit just below the search box", chips is not None and p.find("aria-label=\"Search the lines to record\"")
      < p.find("<div class=tschips") < p.find("<table class=rectbl"))
got = [(l.strip(), int(n)) for _, l, n in labels]
check("Bank charges and Own transfers first, with how many lines each", got[:2] == [("Bank charges", 2), ("Own transfers", 1)])
check("then the accounts suggested from QuickBooks history, most lines first",
      got[2:] == [("Office Supplies", 3), ("Bank Charges", 2), ("Fuel", 1)])
check("no shortcut for kinds with no lines (no student payments here)", "Student payments" not in chips.group(0))
row = lambda who: re.search(r'<tr data-amt="[^"]*" data-cats="([^"]*)">\s*<td><input type=checkbox name=sel value="' + lid(who), p)
check("each row says which shortcuts it belongs to", row("EXCISE DUTY").group(1) == "charge a:84"
      and row("TRANSFER TO CENTENARY").group(1).split()[0] == "xfer" and row("PAPER WORLD").group(1) == "a:83"
      and row("MYSTERY ITEM").group(1) == "")

# Unit: a payee with no history gets no account shortcut; a saved choice counts.
k = c.cursor()
check("line_categories: a card payment is an own transfer", A.line_categories(
      {"who": "PAYMENT THANK YOU", "amount": 5, "xfer_only": True, "acct_id": None}, set()) == ["xfer"])
check("record_chips: accounts capped, unknown accounts left out", len(A.record_chips(
      [{"cats": [f"a:{i}"]} for i in range(20)], {str(i): f"Acct {i}" for i in range(19)})) == A.CHIP_ACCOUNTS)
c.rollback()

node = shutil.which("node")
if node and os.path.isdir(os.path.join(H.HERE, "node_modules", "jsdom")):
    JS = r"""
const { JSDOM } = require("jsdom");
const dom = new JSDOM(require("fs").readFileSync(process.argv[2], "utf8"), { runScripts: "dangerously", pretendToBeVisual: true });
const d = dom.window.document, tb = d.getElementById('rectbl');
const shown = () => [...tb.rows].filter(r => r.hasAttribute('data-cats') && !r.classList.contains('tsx'))
                                .map(r => r.querySelector('td.desc').firstChild.textContent.trim());
const chip = k => d.querySelector('.tschips .chip[data-cat="' + k + '"]');
const inp = d.querySelector('.tsearch[data-table=rectbl] input'), out = {};
out.all = shown().length;
chip('charge').click(); out.charge = shown(); out.pressed = chip('charge').getAttribute('aria-pressed');
out.bar = d.querySelector('.tsbar') ? !d.querySelector('.tsbar').hidden : null;
out.count = d.querySelector('.ts-n').textContent;
chip('a:83').click(); out.office = shown(); out.onlyOne = chip('charge').getAttribute('aria-pressed');
inp.value = '77,000'; inp.dispatchEvent(new dom.window.Event('search')); out.both = shown();
inp.value = ''; chip('a:83').click(); out.off = shown().length;
chip('xfer').click(); inp.value = '';
inp.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' })); out.esc = shown().length;
out.escPressed = d.querySelectorAll('.tschips .chip[aria-pressed=true]').length;
console.log(JSON.stringify(out));
"""
    f = tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8"); f.write(p); f.close()
    g = tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, dir=H.HERE, encoding="utf-8"); g.write(JS); g.close()
    try:
        res = subprocess.run([node, g.name, f.name], capture_output=True, text=True, encoding="utf-8", cwd=H.HERE)
    finally:
        os.unlink(f.name); os.unlink(g.name)
    o = json.loads([l for l in res.stdout.splitlines() if l.startswith("{")][-1]) if "{" in res.stdout else {}
    if not o:
        print(res.stdout, res.stderr)
    check("browser: Bank charges shows every bank charge, and only those",
          o.get("all") == 8 and sorted(o.get("charge", [])) == ["EXCISE DUTY", "LEDGER FEES"] and o.get("pressed") == "true")
    check("…with the count shown above the list", o.get("bar") is True and "Showing 2 of 8" in o.get("count", ""))
    check("browser: an account shortcut shows the lines suggested for it, one shortcut at a time",
          sorted(o.get("office", [])) == ["PAPER WORLD", "PENS LTD", "PENS LTD"] and o.get("onlyOne") == "false")
    check("…and narrows further with words typed", o.get("both") == ["PENS LTD"])
    check("clicking it again shows everything", o.get("off") == 8)
    check("Escape clears the shortcut too", o.get("esc") == 8 and o.get("escPressed") == 0)
else:
    print("SKIP browser check (no node/jsdom)")
sys.exit(T.summary())
