"""Split memory: a payee's last split (a loan instalment as principal + interest) is offered again
on its next bank line -- same accounts, same amounts if the total is unchanged, else scaled.

QuickBooks is fully mocked -- no network. The page's script is run in jsdom.

Run on its own with `python tests/suite_split_memory.py`, or all suites with `python tests/run_all.py`.
"""
import io, json, os, re, shutil, subprocess, sys, tempfile

import harness as H

UGX, USD = "00000000-0000-0000-0000-0000000000a1", "00000000-0000-0000-0000-0000000000a2"
A, c = H.setup(H.account_sql((UGX, "35", "Stanbic UGX", "bank"), (USD, "37", "DFCU USD", "bank")))
cur = c.cursor()
cur.execute("UPDATE account SET currency='USD' WHERE account_id=%s", (USD,)); c.commit()
T = H.Checker()
check = T.check

def acct(i, n, t, ccy):
    return {"Id": i, "Name": n, "AccountType": t, "CurrencyRef": {"value": ccy}}
A._store_coa([acct("35", "Stanbic UGX", "Bank", "UGX"), acct("37", "DFCU USD", "Bank", "USD"),
              acct("70", "Stanbic Loan", "Long Term Liability", "UGX"), acct("92", "Loan Interest", "Expense", "UGX"),
              acct("83", "Office Supplies", "Expense", "UGX"), acct("90", "Tuition", "Income", "UGX")])
POSTS = []
def fake_post(token, entity, body):
    POSTS.append((entity, json.loads(json.dumps(body))))
    return {entity: {"Id": str(900 + len(POSTS))}}
A.qbo_post = fake_post
A.qbo_exchange_rate = lambda token, ccy, d: 3700.0
A.qbo_token = lambda: "tok"
A.qbo_realm = lambda: "REAL-1"
A.qbo_is_connected = lambda: False

def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
def lid(desc):
    return str(q("""SELECT sl.line_id FROM statement_line sl JOIN statement s USING (statement_id)
                    WHERE sl.description=%s ORDER BY s.created_at DESC LIMIT 1""", (desc,))[0][0])

cl = H.login(A)
def upload(acct_name, rows, start, end):
    body = "Date,Description,Amount\n" + "".join(f"{d},{t},{a}\n" for d, t, a in rows)
    r = cl.post(f"/account/{acct_name}/upload", data={"statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
                "period_start": start, "period_end": end}, content_type="multipart/form-data")
    assert r.status_code == 302, r.data[:300]
def page(acct_name):
    return cl.get(f"/account/{acct_name}").data.decode()
def split_of(p, line):
    m = re.search(rf'name="split_{line}" class=split-v value="([^"]*)"( data-from="([^"]*)")?', p)
    if not m or not m.group(1):
        return None, None
    return json.loads(m.group(1).replace("&#34;", '"').replace("&quot;", '"')), (m.group(3) or "").replace("&#39;", "'")
def ticked(p, line):
    return "checked" in p.split(f'value="{line}" class=rsel')[1].split(">")[0]

# ---- June: the first instalment, split by hand ---------------------------------------------------
upload("Stanbic UGX", [("2026-06-05", "LOAN REPAYMENT 0012345 JUN", -1000000), ("2026-06-06", "STATIONERY SHOP", -5000)],
       "2026-06-01", "2026-06-30")
JUN = lid("LOAN REPAYMENT 0012345 JUN")
check("nothing remembered before any split", split_of(page("Stanbic UGX"), JUN)[0] is None)
parts = json.dumps([{"a": "70", "v": "800,000"}, {"a": "92", "v": "200000"}])
cl.post("/account/Stanbic UGX/record", data={"only": JUN, f"split_{JUN}": parts})
check("June instalment recorded as a journal entry", POSTS and POSTS[-1][0] == "JournalEntry")
mem = q("SELECT description, money_out, currency, parts, total FROM split_memory")
check("…and its split remembered for the payee", len(mem) == 1 and mem[0][1] is True and mem[0][2] == "UGX"
      and json.loads(mem[0][3]) == [{"a": "70", "v": "800000"}, {"a": "92", "v": "200000"}] and int(mem[0][4]) == 1000000)

# ---- July: the next instalments -------------------------------------------------------------------
upload("Stanbic UGX", [("2026-07-05", "LOAN REPAYMENT 0012345 JUL", -1000000),
                       ("2026-07-20", "LOAN REPAYMENT 0012345 TOPUP", -1200000),
                       ("2026-07-21", "LOAN REPAYMENT REVERSAL", 1000000),
                       ("2026-07-22", "STATIONERY SHOP", -7000)], "2026-07-01", "2026-07-31")
JUL, TOP, REV, ST = (lid(x) for x in ("LOAN REPAYMENT 0012345 JUL", "LOAN REPAYMENT 0012345 TOPUP",
                                       "LOAN REPAYMENT REVERSAL", "STATIONERY SHOP"))
p = page("Stanbic UGX")
sp, note = split_of(p, JUL)
check("same instalment: split pre-filled with last month's accounts and amounts",
      sp == [{"a": "70", "v": "800000"}, {"a": "92", "v": "200000"}])
check("…says where it came from", note.startswith("Split like 05/06/2026") and "Same total" in note)
check("…and is ticked, ready to record", ticked(p, JUL))
sp2, note2 = split_of(p, TOP)
check("different total: amounts scaled to it, adding up exactly",
      sp2 == [{"a": "70", "v": "960000.00"}, {"a": "92", "v": "240000.00"}])
check("…says to type the right amounts, and waits unticked", "scaled" in note2 and not ticked(p, TOP))
check("money in on a similar line: no split offered", split_of(p, REV)[0] is None)
check("an unrelated line: no split offered", split_of(p, ST)[0] is None)

# The pre-filled split records as it stands.
n = len(POSTS)
cl.post("/account/Stanbic UGX/record", data={"only": JUL, f"split_{JUL}": json.dumps(sp)})
check("recording the pre-filled split posts one journal entry", len(POSTS) == n + 1 and POSTS[-1][0] == "JournalEntry")
check("…still one split remembered for the payee (replaced, not added)", len(q("SELECT 1 FROM split_memory")) == 1)
check("…now dated July", str(q("SELECT line_date FROM split_memory")[0][0]) == "2026-07-05")

# A saved selection wins over the memory.
cl.post("/account/Stanbic UGX/record_save", data={"rowid": [TOP], f"acct_{TOP}": "83", f"split_{TOP}": ""})
check("a saved selection without a split isn't overridden", split_of(page("Stanbic UGX"), TOP)[0] is None)
cl.post("/account/Stanbic UGX/record_discard", data={})
check("…discarding it offers the split again", split_of(page("Stanbic UGX"), TOP)[0] is not None)

# Recording the payee's line whole forgets the split.
cl.post("/account/Stanbic UGX/record", data={"only": TOP, f"acct_{TOP}": "70"})
check("recording a similar line whole forgets the split", not q("SELECT 1 FROM split_memory"))

# A currency deal is a one-off at its own rate: its split (FX in Transit + a loss) isn't remembered.
k = c.cursor()
A.split_remember(k, "FXPLSP SPOT SELL USD-UGX 3720", True, "UGX", [("70", 1), ("92", 1)], 2, None, "t"); c.commit()
check("an FX deal's split isn't remembered", not q("SELECT 1 FROM split_memory"))

# ---- per currency ----------------------------------------------------------------------------------
cur.execute("""INSERT INTO split_memory (org_id, description, money_out, currency, parts, total, line_date)
               VALUES (%s,'LOAN REPAYMENT 0012345 JUN',true,'UGX',%s,1000000,'2026-06-05')""",
            (A.ORG_ID, json.dumps([{"a": "70", "v": "800000"}, {"a": "92", "v": "200000"}]))); c.commit()
upload("DFCU USD", [("2026-07-05", "LOAN REPAYMENT 0012345 JUL", -270)], "2026-07-01", "2026-07-31")
check("a UGX split isn't offered on the USD bank", split_of(page("DFCU USD"), lid("LOAN REPAYMENT 0012345 JUL"))[0] is None)

# An account gone from the chart of accounts: the remembered split isn't offered.
upload("Stanbic UGX", [("2026-08-05", "LOAN REPAYMENT 0012345 AUG", -1000000)], "2026-08-01", "2026-08-31")
AUG = lid("LOAN REPAYMENT 0012345 AUG")
check("remembered split offered on the August line", split_of(page("Stanbic UGX"), AUG)[0] is not None)
q("UPDATE qbo_coa SET active=false WHERE qbo_id='92' RETURNING 1")
check("…not once one of its accounts is inactive", split_of(page("Stanbic UGX"), AUG)[0] is None)
q("UPDATE qbo_coa SET active=true WHERE qbo_id='92' RETURNING 1")

# ---- the page's script -----------------------------------------------------------------------------
html = page("Stanbic UGX")
node = shutil.which("node")
if not node or not os.path.isdir(os.path.join(H.HERE, "node_modules", "jsdom")):
    check("jsdom available (run: cd tests && npm install)", False)
else:
    JS = r"""
const { JSDOM, VirtualConsole } = require("jsdom");
const fs = require("fs");
const errors = []; const vc = new VirtualConsole(); vc.on("jsdomError", e => errors.push(String(e.message || e)));
const dom = new JSDOM(fs.readFileSync(process.argv[2], "utf8"), { runScripts: "dangerously", virtualConsole: vc, pretendToBeVisual: true });
const w = dom.window, d = w.document; w.HTMLElement.prototype.scrollIntoView = function () {};
const out = {};
const tr = d.querySelector(`input.rsel[value="${process.argv[3]}"]`).closest("tr"), srow = tr.nextElementSibling;
out.open = !srow.hidden;
out.amts = [...srow.querySelectorAll(".splitamt")].map(i => i.value);
out.accts = [...srow.querySelectorAll(".splitline .acct-q")].map(i => i.value);
out.note = (srow.querySelector(".splitfrom") || {}).textContent || "";
out.rem = srow.querySelector(".splitrem").textContent;
out.mainOff = tr.querySelector(".acctbox.main").classList.contains("off");
srow.querySelector("[data-cancel]").click();
out.noteGone = !srow.querySelector(".splitfrom"); out.cleared = tr.querySelector(".split-v").value === "";
out.errors = errors;
console.log(JSON.stringify(out));
"""
    f = tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8"); f.write(html); f.close()
    g = tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, dir=H.HERE, encoding="utf-8"); g.write(JS); g.close()
    try:
        res = subprocess.run([node, g.name, f.name, AUG], capture_output=True, text=True, encoding="utf-8", cwd=H.HERE)
    finally:
        os.unlink(f.name); os.unlink(g.name)
    if res.stderr.strip():
        print(res.stderr[-2000:])
    o = json.loads([l for l in res.stdout.splitlines() if l.startswith("{")][-1]) if "{" in res.stdout else {}
    print("   ", o)
    check("browser: the remembered split opens on its own", o.get("open") and o.get("amts") == ["800,000", "200,000"]
          and o.get("mainOff"))
    check("browser: …with its accounts named", any("Loan" in x for x in o.get("accts") or [])
          and any("Interest" in x for x in o.get("accts") or []))
    check("browser: …a note saying where it came from", o.get("note", "").startswith("Split like 05/06/2026"))
    check("browser: …adds up", o.get("rem") == "Adds up to 1,000,000.00")
    check("browser: Remove split clears it and the note", o.get("noteGone") and o.get("cleared"))
    check("browser: no script errors", o.get("errors") == [])
sys.exit(T.summary())
