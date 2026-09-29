"""UI: the account page's own JavaScript, run in a simulated browser (jsdom) against a real render.

Seeds a statement with things for the manual-match panel and the record table to show, renders
/account/<name> through the app, then runs tests/ui/account_page.test.js on that HTML.
QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_ui.py`, or all suites with `python tests/run_all.py`.
"""
import io, os, shutil, subprocess, sys, tempfile

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
    check("browser test ran to the end", out.returncode in (0, 1) and len(lines) > 10)
    if out.stderr.strip():
        print(out.stderr[-2000:])
sys.exit(T.summary())
