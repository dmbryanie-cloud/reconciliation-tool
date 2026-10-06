"""Possible transfers leave bank charges out (every bank takes the same small charges, so they look
like transfers without being any), and "Not a transfer" / Restore handle hundreds ticked at once.

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_transfer_charges.py`, or all suites with `python tests/run_all.py`.
"""
import io, re, sys, uuid

import harness as H

STB, CEN = "00000000-0000-0000-0000-0000000000a1", "00000000-0000-0000-0000-0000000000a2"
A, c = H.setup(H.account_sql((STB, "35", "Stanbic UGX", "bank"), (CEN, "36", "Centenary UGX", "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
A.qbo_is_connected = lambda: False

def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
def lid(desc):
    return str(q("SELECT line_id FROM statement_line WHERE description=%s", (desc,))[0][0])
cl = H.login(A)
def upload(acct_name, rows):
    body = "Date,Description,Amount\n" + "".join(f"{d},{t},{a}\n" for d, t, a in rows)
    cl.post(f"/account/{acct_name}/upload", data={"replace": "1", "statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
            "period_start": "2026-08-01", "period_end": "2026-08-31"}, content_type="multipart/form-data")
def xfer_sec(p):
    m = re.search(r"<h2 id=sec-transfers.*?(<h2 id=|$)", p, re.S)
    return m.group(0) if m else ""

# Centenary's books carry its own bank charges; its statement has a deposit of the same small amount
# and the other side of a real transfer.
for i, (d, amt, who) in enumerate([("2026-08-03", -150, "Bank charges"), ("2026-08-05", -2500, "Excise duty")]):
    q("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency,
         description, counterparty, last_modified) VALUES (%s,%s,%s,'Purchase',%s,%s,'UGX',%s,%s,now()) RETURNING 1""",
      (A.ORG_ID, CEN, f"c{i}", d, amt, who, who))
upload("Centenary UGX", [("2026-08-02", "FROM STANBIC", 500000), ("2026-08-03", "CASH DEPOSIT", 150)])
upload("Stanbic UGX", [("2026-08-02", "TRANSFER TO CENTENARY", -500000), ("2026-08-03", "EXCISE DUTY", -150),
                       ("2026-08-05", "PAYMENT ACME LTD", -2500)])
p = xfer_sec(cl.get("/account/Stanbic UGX").data.decode())
check("a real transfer is still suggested", "TRANSFER TO CENTENARY" in p and "FROM STANBIC" in p)
check("a bank charge isn't suggested as a transfer (neither against another statement nor another bank's books)",
      "EXCISE DUTY" not in p and "CASH DEPOSIT" not in p)
check("…nor is a payment whose only counterpart is another bank's charge", "PAYMENT ACME LTD" not in p)
check("…so just the one suggestion", "Possible transfers between your own accounts (1)" in p)

# ---- Not a transfer, hundreds ticked at once: saved in one go ---------------------------------------------------------
upload("Stanbic UGX", [(f"2026-08-{1 + i % 28:02d}", f"PAYMENT {i}", -(1000 + i)) for i in range(400)])
lines = [str(r[0]) for r in q("""SELECT line_id FROM statement_line sl JOIN statement s USING (statement_id)
                                 WHERE s.account_id=%s""", (STB,))]
picks = [f"{l}|line:{uuid.uuid4()}" for l in lines for _ in range(2)]          # 800 suggestions
cen_line = lid("FROM STANBIC")
r = cl.post("/account/Stanbic UGX/transfer_dismiss", data={"pick": picks + [f"{cen_line}|line:x", "junk|line:y", f"{lines[0]}|"]})
n = q("SELECT count(*) FROM transfer_dismissal")[0][0]
check("800 ticked as Not a transfer are all hidden", r.status_code == 302 and n == 800)
check("…and it says how many", "Hidden 800 suggestions" in cl.get("/account/Stanbic UGX").data.decode())
check("…another account's line, a bad id or a missing counterpart are ignored",
      q("SELECT count(*) FROM transfer_dismissal WHERE line_id=%s", (cen_line,))[0][0] == 0)
cl.post("/account/Stanbic UGX/transfer_dismiss", data={"pick": picks[:10]})
check("ticking already-hidden ones again changes nothing", q("SELECT count(*) FROM transfer_dismissal")[0][0] == 800
      and "Nothing hidden" in cl.get("/account/Stanbic UGX").data.decode())
cl.post("/account/Stanbic UGX/transfer_restore", data={"pick": picks[:600]})
check("Restore selected brings back the ticked ones only", q("SELECT count(*) FROM transfer_dismissal")[0][0] == 200
      and "Restored 600 suggestions" in cl.get("/account/Stanbic UGX").data.decode())
cl.post("/account/Centenary UGX/transfer_restore", data={"pick": picks[600:]})
check("…and only from the account the line is on", q("SELECT count(*) FROM transfer_dismissal")[0][0] == 200)
cl.post("/account/Stanbic UGX/transfer_dismiss", data={"line": lines[0], "other": "line:solo"})
check("one from its own row still works", q("SELECT count(*) FROM transfer_dismissal WHERE other='line:solo'")[0][0] == 1)

sys.exit(T.summary())
