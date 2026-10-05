"""Copies in a period QuickBooks had already reconciled: everything recorded from here for those dates
is in QuickBooks twice. A check lists them; confirming deletes them in QuickBooks and matches each
month's bank charges to QuickBooks' combined charge entries where the totals agree.

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_period_copies.py`, or all suites with `python tests/run_all.py`.
"""
import html, io, re, sys
from decimal import Decimal as D

import harness as H

ACCT = "00000000-0000-0000-0000-0000000000a1"
NAME = "Stanbic UGX 10202"
A, c = H.setup(H.account_sql((ACCT, "35", NAME, "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
CH = "Operational Expenses:Finance Expenses:Charges & Fees:Bank charges UGX"
A._store_coa([{"Id": "35", "Name": NAME, "AccountType": "Bank", "CurrencyRef": {"value": "UGX"}},
              {"Id": "83", "Name": "Bank charges UGX", "AccountType": "Expense", "CurrencyRef": {"value": "UGX"}}])
POSTS, DELETES = [], []
def fake_post(token, entity, body):
    POSTS.append((entity, body)); return {entity: {"Id": str(54200 + len(POSTS))}}
def fake_delete(token, entity, i, st):
    DELETES.append((entity, i)); return {}
A.qbo_post, A.qbo_delete = fake_post, fake_delete
A.qbo_read = lambda token, entity, i: {"Id": i, "SyncToken": "1"}
A.qbo_token = lambda: "tok"
A.qbo_realm = lambda: "REAL-1"
A.qbo_is_connected = lambda: False


def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
def lid(desc):
    return str(q("SELECT line_id FROM statement_line WHERE description=%s", (desc,))[0][0])
def book(i, d, amt, text, cat):
    return str(q("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency, description,
                    category, last_modified) VALUES (%s,%s,%s,'Purchase',%s,%s,'UGX',%s,%s,now()) RETURNING txn_id""",
                 (A.ORG_ID, ACCT, i, d, amt, text, cat))[0][0])
cl = H.login(A)
def page(args=""):
    return html.unescape(cl.get(f"/account/{NAME}{args}").data.decode())
def msg():
    m = re.search(r"<div id=flash[^>]*>(.*?)</div>", page(), re.S)
    return re.sub(r"<[^>]+>", "", m.group(1)) if m else ""

# QuickBooks (reconciled to 10/11/2025) has the charges added up, booked days away from the bank's lines.
SEP = book("50296", "2025-09-01", -6900, "FEE ACH INWD CR", CH)
OCT = book("50344", "2025-10-10", -2300, "", CH)
body = ("Date,Description,Amount\n"
        "2025-09-08,EFT BOL FEES INST ID 1 Fee Collection,-2000\n2025-09-08,Excise Duty EFT BOL FEES 1,-300\n"
        "2025-09-12,FEE ACH INWD CR,-4000\n2025-09-12,GOVERNMENT EXCISE DUTY CHARGE,-600\n"
        "2025-10-20,EFT BOL FEES INST ID 2 Fee Collection,-2000\n2025-10-20,Excise Duty EFT BOL FEES 2,-300\n"
        "2025-11-20,MONTHLY MANAGEMENT FEE,-36000\n2025-09-15,GODWIN KAB MB TRANSFER,22527000\n")
cl.post(f"/account/{NAME}/upload", data={"statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
        "period_start": "2025-09-01", "period_end": "2025-11-30"}, content_type="multipart/form-data")
CHG = [lid(t) for t in ("EFT BOL FEES INST ID 1 Fee Collection", "Excise Duty EFT BOL FEES 1", "FEE ACH INWD CR",
                        "GOVERNMENT EXCISE DUTY CHARGE", "EFT BOL FEES INST ID 2 Fee Collection", "Excise Duty EFT BOL FEES 2")]
NOV, XF = lid("MONTHLY MANAGEMENT FEE"), lid("GODWIN KAB MB TRANSFER")
# Recorded from here anyway (copies), and one transfer.
cl.post(f"/account/{NAME}/record", data={"bulk": "1", "sel": CHG + [NOV], **{f"acct_{x}": "83" for x in CHG + [NOV]}})
assert len(POSTS) == 7, len(POSTS)
q("""INSERT INTO writeback_log (line_id, status, qbo_type, qbo_id, created_by) VALUES (%s,'done','Transfer','54504','x') RETURNING 1""", (XF,))

# ---- the check --------------------------------------------------------------------------------------------
check("the account menu offers the check", "Copies in a period QuickBooks reconciled" in page() and "id=dr-qrec" in page())
p = page("?qrec=2025-11-10")
check("checking 10/11/2025 lists the six copies dated up to then, with their total",
      "Recorded from here on or before 10/11/2025 (6)" in p and "6 entries" in p and "-9,200.00" in p)
check("…not the one dated after", "MONTHLY MANAGEMENT FEE" not in p.split("Recorded from here on or before")[1].split("</table>")[0])
check("…the transfer is pointed to Undo instead", "1 transfer recorded for those dates" in p and "use Undo under Recorded transfers" in p)
check("…nothing changed by looking", not DELETES)

# ---- confirming -------------------------------------------------------------------------------------------
cl.post(f"/account/{NAME}/period_copies", data={"upto": "2025-11-10", "line": CHG + [NOV, "not-a-line"]})
m = msg()
check("deletes the six copies in QuickBooks (not the November one, nor anything not listed)", len(DELETES) == 6
      and q("SELECT status FROM writeback_log WHERE line_id=%s", (NOV,)) == [("done",)])
check("…says so, and which months' charges it matched", "Deleted 6 entries recorded from here on or before 10/11/2025" in m
      and "for 09/2025, 10/2025" in m)
check("…September's four charge lines matched to QuickBooks' 6,900 entry",
      q("""SELECT count(*) FROM match m JOIN match_statement_line x USING (match_id) JOIN match_book_txn y USING (match_id)
           WHERE m.status='confirmed' AND y.txn_id=%s::uuid""", (SEP,))[0][0] == 4)
check("…October's two to its 2,300 entry", q("""SELECT count(*) FROM match m JOIN match_statement_line x USING (match_id)
      JOIN match_book_txn y USING (match_id) WHERE m.status='confirmed' AND y.txn_id=%s::uuid""", (OCT,))[0][0] == 2)
check("…the log says why each was deleted", q("SELECT error FROM writeback_log WHERE line_id=%s", (CHG[0],))[0][0]
      == "deleted by Admin: QuickBooks was already reconciled to 10/11/2025")
check("…the copies are gone from the books here", q("""SELECT count(*) FROM book_txn WHERE source_txn_id IN
      (SELECT qbo_id FROM writeback_log WHERE error LIKE 'deleted by%%') AND NOT is_deleted""")[0][0] == 0)
check("checking again: nothing left to delete", "Nothing recorded from here on or before that date." in page("?qrec=2025-11-10"))
check("activity log records it", q("SELECT 1 FROM activity_log WHERE action LIKE 'deleted 6 entries recorded from here on or before 10/11/2025%%'"))

# ---- limits ------------------------------------------------------------------------------------------------
q("UPDATE statement SET signed_off_at=now() RETURNING 1")
n = len(DELETES)
cl.post(f"/account/{NAME}/period_copies", data={"upto": "2025-12-31", "line": [NOV]})
check("a signed-off reconciliation: nothing deleted", len(DELETES) == n)
check("only users allowed to undo can do it", A.PERM_BY_ENDPOINT.get("period_copies_fix") == "undo")
sys.exit(T.summary())
