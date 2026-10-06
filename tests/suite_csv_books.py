"""Books from a CSV are only for accounts not linked to QuickBooks. On a linked account the import
is refused and not offered, and any CSV rows already there never count as books -- a bank
statement imported as books by mistake paired every bank line with a copy of itself.

QuickBooks isn't called. Run on its own with `python tests/suite_csv_books.py`, or all suites with
`python tests/run_all.py`.
"""
import html, io, re, sys

import harness as H

LINKED, OFFLINE = "00000000-0000-0000-0000-0000000000a1", "00000000-0000-0000-0000-0000000000a2"
A, c = H.setup(H.account_sql((LINKED, "35", "Stanbic 7994", "bank"), (OFFLINE, "36", "Petty Bank", "bank")))
cur = c.cursor()
cur.execute("UPDATE account SET source_account_id='' WHERE account_id=%s", (OFFLINE,)); c.commit()   # not linked
T = H.Checker()
check = T.check
A.qbo_is_connected = lambda: False


def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r

cl = H.login(A)
BOOKS = "Date,Description,Amount\n2026-06-05,FUEL,-50000\n2026-06-20,FEES BANKED,200000\n"
def import_books(acct):
    cl.post(f"/account/{acct}/import_books", data={"books": (io.BytesIO(BOOKS.encode()), "books.csv")},
            content_type="multipart/form-data")
    p = cl.get(f"/account/{acct}").data.decode()
    m = re.search(r"<div id=flash[^>]*>(.*?)</div>", p, re.S)
    return html.unescape(re.sub(r"<[^>]+>", "", m.group(1))) if m else ""
def upload(acct):
    body = "Date,Description,Amount\n2026-06-05,FUEL,-50000\n2026-06-20,FEES BANKED,200000\n"
    cl.post(f"/account/{acct}/upload", data={"replace": "1", "statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
            "period_start": "2026-06-01", "period_end": "2026-06-30"}, content_type="multipart/form-data")
def pool(acct_uuid):
    k = c.cursor(); s = A._latest_statement(k, acct_uuid); r = A.reconcile(k, acct_uuid, s); c.rollback()
    return r["pool"]

# ---- linked to QuickBooks ----------------------------------------------------------------------------
m = import_books("Stanbic 7994")
check("linked account: CSV books refused, saying why", "linked to QuickBooks" in m and "Upload statement" in m)
check("…nothing imported", not q("SELECT 1 FROM book_txn WHERE account_id=%s", (LINKED,)))
check("…and the menu doesn't offer it", "Import books from a CSV" not in cl.get("/account/Stanbic 7994").data.decode())
# Rows imported before this guard existed don't count as books.
cur.execute("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency, description,
               last_modified) VALUES (%s,%s,'k1','CSV','2026-06-05',-50000,'UGX','FUEL',now()),
                                     (%s,%s,'77','Purchase','2026-06-20',200000,'UGX','Fees',now())""",
            (A.ORG_ID, LINKED, A.ORG_ID, LINKED)); c.commit()
upload("Stanbic 7994")
p = pool(LINKED)
check("…old CSV rows there aren't books: not in the pool, so no line pairs with its own copy",
      [t[3] for t in p] == ["Fees"])
check("…the FUEL line stays unmatched", not q("""SELECT 1 FROM match m JOIN match_statement_line msl USING (match_id)
      JOIN statement_line sl USING (line_id) WHERE sl.description='FUEL' AND m.status<>'rejected'"""))

# ---- not linked: books from a CSV as before ----------------------------------------------------------
m = import_books("Petty Bank")
check("offline account: CSV books imported", "Imported 2 book transactions" in m)
check("…the menu offers it", "Import books from a CSV" in cl.get("/account/Petty Bank").data.decode())
upload("Petty Bank")
check("…and they're its books", sorted(t[3] for t in pool(OFFLINE)) == ["FEES BANKED", "FUEL"])
sys.exit(T.summary())
