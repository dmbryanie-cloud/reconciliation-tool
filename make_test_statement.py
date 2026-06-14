import os
import sys
import psycopg2
from datetime import timedelta
from decimal import Decimal

DB_URL = os.environ["SUPABASE_DB_URL"]
ORG_ID = "00000000-0000-0000-0000-000000000001"
ACCT_NAME = sys.argv[1] if len(sys.argv) > 1 else "Checking"

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()
cur.execute("SELECT account_id, currency FROM account WHERE name=%s LIMIT 1;", (ACCT_NAME,))
row = cur.fetchone()
if not row:
    print(f"No account named '{ACCT_NAME}'."); raise SystemExit
acct_uuid, currency = row

cur.execute("SELECT posted_date, amount, description, counterparty FROM book_txn WHERE account_id=%s ORDER BY posted_date;", (acct_uuid,))
txns = cur.fetchall()
if not txns:
    print(f"No book transactions for {ACCT_NAME}."); raise SystemExit

period_start, period_end = txns[0][0], txns[-1][0]
cur.execute("DELETE FROM statement WHERE account_id=%s AND period_start=%s AND period_end=%s;",
            (acct_uuid, period_start, period_end))

# Reserve 3 transactions to combine into one "batched" statement line
batch, base = ([], txns)
if len(txns) >= 6:
    batch, base = txns[-3:], txns[:-3]

lines = [{"date": d, "amount": Decimal(str(a)), "desc": ds, "cp": cp} for d, a, ds, cp in base]
notes = []
if len(lines) >= 1: lines[0]["date"] += timedelta(days=1); notes.append("shifted 1 date   -> should still match")
if len(lines) >= 2: lines[1]["amount"] += Decimal("5.00"); notes.append("changed 1 amount -> fuzzy mismatch")
if len(lines) >= 3: lines.pop(); notes.append("dropped 1 line   -> outstanding in books")
lines.append({"date": period_end, "amount": Decimal("-88.00"), "desc": "Electricity", "cp": "Kampala Power Co"})
notes.append("added 1 unrecorded expense -> write-back target")

if batch:
    bsum = sum((Decimal(str(b[1])) for b in batch), Decimal(0))
    bdate = max(b[0] for b in batch)
    lines.append({"date": bdate, "amount": bsum, "desc": "Batched deposit (combined)", "cp": "Batch"})
    notes.append(f"combined {len(batch)} entries into 1 line -> should be a many-to-one match")

closing = sum((l["amount"] for l in lines), Decimal(0))
cur.execute("""INSERT INTO statement (org_id, account_id, period_start, period_end,
                                      opening_balance, closing_balance, currency, source_format)
               VALUES (%s,%s,%s,%s,0,%s,%s,'csv') RETURNING statement_id;""",
            (ORG_ID, acct_uuid, period_start, period_end, closing, currency))
statement_id = cur.fetchone()[0]
for i, l in enumerate(lines):
    cur.execute("""INSERT INTO statement_line (org_id, statement_id, posted_date, amount,
                                               currency, description, counterparty, dedupe_key)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s);""",
                (ORG_ID, statement_id, l["date"], l["amount"], currency, l["desc"], l["cp"], f"L{i}"))
conn.commit()
print(f"Built statement for {ACCT_NAME}: {period_start} to {period_end}, {len(lines)} lines. Planted:")
for n in notes: print("  - " + n)
cur.close(); conn.close()