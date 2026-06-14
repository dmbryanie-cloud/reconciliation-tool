import os
import csv
import psycopg2
from decimal import Decimal
from datetime import datetime
import hashlib

DB_URL = os.environ["SUPABASE_DB_URL"]
ORG_ID = "00000000-0000-0000-0000-000000000001"
CSV_FILE = "bank_statement.csv"

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

cur.execute("SELECT account_id, currency FROM account WHERE type='bank' AND name='Checking' LIMIT 1;")
checking_uuid, currency = cur.fetchone()

# Read the CSV file into rows
rows = []
with open(CSV_FILE, newline="") as f:
    for r in csv.DictReader(f):
        rows.append({
            "date": datetime.strptime(r["Date"].strip(), "%Y-%m-%d").date(),
            "amount": Decimal(r["Amount"].strip()),
            "desc": r["Description"].strip(),
        })

if not rows:
    print("No rows found in the CSV.")
    raise SystemExit

period_start = min(r["date"] for r in rows)
period_end = max(r["date"] for r in rows)
closing = sum(r["amount"] for r in rows)
print(f"Parsed {len(rows)} lines from {CSV_FILE}, covering {period_start} to {period_end}.")

# Re-runnable: clear any earlier statement for this exact period
cur.execute("DELETE FROM statement WHERE account_id=%s AND period_start=%s AND period_end=%s;",
            (checking_uuid, period_start, period_end))

cur.execute("""
    INSERT INTO statement (org_id, account_id, period_start, period_end,
                           opening_balance, closing_balance, currency, source_format)
    VALUES (%s, %s, %s, %s, 0, %s, %s, 'csv')
    RETURNING statement_id;
""", (ORG_ID, checking_uuid, period_start, period_end, closing, currency))
statement_id = cur.fetchone()[0]

for r in rows:
    # Deterministic dedupe key: same line on re-import produces the same key
    raw = f"{r['date']}|{r['amount']}|{r['desc'].lower()}"
    dedupe_key = hashlib.sha256(raw.encode()).hexdigest()[:32]
    cur.execute("""
        INSERT INTO statement_line (org_id, statement_id, posted_date, amount,
                                    currency, description, dedupe_key)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (statement_id, dedupe_key) DO NOTHING;
    """, (ORG_ID, statement_id, r["date"], r["amount"], currency, r["desc"], dedupe_key))

conn.commit()

cur.execute("SELECT count(*) FROM statement_line WHERE statement_id=%s;", (statement_id,))
print(f"statement_line now holds {cur.fetchone()[0]} lines from the real file.")
print("\nNow run:  python3 match.py")

cur.close()
conn.close()