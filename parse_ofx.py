import os
import hashlib
import psycopg2
from decimal import Decimal
from ofxparse import OfxParser

DB_URL = os.environ["SUPABASE_DB_URL"]
ORG_ID = "00000000-0000-0000-0000-000000000001"
OFX_FILE = "bank_statement.ofx"

with open(OFX_FILE, "rb") as f:
    ofx = OfxParser.parse(f)

txns = ofx.account.statement.transactions
if not txns:
    print("No transactions found in the OFX file.")
    raise SystemExit

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()
cur.execute("SELECT account_id, currency FROM account WHERE type='bank' AND name='Checking' LIMIT 1;")
checking_uuid, currency = cur.fetchone()

dates = [t.date.date() for t in txns]
p_start, p_end = min(dates), max(dates)
closing = sum((Decimal(str(t.amount)) for t in txns), Decimal(0))
print(f"Parsed {len(txns)} transactions from {OFX_FILE}, covering {p_start} to {p_end}.")

cur.execute("DELETE FROM statement WHERE account_id=%s AND period_start=%s AND period_end=%s;",
            (checking_uuid, p_start, p_end))
cur.execute("""INSERT INTO statement (org_id, account_id, period_start, period_end,
                                      opening_balance, closing_balance, currency, source_format)
               VALUES (%s,%s,%s,%s,0,%s,%s,'ofx') RETURNING statement_id;""",
            (ORG_ID, checking_uuid, p_start, p_end, closing, currency))
statement_id = cur.fetchone()[0]

for t in txns:
    fitid = (t.id or "").strip()
    # FITID is the bank's own unique id — a perfect dedupe key
    key = fitid if fitid else hashlib.sha256(
        f"{t.date.date()}|{t.amount}|{(t.payee or '').lower()}".encode()).hexdigest()[:32]
    desc = (t.payee or t.memo or "").strip()
    cur.execute("""INSERT INTO statement_line (org_id, statement_id, posted_date, amount,
                                               currency, description, external_id, dedupe_key)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (statement_id, dedupe_key) DO NOTHING;""",
                (ORG_ID, statement_id, t.date.date(), Decimal(str(t.amount)),
                 currency, desc, fitid or None, key))

conn.commit()
cur.execute("SELECT count(*) FROM statement_line WHERE statement_id=%s;", (statement_id,))
print(f"statement_line now holds {cur.fetchone()[0]} lines from the OFX file (deduped by FITID).")
cur.close()
conn.close()