import os
import sys
import psycopg2

DB_URL = os.environ["SUPABASE_DB_URL"]
ACCT_NAME = sys.argv[1] if len(sys.argv) > 1 else "Checking"

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

# Give the statement table a place to record a sign-off (safe to run repeatedly)
cur.execute("ALTER TABLE statement ADD COLUMN IF NOT EXISTS signed_off_at timestamptz;")
cur.execute("ALTER TABLE statement ADD COLUMN IF NOT EXISTS signed_off_by text;")
conn.commit()

cur.execute("SELECT account_id, type FROM account WHERE name=%s LIMIT 1;", (ACCT_NAME,))
row = cur.fetchone()
if not row:
    print(f"No account named '{ACCT_NAME}'."); raise SystemExit
acct_uuid, acct_type = row

cur.execute("""SELECT statement_id, period_start, period_end, signed_off_at
               FROM statement WHERE account_id=%s ORDER BY created_at DESC LIMIT 1;""", (acct_uuid,))
srow = cur.fetchone()
if not srow:
    print(f"No statement for {ACCT_NAME}. Run make_test_statement.py and match.py first."); raise SystemExit
statement_id, p_start, p_end, signed_off_at = srow

print(f"Reviewing {ACCT_NAME} reconciliation  {p_start} -> {p_end}")
if signed_off_at:
    print(f"  (NOTE: already signed off at {signed_off_at})")
print()

# Only fuzzy and many-to-one matches need a human eye; exact ones are trusted.
cur.execute("""SELECT match_id, match_type, amount_delta, status
               FROM match WHERE statement_id=%s AND match_type IN ('fuzzy','many_to_one')
               ORDER BY match_type;""", (statement_id,))
to_review = cur.fetchall()

def describe(mid):
    cur.execute("""SELECT sl.posted_date, sl.amount, coalesce(sl.counterparty, sl.description,'')
                   FROM match_statement_line msl JOIN statement_line sl ON sl.line_id=msl.line_id
                   WHERE msl.match_id=%s;""", (mid,))
    sls = cur.fetchall()
    cur.execute("""SELECT bt.posted_date, bt.amount, coalesce(bt.counterparty, bt.description,'')
                   FROM match_book_txn mbt JOIN book_txn bt ON bt.txn_id=mbt.txn_id
                   WHERE mbt.match_id=%s;""", (mid,))
    return sls, cur.fetchall()

if not to_review:
    print("No fuzzy or many-to-one matches to review — everything matched exactly.\n")
else:
    print(f"{len(to_review)} match(es) need your review. Type y or n then Enter for each:\n")
    for mid, mtype, delta, status in to_review:
        sls, bts = describe(mid)
        print(f"[{mtype}]")
        for d, a, w in sls: print(f"   statement: {d} | {a:>10} | {w}")
        for d, a, w in bts: print(f"   books:     {d} | {a:>10} | {w}")
        if delta and delta != 0:
            print(f"   discrepancy: {delta}")
        ans = input("   Keep this match? [y/n] ").strip().lower()
        new_status = "rejected" if ans == "n" else "confirmed"
        cur.execute("UPDATE match SET status=%s WHERE match_id=%s;", (new_status, mid))
        print(f"   -> {new_status}\n")
    conn.commit()

# Recompute what's resolved vs outstanding, ignoring rejected matches
cur.execute("""SELECT msl.line_id FROM match m JOIN match_statement_line msl ON msl.match_id=m.match_id
               WHERE m.statement_id=%s AND m.status<>'rejected';""", (statement_id,))
matched_lines = {r[0] for r in cur.fetchall()}
cur.execute("""SELECT mbt.txn_id FROM match m JOIN match_book_txn mbt ON mbt.match_id=m.match_id
               WHERE m.statement_id=%s AND m.status<>'rejected';""", (statement_id,))
matched_txns = {r[0] for r in cur.fetchall()}

cur.execute("SELECT line_id, posted_date, amount, coalesce(counterparty, description,'') FROM statement_line WHERE statement_id=%s;", (statement_id,))
lines = cur.fetchall()
cur.execute("""SELECT txn_id, posted_date, amount, coalesce(counterparty, description,'')
               FROM book_txn WHERE account_id=%s AND posted_date BETWEEN %s AND %s
                 AND is_void=false AND is_deleted=false;""", (acct_uuid, p_start, p_end))
txns = cur.fetchall()
on_stmt = [l for l in lines if l[0] not in matched_lines]
in_books = [t for t in txns if t[0] not in matched_txns]

print("===== STILL UNRESOLVED =====")
print(f"On statement, not in books ({len(on_stmt)}):")
for _, d, a, w in on_stmt: print(f"   {d} | {a:>10} | {w}")
print(f"In books, not on statement ({len(in_books)}):")
for _, d, a, w in in_books: print(f"   {d} | {a:>10} | {w}")

unresolved = len(on_stmt) + len(in_books)
print()
if unresolved:
    print(f"{unresolved} unresolved exception(s) — real differences (a fee to record, an")
    print("outstanding payment). Normal at sign-off, as long as you've looked them over.")
ans = input("\nSign off this reconciliation as reviewed and complete? [y/n] ").strip().lower()
if ans == "y":
    cur.execute("UPDATE statement SET signed_off_at=now(), signed_off_by='you' WHERE statement_id=%s;", (statement_id,))
    conn.commit()
    print(f"\nSigned off. {ACCT_NAME} {p_start}->{p_end} is locked as reviewed.")
else:
    print("\nLeft open — not signed off.")

cur.close()
conn.close()