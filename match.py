import os
import sys
import itertools
import psycopg2
from decimal import Decimal

DB_URL = os.environ["SUPABASE_DB_URL"]
ORG_ID = "00000000-0000-0000-0000-000000000001"
DATE_TOLERANCE_DAYS = 3
GROUP_WINDOW_DAYS = 60   # how far apart batched items may sit (tunable)
MAX_GROUP = 4            # largest batch the matcher will try to assemble
ACCT_NAME = sys.argv[1] if len(sys.argv) > 1 else "Checking"

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()
cur.execute("SELECT account_id, type FROM account WHERE name=%s LIMIT 1;", (ACCT_NAME,))
row = cur.fetchone()
if not row:
    print(f"No account named '{ACCT_NAME}'."); raise SystemExit
acct_uuid, acct_type = row

cur.execute("SELECT statement_id, period_start, period_end FROM statement WHERE account_id=%s ORDER BY created_at DESC LIMIT 1;", (acct_uuid,))
srow = cur.fetchone()
if not srow:
    print(f"No statement for {ACCT_NAME}. Run: python3 make_test_statement.py {ACCT_NAME}"); raise SystemExit
statement_id, p_start, p_end = srow

cur.execute("DELETE FROM match WHERE statement_id=%s;", (statement_id,))
cur.execute("SELECT line_id, posted_date, amount, coalesce(counterparty, description,'') FROM statement_line WHERE statement_id=%s;", (statement_id,))
lines = cur.fetchall()
cur.execute("""SELECT txn_id, posted_date, amount, coalesce(counterparty, description,'')
               FROM book_txn WHERE account_id=%s AND posted_date BETWEEN %s AND %s
                 AND is_void=false AND is_deleted=false;""", (acct_uuid, p_start, p_end))
txns = cur.fetchall()

print(f"Reconciling {ACCT_NAME} ({acct_type})  {p_start} -> {p_end}")
print(f"  {len(lines)} statement lines vs {len(txns)} book transactions\n")

used, matched_lines = set(), set()

def write_match(line_ids, txn_ids, mtype, conf, delta):
    cur.execute("INSERT INTO match (org_id, statement_id, status, match_type, confidence, amount_delta) VALUES (%s,%s,'confirmed',%s,%s,%s) RETURNING match_id;",
                (ORG_ID, statement_id, mtype, conf, delta))
    m_id = cur.fetchone()[0]
    for l in line_ids: cur.execute("INSERT INTO match_statement_line (match_id, line_id) VALUES (%s,%s);", (m_id, l))
    for t in txn_ids: cur.execute("INSERT INTO match_book_txn (match_id, txn_id) VALUES (%s,%s);", (m_id, t))

# Pass 1 - exact (1:1)
exact = 0
for l_id, ld, la, lw in lines:
    for t_id, td, ta, tw in txns:
        if t_id in used: continue
        if la == ta and abs((ld - td).days) <= DATE_TOLERANCE_DAYS:
            write_match([l_id], [t_id], "exact", 1.0, 0); used.add(t_id); matched_lines.add(l_id); exact += 1; break

# Pass 2 - fuzzy (1:1, same payee, amount differs)
fuzzy = []
for l_id, ld, la, lw in lines:
    if l_id in matched_lines: continue
    for t_id, td, ta, tw in txns:
        if t_id in used: continue
        if lw and tw and lw.strip().lower() == tw.strip().lower() and abs((ld - td).days) <= DATE_TOLERANCE_DAYS:
            write_match([l_id], [t_id], "fuzzy", 0.6, la - ta); used.add(t_id); matched_lines.add(l_id)
            fuzzy.append((lw, la, ta, la - ta)); break

# Pass 3 - many-to-one: one statement line = a combination of book transactions
groups = []
for l_id, ld, la, lw in lines:
    if l_id in matched_lines: continue
    cands = [(t_id, ta) for (t_id, td, ta, tw) in txns
             if t_id not in used and abs((ld - td).days) <= GROUP_WINDOW_DAYS][:12]
    found = None
    for k in range(2, min(MAX_GROUP, len(cands)) + 1):
        for combo in itertools.combinations(cands, k):
            if sum((c[1] for c in combo), Decimal(0)) == la:
                found = combo; break
        if found: break
    if found:
        t_ids = [c[0] for c in found]
        write_match([l_id], t_ids, "many_to_one", 0.8, 0)
        matched_lines.add(l_id); used.update(t_ids)
        groups.append((lw, la, len(t_ids)))

conn.commit()
on_stmt = [l for l in lines if l[0] not in matched_lines]
in_books = [t for t in txns if t[0] not in used]
stmt_total = sum((l[2] for l in lines), Decimal(0))
books_total = sum((t[2] for t in txns), Decimal(0))
missing = sum((l[2] for l in on_stmt), Decimal(0))
outstanding = sum((t[2] for t in in_books), Decimal(0))
fuzzy_delta = sum((f[3] for f in fuzzy), Decimal(0))

print("===== SUMMARY =====")
print(f"Exact matches:       {exact}")
print(f"Fuzzy matches:       {len(fuzzy)}")
for w, la, ta, d in fuzzy: print(f"   {w}: statement {la} vs books {ta} (off by {d})")
print(f"Many-to-one matches: {len(groups)}")
for w, la, n in groups: print(f"   '{w}' line of {la} = {n} book entries combined")
print(f"\nOn statement, not in books ({len(on_stmt)}), total {missing}:")
for _, d, a, w in on_stmt: print(f"   {d} | {a:>10} | {w}")
print(f"In books, not on statement ({len(in_books)}), total {outstanding}:")
for _, d, a, w in in_books: print(f"   {d} | {a:>10} | {w}")
print("\n===== HOW THEY DIFFER =====")
diff = stmt_total - books_total
print(f"Difference: {diff}")
print("Every difference explained." if (missing - outstanding + fuzzy_delta) == diff else "Unexplained gap remains.")

cur.close(); conn.close()