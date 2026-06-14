import os
import psycopg2
from decimal import Decimal

DB_URL = os.environ["SUPABASE_DB_URL"]
conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

cur.execute("SELECT account_id, name FROM account WHERE type='bank' AND name='Checking' LIMIT 1;")
checking_uuid, account_name = cur.fetchone()
cur.execute("SELECT statement_id, period_start, period_end FROM statement WHERE account_id=%s ORDER BY created_at DESC LIMIT 1;", (checking_uuid,))
statement_id, p_start, p_end = cur.fetchone()

cur.execute("SELECT line_id, posted_date, amount, coalesce(counterparty, description,'') FROM statement_line WHERE statement_id=%s ORDER BY posted_date;", (statement_id,))
lines = cur.fetchall()
cur.execute("SELECT txn_id, posted_date, amount, coalesce(counterparty, description,'') FROM book_txn WHERE account_id=%s AND posted_date BETWEEN %s AND %s AND is_void=false AND is_deleted=false ORDER BY posted_date;", (checking_uuid, p_start, p_end))
txns = cur.fetchall()
cur.execute("""SELECT m.match_type, m.amount_delta, sl.posted_date, sl.amount, coalesce(sl.counterparty, sl.description,''), bt.amount
               FROM match m JOIN match_statement_line msl ON msl.match_id=m.match_id JOIN statement_line sl ON sl.line_id=msl.line_id
               JOIN match_book_txn mbt ON mbt.match_id=m.match_id JOIN book_txn bt ON bt.txn_id=mbt.txn_id
               WHERE m.statement_id=%s ORDER BY sl.posted_date;""", (statement_id,))
matched = cur.fetchall()
cur.execute("SELECT msl.line_id FROM match m JOIN match_statement_line msl ON msl.match_id=m.match_id WHERE m.statement_id=%s;", (statement_id,))
ml = {r[0] for r in cur.fetchall()}
cur.execute("SELECT mbt.txn_id FROM match m JOIN match_book_txn mbt ON mbt.match_id=m.match_id WHERE m.statement_id=%s;", (statement_id,))
mt = {r[0] for r in cur.fetchall()}
cur.close(); conn.close()

on_stmt = [l for l in lines if l[0] not in ml]
in_books = [t for t in txns if t[0] not in mt]
diff = sum((l[2] for l in lines), Decimal(0)) - sum((t[2] for t in txns), Decimal(0))

def rows_matched(rs):
    return "".join(f"<tr><td>{d}</td><td>{w}</td><td><span class='tag {mt}'>{mt}{' · off '+format(dl,'.2f') if dl!=0 else ''}</span></td><td class=a>{s:.2f}</td><td class=a>{b:.2f}</td></tr>" for mt,dl,d,s,w,b in rs)
def rows_exc(rs):
    return "".join(f"<tr><td>{d}</td><td>{w}</td><td class=a>{a:.2f}</td></tr>" for _,d,a,w in rs)

html = f"""<!doctype html><meta charset=utf-8><title>Reconciliation</title>
<style>body{{font-family:system-ui,sans-serif;background:#f5f5f3;color:#2b2b29;padding:32px;max-width:880px;margin:auto}}
h1{{font-size:22px;margin:0}}.sub{{color:#73726c;margin:4px 0 24px}}
.cards{{display:flex;gap:12px;margin-bottom:24px;flex-wrap:wrap}}.card{{flex:1;min-width:140px;background:#fff;border:1px solid #e3e2dd;border-radius:10px;padding:16px}}
.label{{font-size:12px;color:#73726c;text-transform:uppercase}}.val{{font-size:24px;font-weight:600;margin-top:6px}}.bad{{color:#b3471f}}.ok{{color:#3a7d44}}
h2{{font-size:15px;margin:24px 0 8px}}table{{width:100%;border-collapse:collapse;background:#fff;border:1px solid #e3e2dd;border-radius:10px;overflow:hidden}}
th,td{{text-align:left;padding:9px 14px;border-bottom:1px solid #efeeea;font-size:14px}}th{{background:#faf9f7;color:#73726c;font-size:12px;text-transform:uppercase}}
.a{{text-align:right;font-variant-numeric:tabular-nums}}.tag{{font-size:11px;padding:2px 8px;border-radius:20px}}.tag.exact{{background:#e7f1e9;color:#3a7d44}}.tag.fuzzy{{background:#fbf0dd;color:#9a6a16}}.exc th{{background:#fcefe9;color:#b3471f}}</style>
<h1>Reconciliation — {account_name}</h1><div class=sub>Statement period {p_start} to {p_end}</div>
<div class=cards>
<div class=card><div class=label>Exact matches</div><div class=val>{sum(1 for m in matched if m[0]=='exact')}</div></div>
<div class=card><div class=label>Discrepancies</div><div class=val>{sum(1 for m in matched if m[0]=='fuzzy')}</div></div>
<div class=card><div class=label>Exceptions</div><div class=val>{len(on_stmt)+len(in_books)}</div></div>
<div class=card><div class=label>Difference</div><div class="val {'ok' if diff==0 else 'bad'}">{diff:.2f}</div></div></div>
<h2>Matched ({len(matched)})</h2><table><tr><th>Date</th><th>Payee</th><th></th><th class=a>Statement</th><th class=a>Books</th></tr>{rows_matched(matched)}</table>
<h2>On statement, not in books ({len(on_stmt)})</h2><table class=exc><tr><th>Date</th><th>Description</th><th class=a>Amount</th></tr>{rows_exc(on_stmt)}</table>
<h2>In books, not on statement ({len(in_books)})</h2><table class=exc><tr><th>Date</th><th>Description</th><th class=a>Amount</th></tr>{rows_exc(in_books)}</table>"""

with open("reconciliation.html", "w") as f:
    f.write(html)
print("Wrote reconciliation.html — right-click it in the file list and choose Download, then open it in your browser.")