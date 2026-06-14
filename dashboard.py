import os
import psycopg2
from decimal import Decimal
from datetime import datetime, timezone, timedelta
EAT = timezone(timedelta(hours=3))

DB_URL = os.environ["SUPABASE_DB_URL"]
conn = psycopg2.connect(DB_URL)
cur = conn.cursor()
cur.execute("ALTER TABLE statement ADD COLUMN IF NOT EXISTS signed_off_at timestamptz;")
cur.execute("ALTER TABLE statement ADD COLUMN IF NOT EXISTS signed_off_by text;")
conn.commit()

cur.execute("SELECT account_id, name, type FROM account ORDER BY type, name;")
accounts = cur.fetchall()

rows, tot_recon, tot_signed, tot_exc = [], 0, 0, 0
for acct_uuid, name, atype in accounts:
    cur.execute("""SELECT statement_id, period_start, period_end, signed_off_at
                   FROM statement WHERE account_id=%s ORDER BY created_at DESC LIMIT 1;""", (acct_uuid,))
    s = cur.fetchone()
    if not s:
        rows.append({"name": name, "type": atype, "status": "none"}); continue
    sid, p_start, p_end, signed = s
    cur.execute("SELECT match_type, count(*) FROM match WHERE statement_id=%s AND status<>'rejected' GROUP BY match_type;", (sid,))
    mc = dict(cur.fetchall())
    cur.execute("SELECT msl.line_id FROM match m JOIN match_statement_line msl ON msl.match_id=m.match_id WHERE m.statement_id=%s AND m.status<>'rejected';", (sid,))
    ml = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT mbt.txn_id FROM match m JOIN match_book_txn mbt ON mbt.match_id=m.match_id WHERE m.statement_id=%s AND m.status<>'rejected';", (sid,))
    mt = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT line_id, amount FROM statement_line WHERE statement_id=%s;", (sid,))
    lines = cur.fetchall()
    cur.execute("SELECT txn_id, amount FROM book_txn WHERE account_id=%s AND posted_date BETWEEN %s AND %s AND is_void=false AND is_deleted=false;", (acct_uuid, p_start, p_end))
    txns = cur.fetchall()
    exc = len([l for l in lines if l[0] not in ml]) + len([t for t in txns if t[0] not in mt])
    diff = sum((l[1] for l in lines), Decimal(0)) - sum((t[1] for t in txns), Decimal(0))
    rows.append({"name": name, "type": atype, "status": "signed" if signed else "open",
                 "p_start": p_start, "p_end": p_end, "exact": mc.get("exact", 0),
                 "fuzzy": mc.get("fuzzy", 0), "m2o": mc.get("many_to_one", 0), "exc": exc, "diff": diff})
    tot_recon += 1
    tot_signed += 1 if signed else 0
    tot_exc += exc
cur.close(); conn.close()

PILL = {"none": ("Not reconciled", "#73726c", "#eeede9"),
        "open": ("Reconciled · open", "#9a6a16", "#fbf0dd"),
        "signed": ("Signed off", "#3a7d44", "#e7f1e9")}

def row_html(r):
    label, fg, bg = PILL[r["status"]]
    pill = f'<span style="background:{bg};color:{fg};padding:3px 10px;border-radius:20px;font-size:12px">{label}</span>'
    badge = "bank" if r["type"] == "bank" else "credit card"
    if r["status"] == "none":
        return f"<tr><td><b>{r['name']}</b></td><td>{badge}</td><td>{pill}</td><td>—</td><td>—</td><td>—</td><td class=a>—</td></tr>"
    matches = f"{r['exact']} exact"
    if r['fuzzy']: matches += f", {r['fuzzy']} fuzzy"
    if r['m2o']: matches += f", {r['m2o']} batched"
    if r["diff"] == 0:
        diff_cell, dc = f"{r['diff']:.2f}", "#3a7d44"
    elif r["exc"] > 0:
        diff_cell, dc = f"{r['diff']:.2f} · explained", "#73726c"
    else:
        diff_cell, dc = f"{r['diff']:.2f} · UNEXPLAINED", "#b3471f"
    return (f"<tr><td><b>{r['name']}</b></td><td>{badge}</td><td>{pill}</td>"
            f"<td>{r['p_start']} → {r['p_end']}</td><td>{matches}</td>"
            f"<td>{r['exc']}</td><td class=a style='color:{dc}'>{diff_cell}</td></tr>")

html = f"""<!doctype html><meta charset=utf-8><title>Reconciliation Dashboard</title>
<style>body{{font-family:system-ui,-apple-system,sans-serif;background:#f5f5f3;color:#2b2b29;padding:32px;max-width:960px;margin:auto}}
h1{{font-size:24px;margin:0}}.sub{{color:#73726c;margin:4px 0 24px}}
.cards{{display:flex;gap:12px;margin-bottom:28px;flex-wrap:wrap}}
.card{{flex:1;min-width:150px;background:#fff;border:1px solid #e3e2dd;border-radius:10px;padding:16px}}
.label{{font-size:12px;color:#73726c;text-transform:uppercase;letter-spacing:.04em}}.val{{font-size:26px;font-weight:600;margin-top:6px}}
table{{width:100%;border-collapse:collapse;background:#fff;border:1px solid #e3e2dd;border-radius:10px;overflow:hidden}}
th,td{{text-align:left;padding:11px 14px;border-bottom:1px solid #efeeea;font-size:14px}}
th{{background:#faf9f7;color:#73726c;font-size:12px;text-transform:uppercase;letter-spacing:.03em}}
tr:last-child td{{border-bottom:none}}.a{{text-align:right;font-variant-numeric:tabular-nums}}</style>
<h1>Reconciliation Dashboard</h1>
<div class=sub>All accounts · generated {datetime.now(EAT):%Y-%m-%d %H:%M} EAT</div>
<div class=cards>
<div class=card><div class=label>Accounts</div><div class=val>{len(rows)}</div></div>
<div class=card><div class=label>Reconciled</div><div class=val>{tot_recon}</div></div>
<div class=card><div class=label>Signed off</div><div class=val>{tot_signed}</div></div>
<div class=card><div class=label>Open exceptions</div><div class=val style="color:{'#b3471f' if tot_exc else '#3a7d44'}">{tot_exc}</div></div>
</div>
<table><tr><th>Account</th><th>Type</th><th>Status</th><th>Period</th><th>Matches</th><th>Exceptions</th><th class=a>Difference</th></tr>
{''.join(row_html(r) for r in rows)}
</table>"""

with open("dashboard.html", "w") as f:
    f.write(html)
print("Wrote dashboard.html — right-click it in the file list → Download → open in your browser.")