import os
import io
import csv
import hashlib
import psycopg2
from decimal import Decimal
from datetime import datetime
from flask import Flask, render_template_string, request, redirect, session, url_for

DB_URL = os.environ["SUPABASE_DB_URL"]
ORG_ID = "00000000-0000-0000-0000-000000000001"
DATE_TOLERANCE_DAYS = 3
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "change-me")
APP_PASSWORD = os.environ.get("APP_PASSWORD", "")

LOGIN_PAGE = """
<!doctype html><meta charset=utf-8><title>Sign in</title>
<style>
  body{font-family:system-ui,sans-serif;background:#f5f5f3;display:flex;align-items:center;justify-content:center;height:100vh;margin:0}
  .box{background:#fff;border:1px solid #e3e2dd;border-radius:12px;padding:32px;width:300px}
  h1{font-size:18px;margin:0 0 16px}
  input{width:100%;padding:10px;border:1px solid #d8d7d2;border-radius:8px;box-sizing:border-box;font-size:14px}
  button{width:100%;margin-top:12px;padding:10px;background:#2b2b29;color:#fff;border:none;border-radius:8px;cursor:pointer;font-size:14px}
  .err{color:#b3471f;font-size:13px;margin-top:10px}
</style>
<div class=box>
  <h1>Reconciliation Tool</h1>
  <form method=post>
    <input type=password name=password placeholder=Password autofocus>
    <button type=submit>Sign in</button>
    {% if error %}<div class=err>{{ error }}</div>{% endif %}
  </form>
</div>
"""

@app.before_request
def require_login():
    if request.endpoint in ("login", "static"):
        return
    if not session.get("authed"):
        return redirect(url_for("login"))

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if APP_PASSWORD and request.form.get("password") == APP_PASSWORD:
            session["authed"] = True
            return redirect(url_for("home"))
        return render_template_string(LOGIN_PAGE, error="Incorrect password")
    return render_template_string(LOGIN_PAGE, error=None)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ---------- helpers ----------
def get_conn():
    return psycopg2.connect(DB_URL)


def get_checking(cur):
    cur.execute(
        "SELECT account_id, name, currency FROM account WHERE type='bank' AND name='Checking' LIMIT 1;"
    )
    return cur.fetchone()


def parse_date(s):
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%m-%d-%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    raise ValueError(f"Unrecognized date: {s}")


def parse_amount(s):
    s = s.strip().replace("$", "").replace(",", "")
    neg = s.startswith("(") and s.endswith(")")
    if neg:
        s = s[1:-1]
    val = Decimal(s)
    return -val if neg else val


def find_key(fieldnames, *candidates):
    lookup = {(fn or "").strip().lower(): fn for fn in fieldnames}
    for c in candidates:
        if c in lookup:
            return lookup[c]
    return None


# ---------- parse an uploaded CSV into a statement ----------
def ingest_csv(text):
    reader = csv.DictReader(io.StringIO(text))
    dk = find_key(reader.fieldnames, "date", "posted date", "transaction date")
    ak = find_key(reader.fieldnames, "amount", "value")
    nk = find_key(reader.fieldnames, "description", "payee", "memo", "narrative")
    if not (dk and ak):
        raise ValueError(
            f"CSV must have Date and Amount columns. Found: {reader.fieldnames}"
        )

    rows = []
    for r in reader:
        if not (r.get(dk) or "").strip():
            continue
        rows.append(
            {
                "date": parse_date(r[dk]),
                "amount": parse_amount(r[ak]),
                "desc": (r.get(nk) or "").strip() if nk else "",
            }
        )
    if not rows:
        raise ValueError("No data rows found in the file.")

    conn = get_conn()
    cur = conn.cursor()
    checking_uuid, _, currency = get_checking(cur)
    p_start = min(r["date"] for r in rows)
    p_end = max(r["date"] for r in rows)
    closing = sum((r["amount"] for r in rows), Decimal(0))

    cur.execute(
        "DELETE FROM statement WHERE account_id=%s AND period_start=%s AND period_end=%s;",
        (checking_uuid, p_start, p_end),
    )
    cur.execute(
        """INSERT INTO statement (org_id, account_id, period_start, period_end,
                                          opening_balance, closing_balance, currency, source_format)
                   VALUES (%s,%s,%s,%s,0,%s,%s,'csv') RETURNING statement_id;""",
        (ORG_ID, checking_uuid, p_start, p_end, closing, currency),
    )
    statement_id = cur.fetchone()[0]
    for r in rows:
        key = hashlib.sha256(
            f"{r['date']}|{r['amount']}|{r['desc'].lower()}".encode()
        ).hexdigest()[:32]
        cur.execute(
            """INSERT INTO statement_line (org_id, statement_id, posted_date, amount,
                                                   currency, description, dedupe_key)
                       VALUES (%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (statement_id, dedupe_key) DO NOTHING;""",
            (ORG_ID, statement_id, r["date"], r["amount"], currency, r["desc"], key),
        )
    conn.commit()
    cur.close()
    conn.close()
    return statement_id


# ---------- two-pass matcher ----------
def run_matcher(statement_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT account_id, period_start, period_end FROM statement WHERE statement_id=%s;",
        (statement_id,),
    )
    checking_uuid, p_start, p_end = cur.fetchone()
    cur.execute("DELETE FROM match WHERE statement_id=%s;", (statement_id,))

    cur.execute(
        """SELECT line_id, posted_date, amount, coalesce(counterparty, description,'')
                   FROM statement_line WHERE statement_id=%s;""",
        (statement_id,),
    )
    lines = cur.fetchall()
    cur.execute(
        """SELECT txn_id, posted_date, amount, coalesce(counterparty, description,'')
                   FROM book_txn WHERE account_id=%s AND posted_date BETWEEN %s AND %s
                     AND is_void=false AND is_deleted=false;""",
        (checking_uuid, p_start, p_end),
    )
    txns = cur.fetchall()

    used, matched_lines = set(), set()

    def write(l_id, t_id, mtype, conf, delta):
        cur.execute(
            """INSERT INTO match (org_id, statement_id, status, match_type, confidence, amount_delta)
                       VALUES (%s,%s,'confirmed',%s,%s,%s) RETURNING match_id;""",
            (ORG_ID, statement_id, mtype, conf, delta),
        )
        m_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO match_statement_line (match_id, line_id) VALUES (%s,%s);",
            (m_id, l_id),
        )
        cur.execute(
            "INSERT INTO match_book_txn (match_id, txn_id) VALUES (%s,%s);",
            (m_id, t_id),
        )

    for l_id, ld, la, lw in lines:
        for t_id, td, ta, tw in txns:
            if t_id in used:
                continue
            if la == ta and abs((ld - td).days) <= DATE_TOLERANCE_DAYS:
                write(l_id, t_id, "exact", 1.0, 0)
                used.add(t_id)
                matched_lines.add(l_id)
                break
    for l_id, ld, la, lw in lines:
        if l_id in matched_lines:
            continue
        for t_id, td, ta, tw in txns:
            if t_id in used:
                continue
            if (
                lw
                and tw
                and lw.strip().lower() == tw.strip().lower()
                and abs((ld - td).days) <= DATE_TOLERANCE_DAYS
            ):
                write(l_id, t_id, "fuzzy", 0.6, la - ta)
                used.add(t_id)
                matched_lines.add(l_id)
                break

    conn.commit()
    cur.close()
    conn.close()


# ---------- routes ----------
@app.route("/upload", methods=["POST"])
def upload():
    f = request.files.get("statement")
    if not f or not f.filename:
        return "No file selected. <a href='/'>Back</a>"
    try:
        text = f.read().decode("utf-8-sig")
        statement_id = ingest_csv(text)
        run_matcher(statement_id)
    except Exception as e:
        return f"Could not process that file: {e} <br><a href='/'>Back</a>"
    return redirect("/")


@app.route("/")
def home():
    conn = get_conn()
    cur = conn.cursor()
    checking_uuid, account_name, _ = get_checking(cur)
    cur.execute(
        """SELECT statement_id, period_start, period_end FROM statement
                   WHERE account_id=%s ORDER BY created_at DESC LIMIT 1;""",
        (checking_uuid,),
    )
    s = cur.fetchone()

    ctx = {"account_name": account_name, "has_results": False}
    if s:
        statement_id, p_start, p_end = s
        cur.execute(
            """SELECT line_id, posted_date, amount, coalesce(counterparty, description,'')
                       FROM statement_line WHERE statement_id=%s ORDER BY posted_date;""",
            (statement_id,),
        )
        lines = cur.fetchall()
        cur.execute(
            """SELECT txn_id, posted_date, amount, coalesce(counterparty, description,'')
                       FROM book_txn WHERE account_id=%s AND posted_date BETWEEN %s AND %s
                         AND is_void=false AND is_deleted=false ORDER BY posted_date;""",
            (checking_uuid, p_start, p_end),
        )
        txns = cur.fetchall()
        cur.execute(
            """SELECT m.match_type, m.amount_delta, sl.posted_date, sl.amount,
                              coalesce(sl.counterparty, sl.description,''), bt.amount
                       FROM match m
                       JOIN match_statement_line msl ON msl.match_id=m.match_id
                       JOIN statement_line sl ON sl.line_id=msl.line_id
                       JOIN match_book_txn mbt ON mbt.match_id=m.match_id
                       JOIN book_txn bt ON bt.txn_id=mbt.txn_id
                       WHERE m.statement_id=%s ORDER BY sl.posted_date;""",
            (statement_id,),
        )
        matched = cur.fetchall()
        cur.execute(
            "SELECT msl.line_id FROM match m JOIN match_statement_line msl ON msl.match_id=m.match_id WHERE m.statement_id=%s;",
            (statement_id,),
        )
        ml = {r[0] for r in cur.fetchall()}
        cur.execute(
            "SELECT mbt.txn_id FROM match m JOIN match_book_txn mbt ON mbt.match_id=m.match_id WHERE m.statement_id=%s;",
            (statement_id,),
        )
        mt = {r[0] for r in cur.fetchall()}
        stmt_total = sum((l[2] for l in lines), Decimal(0))
        books_total = sum((t[2] for t in txns), Decimal(0))
        ctx.update(
            {
                "has_results": True,
                "p_start": p_start,
                "p_end": p_end,
                "n_exact": sum(1 for m in matched if m[0] == "exact"),
                "n_fuzzy": sum(1 for m in matched if m[0] == "fuzzy"),
                "matched": matched,
                "on_stmt": [l for l in lines if l[0] not in ml],
                "in_books": [t for t in txns if t[0] not in mt],
                "diff": stmt_total - books_total,
            }
        )
    cur.close()
    conn.close()
    return render_template_string(TEMPLATE, **ctx)


TEMPLATE = """
<!doctype html><html><head><meta charset="utf-8"><title>Reconciliation</title>
<style>
  body { font-family:-apple-system,system-ui,sans-serif; background:#f5f5f3; color:#2b2b29; margin:0; padding:32px; }
  .wrap { max-width:880px; margin:0 auto; }
  h1 { font-size:22px; margin:0 0 4px; } .sub { color:#73726c; margin-bottom:24px; }
  .upload { background:#fff; border:1px solid #e3e2dd; border-radius:10px; padding:18px; margin-bottom:28px; display:flex; gap:12px; align-items:center; }
  .upload button { background:#2b2b29; color:#fff; border:none; padding:9px 16px; border-radius:8px; cursor:pointer; font-size:14px; }
  .cards { display:flex; gap:12px; margin-bottom:28px; flex-wrap:wrap; }
  .card { flex:1; min-width:140px; background:#fff; border:1px solid #e3e2dd; border-radius:10px; padding:16px; }
  .card .label { font-size:12px; color:#73726c; text-transform:uppercase; letter-spacing:.04em; }
  .card .val { font-size:24px; font-weight:600; margin-top:6px; }
  .diff.ok { color:#3a7d44; } .diff.bad { color:#b3471f; }
  h2 { font-size:15px; margin:28px 0 8px; }
  table { width:100%; border-collapse:collapse; background:#fff; border:1px solid #e3e2dd; border-radius:10px; overflow:hidden; }
  th,td { text-align:left; padding:9px 14px; border-bottom:1px solid #efeeea; font-size:14px; }
  th { background:#faf9f7; color:#73726c; font-weight:600; font-size:12px; text-transform:uppercase; letter-spacing:.03em; }
  tr:last-child td { border-bottom:none; }
  .amt { text-align:right; font-variant-numeric:tabular-nums; }
  .tag { font-size:11px; padding:2px 8px; border-radius:20px; }
  .tag.exact { background:#e7f1e9; color:#3a7d44; } .tag.fuzzy { background:#fbf0dd; color:#9a6a16; }
  .exc th { background:#fcefe9; color:#b3471f; }
</style></head><body><div class="wrap">
  <h1>Reconciliation — {{ account_name }}</h1>
  {% if has_results %}<div class="sub">Statement period {{ p_start }} to {{ p_end }}</div>{% else %}<div class="sub">No statement yet — upload one to begin.</div>{% endif %}

  <form class="upload" action="/upload" method="post" enctype="multipart/form-data">
    <input type="file" name="statement" accept=".csv" required>
    <button type="submit">Upload &amp; reconcile</button>
  </form>

  {% if has_results %}
  <div class="cards">
    <div class="card"><div class="label">Exact matches</div><div class="val">{{ n_exact }}</div></div>
    <div class="card"><div class="label">Discrepancies</div><div class="val">{{ n_fuzzy }}</div></div>
    <div class="card"><div class="label">Exceptions</div><div class="val">{{ on_stmt|length + in_books|length }}</div></div>
    <div class="card"><div class="label">Difference</div><div class="val diff {{ 'ok' if diff == 0 else 'bad' }}">{{ "%.2f"|format(diff) }}</div></div>
  </div>
  <h2>Matched ({{ matched|length }})</h2>
  <table><tr><th>Date</th><th>Payee</th><th></th><th class="amt">Statement</th><th class="amt">Books</th></tr>
  {% for mtype, delta, d, samt, who, bamt in matched %}
    <tr><td>{{ d }}</td><td>{{ who }}</td>
      <td><span class="tag {{ mtype }}">{{ mtype }}{% if delta != 0 %} · off {{ "%.2f"|format(delta) }}{% endif %}</span></td>
      <td class="amt">{{ "%.2f"|format(samt) }}</td><td class="amt">{{ "%.2f"|format(bamt) }}</td></tr>
  {% endfor %}</table>
  <h2>On statement, not in books ({{ on_stmt|length }})</h2>
  <table class="exc"><tr><th>Date</th><th>Description</th><th class="amt">Amount</th></tr>
  {% for _, d, a, who in on_stmt %}<tr><td>{{ d }}</td><td>{{ who }}</td><td class="amt">{{ "%.2f"|format(a) }}</td></tr>{% endfor %}</table>
  <h2>In books, not on statement ({{ in_books|length }})</h2>
  <table class="exc"><tr><th>Date</th><th>Description</th><th class="amt">Amount</th></tr>
  {% for _, d, a, who in in_books %}<tr><td>{{ d }}</td><td>{{ who }}</td><td class="amt">{{ "%.2f"|format(a) }}</td></tr>{% endfor %}</table>
  {% endif %}
</div></body></html>
"""

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)