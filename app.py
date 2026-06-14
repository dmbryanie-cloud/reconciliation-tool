import os
import io
import csv
import hashlib
import itertools
import psycopg2
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from flask import Flask, render_template_string, request, redirect, session, url_for

DB_URL = os.environ["SUPABASE_DB_URL"]
ORG_ID = "00000000-0000-0000-0000-000000000001"
DATE_TOLERANCE_DAYS = 3
GROUP_WINDOW_DAYS = 60
MAX_GROUP = 4
EAT = timezone(timedelta(hours=3))

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "change-me")
APP_PASSWORD = os.environ.get("APP_PASSWORD", "")


def get_conn():
    return psycopg2.connect(DB_URL)


# Make sure sign-off columns exist (runs once at startup)
try:
    _c = get_conn(); _cur = _c.cursor()
    _cur.execute("ALTER TABLE statement ADD COLUMN IF NOT EXISTS signed_off_at timestamptz;")
    _cur.execute("ALTER TABLE statement ADD COLUMN IF NOT EXISTS signed_off_by text;")
    _c.commit(); _cur.close(); _c.close()
except Exception as e:
    print("startup check:", e)


# ---------------- shared styling ----------------
CSS = """<style>
body{font-family:system-ui,-apple-system,sans-serif;background:#f5f5f3;color:#2b2b29;margin:0}
.nav{background:#fff;border-bottom:1px solid #e3e2dd;padding:14px 24px;display:flex;justify-content:space-between;align-items:center}
.nav .brand{font-weight:600}.nav a{color:#73726c;text-decoration:none;font-size:14px;margin-left:16px}
.wrap{max-width:960px;margin:0 auto;padding:28px 24px}
h1{font-size:22px;margin:0 0 4px}.sub{color:#73726c;margin:4px 0 24px;font-size:14px}
.cards{display:flex;gap:12px;margin-bottom:24px;flex-wrap:wrap}
.card{flex:1;min-width:140px;background:#fff;border:1px solid #e3e2dd;border-radius:10px;padding:16px}
.label{font-size:12px;color:#73726c;text-transform:uppercase;letter-spacing:.04em}.val{font-size:24px;font-weight:600;margin-top:6px}
.upload{background:#fff;border:1px solid #e3e2dd;border-radius:10px;padding:16px;margin-bottom:24px}
.upload button{background:#2b2b29;color:#fff;border:none;padding:9px 16px;border-radius:8px;cursor:pointer;font-size:14px}
table{width:100%;border-collapse:collapse;background:#fff;border:1px solid #e3e2dd;border-radius:10px;overflow:hidden}
th,td{text-align:left;padding:11px 14px;border-bottom:1px solid #efeeea;font-size:14px;white-space:nowrap}
th{background:#faf9f7;color:#73726c;font-size:12px;text-transform:uppercase;letter-spacing:.03em}
tr:last-child td{border-bottom:none}.a{text-align:right;font-variant-numeric:tabular-nums}
td a{color:#2b2b29;text-decoration:none}.tag{font-size:11px;padding:2px 8px;border-radius:20px}
.tag.exact{background:#e7f1e9;color:#3a7d44}.tag.fuzzy{background:#fbf0dd;color:#9a6a16}
.pill{font-size:12px;padding:3px 10px;border-radius:20px}
.pill.none{background:#eeede9;color:#73726c}.pill.open{background:#fbf0dd;color:#9a6a16}.pill.signed{background:#e7f1e9;color:#3a7d44}
.exc th{background:#fcefe9;color:#b3471f}
</style>"""

LOGIN_PAGE = """<!doctype html><meta charset=utf-8><title>Sign in</title>
<style>body{font-family:system-ui,sans-serif;background:#f5f5f3;display:flex;align-items:center;justify-content:center;height:100vh;margin:0}
.box{background:#fff;border:1px solid #e3e2dd;border-radius:12px;padding:32px;width:300px}h1{font-size:18px;margin:0 0 16px}
input{width:100%;padding:10px;border:1px solid #d8d7d2;border-radius:8px;box-sizing:border-box;font-size:14px}
button{width:100%;margin-top:12px;padding:10px;background:#2b2b29;color:#fff;border:none;border-radius:8px;cursor:pointer;font-size:14px}
.err{color:#b3471f;font-size:13px;margin-top:10px}</style>
<div class=box><h1>Reconciliation Tool</h1><form method=post>
<input type=password name=password placeholder=Password autofocus><button type=submit>Sign in</button>
{% if error %}<div class=err>{{ error }}</div>{% endif %}</form></div>"""


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
            return redirect(url_for("dashboard"))
        return render_template_string(LOGIN_PAGE, error="Incorrect password")
    return render_template_string(LOGIN_PAGE, error=None)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ---------------- CSV parsing helpers ----------------
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
    if neg: s = s[1:-1]
    v = Decimal(s)
    return -v if neg else v

def find_key(fieldnames, *cands):
    lookup = {(f or "").strip().lower(): f for f in fieldnames}
    for c in cands:
        if c in lookup: return lookup[c]
    return None


def ingest_csv(text, account_name):
    reader = csv.DictReader(io.StringIO(text))
    dk = find_key(reader.fieldnames, "date", "posted date", "transaction date")
    ak = find_key(reader.fieldnames, "amount", "value")
    nk = find_key(reader.fieldnames, "description", "payee", "memo", "narrative")
    if not (dk and ak):
        raise ValueError(f"CSV needs Date and Amount columns. Found: {reader.fieldnames}")
    rows = []
    for r in reader:
        if not (r.get(dk) or "").strip(): continue
        rows.append({"date": parse_date(r[dk]), "amount": parse_amount(r[ak]),
                     "desc": (r.get(nk) or "").strip() if nk else ""})
    if not rows: raise ValueError("No data rows found.")
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id, currency FROM account WHERE name=%s LIMIT 1;", (account_name,))
    arow = cur.fetchone()
    if not arow: raise ValueError(f"Unknown account: {account_name}")
    acct_uuid, currency = arow
    p_start = min(r["date"] for r in rows); p_end = max(r["date"] for r in rows)
    closing = sum((r["amount"] for r in rows), Decimal(0))
    cur.execute("DELETE FROM statement WHERE account_id=%s AND period_start=%s AND period_end=%s;",
                (acct_uuid, p_start, p_end))
    cur.execute("""INSERT INTO statement (org_id, account_id, period_start, period_end,
                   opening_balance, closing_balance, currency, source_format)
                   VALUES (%s,%s,%s,%s,0,%s,%s,'csv') RETURNING statement_id;""",
                (ORG_ID, acct_uuid, p_start, p_end, closing, currency))
    sid = cur.fetchone()[0]
    for r in rows:
        key = hashlib.sha256(f"{r['date']}|{r['amount']}|{r['desc'].lower()}".encode()).hexdigest()[:32]
        cur.execute("""INSERT INTO statement_line (org_id, statement_id, posted_date, amount,
                       currency, description, dedupe_key) VALUES (%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (statement_id, dedupe_key) DO NOTHING;""",
                    (ORG_ID, sid, r["date"], r["amount"], currency, r["desc"], key))
    conn.commit(); cur.close(); conn.close()
    return sid


def run_matcher(statement_id):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id, period_start, period_end FROM statement WHERE statement_id=%s;", (statement_id,))
    acct_uuid, p_start, p_end = cur.fetchone()
    cur.execute("DELETE FROM match WHERE statement_id=%s;", (statement_id,))
    cur.execute("SELECT line_id, posted_date, amount, coalesce(counterparty, description,'') FROM statement_line WHERE statement_id=%s;", (statement_id,))
    lines = cur.fetchall()
    cur.execute("""SELECT txn_id, posted_date, amount, coalesce(counterparty, description,'')
                   FROM book_txn WHERE account_id=%s AND posted_date BETWEEN %s AND %s
                   AND is_void=false AND is_deleted=false;""", (acct_uuid, p_start, p_end))
    txns = cur.fetchall()
    used, matched_lines = set(), set()
    def wm(lids, tids, mt, conf, delta):
        cur.execute("INSERT INTO match (org_id, statement_id, status, match_type, confidence, amount_delta) VALUES (%s,%s,'confirmed',%s,%s,%s) RETURNING match_id;",
                    (ORG_ID, statement_id, mt, conf, delta))
        mid = cur.fetchone()[0]
        for l in lids: cur.execute("INSERT INTO match_statement_line (match_id, line_id) VALUES (%s,%s);", (mid, l))
        for t in tids: cur.execute("INSERT INTO match_book_txn (match_id, txn_id) VALUES (%s,%s);", (mid, t))
    for l_id, ld, la, lw in lines:
        for t_id, td, ta, tw in txns:
            if t_id in used: continue
            if la == ta and abs((ld - td).days) <= DATE_TOLERANCE_DAYS:
                wm([l_id], [t_id], "exact", 1.0, 0); used.add(t_id); matched_lines.add(l_id); break
    for l_id, ld, la, lw in lines:
        if l_id in matched_lines: continue
        for t_id, td, ta, tw in txns:
            if t_id in used: continue
            if lw and tw and lw.strip().lower() == tw.strip().lower() and abs((ld - td).days) <= DATE_TOLERANCE_DAYS:
                wm([l_id], [t_id], "fuzzy", 0.6, la - ta); used.add(t_id); matched_lines.add(l_id); break
    for l_id, ld, la, lw in lines:
        if l_id in matched_lines: continue
        cands = [(t, a) for (t, d, a, w) in txns if t not in used and abs((ld - d).days) <= GROUP_WINDOW_DAYS][:12]
        found = None
        for k in range(2, min(MAX_GROUP, len(cands)) + 1):
            for combo in itertools.combinations(cands, k):
                if sum((c[1] for c in combo), Decimal(0)) == la:
                    found = combo; break
            if found: break
        if found:
            tids = [c[0] for c in found]; wm([l_id], tids, "many_to_one", 0.8, 0)
            matched_lines.add(l_id); used.update(tids)
    conn.commit(); cur.close(); conn.close()


def account_summary(cur, acct_uuid, name, atype):
    cur.execute("SELECT statement_id, period_start, period_end, signed_off_at FROM statement WHERE account_id=%s ORDER BY created_at DESC LIMIT 1;", (acct_uuid,))
    s = cur.fetchone()
    if not s: return {"name": name, "type": atype, "status": "none"}
    sid, ps, pe, signed = s
    cur.execute("SELECT match_type, count(*) FROM match WHERE statement_id=%s AND status<>'rejected' GROUP BY match_type;", (sid,))
    mc = dict(cur.fetchall())
    cur.execute("SELECT msl.line_id FROM match m JOIN match_statement_line msl ON msl.match_id=m.match_id WHERE m.statement_id=%s AND m.status<>'rejected';", (sid,))
    ml = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT mbt.txn_id FROM match m JOIN match_book_txn mbt ON mbt.match_id=m.match_id WHERE m.statement_id=%s AND m.status<>'rejected';", (sid,))
    mt = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT line_id, amount FROM statement_line WHERE statement_id=%s;", (sid,))
    lines = cur.fetchall()
    cur.execute("SELECT txn_id, amount FROM book_txn WHERE account_id=%s AND posted_date BETWEEN %s AND %s AND is_void=false AND is_deleted=false;", (acct_uuid, ps, pe))
    txns = cur.fetchall()
    exc = len([l for l in lines if l[0] not in ml]) + len([t for t in txns if t[0] not in mt])
    diff = sum((l[1] for l in lines), Decimal(0)) - sum((t[1] for t in txns), Decimal(0))
    return {"name": name, "type": atype, "status": "signed" if signed else "open",
            "p_start": ps, "p_end": pe, "exact": mc.get("exact", 0), "fuzzy": mc.get("fuzzy", 0),
            "m2o": mc.get("many_to_one", 0), "exc": exc, "diff": diff}


DASH_TEMPLATE = """<!doctype html><html><head><meta charset=utf-8><title>Dashboard</title>""" + CSS + """</head><body>
<div class=nav><span class=brand>Reconciliation Tool</span><a href="{{ url_for('logout') }}">Sign out</a></div>
<div class=wrap><h1>All accounts</h1><div class=sub>generated {{ now }} EAT</div>
<div class=cards>
<div class=card><div class=label>Accounts</div><div class=val>{{ rows|length }}</div></div>
<div class=card><div class=label>Reconciled</div><div class=val>{{ n_recon }}</div></div>
<div class=card><div class=label>Signed off</div><div class=val>{{ n_signed }}</div></div>
<div class=card><div class=label>Open exceptions</div><div class=val style="color:{{ '#b3471f' if tot_exc else '#3a7d44' }}">{{ tot_exc }}</div></div>
</div>
<table><tr><th>Account</th><th>Type</th><th>Status</th><th>Period</th><th>Matches</th><th>Exceptions</th><th class=a>Difference</th></tr>
{% for r in rows %}<tr>
<td><a href="{{ url_for('detail', name=r.name) }}"><b>{{ r.name }}</b></a></td>
<td>{{ 'bank' if r.type=='bank' else 'credit card' }}</td>
<td>{% if r.status=='none' %}<span class="pill none">Not reconciled</span>{% elif r.status=='signed' %}<span class="pill signed">Signed off</span>{% else %}<span class="pill open">Reconciled · open</span>{% endif %}</td>
{% if r.status=='none' %}<td>—</td><td>—</td><td>—</td><td class=a>—</td>
{% else %}<td>{{ r.p_start }} → {{ r.p_end }}</td>
<td>{{ r.exact }} exact{% if r.fuzzy %}, {{ r.fuzzy }} fuzzy{% endif %}{% if r.m2o %}, {{ r.m2o }} batched{% endif %}</td>
<td>{{ r.exc }}</td>
<td class=a>{% if r.diff==0 %}<span style="color:#3a7d44">0.00</span>{% elif r.exc>0 %}<span style="color:#73726c">{{ "%.2f"|format(r.diff) }} · explained</span>{% else %}<span style="color:#b3471f">{{ "%.2f"|format(r.diff) }} · UNEXPLAINED</span>{% endif %}</td>
{% endif %}</tr>{% endfor %}
</table></div></body></html>"""


@app.route("/")
def dashboard():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id, name, type FROM account ORDER BY type, name;")
    accts = cur.fetchall()
    rows = [account_summary(cur, a, n, t) for a, n, t in accts]
    cur.close(); conn.close()
    n_recon = sum(1 for r in rows if r["status"] != "none")
    n_signed = sum(1 for r in rows if r["status"] == "signed")
    tot_exc = sum(r.get("exc", 0) for r in rows)
    return render_template_string(DASH_TEMPLATE, rows=rows, n_recon=n_recon, n_signed=n_signed,
                                  tot_exc=tot_exc, now=datetime.now(EAT).strftime("%Y-%m-%d %H:%M"))


DETAIL_TEMPLATE = """<!doctype html><html><head><meta charset=utf-8><title>{{ name }}</title>""" + CSS + """</head><body>
<div class=nav><span class=brand>Reconciliation Tool</span><span><a href="{{ url_for('dashboard') }}">← All accounts</a><a href="{{ url_for('logout') }}">Sign out</a></span></div>
<div class=wrap><h1>{{ name }}</h1>
{% if has_results %}<div class=sub>Statement period {{ p_start }} to {{ p_end }}</div>{% else %}<div class=sub>No statement yet — upload one to reconcile.</div>{% endif %}
<form class=upload action="{{ url_for('upload', name=name) }}" method=post enctype=multipart/form-data>
<input type=file name=statement accept=.csv required> <button type=submit>Upload &amp; reconcile</button></form>
{% if has_results %}
<div class=cards>
<div class=card><div class=label>Exact</div><div class=val>{{ n_exact }}</div></div>
<div class=card><div class=label>Discrepancies</div><div class=val>{{ n_fuzzy }}</div></div>
<div class=card><div class=label>Batched</div><div class=val>{{ n_m2o }}</div></div>
<div class=card><div class=label>Exceptions</div><div class=val>{{ on_stmt|length + in_books|length }}</div></div>
<div class=card><div class=label>Difference</div><div class=val style="color:{{ '#3a7d44' if diff==0 else ('#73726c' if (on_stmt|length + in_books|length)>0 else '#b3471f') }}">{{ "%.2f"|format(diff) }}</div></div>
</div>
<h2 style="font-size:15px">Matched ({{ matched|length }}{% if n_m2o %} + {{ n_m2o }} batched{% endif %})</h2>
<table><tr><th>Date</th><th>Payee</th><th></th><th class=a>Statement</th><th class=a>Books</th></tr>
{% for mt, delta, d, samt, who, bamt in matched %}<tr><td>{{ d }}</td><td>{{ who }}</td>
<td><span class="tag {{ mt }}">{{ mt }}{% if delta != 0 %} · off {{ "%.2f"|format(delta) }}{% endif %}</span></td>
<td class=a>{{ "%.2f"|format(samt) }}</td><td class=a>{{ "%.2f"|format(bamt) }}</td></tr>{% endfor %}</table>
<h2 style="font-size:15px">On statement, not in books ({{ on_stmt|length }})</h2>
<table class=exc><tr><th>Date</th><th>Description</th><th class=a>Amount</th></tr>
{% for _, d, a, who in on_stmt %}<tr><td>{{ d }}</td><td>{{ who }}</td><td class=a>{{ "%.2f"|format(a) }}</td></tr>{% endfor %}</table>
<h2 style="font-size:15px">In books, not on statement ({{ in_books|length }})</h2>
<table class=exc><tr><th>Date</th><th>Description</th><th class=a>Amount</th></tr>
{% for _, d, a, who in in_books %}<tr><td>{{ d }}</td><td>{{ who }}</td><td class=a>{{ "%.2f"|format(a) }}</td></tr>{% endfor %}</table>
{% endif %}</div></body></html>"""


def compute_detail(cur, acct_uuid):
    cur.execute("SELECT statement_id, period_start, period_end FROM statement WHERE account_id=%s ORDER BY created_at DESC LIMIT 1;", (acct_uuid,))
    s = cur.fetchone()
    if not s: return {"has_results": False}
    sid, ps, pe = s
    cur.execute("SELECT line_id, posted_date, amount, coalesce(counterparty, description,'') FROM statement_line WHERE statement_id=%s ORDER BY posted_date;", (sid,))
    lines = cur.fetchall()
    cur.execute("SELECT txn_id, posted_date, amount, coalesce(counterparty, description,'') FROM book_txn WHERE account_id=%s AND posted_date BETWEEN %s AND %s AND is_void=false AND is_deleted=false ORDER BY posted_date;", (acct_uuid, ps, pe))
    txns = cur.fetchall()
    cur.execute("""SELECT m.match_type, m.amount_delta, sl.posted_date, sl.amount,
                   coalesce(sl.counterparty, sl.description,''), bt.amount FROM match m
                   JOIN match_statement_line msl ON msl.match_id=m.match_id JOIN statement_line sl ON sl.line_id=msl.line_id
                   JOIN match_book_txn mbt ON mbt.match_id=m.match_id JOIN book_txn bt ON bt.txn_id=mbt.txn_id
                   WHERE m.statement_id=%s AND m.match_type IN ('exact','fuzzy') ORDER BY sl.posted_date;""", (sid,))
    matched = cur.fetchall()
    cur.execute("SELECT count(*) FROM match WHERE statement_id=%s AND match_type='many_to_one' AND status<>'rejected';", (sid,))
    n_m2o = cur.fetchone()[0]
    cur.execute("SELECT msl.line_id FROM match m JOIN match_statement_line msl ON msl.match_id=m.match_id WHERE m.statement_id=%s AND m.status<>'rejected';", (sid,))
    ml = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT mbt.txn_id FROM match m JOIN match_book_txn mbt ON mbt.match_id=m.match_id WHERE m.statement_id=%s AND m.status<>'rejected';", (sid,))
    mt = {r[0] for r in cur.fetchall()}
    st = sum((l[2] for l in lines), Decimal(0)); bt_ = sum((t[2] for t in txns), Decimal(0))
    return {"has_results": True, "p_start": ps, "p_end": pe,
            "n_exact": sum(1 for m in matched if m[0] == "exact"),
            "n_fuzzy": sum(1 for m in matched if m[0] == "fuzzy"), "n_m2o": n_m2o,
            "matched": matched, "on_stmt": [l for l in lines if l[0] not in ml],
            "in_books": [t for t in txns if t[0] not in mt], "diff": st - bt_}


@app.route("/account/<name>")
def detail(name):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id, type FROM account WHERE name=%s LIMIT 1;", (name,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close(); return "Unknown account", 404
    acct_uuid, atype = row
    d = compute_detail(cur, acct_uuid)
    cur.close(); conn.close()
    return render_template_string(DETAIL_TEMPLATE, name=name, atype=atype, **d)


@app.route("/account/<name>/upload", methods=["POST"])
def upload(name):
    f = request.files.get("statement")
    if not f or not f.filename:
        return redirect(url_for("detail", name=name))
    try:
        sid = ingest_csv(f.read().decode("utf-8-sig"), name)
        run_matcher(sid)
    except Exception as e:
        return f"Could not process file: {e} <br><a href='{url_for('detail', name=name)}'>Back</a>"
    return redirect(url_for("detail", name=name))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))