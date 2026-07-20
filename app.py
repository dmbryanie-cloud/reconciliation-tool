import os
import io
import csv
import hashlib
import itertools
import re
import uuid
import psycopg2
from collections import Counter
from difflib import SequenceMatcher
from psycopg2.extras import execute_values
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from flask import Flask, render_template_string, request, redirect, session, url_for, Response
import json, base64, urllib.request, urllib.parse, urllib.error

DB_URL = os.environ["SUPABASE_DB_URL"]
ORG_ID = "00000000-0000-0000-0000-000000000001"
DATE_TOLERANCE_DAYS = 3
GROUP_WINDOW_DAYS = 60
MAX_GROUP = 5         # max items combined in a batch
M2O_MAX_LINES = 1000  # skip combinatorial pass above this many unmatched items
M2O_MAX_CANDS = 10    # candidates considered per item (sorted by date-closeness)
EAT = timezone(timedelta(hours=3))

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "change-me")
APP_PASSWORD = os.environ.get("APP_PASSWORD", "")

QBO_REALM_ID = os.environ.get("QBO_REALM_ID", "")
QBO_BASE = "https://sandbox-quickbooks.api.intuit.com"


def get_conn():
    return psycopg2.connect(DB_URL)


# ---------------- QuickBooks auth (self-healing token) ----------------
def _refresh_with(refresh_token):
    cid = os.environ["QBO_CLIENT_ID"]; secret = os.environ["QBO_CLIENT_SECRET"]
    auth = base64.b64encode(f"{cid}:{secret}".encode()).decode()
    data = urllib.parse.urlencode({"grant_type": "refresh_token", "refresh_token": refresh_token}).encode()
    req = urllib.request.Request("https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer", data=data, method="POST")
    req.add_header("Authorization", f"Basic {auth}")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    req.add_header("Accept", "application/json")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())

def _get_stored_refresh():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS qbo_auth (id int PRIMARY KEY, refresh_token text, updated_at timestamptz DEFAULT now());")
    conn.commit()
    cur.execute("SELECT refresh_token FROM qbo_auth WHERE id=1;")
    row = cur.fetchone()
    cur.close(); conn.close()
    return row[0] if row and row[0] else None

def _store_refresh(token):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""INSERT INTO qbo_auth (id, refresh_token, updated_at) VALUES (1,%s,now())
                   ON CONFLICT (id) DO UPDATE SET refresh_token=EXCLUDED.refresh_token, updated_at=now();""", (token,))
    conn.commit(); cur.close(); conn.close()

def qbo_token():
    candidates = [t for t in (_get_stored_refresh(), os.environ.get("QBO_REFRESH_TOKEN", "")) if t]
    if not candidates:
        raise RuntimeError("No refresh token found in the database or the QBO_REFRESH_TOKEN env var.")
    detail = None
    for rt in candidates:
        try:
            result = _refresh_with(rt)
            _store_refresh(result.get("refresh_token", rt))
            return result["access_token"]
        except urllib.error.HTTPError as e:
            try: body = e.read().decode()
            except Exception: body = ""
            detail = f"HTTP {e.code} {body[:200]}"
            continue
    raise RuntimeError(f"Token refresh rejected by QuickBooks — {detail}")

def qbo_query(entity, token):
    q = f"SELECT * FROM {entity} MAXRESULTS 1000"
    url = f"{QBO_BASE}/v3/company/{QBO_REALM_ID}/query?query=" + urllib.parse.quote(q)
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/json")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read()).get("QueryResponse", {}).get(entity, [])

def _D(x): return Decimal(str(x or 0))

def _h_purchase(e, acct, atype):
    if e.get("AccountRef", {}).get("value") != acct: return None
    amt = _D(e.get("TotalAmt")); is_credit = e.get("Credit", False)
    signed = (-amt if is_credit else amt) if atype == "credit_card" else (amt if is_credit else -amt)
    cat = None
    for ln in e.get("Line", []):
        det = ln.get("AccountBasedExpenseLineDetail")
        if det: cat = det.get("AccountRef", {}).get("name"); break
    return signed, e.get("EntityRef", {}).get("name"), e.get("PrivateNote"), cat

def _h_deposit(e, acct, atype):
    if e.get("DepositToAccountRef", {}).get("value") != acct: return None
    cat = None
    for ln in e.get("Line", []):
        det = ln.get("DepositLineDetail")
        if det: cat = det.get("AccountRef", {}).get("name"); break
    return _D(e.get("TotalAmt")), None, e.get("PrivateNote"), cat

def _h_transfer(e, acct, atype):
    if e.get("ToAccountRef", {}).get("value") == acct:
        return _D(e.get("Amount")), "Transfer in", e.get("PrivateNote"), e.get("FromAccountRef", {}).get("name")
    if e.get("FromAccountRef", {}).get("value") == acct:
        return -_D(e.get("Amount")), "Transfer out", e.get("PrivateNote"), e.get("ToAccountRef", {}).get("name")
    return None

def _h_billpayment(e, acct, atype):
    amt = _D(e.get("TotalAmt"))
    if e.get("CheckPayment", {}).get("BankAccountRef", {}).get("value") == acct:
        return -amt, e.get("VendorRef", {}).get("name"), e.get("PrivateNote"), None
    if e.get("CreditCardPayment", {}).get("CCAccountRef", {}).get("value") == acct:
        return amt, e.get("VendorRef", {}).get("name"), e.get("PrivateNote"), None
    return None

def _h_payment(e, acct, atype):
    if e.get("DepositToAccountRef", {}).get("value") != acct: return None
    return _D(e.get("TotalAmt")), e.get("CustomerRef", {}).get("name"), e.get("PrivateNote"), None

def _h_journalentry(e, acct, atype):
    net, hit, cat = Decimal(0), False, None
    for ln in e.get("Line", []):
        det = ln.get("JournalEntryLineDetail")
        if not det: continue
        if det.get("AccountRef", {}).get("value") == acct:
            amt = _D(ln.get("Amount"))
            want = "Credit" if atype == "credit_card" else "Debit"
            net += amt if det.get("PostingType") == want else -amt
            hit = True
        elif cat is None:
            cat = det.get("AccountRef", {}).get("name")
    return (net, "Journal entry", e.get("PrivateNote"), cat) if hit else None

QBO_HANDLERS = {"Purchase": _h_purchase, "Deposit": _h_deposit, "Transfer": _h_transfer,
                "BillPayment": _h_billpayment, "Payment": _h_payment, "JournalEntry": _h_journalentry}

def sync_from_quickbooks():
    token = qbo_token()
    cache = {}
    for etype in QBO_HANDLERS:
        try:
            cache[etype] = qbo_query(etype, token)
        except urllib.error.HTTPError:
            cache[etype] = []
    conn = get_conn(); cur = conn.cursor()
    cur.execute("ALTER TABLE book_txn ADD COLUMN IF NOT EXISTS category text;")
    conn.commit()
    cur.execute("SELECT account_id, source_account_id, name, type FROM account ORDER BY type, name;")
    accounts = cur.fetchall()
    total = 0
    for acct_uuid, acct_qbo, name, atype in accounts:
        for etype, handler in QBO_HANDLERS.items():
            for e in cache[etype]:
                try:
                    res = handler(e, acct_qbo, atype)
                except Exception:
                    continue
                if not res: continue
                amount, cp, desc, cat = res
                cur.execute("""
                    INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type,
                                          posted_date, amount, currency, description, counterparty,
                                          reference, category, cleared_status, last_modified)
                    VALUES (%(org)s,%(acct)s,%(sid)s,%(stype)s,%(date)s,%(amt)s,%(cur)s,%(desc)s,%(cp)s,%(ref)s,%(cat)s,'unknown',%(lm)s)
                    ON CONFLICT (account_id, source_txn_type, source_txn_id) DO UPDATE SET
                      posted_date=EXCLUDED.posted_date, amount=EXCLUDED.amount, currency=EXCLUDED.currency,
                      description=EXCLUDED.description, counterparty=EXCLUDED.counterparty,
                      reference=EXCLUDED.reference, category=EXCLUDED.category, last_modified=EXCLUDED.last_modified;
                """, {"org": ORG_ID, "acct": acct_uuid, "sid": e.get("Id"), "stype": etype,
                      "date": e.get("TxnDate"), "amt": amount,
                      "cur": e.get("CurrencyRef", {}).get("value", "USD"),
                      "desc": desc, "cp": cp, "ref": e.get("DocNumber"), "cat": cat,
                      "lm": e.get("MetaData", {}).get("LastUpdatedTime")})
                total += 1
    conn.commit(); cur.close(); conn.close()
    return total


# Make sure sign-off columns exist (runs once at startup)
try:
    _c = get_conn(); _cur = _c.cursor()
    _cur.execute("ALTER TABLE statement ADD COLUMN IF NOT EXISTS signed_off_at timestamptz;")
    _cur.execute("ALTER TABLE statement ADD COLUMN IF NOT EXISTS signed_off_by text;")
    _c.commit(); _cur.close(); _c.close()
except Exception as e:
    print("startup check:", e)


# ---------------- learned categorization (memory) ----------------
_STOPWORDS = {"and","the","of","inc","llc","ltd","co","corp","company",
              "services","service","pos","debit","purchase","payment",
              "card","visa","ach","ppd","tst"}

def _normalize(name):
    if not name: return ""
    s = re.sub(r"[^a-z0-9 ]", " ", name.lower())
    return re.sub(r"\s+", " ", s).strip()

def _mtokens(name):
    return [t for t in _normalize(name).split() if len(t) > 1 and not t.isdigit() and t not in _STOPWORDS]

def _tok_match(a, b):
    return a == b or (len(a) >= 3 and len(b) >= 3 and (a.startswith(b) or b.startswith(a)))

def _coverage(known, inn):
    if not known: return 0.0
    return sum(1 for k in known if any(_tok_match(k, i) for i in inn)) / len(known)

def ensure_corrections_table():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS payee_correction (
        id serial PRIMARY KEY, org_id uuid NOT NULL, payee text NOT NULL,
        category text NOT NULL, created_at timestamptz NOT NULL DEFAULT now());""")
    conn.commit(); cur.close(); conn.close()

def record_correction(payee, category):
    ensure_corrections_table()
    conn = get_conn(); cur = conn.cursor()
    cur.execute("INSERT INTO payee_correction (org_id, payee, category) VALUES (%s,%s,%s);", (ORG_ID, payee, category))
    conn.commit(); cur.close(); conn.close()

def build_memory():
    ensure_corrections_table()
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""SELECT lower(trim(counterparty)), category FROM book_txn
                   WHERE category IS NOT NULL AND counterparty IS NOT NULL AND trim(counterparty)<>'';""")
    hist_rows = cur.fetchall()
    cur.execute("SELECT lower(trim(payee)), category FROM payee_correction ORDER BY created_at;")
    corr_rows = cur.fetchall()
    cur.close(); conn.close()
    byp = {}
    for p, c in hist_rows: byp.setdefault(p, Counter())[c] += 1
    history = {}
    for p, counts in byp.items():
        total = sum(counts.values()); bc, bn = counts.most_common(1)[0]
        history[p] = (bc, bn / total, total)
    corrections = {}
    for p, c in corr_rows: corrections[p] = c
    return {"history": history, "corrections": corrections}

def _best_match(keys, payee):
    in_toks, in_norm = _mtokens(payee), _normalize(payee)
    best_key, best = None, 0.0
    for key in keys:
        kt = _mtokens(key)
        if not kt: continue
        score = max(_coverage(kt, in_toks), SequenceMatcher(None, in_norm, _normalize(key)).ratio())
        if score > best: best_key, best = key, score
    return best_key, best

def suggest_category(memory, payee, threshold=0.6):
    if not payee: return None, 0.0, None, 0.0, None
    key = payee.strip().lower()
    corr, hist = memory["corrections"], memory["history"]
    if key in corr: return corr[key], 1.0, key, 1.0, "your correction"
    bk, score = _best_match(corr.keys(), payee)
    if bk and score >= threshold: return corr[bk], 1.0, bk, score, "your correction"
    if key in hist:
        c, conf, _ = hist[key]; return c, conf, key, 1.0, "history"
    bk, score = _best_match(hist.keys(), payee)
    if bk and score >= threshold:
        c, conf, _ = hist[bk]; return c, conf, bk, score, "history"
    return None, 0.0, None, 0.0, None


# ---------------- QuickBooks write-back (expenses + deposits) ----------------
def _expense_accounts(token):
    return [a for a in qbo_query("Account", token) if a.get("AccountType") == "Expense"]

def _resolve_account(category, expense_accts):
    by_name = {a["Name"]: a["Id"] for a in expense_accts}
    by_fqn = {a.get("FullyQualifiedName", a["Name"]): a["Id"] for a in expense_accts}
    default = next((a for a in expense_accts if "office" in a["Name"].lower() or "misc" in a["Name"].lower()),
                   expense_accts[0] if expense_accts else None)
    if category and category in by_fqn: return by_fqn[category], category
    if category and category in by_name: return by_name[category], category
    if category:
        leaf = category.split(":")[-1]
        if leaf in by_name: return by_name[leaf], leaf
    return (default["Id"], default["Name"]) if default else (None, None)

def create_purchase(token, paid_from_qbo, expense_id, amount_abs, txn_date, note):
    body = {"AccountRef": {"value": paid_from_qbo}, "PaymentType": "Cash",
            "TxnDate": txn_date, "PrivateNote": note,
            "Line": [{"DetailType": "AccountBasedExpenseLineDetail", "Amount": amount_abs,
                      "AccountBasedExpenseLineDetail": {"AccountRef": {"value": expense_id}}}]}
    url = f"{QBO_BASE}/v3/company/{QBO_REALM_ID}/purchase"
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())

def _income_accounts(token):
    return [a for a in qbo_query("Account", token) if a.get("AccountType") in ("Income", "Other Income")]

def _resolve_income(category, income_accts):
    by_name = {a["Name"]: a["Id"] for a in income_accts}
    by_fqn = {a.get("FullyQualifiedName", a["Name"]): a["Id"] for a in income_accts}
    default = next((a for a in income_accts if any(k in a["Name"].lower() for k in ("sales", "income", "service", "fee"))),
                   income_accts[0] if income_accts else None)
    if category and category in by_fqn: return by_fqn[category], category
    if category and category in by_name: return by_name[category], category
    if category:
        leaf = category.split(":")[-1]
        if leaf in by_name: return by_name[leaf], leaf
    return (default["Id"], default["Name"]) if default else (None, None)

def create_deposit(token, deposit_to_qbo, income_id, amount_abs, txn_date, note):
    body = {"DepositToAccountRef": {"value": deposit_to_qbo}, "TxnDate": txn_date, "PrivateNote": note,
            "Line": [{"Amount": amount_abs, "DetailType": "DepositLineDetail",
                      "DepositLineDetail": {"AccountRef": {"value": income_id}}}]}
    url = f"{QBO_BASE}/v3/company/{QBO_REALM_ID}/deposit"
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


# ---------------- shared styling ----------------
CSS = """<style>
:root{
  --bg:#eef1f5;--panel:#fff;--ink:#16202e;--muted:#667085;
  --line:#e4e7ec;--line-soft:#eef0f3;
  --accent:#0f766e;--accent-soft:#d6efea;
  --ok:#047857;--ok-soft:#d7f3e3;--warn:#b45309;--warn-soft:#fbedcf;
  --bad:#b42318;--bad-soft:#fbe2de;--none:#667085;--none-soft:#ebedf0;
  --radius:13px;--shadow:0 1px 2px rgba(16,24,40,.05),0 4px 14px rgba(16,24,40,.06);
  --lift:0 8px 20px rgba(16,24,40,.10);
}
*{box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;background:radial-gradient(1100px 520px at 85% -8%,rgba(15,118,110,.07),transparent 60%),linear-gradient(180deg,#eff3f8 0%,#e6ecf3 65%,#e1e8f0 100%);background-attachment:fixed;min-height:100vh;color:var(--ink);margin:0;font-size:15px;line-height:1.5;-webkit-font-smoothing:antialiased}
a{color:inherit;text-decoration:none}
.nav{background:var(--panel);border-bottom:1px solid var(--line);padding:15px 24px;display:flex;justify-content:space-between;align-items:center;position:sticky;top:0;z-index:5}
.nav .brand{font-weight:650;letter-spacing:-.01em;display:flex;align-items:center;gap:9px}
.nav .brand .dot{width:9px;height:9px;border-radius:50%;background:var(--accent);box-shadow:0 0 0 3px var(--accent-soft)}
.nav .links a{color:var(--muted);font-size:14px;margin-left:18px}
.nav .links a:hover{color:var(--ink)}
.wrap{max-width:1000px;margin:0 auto;padding:34px 24px 64px}
h1{font-size:26px;font-weight:680;letter-spacing:-.02em;margin:0 0 5px}
h2{font-size:15px;font-weight:650;letter-spacing:-.01em;color:var(--ink);margin:30px 0 12px;border-radius:7px;padding:2px 6px;margin-left:-6px}
.sub{color:var(--muted);margin:0 0 26px;font-size:14px}
.btn{background:var(--ink);color:#fff;border:none;padding:10px 17px;border-radius:9px;cursor:pointer;font-size:14px;font-weight:550;transition:opacity .15s ease}
.btn:hover{opacity:.9}
.btn-go{background:var(--ok);color:#fff;border:none;padding:9px 16px;border-radius:9px;cursor:pointer;font-size:14px;font-weight:550}
.btn-go:hover{opacity:.92}
.btn-sm{background:var(--panel);border:1px solid var(--line);padding:6px 12px;border-radius:7px;cursor:pointer;font-size:13px;font-weight:500;color:var(--ink);transition:border-color .15s,background .15s}
.btn-sm:hover{border-color:#cdd2da;background:#fafbfc}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(155px,1fr));gap:14px;margin-bottom:8px}
.tile{appearance:none;text-align:left;background:var(--panel);border:1px solid var(--line);border-radius:var(--radius);padding:18px;cursor:pointer;font:inherit;color:inherit;box-shadow:var(--shadow);transition:transform .15s ease,box-shadow .15s ease,border-color .15s ease}
.tile:hover{transform:translateY(-2px);box-shadow:var(--lift)}
.tile:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
.tile .t-label{font-size:11.5px;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;font-weight:600}
.tile .t-val{font-size:30px;font-weight:700;margin-top:9px;letter-spacing:-.02em;font-variant-numeric:tabular-nums}
.tile.active{border-color:var(--accent);box-shadow:inset 0 0 0 1px var(--accent),var(--shadow)}
.tile.active .t-label{color:var(--accent)}
.tile .t-val.warn{color:var(--bad)}
.t-top{display:flex;align-items:center;gap:7px}
.t-ic{color:var(--muted);display:inline-flex}.t-ic svg{width:16px;height:16px;display:block}
.tile.active .t-ic{color:var(--accent)}
.flash{animation:flashbg 1.3s ease}@keyframes flashbg{0%{background:var(--accent-soft)}100%{background:transparent}}
.fbar{font-size:13.5px;color:var(--muted);margin:14px 0 0;display:none}
.fbar a{color:var(--accent);font-weight:600;cursor:pointer}
.cards{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:24px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:var(--radius);padding:16px;box-shadow:var(--shadow)}
.card .label{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.05em;font-weight:600}
.card .val{font-size:23px;font-weight:680;margin-top:7px;font-variant-numeric:tabular-nums;letter-spacing:-.01em}
table{width:100%;border-collapse:separate;border-spacing:0;background:var(--panel);border:1px solid var(--line);border-radius:var(--radius);overflow:hidden;box-shadow:var(--shadow);margin:14px 0 26px}
th,td{text-align:left;padding:13px 16px;border-bottom:1px solid var(--line-soft);font-size:14px;white-space:nowrap}
th{background:#fafbfc;color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.04em;font-weight:600}
tbody tr:last-child td{border-bottom:none}
tbody tr{transition:background .12s ease}
tbody tr:hover{background:#f7f9fb}
.a{text-align:right;font-variant-numeric:tabular-nums}
.pill{font-size:12px;padding:4px 11px;border-radius:999px;font-weight:600;display:inline-flex;align-items:center;gap:6px}
.pill::before{content:'';width:6px;height:6px;border-radius:50%;background:currentColor}
.pill.none{background:var(--none-soft);color:var(--none)}
.pill.open{background:var(--warn-soft);color:var(--warn)}
.pill.signed{background:var(--ok-soft);color:var(--ok)}
.tag{font-size:11px;padding:2px 9px;border-radius:999px;font-weight:600}
.tag.exact{background:var(--ok-soft);color:var(--ok)}
.tag.fuzzy{background:var(--warn-soft);color:var(--warn)}
.upload{background:var(--panel);border:1px solid var(--line);border-radius:var(--radius);padding:18px;margin-bottom:26px;box-shadow:var(--shadow)}
.u-label{font-size:13px;color:var(--muted);margin-bottom:7px;font-weight:550}
.upload input[type=file]{font-size:13px}
.exc th{background:#fdf2ef;color:var(--bad)}
.ok{color:var(--ok)}.warn{color:var(--warn)}.bad{color:var(--bad)}.muted{color:var(--muted)}
@media (max-width:760px){
  .tiles{grid-template-columns:repeat(2,1fr)}
  .cards{grid-template-columns:repeat(2,1fr)}
  .wrap{padding:22px 15px 48px}
  h1{font-size:22px}
  .nav{padding:13px 16px}
}
@media (prefers-reduced-motion:reduce){*{transition:none !important}}
#loadingov{position:fixed;inset:0;background:rgba(238,243,248,.82);display:none;align-items:center;justify-content:center;flex-direction:column;gap:16px;z-index:9999}
#loadingov.on{display:flex}
#loadingov .spin{width:44px;height:44px;border:3px solid var(--line);border-top-color:var(--accent);border-radius:50%;animation:spin .8s linear infinite}
#loadingov .msg{color:var(--muted);font-size:14px;font-weight:600}
@keyframes spin{to{transform:rotate(360deg)}}
</style>"""

LOGIN_PAGE = """<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Sign in · Reconciliation Tool</title>
<style>
*{box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:24px;color:#16202e;
  background:linear-gradient(rgba(255,255,255,.04) 1px,transparent 1px) 0 0/30px 30px,linear-gradient(90deg,rgba(255,255,255,.04) 1px,transparent 1px) 0 0/30px 30px,radial-gradient(900px 480px at 72% 8%,rgba(16,185,129,.18),transparent 58%),linear-gradient(155deg,#16323a 0%,#111d29 52%,#0f3a36 100%)}
.card{background:#fff;border-radius:18px;width:100%;max-width:372px;padding:38px 34px;position:relative;overflow:hidden;
  box-shadow:0 24px 60px rgba(8,15,30,.40),0 2px 10px rgba(8,15,30,.22)}
.card::before{content:'';position:absolute;top:0;left:0;right:0;height:4px;background:linear-gradient(90deg,#0f766e,#10b981)}
.emblem{width:46px;height:46px;border-radius:12px;background:#ecfdf5;color:#0f766e;display:flex;align-items:center;justify-content:center;margin-bottom:16px}.emblem svg{width:25px;height:25px}
.brand{display:flex;align-items:center;gap:10px;font-weight:680;font-size:19px;letter-spacing:-.01em}
.brand .dot{width:11px;height:11px;border-radius:50%;background:#0f766e;box-shadow:0 0 0 4px #d6efea}
.tag{color:#667085;font-size:14px;margin:10px 0 26px;line-height:1.5}
label{display:block;font-size:12px;font-weight:600;color:#475467;margin-bottom:7px;text-transform:uppercase;letter-spacing:.05em}
input{width:100%;padding:12px 13px;border:1px solid #d8dee6;border-radius:10px;font-size:15px;outline:none;transition:border-color .15s,box-shadow .15s}
input:focus{border-color:#0f766e;box-shadow:0 0 0 3px #d6efea}
button{width:100%;margin-top:18px;padding:12px;background:#16202e;color:#fff;border:none;border-radius:10px;cursor:pointer;font-size:15px;font-weight:600;transition:opacity .15s}
button:hover{opacity:.92}
.err{color:#b42318;font-size:13px;margin-top:13px;background:#fbe2de;padding:9px 12px;border-radius:8px}
.foot{text-align:center;color:#98a2b3;font-size:12px;margin-top:24px}
</style></head><body>
<div class=card>
<div class=emblem><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3.5v17"/><path d="M7 6.5h10"/><path d="M7 6.5 4 12.8a3 3 0 0 0 6 0L7 6.5Z"/><path d="M17 6.5l-3 6.3a3 3 0 0 0 6 0L17 6.5Z"/><path d="M8.5 20.5h7"/></svg></div>
<div class=brand><span class=dot></span>Reconciliation Tool</div>
<p class=tag>Match your books to your bank statements, with confidence.</p>
<form method=post>
<label for=pw>Password</label>
<input id=pw type=password name=password placeholder="Enter your password" autofocus>
<button type=submit>Sign in</button>
{% if error %}<div class=err>{{ error }}</div>{% endif %}
</form>
<div class=foot>Private · access by password</div>
</div>
</body></html>"""


@app.template_filter("money")
def _money(x):
    if x is None:
        return ""
    try:
        return f"{Decimal(x):,.2f}"
    except Exception:
        return str(x)


@app.route("/health")
def health():
    # Lightweight touch so a single keep-warm ping keeps BOTH Render and Supabase awake.
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT 1;"); cur.fetchone()
        cur.close(); conn.close()
    except Exception:
        pass  # always report healthy for the uptime monitor, even if the DB is briefly cold
    return "ok", 200


@app.before_request
def require_login():
    if request.endpoint in ("login", "static", "health"):
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


# ---------------- statement parsing (CSV + OFX) ----------------
def _detect_dayfirst(samples):
    # Decide day-first vs month-first for slash/dash dates by scanning the column.
    for s in samples:
        s = (s or "").strip().split()[0]
        m = re.match(r"^(\d{1,2})[/-](\d{1,2})[/-]\d{2,4}$", s)
        if not m:
            continue
        a, b = int(m.group(1)), int(m.group(2))
        if a > 12:
            return True    # first field must be the day
        if b > 12:
            return False   # second field must be the day -> month-first
    return True            # ambiguous -> default day-first

def parse_date(s, dayfirst=True):
    s = (s or "").strip()
    if not s:
        raise ValueError("empty date")
    s = s.split()[0]  # drop any trailing time portion
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    m = re.match(r"^(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})$", s)
    if m:
        a, b, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if y < 100:
            y += 2000
        day, mon = (a, b) if dayfirst else (b, a)
        if mon > 12 and day <= 12:   # safety auto-correct if the guess is impossible
            day, mon = mon, day
        return datetime(y, mon, day).date()
    for fmt in ("%b %d, %Y", "%d %b %Y", "%d-%b-%Y", "%B %d, %Y", "%d %B %Y"):
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

def _ledger_reader(text):
    lines = text.splitlines()
    start = 0
    for i, ln in enumerate(lines):
        low = ln.lower()
        if ("date" in low) and any(w in low for w in
                ("amount", "debit", "credit", "payment", "deposit", "memo", "description", "payee", "name")):
            start = i; break
    return csv.DictReader(io.StringIO("\n".join(lines[start:])))

def _amount_col(fns):
    low = [((f or "").strip().lower(), f) for f in fns]
    for lk, orig in low:
        if lk in ("amount", "value"):
            return orig
    for lk, orig in low:
        if "amount" in lk and not any(x in lk for x in ("credit", "debit", "balance", "running", "foreign", "fx")):
            return orig
    return None

def _sub_col(fns, *cands):
    low = [((f or "").strip().lower(), f) for f in fns]
    for c in cands:
        for lk, orig in low:
            if lk == c:
                return orig
    for c in cands:
        for lk, orig in low:
            if c in lk:
                return orig
    return None

def _parse_ledger(text, want_category=False):
    reader = _ledger_reader(text)
    fns = reader.fieldnames or []
    dk = _sub_col(fns, "transaction date", "posted date", "posting date", "trans date", "date")
    ak = _amount_col(fns)
    dr = _sub_col(fns, "debit", "withdrawal", "payment")
    crk = _sub_col(fns, "credit", "deposit")
    nk = _sub_col(fns, "payee", "description", "name", "memo", "narrative")
    ck = _sub_col(fns, "split", "category") if want_category else None
    if not dk or not (ak or dr or crk):
        raise ValueError(f"Couldn't find a Date column and an Amount (or Debit/Credit, Payment/Deposit) column. Found columns: {fns}")
    raw = list(reader)
    dayfirst = _detect_dayfirst([r.get(dk) or "" for r in raw])
    rows = []
    for r in raw:
        ds = (r.get(dk) or "").strip()
        if not ds:
            continue
        try:
            d = parse_date(ds, dayfirst=dayfirst)
        except ValueError:
            continue
        if ak and (r.get(ak) or "").strip():
            try:
                amount = parse_amount(r[ak])
            except Exception:
                continue
        else:
            try:
                deb = abs(parse_amount(r[dr])) if (dr and (r.get(dr) or "").strip()) else Decimal(0)
                cre = abs(parse_amount(r[crk])) if (crk and (r.get(crk) or "").strip()) else Decimal(0)
            except Exception:
                continue
            amount = cre - deb
        if amount == 0:
            continue
        desc = (r.get(nk) or "").strip() if nk else ""
        row = {"date": d, "amount": amount, "desc": desc}
        if want_category:
            row["category"] = ((r.get(ck) or "").strip() or None) if ck else None
        rows.append(row)
    return rows

def parse_csv(text):
    return _parse_ledger(text, want_category=False)

def parse_ofx(text):
    rows = []
    for part in re.split(r"<STMTTRN>", text, flags=re.IGNORECASE)[1:]:
        block = re.split(r"</STMTTRN>", part, flags=re.IGNORECASE)[0]
        def tag(nm):
            mm = re.search(r"<" + nm + r">([^<\r\n]+)", block, flags=re.IGNORECASE)
            return mm.group(1).strip() if mm else None
        dt = tag("DTPOSTED"); amt = tag("TRNAMT")
        if not dt or amt is None:
            continue
        rows.append({"date": datetime.strptime(dt[:8], "%Y%m%d").date(),
                     "amount": Decimal(amt.replace(",", "")),
                     "desc": tag("NAME") or tag("MEMO") or "",
                     "fitid": tag("FITID")})
    return rows

def _save_statement(rows, account_name, source_format):
    if not rows: raise ValueError("No transactions found in the file.")
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id, currency FROM account WHERE name=%s LIMIT 1;", (account_name,))
    arow = cur.fetchone()
    if not arow:
        cur.close(); conn.close(); raise ValueError(f"Unknown account: {account_name}")
    acct_uuid, currency = arow
    p_start = min(r["date"] for r in rows); p_end = max(r["date"] for r in rows)
    closing = sum((r["amount"] for r in rows), Decimal(0))
    cur.execute("DELETE FROM statement WHERE account_id=%s AND period_start=%s AND period_end=%s;",
                (acct_uuid, p_start, p_end))
    cur.execute("""INSERT INTO statement (org_id, account_id, period_start, period_end,
                   opening_balance, closing_balance, currency, source_format)
                   VALUES (%s,%s,%s,%s,0,%s,%s,%s) RETURNING statement_id;""",
                (ORG_ID, acct_uuid, p_start, p_end, closing, currency, source_format))
    sid = cur.fetchone()[0]
    for r in rows:
        key = r.get("fitid") or hashlib.sha256(f"{r['date']}|{r['amount']}|{(r.get('desc') or '').lower()}".encode()).hexdigest()[:32]
        cur.execute("""INSERT INTO statement_line (org_id, statement_id, posted_date, amount,
                       currency, description, dedupe_key) VALUES (%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (statement_id, dedupe_key) DO NOTHING;""",
                    (ORG_ID, sid, r["date"], r["amount"], currency, r.get("desc") or "", key))
    conn.commit(); cur.close(); conn.close()
    return sid

def ingest_file(text, filename, account_name):
    is_ofx = (filename or "").lower().endswith(".ofx") or "<OFX>" in text[:3000].upper()
    rows = parse_ofx(text) if is_ofx else parse_csv(text)
    sid = _save_statement(rows, account_name, "ofx" if is_ofx else "csv")
    return sid, len(rows)


def parse_books_csv(text):
    return _parse_ledger(text, want_category=True)


def ingest_books(text, account_name):
    rows = parse_books_csv(text)
    if not rows:
        raise ValueError("No transactions found in the books CSV.")
    conn = get_conn(); cur = conn.cursor()
    cur.execute("ALTER TABLE book_txn ADD COLUMN IF NOT EXISTS category text;")
    cur.execute("SELECT account_id, currency FROM account WHERE name=%s LIMIT 1;", (account_name,))
    arow = cur.fetchone()
    if not arow:
        cur.close(); conn.close(); raise ValueError(f"Unknown account: {account_name}")
    acct_uuid, currency = arow
    # Replace any prior CSV-imported books for this account (idempotent); never touches API-synced rows.
    cur.execute("DELETE FROM book_txn WHERE account_id=%s AND source_txn_type='CSV';", (acct_uuid,))
    n = 0
    for r in rows:
        desc = r.get("desc") or ""
        key = hashlib.sha256(f"{r['date']}|{r['amount']}|{desc.lower()}".encode()).hexdigest()[:32]
        cur.execute("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type,
                       posted_date, amount, currency, description, counterparty, category, cleared_status, is_void, is_deleted, last_modified)
                       VALUES (%s,%s,%s,'CSV',%s,%s,%s,%s,%s,%s,'unknown',false,false,now())
                       ON CONFLICT (account_id, source_txn_type, source_txn_id) DO UPDATE SET
                         amount=EXCLUDED.amount, description=EXCLUDED.description,
                         counterparty=EXCLUDED.counterparty, category=EXCLUDED.category, last_modified=now();""",
                    (ORG_ID, acct_uuid, key, r["date"], r["amount"], currency, desc, desc, r.get("category")))
        n += 1
    conn.commit(); cur.close(); conn.close()
    return n


def run_matcher(statement_id):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id, period_start, period_end FROM statement WHERE statement_id=%s;", (statement_id,))
    acct_uuid, p_start, p_end = cur.fetchone()
    cur.execute("DELETE FROM match WHERE statement_id=%s;", (statement_id,))
    cur.execute("SELECT line_id, posted_date, amount, coalesce(counterparty, description,'') FROM statement_line WHERE statement_id=%s;", (statement_id,))
    lines = cur.fetchall()
    cur.execute("""SELECT txn_id, posted_date, amount, coalesce(counterparty, description,'')
                   FROM book_txn WHERE account_id=%s AND posted_date BETWEEN %s AND %s
                   AND coalesce(is_void,false)=false AND coalesce(is_deleted,false)=false;""", (acct_uuid, p_start, p_end))
    txns = cur.fetchall()

    used, matched_lines, matches = set(), set(), []
    def add(lids, tids, mt, conf, delta):
        matches.append((str(uuid.uuid4()), mt, conf, delta, lids, tids))

    # pass 1: exact (amount equal, date within tolerance)
    for l_id, ld, la, lw in lines:
        for t_id, td, ta, tw in txns:
            if t_id in used:
                continue
            if la == ta and abs((ld - td).days) <= DATE_TOLERANCE_DAYS:
                add([l_id], [t_id], "exact", 1.0, 0); used.add(t_id); matched_lines.add(l_id); break

    # pass 2: fuzzy (same payee, amount differs)
    for l_id, ld, la, lw in lines:
        if l_id in matched_lines:
            continue
        for t_id, td, ta, tw in txns:
            if t_id in used:
                continue
            if lw and tw and lw.strip().lower() == tw.strip().lower() and abs((ld - td).days) <= DATE_TOLERANCE_DAYS:
                add([l_id], [t_id], "fuzzy", 0.6, la - ta); used.add(t_id); matched_lines.add(l_id); break

    # pass 3: many-to-one, BOTH directions (bounded; candidates sorted by date-closeness)
    # 3a forward: one statement line = sum of several book txns
    unmatched = [(l_id, ld, la) for (l_id, ld, la, lw) in lines if l_id not in matched_lines]
    if len(unmatched) <= M2O_MAX_LINES:
        for l_id, ld, la in unmatched:
            cands = sorted([(t, a, d) for (t, d, a, w) in txns
                            if t not in used and abs((ld - d).days) <= GROUP_WINDOW_DAYS],
                           key=lambda c: abs((ld - c[2]).days))[:M2O_MAX_CANDS]
            found = None
            for k in range(2, min(MAX_GROUP, len(cands)) + 1):
                for combo in itertools.combinations(cands, k):
                    if sum((c[1] for c in combo), Decimal(0)) == la:
                        found = combo; break
                if found:
                    break
            if found:
                tids = [c[0] for c in found]
                add([l_id], tids, "many_to_one", 0.8, 0)
                matched_lines.add(l_id); used.update(tids)
    # 3b reverse: one book txn = sum of several statement lines (e.g. bank charge + its excise duty)
    unmatched_t = [(t, td, ta) for (t, td, ta, tw) in txns if t not in used]
    if len(unmatched_t) <= M2O_MAX_LINES:
        for t_id, td, ta in unmatched_t:
            cands = sorted([(l, a, d) for (l, d, a, w) in lines
                            if l not in matched_lines and abs((td - d).days) <= GROUP_WINDOW_DAYS],
                           key=lambda c: abs((td - c[2]).days))[:M2O_MAX_CANDS]
            found = None
            for k in range(2, min(MAX_GROUP, len(cands)) + 1):
                for combo in itertools.combinations(cands, k):
                    if sum((c[1] for c in combo), Decimal(0)) == ta:
                        found = combo; break
                if found:
                    break
            if found:
                lids = [c[0] for c in found]
                add(lids, [t_id], "many_to_one", 0.8, 0)
                used.add(t_id)
                for lid in lids:
                    matched_lines.add(lid)

    # pass 4: opposite-sign proposals (reviewable) — e.g. transfers signed the other way in QBO.
    # Stored as 'manual' (an allowed match_type) so the user confirms or rejects each.
    for l_id, ld, la, lw in lines:
        if l_id in matched_lines:
            continue
        for t_id, td, ta, tw in txns:
            if t_id in used:
                continue
            if la == -ta and abs((ld - td).days) <= DATE_TOLERANCE_DAYS:
                add([l_id], [t_id], "manual", 0.5, 0)
                used.add(t_id); matched_lines.add(l_id); break

    # bulk insert (few round-trips instead of hundreds)
    if matches:
        execute_values(cur,
            "INSERT INTO match (match_id, org_id, statement_id, status, match_type, confidence, amount_delta) VALUES %s",
            [(m[0], ORG_ID, statement_id, "confirmed", m[1], m[2], m[3]) for m in matches])
        msl = [(m[0], lid) for m in matches for lid in m[4]]
        if msl:
            execute_values(cur, "INSERT INTO match_statement_line (match_id, line_id) VALUES %s", msl)
        mbt = [(m[0], tid) for m in matches for tid in m[5]]
        if mbt:
            execute_values(cur, "INSERT INTO match_book_txn (match_id, txn_id) VALUES %s", mbt)
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
    cur.execute("SELECT txn_id, amount FROM book_txn WHERE account_id=%s AND posted_date BETWEEN %s AND %s AND coalesce(is_void,false)=false AND coalesce(is_deleted,false)=false;", (acct_uuid, ps, pe))
    txns = cur.fetchall()
    exc = len([l for l in lines if l[0] not in ml]) + len([t for t in txns if t[0] not in mt])
    diff = sum((l[1] for l in lines), Decimal(0)) - sum((t[1] for t in txns), Decimal(0))
    return {"name": name, "type": atype, "status": "signed" if signed else "open",
            "p_start": ps, "p_end": pe, "exact": mc.get("exact", 0), "fuzzy": mc.get("fuzzy", 0),
            "m2o": mc.get("many_to_one", 0), "exc": exc, "diff": diff}


DASH_TEMPLATE = """<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Dashboard · Reconciliation Tool</title>""" + CSS + """</head><body>
<div class=nav><span class=brand><span class=dot></span>Reconciliation Tool</span><span class=links><a href="{{ url_for('logout') }}">Sign out</a></span></div>
<div class=wrap>
<h1>All accounts</h1>
<div class=sub>Updated {{ now }} EAT</div>
<form method=post action="{{ url_for('sync') }}" style="margin-bottom:24px" onsubmit="var b=this.querySelector('button');b.textContent='Syncing\u2026';b.disabled=true;">
<button type=submit class=btn>Sync from QuickBooks</button></form>
{% if sync_msg %}<div class=sub style="color:var(--ok);margin-top:-16px">{{ sync_msg }}</div>{% endif %}
<div class=tiles>
<button class="tile active" data-filter="all"><div class=t-top><span class=t-ic><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3 3 8l9 5 9-5-9-5Z"/><path d="m3 13 9 5 9-5"/></svg></span><span class=t-label>Accounts</span></div><div class=t-val>{{ rows|length }}</div></button>
<button class="tile" data-filter="reconciled"><div class=t-top><span class=t-ic><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="m8.4 12 2.4 2.4L16 9"/></svg></span><span class=t-label>Reconciled</span></div><div class=t-val>{{ n_recon }}</div></button>
<button class="tile" data-filter="signed"><div class=t-top><span class=t-ic><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3 5 6v5.5c0 4 3 6.5 7 7.5 4-1 7-3.5 7-7.5V6l-7-3Z"/><path d="m9 12 2 2 4-4"/></svg></span><span class=t-label>Signed off</span></div><div class=t-val>{{ n_signed }}</div></button>
<button class="tile" data-filter="exceptions"><div class=t-top><span class=t-ic><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M12 4 2.5 20h19L12 4Z"/><path d="M12 10v4.5"/><path d="M12 17.6h.01"/></svg></span><span class=t-label>Open exceptions</span></div><div class="t-val {{ 'warn' if tot_exc else '' }}">{{ tot_exc }}</div></button>
</div>
<div class=fbar id=fbar></div>
<table>
<thead><tr><th>Account</th><th>Type</th><th>Status</th><th>Period</th><th>Matches</th><th>Exceptions</th><th class=a>Difference</th></tr></thead>
<tbody>
{% for r in rows %}<tr data-status="{{ r.status }}" data-exc="{{ r.get('exc',0) }}">
<td><a href="{{ url_for('detail', name=r.name) }}"><b>{{ r.name }}</b></a></td>
<td>{{ 'bank' if r.type=='bank' else 'credit card' }}</td>
<td>{% if r.status=='none' %}<span class="pill none">Not reconciled</span>{% elif r.status=='signed' %}<span class="pill signed">Signed off</span>{% else %}<span class="pill open">Reconciled</span>{% endif %}</td>
{% if r.status=='none' %}<td class=muted>—</td><td class=muted>—</td><td class=muted>—</td><td class="a muted">—</td>
{% else %}<td>{{ r.p_start }} → {{ r.p_end }}</td>
<td>{{ r.exact }} exact{% if r.fuzzy %}, {{ r.fuzzy }} fuzzy{% endif %}{% if r.m2o %}, {{ r.m2o }} batched{% endif %}</td>
<td>{{ r.exc }}</td>
<td class=a>{% if r.diff==0 %}<span class=ok>0.00</span>{% elif r.exc>0 %}<span class=muted>{{ r.diff|money }} · explained</span>{% else %}<span class=bad>{{ r.diff|money }} · unexplained</span>{% endif %}</td>
{% endif %}</tr>{% endfor %}
</tbody></table>
<script>
function flt(f){
  document.querySelectorAll('.tile').forEach(function(t){t.classList.toggle('active', t.getAttribute('data-filter')===f)});
  var total=document.querySelectorAll('tbody tr').length, shown=0;
  document.querySelectorAll('tbody tr').forEach(function(tr){
    var st=tr.getAttribute('data-status'), exc=parseInt(tr.getAttribute('data-exc')||'0',10), show=true;
    if(f==='reconciled') show = st!=='none';
    else if(f==='signed') show = st==='signed';
    else if(f==='exceptions') show = exc>0;
    tr.style.display = show ? '' : 'none'; if(show) shown++;
  });
  var bar=document.getElementById('fbar');
  var labels={reconciled:'reconciled', signed:'signed off', exceptions:'with open exceptions'};
  if(f==='all'){ bar.style.display='none'; }
  else{
    bar.style.display='block';
    bar.textContent='Showing '+shown+' of '+total+' accounts '+labels[f]+'.\u00a0';
    var a=document.createElement('a'); a.textContent='Show all'; a.onclick=function(){flt('all')};
    bar.appendChild(a);
  }
}
document.querySelectorAll('.tile').forEach(function(t){t.addEventListener('click',function(){flt(t.getAttribute('data-filter'))})});
</script>
</div><div id=loadingov><div class=spin></div><div class=msg id=loadingmsg>Loading...</div></div>
<script>(function(){
var ov=document.getElementById('loadingov'),msg=document.getElementById('loadingmsg'),timer;
function show(t){if(msg&&t)msg.textContent=t;if(ov)ov.classList.add('on');}
function schedule(t){clearTimeout(timer);timer=setTimeout(function(){show(t);},180);}
document.addEventListener('click',function(e){
var a=e.target.closest?e.target.closest('a'):null;if(!a)return;
var href=a.getAttribute('href')||'';if(!href)return;
if(a.target==='_blank'||a.hasAttribute('download'))return;
if(href[0]==='#'||href.indexOf('javascript:')===0||href.indexOf('mailto:')===0)return;
if(href.indexOf('.csv')>-1||href.indexOf('/template/')>-1)return;
if(e.metaKey||e.ctrlKey||e.shiftKey||e.altKey)return;
schedule('Loading...');});
document.addEventListener('submit',function(e){
var act=(e.target.getAttribute&&e.target.getAttribute('action'))||'';var t='Working...';
if(act.indexOf('/upload')>-1)t='Reconciling your statement...';
else if(act.indexOf('/import_books')>-1)t='Importing your books...';
else if(act.indexOf('/sync')>-1)t='Syncing from QuickBooks...';
else if(act.indexOf('/clear')>-1)t='Clearing account data...';
else if(act.indexOf('/signoff')>-1)t='Signing off...';
else if(act.indexOf('/reopen')>-1)t='Reopening...';
schedule(t);});
})();</script>
</body></html>"""


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
    sync_msg = session.pop("sync_msg", None)
    return render_template_string(DASH_TEMPLATE, rows=rows, n_recon=n_recon, n_signed=n_signed,
                                  tot_exc=tot_exc, sync_msg=sync_msg, now=datetime.now(EAT).strftime("%Y-%m-%d %H:%M"))


DETAIL_TEMPLATE = """<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>{{ name }} · Reconciliation Tool</title>""" + CSS + """</head><body>
<div class=nav><span class=brand><span class=dot></span>Reconciliation Tool</span><span class=links><a href="{{ url_for('dashboard') }}">← All accounts</a><a href="{{ url_for('logout') }}">Sign out</a></span></div>
<div class=wrap><h1>{{ name }}</h1>
{% if has_results %}<div class=sub>Statement period {{ p_start }} to {{ p_end }}</div>{% else %}<div class=sub>No statement yet — upload one to reconcile.</div>{% endif %}
{% if detail_msg %}<div style="background:var(--accent-soft);color:var(--accent);padding:11px 14px;border-radius:9px;font-size:14px;margin-bottom:18px;font-weight:550">{{ detail_msg }}</div>{% endif %}
<div class=upload>
<form action="{{ url_for('upload', name=name) }}" method=post enctype=multipart/form-data style="margin-bottom:14px">
<div class=u-label>Bank statement (CSV or OFX) · <a href="{{ url_for('template', kind='bank') }}" style="color:var(--accent);font-weight:600">download template</a></div>
<input type=file name=statement accept=.csv,.ofx required> <button type=submit class=btn>Upload &amp; reconcile</button></form>
<form action="{{ url_for('import_books', name=name) }}" method=post enctype=multipart/form-data>
<div class=u-label>Books from QuickBooks (CSV export) · <a href="{{ url_for('template', kind='books') }}" style="color:var(--accent);font-weight:600">download template</a></div>
<input type=file name=books accept=.csv required> <button type=submit class=btn>Import books</button></form>
<div style="margin-top:15px;border-top:1px solid var(--line-soft);padding-top:13px;display:flex;align-items:center;gap:10px;flex-wrap:wrap">
<form method=post action="{{ url_for('clear_account', name=name) }}" onsubmit="return confirm('Clear ALL statements and book transactions for this account? This removes old synced or imported data so you can start fresh offline. This cannot be undone.');" style="display:inline">
<button type=submit class=btn-sm style="color:var(--bad);border-color:var(--bad-soft)">Clear this account&#39;s data</button></form>
<span style="color:var(--muted);font-size:12px">Removes old synced/imported data for a clean offline slate</span>
</div>
</div>
{% if has_results %}
<div class=tiles id=dtiles>
<button class="tile" data-target="sec-matched"><div class=t-top><span class=t-ic><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="m5 12.5 4.5 4.5L19 7"/></svg></span><span class=t-label>Exact</span></div><div class=t-val>{{ n_exact }}</div></button>
<button class="tile" data-target="sec-review" data-fallback="sec-matched"><div class=t-top><span class=t-ic><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M5 9.5h14"/><path d="M5 14.5h14"/><path d="M16 4 8 20"/></svg></span><span class=t-label>Discrepancies</span></div><div class=t-val>{{ n_fuzzy }}</div></button>
<button class="tile" data-target="sec-review" data-fallback="sec-matched"><div class=t-top><span class=t-ic><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3.5" width="8" height="8" rx="1.5"/><rect x="13" y="12.5" width="8" height="8" rx="1.5"/><path d="M13 7.5h3a2 2 0 0 1 2 2v3"/></svg></span><span class=t-label>Batched</span></div><div class=t-val>{{ n_m2o }}</div></button>
<button class="tile" data-target="sec-exceptions"><div class=t-top><span class=t-ic><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M12 4 2.5 20h19L12 4Z"/><path d="M12 10v4.5"/><path d="M12 17.6h.01"/></svg></span><span class=t-label>Exceptions</span></div><div class=t-val>{{ writebacks|length + deposits|length + on_stmt|length + in_books|length }}</div></button>
<button class="tile" data-target="sec-exceptions"><div class=t-top><span class=t-ic><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3.5v17"/><path d="M7 6.5h10"/><path d="M7 6.5 4 12.8a3 3 0 0 0 6 0L7 6.5Z"/><path d="M17 6.5l-3 6.3a3 3 0 0 0 6 0L17 6.5Z"/><path d="M8.5 20.5h7"/></svg></span><span class=t-label>Difference</span></div><div class=t-val style="color:{{ '#047857' if diff==0 else ('#667085' if (writebacks|length + deposits|length + on_stmt|length + in_books|length)>0 else '#b42318') }}">{{ diff|money }}</div></button>
</div>
<div style="margin-bottom:24px">
{% if signed_off %}<span class="pill signed">Signed off {{ signed_off }}</span>
<form method=post action="{{ url_for('reopen', name=name) }}" style="display:inline;margin-left:8px" onsubmit="return confirm('Reopen this reconciliation? You can sign it off again afterward.');"><button type=submit class=btn-sm>Undo sign-off</button></form>
{% else %}<form method=post action="{{ url_for('signoff', name=name) }}" style="display:inline"><button type=submit class=btn-go>Sign off this reconciliation</button></form>{% endif %}
<a href="{{ url_for('exceptions_csv', name=name) }}" class=btn-sm style="display:inline-block;text-decoration:none;margin-left:8px">Download exceptions (CSV)</a>
<a href="{{ url_for('qbo_import_csv', name=name) }}" class=btn-sm style="display:inline-block;text-decoration:none;margin-left:8px">Download for QuickBooks (CSV)</a>
</div>
{% if reviewable %}
<h2 id=sec-review style="font-size:15px">Needs review ({{ reviewable|length }})</h2>
{% if n_signflip %}<div class=sub style="margin:-4px 0 12px">{{ n_signflip }} opposite-sign proposal{{ '' if n_signflip==1 else 's' }} below (same amount, flipped sign) — confirm the real matches, reject the rest.</div>{% endif %}
<table><tr><th>Type</th><th>Statement side</th><th>Books side</th><th>Status</th><th></th></tr>
{% for r in reviewable %}<tr>
<td><span class="tag {{ 'fuzzy' if r.type in ('fuzzy','manual') else 'exact' }}">{{ 'discrepancy' if r.type=='fuzzy' else ('opposite sign' if r.type=='manual' else 'batched') }}</span></td>
<td>{% for d,a,w in r.sls %}{{ d }} · {{ a|money }} · {{ w }}{% endfor %}{% if r.delta and r.delta != 0 %}<br><span style="color:#9a6a16">off {{ r.delta|money }}</span>{% endif %}</td>
<td>{% for d,a,w in r.bts %}{{ d }} · {{ a|money }} · {{ w }}<br>{% endfor %}</td>
<td>{% if r.status=='rejected' %}<span style="color:#b3471f">rejected</span>{% else %}<span style="color:#3a7d44">confirmed</span>{% endif %}</td>
<td><form method=post action="{{ url_for('review_match', name=name, match_id=r.id) }}">
{% if r.status=='rejected' %}<input type=hidden name=status value=confirmed><button type=submit class=btn-sm>Restore</button>
{% else %}<input type=hidden name=status value=rejected><button type=submit class=btn-sm>Reject</button>{% endif %}
</form></td>
</tr>{% endfor %}</table>
{% endif %}
<h2 id=sec-matched style="font-size:15px">Matched ({{ matched|length }}{% if n_m2o %} + {{ n_m2o }} batched{% endif %})</h2>
<table><tr><th>Date</th><th>Payee</th><th></th><th class=a>Statement</th><th class=a>Books</th></tr>
{% for mt, delta, d, samt, who, bamt in matched %}<tr><td>{{ d }}</td><td>{{ who }}</td>
<td><span class="tag {{ mt }}">{{ mt }}{% if delta and delta != 0 %} · off {{ delta|money }}{% endif %}</span></td>
<td class=a>{{ samt|money }}</td><td class=a>{{ bamt|money }}</td></tr>{% endfor %}</table>
{% if writebacks %}
<h2 style="font-size:15px">Unrecorded expenses — add to QuickBooks ({{ writebacks|length }})</h2>
<table><tr><th>Date</th><th>Payee</th><th class=a>Amount</th><th>Record in QuickBooks</th></tr>
{% for w in writebacks %}<tr>
<td>{{ w.date }}</td>
<td>{{ w.who }}{% if w.matched and w.matched != w.who.strip().lower() %}<br><span style="color:#73726c;font-size:12px">recognized '{{ w.matched }}' {{ "%.0f"|format(w.score*100) }}%</span>{% endif %}</td>
<td class=a>{{ w.amount|money }}</td>
<td><form method=post action="{{ url_for('writeback', name=name, line_id=w.line_id) }}" onsubmit="var b=this.querySelector('button');b.textContent='Creating…';b.disabled=true;" style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">
<input type=text name=category value="{{ w.cat or '' }}" placeholder="category" style="padding:6px 8px;border:1px solid #d8d7d2;border-radius:6px;font-size:13px;width:180px">
<button type=submit class=btn-sm>Create</button>
{% if w.source %}<span style="color:#73726c;font-size:12px">via {{ w.source }}{% if w.conf %} · {{ "%.0f"|format(w.conf*100) }}%{% endif %}</span>{% endif %}
</form></td>
</tr>{% endfor %}</table>
{% endif %}
{% if deposits %}
<h2 style="font-size:15px">Unrecorded deposits — add to QuickBooks ({{ deposits|length }})</h2>
<table><tr><th>Date</th><th>Source</th><th class=a>Amount</th><th>Record in QuickBooks</th></tr>
{% for w in deposits %}<tr>
<td>{{ w.date }}</td><td>{{ w.who }}</td><td class=a>{{ w.amount|money }}</td>
<td><form method=post action="{{ url_for('deposit_writeback', name=name, line_id=w.line_id) }}" onsubmit="var b=this.querySelector('button');b.textContent='Creating…';b.disabled=true;" style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">
<input type=text name=category placeholder="income account" style="padding:6px 8px;border:1px solid #d8d7d2;border-radius:6px;font-size:13px;width:180px">
<button type=submit class=btn-sm>Create</button>
</form></td>
</tr>{% endfor %}</table>
{% endif %}
<h2 id=sec-exceptions style="font-size:15px">On statement, not in books ({{ on_stmt|length }})</h2>
<table class=exc><tr><th>Date</th><th>Description</th><th class=a>Amount</th></tr>
{% for _, d, a, who in on_stmt %}<tr><td>{{ d }}</td><td>{{ who }}</td><td class=a>{{ a|money }}</td></tr>{% endfor %}</table>
<h2 style="font-size:15px">In books, not on statement ({{ in_books|length }})</h2>
<table class=exc><tr><th>Date</th><th>Description</th><th class=a>Amount</th></tr>
{% for _, d, a, who in in_books %}<tr><td>{{ d }}</td><td>{{ who }}</td><td class=a>{{ a|money }}</td></tr>{% endfor %}</table>
{% endif %}
<script>(function(){function go(btn){document.querySelectorAll('#dtiles .tile').forEach(function(t){t.classList.toggle('active',t===btn)});var el=document.getElementById(btn.getAttribute('data-target'));if(!el){var fb=btn.getAttribute('data-fallback'); if(fb) el=document.getElementById(fb);}if(el){el.scrollIntoView({behavior:'smooth',block:'start'}); el.classList.remove('flash'); void el.offsetWidth; el.classList.add('flash');}}document.querySelectorAll('#dtiles .tile').forEach(function(t){t.addEventListener('click',function(){go(t)})});})();</script>
</div><div id=loadingov><div class=spin></div><div class=msg id=loadingmsg>Loading...</div></div>
<script>(function(){
var ov=document.getElementById('loadingov'),msg=document.getElementById('loadingmsg'),timer;
function show(t){if(msg&&t)msg.textContent=t;if(ov)ov.classList.add('on');}
function schedule(t){clearTimeout(timer);timer=setTimeout(function(){show(t);},180);}
document.addEventListener('click',function(e){
var a=e.target.closest?e.target.closest('a'):null;if(!a)return;
var href=a.getAttribute('href')||'';if(!href)return;
if(a.target==='_blank'||a.hasAttribute('download'))return;
if(href[0]==='#'||href.indexOf('javascript:')===0||href.indexOf('mailto:')===0)return;
if(href.indexOf('.csv')>-1||href.indexOf('/template/')>-1)return;
if(e.metaKey||e.ctrlKey||e.shiftKey||e.altKey)return;
schedule('Loading...');});
document.addEventListener('submit',function(e){
var act=(e.target.getAttribute&&e.target.getAttribute('action'))||'';var t='Working...';
if(act.indexOf('/upload')>-1)t='Reconciling your statement...';
else if(act.indexOf('/import_books')>-1)t='Importing your books...';
else if(act.indexOf('/sync')>-1)t='Syncing from QuickBooks...';
else if(act.indexOf('/clear')>-1)t='Clearing account data...';
else if(act.indexOf('/signoff')>-1)t='Signing off...';
else if(act.indexOf('/reopen')>-1)t='Reopening...';
schedule(t);});
})();</script>
</body></html>"""


def compute_detail(cur, acct_uuid, atype="bank"):
    cur.execute("SELECT statement_id, period_start, period_end, signed_off_at FROM statement WHERE account_id=%s ORDER BY created_at DESC LIMIT 1;", (acct_uuid,))
    s = cur.fetchone()
    if not s: return {"has_results": False}
    sid, ps, pe, signed = s
    cur.execute("SELECT line_id, posted_date, amount, coalesce(counterparty, description,'') FROM statement_line WHERE statement_id=%s ORDER BY posted_date;", (sid,))
    lines = cur.fetchall()
    cur.execute("SELECT txn_id, posted_date, amount, coalesce(counterparty, description,'') FROM book_txn WHERE account_id=%s AND posted_date BETWEEN %s AND %s AND coalesce(is_void,false)=false AND coalesce(is_deleted,false)=false ORDER BY posted_date;", (acct_uuid, ps, pe))
    txns = cur.fetchall()
    cur.execute("""SELECT m.match_type, m.amount_delta, sl.posted_date, sl.amount,
                   coalesce(sl.counterparty, sl.description,''), bt.amount FROM match m
                   JOIN match_statement_line msl ON msl.match_id=m.match_id JOIN statement_line sl ON sl.line_id=msl.line_id
                   JOIN match_book_txn mbt ON mbt.match_id=m.match_id JOIN book_txn bt ON bt.txn_id=mbt.txn_id
                   WHERE m.statement_id=%s AND m.match_type IN ('exact','fuzzy') AND m.status<>'rejected' ORDER BY sl.posted_date;""", (sid,))
    matched = cur.fetchall()
    cur.execute("SELECT count(*) FROM match WHERE statement_id=%s AND match_type='many_to_one' AND status<>'rejected';", (sid,))
    n_m2o = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM match WHERE statement_id=%s AND match_type='manual' AND status<>'rejected';", (sid,))
    n_signflip = cur.fetchone()[0]
    reviewable = []
    cur.execute("SELECT match_id, match_type, status, amount_delta FROM match WHERE statement_id=%s AND match_type IN ('fuzzy','many_to_one','manual') ORDER BY match_type;", (sid,))
    for mid, mtype, status, delta in cur.fetchall():
        cur.execute("SELECT sl.posted_date, sl.amount, coalesce(sl.counterparty, sl.description,'') FROM match_statement_line msl JOIN statement_line sl ON sl.line_id=msl.line_id WHERE msl.match_id=%s;", (mid,))
        sls = cur.fetchall()
        cur.execute("SELECT bt.posted_date, bt.amount, coalesce(bt.counterparty, bt.description,'') FROM match_book_txn mbt JOIN book_txn bt ON bt.txn_id=mbt.txn_id WHERE mbt.match_id=%s;", (mid,))
        bts = cur.fetchall()
        reviewable.append({"id": mid, "type": mtype, "status": status, "delta": delta, "sls": sls, "bts": bts})
    cur.execute("SELECT msl.line_id FROM match m JOIN match_statement_line msl ON msl.match_id=m.match_id WHERE m.statement_id=%s AND m.status<>'rejected';", (sid,))
    ml = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT mbt.txn_id FROM match m JOIN match_book_txn mbt ON mbt.match_id=m.match_id WHERE m.statement_id=%s AND m.status<>'rejected';", (sid,))
    mt = {r[0] for r in cur.fetchall()}
    st = sum((l[2] for l in lines), Decimal(0)); bt_ = sum((t[2] for t in txns), Decimal(0))
    mem = build_memory()
    writebacks, deposits, on_stmt_in = [], [], []
    for (lid, dd, a, who) in [l for l in lines if l[0] not in ml]:
        if a < 0:
            cat, conf, matched_p, score, source = suggest_category(mem, who)
            writebacks.append({"line_id": lid, "date": dd, "amount": a, "who": who,
                               "cat": cat, "conf": conf, "matched": matched_p, "score": score, "source": source})
        elif a > 0 and atype == "bank":
            deposits.append({"line_id": lid, "date": dd, "amount": a, "who": who})
        else:
            on_stmt_in.append((lid, dd, a, who))
    return {"has_results": True, "p_start": ps, "p_end": pe,
            "signed_off": signed.strftime("%Y-%m-%d") if signed else None,
            "n_exact": sum(1 for m in matched if m[0] == "exact"),
            "n_fuzzy": sum(1 for m in matched if m[0] == "fuzzy"), "n_m2o": n_m2o, "n_signflip": n_signflip,
            "matched": matched, "reviewable": reviewable, "writebacks": writebacks, "deposits": deposits,
            "on_stmt": on_stmt_in,
            "in_books": [t for t in txns if t[0] not in mt], "diff": st - bt_}


@app.route("/account/<name>")
def detail(name):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id, type FROM account WHERE name=%s LIMIT 1;", (name,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close(); return "Unknown account", 404
    acct_uuid, atype = row
    d = compute_detail(cur, acct_uuid, atype)
    cur.close(); conn.close()
    return render_template_string(DETAIL_TEMPLATE, name=name, atype=atype,
                                  detail_msg=session.pop("detail_msg", None), **d)


@app.route("/account/<name>/upload", methods=["POST"])
def upload(name):
    f = request.files.get("statement")
    if not f or not f.filename:
        return redirect(url_for("detail", name=name))
    try:
        sid, n = ingest_file(f.read().decode("utf-8-sig", errors="ignore"), f.filename, name)
        run_matcher(sid)
        session["detail_msg"] = f"Loaded {n} statement lines and reconciled."
    except Exception as e:
        return f"Could not process file: {e} <br><a href='{url_for('detail', name=name)}'>Back</a>"
    return redirect(url_for("detail", name=name))


@app.route("/account/<name>/import_books", methods=["POST"])
def import_books(name):
    f = request.files.get("books")
    if not f or not f.filename:
        return redirect(url_for("detail", name=name))
    try:
        n = ingest_books(f.read().decode("utf-8-sig", errors="ignore"), name)
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT account_id FROM account WHERE name=%s LIMIT 1;", (name,))
        arow = cur.fetchone()
        sid = None
        if arow:
            cur.execute("SELECT statement_id FROM statement WHERE account_id=%s ORDER BY created_at DESC LIMIT 1;", (arow[0],))
            srow = cur.fetchone(); sid = srow[0] if srow else None
        cur.close(); conn.close()
        if sid:
            run_matcher(sid)
            c2 = get_conn(); cu2 = c2.cursor()
            cu2.execute("UPDATE statement SET signed_off_at=NULL, signed_off_by=NULL WHERE statement_id=%s;", (sid,))
            c2.commit(); cu2.close(); c2.close()
        session["detail_msg"] = f"Imported {n} book transactions."
    except Exception as e:
        return f"Could not import books: {e} <br><a href='{url_for('detail', name=name)}'>Back</a>"
    return redirect(url_for("detail", name=name))


@app.route("/account/<name>/review/<match_id>", methods=["POST"])
def review_match(name, match_id):
    new_status = request.form.get("status")
    if new_status in ("confirmed", "rejected"):
        conn = get_conn(); cur = conn.cursor()
        cur.execute("UPDATE match SET status=%s WHERE match_id=%s;", (new_status, match_id))
        cur.execute("UPDATE statement SET signed_off_at=NULL, signed_off_by=NULL WHERE statement_id=(SELECT statement_id FROM match WHERE match_id=%s);", (match_id,))
        conn.commit(); cur.close(); conn.close()
    return redirect(url_for("detail", name=name))


@app.route("/account/<name>/signoff", methods=["POST"])
def signoff(name):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id FROM account WHERE name=%s LIMIT 1;", (name,))
    row = cur.fetchone()
    if row:
        cur.execute("SELECT statement_id FROM statement WHERE account_id=%s ORDER BY created_at DESC LIMIT 1;", (row[0],))
        srow = cur.fetchone()
        if srow:
            cur.execute("UPDATE statement SET signed_off_at=now(), signed_off_by='you' WHERE statement_id=%s;", (srow[0],))
            conn.commit()
    cur.close(); conn.close()
    return redirect(url_for("detail", name=name))


@app.route("/account/<name>/reopen", methods=["POST"])
def reopen(name):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id FROM account WHERE name=%s LIMIT 1;", (name,))
    row = cur.fetchone()
    if row:
        cur.execute("SELECT statement_id FROM statement WHERE account_id=%s ORDER BY created_at DESC LIMIT 1;", (row[0],))
        srow = cur.fetchone()
        if srow:
            cur.execute("UPDATE statement SET signed_off_at=NULL, signed_off_by=NULL WHERE statement_id=%s;", (srow[0],))
            conn.commit()
    cur.close(); conn.close()
    return redirect(url_for("detail", name=name))


@app.route("/account/<name>/writeback/<line_id>", methods=["POST"])
def writeback(name, line_id):
    chosen = (request.form.get("category") or "").strip()
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id, source_account_id FROM account WHERE name=%s LIMIT 1;", (name,))
    arow = cur.fetchone()
    if not arow:
        cur.close(); conn.close(); return "Unknown account", 404
    acct_uuid, acct_qbo = arow
    cur.execute("SELECT posted_date, amount, coalesce(counterparty, description,'') FROM statement_line WHERE line_id=%s;", (line_id,))
    lrow = cur.fetchone()
    cur.close(); conn.close()
    if not lrow:
        return redirect(url_for("detail", name=name))
    d, amount, who = lrow
    try:
        token = qbo_token()
        expense_accts = _expense_accounts(token)
        acct_id, label = _resolve_account(chosen, expense_accts)
        if not acct_id:
            return f"No expense account found to categorize under. <br><a href='{url_for('detail', name=name)}'>Back</a>"
        result = create_purchase(token, acct_qbo, acct_id, float(abs(amount)), str(d), f"Reconciliation write-back: {who}")
        new_id = (result.get("Purchase") or {}).get("Id")
        sug = suggest_category(build_memory(), who)[0]
        if chosen and chosen != (sug or ""):
            record_correction(who, label)
        conn = get_conn(); cur = conn.cursor()
        if new_id:
            cur.execute("""
                INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type,
                                      posted_date, amount, currency, description, counterparty,
                                      reference, category, cleared_status, last_modified)
                VALUES (%s,%s,%s,'Purchase',%s,%s,'USD',%s,%s,NULL,%s,'unknown',now())
                ON CONFLICT (account_id, source_txn_type, source_txn_id) DO UPDATE SET
                  amount=EXCLUDED.amount, category=EXCLUDED.category, last_modified=EXCLUDED.last_modified;
            """, (ORG_ID, acct_uuid, new_id, d, -abs(amount), who, who, label))
        cur.execute("SELECT statement_id FROM statement WHERE account_id=%s ORDER BY created_at DESC LIMIT 1;", (acct_uuid,))
        srow = cur.fetchone(); conn.commit(); cur.close(); conn.close()
        if srow:
            run_matcher(srow[0])
            c2 = get_conn(); cu2 = c2.cursor()
            cu2.execute("UPDATE statement SET signed_off_at=NULL, signed_off_by=NULL WHERE statement_id=%s;", (srow[0],))
            c2.commit(); cu2.close(); c2.close()
    except urllib.error.HTTPError as e:
        try: body = e.read().decode()[:300]
        except Exception: body = ""
        return f"QuickBooks rejected it (HTTP {e.code}): {body} <br><a href='{url_for('detail', name=name)}'>Back</a>"
    except Exception as e:
        return f"Write-back failed: {e} <br><a href='{url_for('detail', name=name)}'>Back</a>"
    return redirect(url_for("detail", name=name))


@app.route("/account/<name>/deposit/<line_id>", methods=["POST"])
def deposit_writeback(name, line_id):
    chosen = (request.form.get("category") or "").strip()
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id, source_account_id, type FROM account WHERE name=%s LIMIT 1;", (name,))
    arow = cur.fetchone()
    if not arow:
        cur.close(); conn.close(); return "Unknown account", 404
    acct_uuid, acct_qbo, atype = arow
    if atype != "bank":
        cur.close(); conn.close(); return redirect(url_for("detail", name=name))
    cur.execute("SELECT posted_date, amount, coalesce(counterparty, description,'') FROM statement_line WHERE line_id=%s;", (line_id,))
    lrow = cur.fetchone()
    cur.close(); conn.close()
    if not lrow:
        return redirect(url_for("detail", name=name))
    d, amount, who = lrow
    try:
        token = qbo_token()
        income_accts = _income_accounts(token)
        inc_id, label = _resolve_income(chosen, income_accts)
        if not inc_id:
            return f"No income account found to record the deposit against. <br><a href='{url_for('detail', name=name)}'>Back</a>"
        result = create_deposit(token, acct_qbo, inc_id, float(abs(amount)), str(d), f"Reconciliation deposit: {who}")
        new_id = (result.get("Deposit") or {}).get("Id")
        conn = get_conn(); cur = conn.cursor()
        if new_id:
            cur.execute("""
                INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type,
                                      posted_date, amount, currency, description, counterparty,
                                      reference, category, cleared_status, last_modified)
                VALUES (%s,%s,%s,'Deposit',%s,%s,'USD',%s,%s,NULL,%s,'unknown',now())
                ON CONFLICT (account_id, source_txn_type, source_txn_id) DO UPDATE SET
                  amount=EXCLUDED.amount, category=EXCLUDED.category, last_modified=EXCLUDED.last_modified;
            """, (ORG_ID, acct_uuid, new_id, d, abs(amount), who or "Deposit", who, label))
        cur.execute("SELECT statement_id FROM statement WHERE account_id=%s ORDER BY created_at DESC LIMIT 1;", (acct_uuid,))
        srow = cur.fetchone(); conn.commit(); cur.close(); conn.close()
        if srow:
            run_matcher(srow[0])
            c2 = get_conn(); cu2 = c2.cursor()
            cu2.execute("UPDATE statement SET signed_off_at=NULL, signed_off_by=NULL WHERE statement_id=%s;", (srow[0],))
            c2.commit(); cu2.close(); c2.close()
    except urllib.error.HTTPError as e:
        try: body = e.read().decode()[:300]
        except Exception: body = ""
        return f"QuickBooks rejected it (HTTP {e.code}): {body} <br><a href='{url_for('detail', name=name)}'>Back</a>"
    except Exception as e:
        return f"Deposit write-back failed: {e} <br><a href='{url_for('detail', name=name)}'>Back</a>"
    return redirect(url_for("detail", name=name))


BANK_TEMPLATE = (
    "Date,Description,Amount\n"
    "2026-04-10,Customer deposit (money in),408.00\n"
    "2026-03-29,Card purchase (money out),-54.55\n"
)
BOOKS_TEMPLATE = (
    "Date,Description,Amount,Category\n"
    "2026-04-10,Customer deposit,408.00,Sales\n"
    "2026-03-29,Chin's Gas and Oil,-54.55,Automobile:Fuel\n"
)

@app.route("/account/<name>/qbo_import.csv")
def qbo_import_csv(name):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id, type FROM account WHERE name=%s LIMIT 1;", (name,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close(); return f"No account named {name}"
    acct_uuid, atype = row
    d = compute_detail(cur, acct_uuid, atype)
    cur.close(); conn.close()

    def clean_desc(s):
        s = (s or "").replace(",", " ").replace("\n", " ").strip()
        return s or "Transaction"

    buf = io.StringIO()
    wtr = csv.writer(buf)
    wtr.writerow(["Date", "Description", "Amount"])   # exactly 3 columns for QBO bank import
    if d.get("has_results"):
        rows = []
        for wb in d.get("writebacks", []):
            rows.append((wb["date"], wb["who"], wb["amount"]))
        for dp in d.get("deposits", []):
            rows.append((dp["date"], dp["who"], dp["amount"]))
        for (lid, dd, a, who) in d.get("on_stmt", []):
            rows.append((dd, who, a))
        rows.sort(key=lambda r: r[0])
        for dd, who, a in rows:
            if a == 0:
                continue   # QBO rejects zero amounts
            wtr.writerow([dd.strftime("%d/%m/%Y"), clean_desc(who), f"{a:.2f}"])
    return Response(buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={name}_for_quickbooks.csv"})


@app.route("/account/<name>/exceptions.csv")
def exceptions_csv(name):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id, type FROM account WHERE name=%s LIMIT 1;", (name,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close(); return f"No account named {name}"
    acct_uuid, atype = row
    d = compute_detail(cur, acct_uuid, atype)
    cur.close(); conn.close()
    buf = io.StringIO()
    wtr = csv.writer(buf)
    wtr.writerow(["Source", "Date", "Description", "Amount", "Direction", "Suggested category"])
    if d.get("has_results"):
        for wb in d.get("writebacks", []):
            wtr.writerow(["Bank statement", wb["date"], wb["who"], wb["amount"], "Money out", wb.get("cat") or ""])
        for dp in d.get("deposits", []):
            wtr.writerow(["Bank statement", dp["date"], dp["who"], dp["amount"], "Money in", ""])
        for (lid, dd, a, who) in d.get("on_stmt", []):
            wtr.writerow(["Bank statement", dd, who, a, ("Money in" if a > 0 else "Money out"), ""])
        for (tid, dd, a, who) in d.get("in_books", []):
            wtr.writerow(["Books", dd, who, a, "In books, not on statement", ""])
    return Response(buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={name}_exceptions.csv"})


@app.route("/account/<name>/diag")
def diag(name):
    conn = get_conn(); cur = conn.cursor()
    o = []
    cur.execute("SELECT account_id, type, currency FROM account WHERE name=%s LIMIT 1;", (name,))
    row = cur.fetchone()
    if not row:
        return f"No account named {name}"
    acct, atype, ccy = row
    o.append(f"ACCOUNT  name={name}  type={atype}  currency={ccy}")
    o.append(f"         id={acct}")
    cur.execute("SELECT statement_id, period_start, period_end FROM statement WHERE account_id=%s ORDER BY created_at DESC LIMIT 1;", (acct,))
    st = cur.fetchone()
    sid = ps = pe = None
    if st:
        sid, ps, pe = st
        o.append("")
        o.append(f"STATEMENT  period {ps} -> {pe}")
        cur.execute("SELECT count(*) FROM statement_line WHERE statement_id=%s;", (sid,))
        o.append(f"           statement_line rows: {cur.fetchone()[0]}")
        cur.execute("SELECT posted_date, amount, coalesce(description,'') FROM statement_line WHERE statement_id=%s ORDER BY posted_date LIMIT 6;", (sid,))
        o.append("           sample bank lines (date | amount | desc):")
        for d,a,de in cur.fetchall(): o.append(f"             {d} | {a} | {de[:34]}")
    else:
        o.append("STATEMENT  none found")
    o.append("")
    cur.execute("SELECT count(*) FROM book_txn WHERE account_id=%s;", (acct,))
    o.append(f"BOOKS  total rows (all dates): {cur.fetchone()[0]}")
    cur.execute("SELECT source_txn_type, count(*) FROM book_txn WHERE account_id=%s GROUP BY source_txn_type;", (acct,))
    o.append("       by source: " + (", ".join(f"{t}={c}" for t,c in cur.fetchall()) or "(none)"))
    cur.execute("SELECT count(*) FROM book_txn WHERE account_id=%s AND is_void IS NULL;", (acct,))
    o.append(f"       rows with is_void NULL: {cur.fetchone()[0]}")
    if sid:
        cur.execute("SELECT count(*) FROM book_txn WHERE account_id=%s AND posted_date BETWEEN %s AND %s;", (acct, ps, pe))
        o.append(f"       rows inside statement period: {cur.fetchone()[0]}")
        cur.execute("SELECT count(*) FROM book_txn WHERE account_id=%s AND posted_date BETWEEN %s AND %s AND coalesce(is_void,false)=false AND coalesce(is_deleted,false)=false;", (acct, ps, pe))
        o.append(f"       rows visible to matcher (in period): {cur.fetchone()[0]}")
        cur.execute("SELECT posted_date, amount, coalesce(description,''), coalesce(category,'') FROM book_txn WHERE account_id=%s AND posted_date BETWEEN %s AND %s ORDER BY posted_date LIMIT 6;", (acct, ps, pe))
        o.append("       sample book txns in period (date | amount | desc | category):")
        for d,a,de,ca in cur.fetchall(): o.append(f"             {d} | {a} | {de[:22]} | {ca[:20]}")
        cur.execute("""SELECT count(*) FROM statement_line sl WHERE sl.statement_id=%s AND EXISTS
            (SELECT 1 FROM book_txn bt WHERE bt.account_id=%s AND bt.amount=sl.amount
             AND coalesce(bt.is_void,false)=false AND coalesce(bt.is_deleted,false)=false);""", (sid, acct))
        o.append("")
        o.append(f"OVERLAP  bank lines with an exact-amount book match somewhere: {cur.fetchone()[0]}")
        cur.execute("""SELECT count(*) FROM statement_line sl WHERE sl.statement_id=%s AND EXISTS
            (SELECT 1 FROM book_txn bt WHERE bt.account_id=%s AND bt.amount = -sl.amount
             AND coalesce(bt.is_void,false)=false AND coalesce(bt.is_deleted,false)=false);""", (sid, acct))
        o.append(f"         bank lines matching a book amount with OPPOSITE sign: {cur.fetchone()[0]}")
    cur.close(); conn.close()
    return "<pre style='font-size:13px;line-height:1.5;padding:24px;font-family:ui-monospace,monospace'>" + "\n".join(str(x) for x in o) + "</pre>"


@app.route("/account/<name>/clear", methods=["POST"])
def clear_account(name):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id FROM account WHERE name=%s LIMIT 1;", (name,))
    row = cur.fetchone()
    if row:
        acct = row[0]
        cur.execute("DELETE FROM match_statement_line WHERE match_id IN (SELECT match_id FROM match WHERE statement_id IN (SELECT statement_id FROM statement WHERE account_id=%s));", (acct,))
        cur.execute("DELETE FROM match_book_txn WHERE match_id IN (SELECT match_id FROM match WHERE statement_id IN (SELECT statement_id FROM statement WHERE account_id=%s));", (acct,))
        cur.execute("DELETE FROM match WHERE statement_id IN (SELECT statement_id FROM statement WHERE account_id=%s);", (acct,))
        cur.execute("DELETE FROM statement_line WHERE statement_id IN (SELECT statement_id FROM statement WHERE account_id=%s);", (acct,))
        cur.execute("DELETE FROM statement WHERE account_id=%s;", (acct,))
        cur.execute("DELETE FROM book_txn WHERE account_id=%s;", (acct,))
        conn.commit()
        session["detail_msg"] = "Cleared all data for this account. Import your books and upload a statement to start fresh."
    cur.close(); conn.close()
    return redirect(url_for("detail", name=name))


@app.route("/template/<kind>")
def template(kind):
    if kind == "books":
        data, fname = BOOKS_TEMPLATE, "books_template.csv"
    else:
        data, fname = BANK_TEMPLATE, "bank_statement_template.csv"
    return Response(data, mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={fname}"})


@app.route("/sync", methods=["POST"])
def sync():
    try:
        n = sync_from_quickbooks()
        session["sync_msg"] = f"Synced {n} transactions from QuickBooks."
    except Exception as e:
        session["sync_msg"] = f"Sync failed: {e}"
    return redirect(url_for("dashboard"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))