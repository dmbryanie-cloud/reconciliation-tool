import os
import io
import csv
import hashlib
import hmac
import secrets
import itertools
import time
import re
import uuid
import psycopg2
from collections import Counter
from difflib import SequenceMatcher
from psycopg2.extras import execute_values
from decimal import Decimal
from datetime import datetime, timezone, timedelta, date
from flask import Flask, render_template_string, request, redirect, session, url_for, Response
from werkzeug.security import generate_password_hash, check_password_hash
from markupsafe import escape, Markup
import json, base64, urllib.request, urllib.parse, urllib.error
import threading

DB_URL = os.environ["SUPABASE_DB_URL"]
ORG_ID = "00000000-0000-0000-0000-000000000001"
DATE_TOLERANCE_DAYS = 3
CLEARING_WINDOW_DAYS = 31  # a cheque/payment can hit the bank this long after it's booked
GROUP_WINDOW_DAYS = 60
MAX_GROUP = 5         # max items combined in a batch
M2O_MAX_LINES = 1000  # skip combinatorial pass above this many unmatched items
M2O_MAX_CANDS = 10    # candidates considered per item (sorted by date-closeness)
EAT = timezone(timedelta(hours=3))

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "change-me")
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",   # browsers won't send the session on cross-site POSTs
    # HTTPS-only cookie on Render (it sets RENDER); stays off for local http development.
    SESSION_COOKIE_SECURE=bool(os.environ.get("RENDER")) or os.environ.get("SESSION_COOKIE_SECURE") == "1",
)
APP_PASSWORD = os.environ.get("APP_PASSWORD", "")
CONTACT_EMAIL = os.environ.get("CONTACT_EMAIL", "dmbryanie@gmail.com")  # shown on Terms/Privacy pages

QBO_REALM_ID = os.environ.get("QBO_REALM_ID", "")
QBO_BASE = os.environ.get("QBO_BASE", "https://sandbox-quickbooks.api.intuit.com")
QBO_REDIRECT_URI = os.environ.get("QBO_REDIRECT_URI", "https://reconciliation-tool-l2nk.onrender.com/callback")
QBO_SCOPE = "com.intuit.quickbooks.accounting"


def get_conn():
    return psycopg2.connect(DB_URL)


def get_config(key):
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT value FROM app_config WHERE key=%s;", (key,))
        row = cur.fetchone(); cur.close(); conn.close()
        return row[0] if row else None
    except Exception:
        return None


def set_config(key, value):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS app_config (key text PRIMARY KEY, value text);")
    cur.execute("INSERT INTO app_config (key,value) VALUES (%s,%s) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value;", (key, value))
    conn.commit(); cur.close(); conn.close()


# ---------------- background sync ----------------
# A full QuickBooks pull takes minutes on a real company, longer than a web request may run
# (the server cuts it off with a 502). So Sync starts it in a background thread and returns at
# once, and every page shows its progress. The job lives in app_config rather than memory, so
# all server workers see the same one and two syncs can't run side by side.
SYNC_STALE_SECS = 600        # no progress for this long: the server restarted mid-sync
SYNC_IN_BACKGROUND = True    # the tests run it inline


def _load_job():
    try:
        return json.loads(get_config("sync_job") or "{}")
    except Exception:
        return {}


def sync_job():
    """The current or last sync: {} if none. A 'running' job that stopped reporting is 'stalled'."""
    job = _load_job()
    if job.get("state") == "running" and time.time() - job.get("beat", 0) > SYNC_STALE_SECS:
        job["state"] = "stalled"
    return job


def sync_running():
    return sync_job().get("state") == "running"


def _claim_sync(full, by):
    """Mark a sync as running unless one already is. True if this caller got it."""
    conn = get_conn(); cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS app_config (key text PRIMARY KEY, value text);")
    cur.execute("INSERT INTO app_config (key, value) VALUES ('sync_job', '{}') ON CONFLICT (key) DO NOTHING;")
    cur.execute("SELECT value FROM app_config WHERE key='sync_job' FOR UPDATE;")   # one claimer at a time
    try:
        job = json.loads(cur.fetchone()[0] or "{}")
    except Exception:
        job = {}
    if job.get("state") == "running" and time.time() - job.get("beat", 0) <= SYNC_STALE_SECS:
        conn.rollback(); cur.close(); conn.close()
        return False
    now = time.time()
    cur.execute("UPDATE app_config SET value=%s WHERE key='sync_job';",
                (json.dumps({"state": "running", "full": bool(full), "by": by, "started": now, "beat": now,
                             "step": "Starting"}),))
    conn.commit(); cur.close(); conn.close()
    return True


def _sync_step(step):
    """Progress from the running sync; also its heartbeat."""
    job = _load_job()
    job.update(step=step, beat=time.time())
    set_config("sync_job", json.dumps(job))


def _run_sync_job(full):
    try:
        n, detail, mode, timing = sync_from_quickbooks(full=full, progress=_sync_step)
        window = _sync_since()
        scope = f"since {window}" if window else "all history"
        state, msg = "done", (f"Synced {n} transactions from QuickBooks ({scope}, {mode}; {timing}). "
                              f"Records fetched: {detail}.")
    except Exception as e:
        state, msg = "failed", f"Sync failed: {e}"
    try:
        job = _load_job()
        job.update(state=state, msg=msg, finished=time.time(), beat=time.time())
        set_config("sync_job", json.dumps(job))
    except Exception:
        pass   # the job goes stale and the next Sync can start


def start_sync(full=False, by=None):
    """Start a QuickBooks sync in the background. False if one is already running."""
    if not _claim_sync(full, by):
        return False
    if SYNC_IN_BACKGROUND:
        threading.Thread(target=_run_sync_job, args=(full,), daemon=True, name="qbo-sync").start()
    else:
        _run_sync_job(full)
    return True


def _ago(secs):
    m = int(max(0, secs) // 60)
    return "just now" if m < 1 else f"{m} min ago"


def sync_banner():
    """The progress/result strip shown at the top of the dashboard and account pages."""
    try:
        return _sync_banner()
    except Exception:
        return ""   # a status strip must never break the page


def _sync_banner():
    job = sync_job()
    st = job.get("state")
    if st == "running":
        what = "Full sync" if job.get("full") else "Syncing"
        return Markup(
            '<div id=syncbar class=syncbar data-state=running style="background:var(--accent-soft);color:var(--accent);'
            'padding:11px 14px;border-radius:9px;font-size:14px;margin:0 0 18px;font-weight:550;line-height:1.5">'
            f'&#8635; {what} from QuickBooks in progress (started {_ago(time.time() - job.get("started", 0))}): '
            f'<span class=sync-step>{escape(job.get("step") or "")}</span>. You can keep working; '
            'a big company takes several minutes. This page refreshes when it finishes.</div>'
            '<script>(function(){var dirty=false;document.addEventListener("input",function(){dirty=true},true);'
            'document.addEventListener("change",function(){dirty=true},true);var bar=document.getElementById("syncbar");'
            'function tick(){fetch("' + url_for("sync_status") + '",{credentials:"same-origin"}).then(function(r){return r.json()})'
            '.then(function(j){if(j.state!=="running"){if(dirty){bar.innerHTML="Sync finished. <a href=\\"\\" '
            'onclick=\\"location.reload();return false\\">Reload to see the results</a>";}else{location.reload();}return;}'
            'var s=bar.querySelector(".sync-step");if(s)s.textContent=j.step||"";setTimeout(tick,4000);})'
            '.catch(function(){setTimeout(tick,10000)});}setTimeout(tick,4000);})();</script>')
    if st == "stalled":
        return Markup(
            '<div class=syncbar data-state=stalled style="background:#fffbeb;border:1px solid #fde68a;color:#92400e;'
            'padding:11px 14px;border-radius:9px;font-size:14px;margin:0 0 18px;line-height:1.5">The last QuickBooks sync '
            'stopped before finishing (the server restarted). Nothing was lost; press <b>Sync from QuickBooks</b> to run it again.</div>')
    if st in ("done", "failed") and session.get("sync_seen") != job.get("started"):
        session["sync_seen"] = job.get("started")   # the result is shown once
        ok = st == "done"
        return Markup(
            f'<div class=syncbar data-state={st} style="background:{"var(--accent-soft)" if ok else "#fef2f2"};'
            f'color:{"var(--accent)" if ok else "#991b1b"};padding:11px 14px;border-radius:9px;font-size:14px;'
            f'margin:0 0 18px;line-height:1.5">{escape(job.get("msg") or "")}</div>')
    return ""


def check_password(pw):
    if not pw:
        return False
    return bool(APP_PASSWORD) and pw == APP_PASSWORD


def _ensure_users(cur):
    cur.execute("""CREATE TABLE IF NOT EXISTS app_users (
        username text PRIMARY KEY, name text, password_hash text,
        is_admin boolean DEFAULT false, created_at timestamptz DEFAULT now());""")


def get_user(username):
    if not username:
        return None
    try:
        conn = get_conn(); cur = conn.cursor()
        _ensure_users(cur); conn.commit()
        cur.execute("SELECT username, name, password_hash, is_admin FROM app_users WHERE username=%s;", (username.strip().lower(),))
        row = cur.fetchone(); cur.close(); conn.close()
        return row
    except Exception:
        return None


def list_users():
    conn = get_conn(); cur = conn.cursor()
    _ensure_users(cur); conn.commit()
    cur.execute("SELECT username, name, is_admin, created_at FROM app_users ORDER BY created_at;")
    rows = cur.fetchall(); cur.close(); conn.close()
    return rows


def add_user(username, name, password, is_admin):
    conn = get_conn(); cur = conn.cursor()
    _ensure_users(cur)
    cur.execute("""INSERT INTO app_users (username, name, password_hash, is_admin) VALUES (%s,%s,%s,%s)
                   ON CONFLICT (username) DO UPDATE SET name=EXCLUDED.name,
                     password_hash=EXCLUDED.password_hash, is_admin=EXCLUDED.is_admin;""",
                (username.strip().lower(), (name or "").strip() or username.strip().lower(),
                 generate_password_hash(password), bool(is_admin)))
    conn.commit(); cur.close(); conn.close()


def delete_user(username):
    conn = get_conn(); cur = conn.cursor()
    _ensure_users(cur)
    cur.execute("DELETE FROM app_users WHERE username=%s;", (username.strip().lower(),))
    conn.commit(); cur.close(); conn.close()


def update_user(username, name, is_admin, password=None):
    conn = get_conn(); cur = conn.cursor()
    _ensure_users(cur)
    nm = (name or username).strip()
    if password:
        cur.execute("UPDATE app_users SET name=%s, is_admin=%s, password_hash=%s WHERE username=%s;",
                    (nm, bool(is_admin), generate_password_hash(password), username.strip().lower()))
    else:
        cur.execute("UPDATE app_users SET name=%s, is_admin=%s WHERE username=%s;",
                    (nm, bool(is_admin), username.strip().lower()))
    conn.commit(); cur.close(); conn.close()


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

def _ensure_qbo_auth(cur):
    cur.execute("CREATE TABLE IF NOT EXISTS qbo_auth (id int PRIMARY KEY, refresh_token text, realm_id text, updated_at timestamptz DEFAULT now());")
    cur.execute("ALTER TABLE qbo_auth ADD COLUMN IF NOT EXISTS realm_id text;")

def _get_stored_refresh():
    conn = get_conn(); cur = conn.cursor()
    _ensure_qbo_auth(cur); conn.commit()
    cur.execute("SELECT refresh_token FROM qbo_auth WHERE id=1;")
    row = cur.fetchone()
    cur.close(); conn.close()
    return row[0] if row and row[0] else None

def _store_refresh(token, realm=None):
    conn = get_conn(); cur = conn.cursor()
    _ensure_qbo_auth(cur)
    if realm:
        cur.execute("""INSERT INTO qbo_auth (id, refresh_token, realm_id, updated_at) VALUES (1,%s,%s,now())
                       ON CONFLICT (id) DO UPDATE SET refresh_token=EXCLUDED.refresh_token, realm_id=EXCLUDED.realm_id, updated_at=now();""", (token, realm))
    else:
        cur.execute("""INSERT INTO qbo_auth (id, refresh_token, updated_at) VALUES (1,%s,now())
                       ON CONFLICT (id) DO UPDATE SET refresh_token=EXCLUDED.refresh_token, updated_at=now();""", (token,))
    conn.commit(); cur.close(); conn.close()

def qbo_realm():
    try:
        conn = get_conn(); cur = conn.cursor()
        _ensure_qbo_auth(cur); conn.commit()
        cur.execute("SELECT realm_id FROM qbo_auth WHERE id=1;")
        row = cur.fetchone(); cur.close(); conn.close()
        if row and row[0]:
            return row[0]
    except Exception:
        pass
    return QBO_REALM_ID

def _clear_stored_auth():
    conn = get_conn(); cur = conn.cursor()
    _ensure_qbo_auth(cur)
    cur.execute("DELETE FROM qbo_auth WHERE id=1;")
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

QBO_PAGE_SIZE = 1000
QBO_MAX_PAGES = 50  # safety ceiling: 50,000 records per entity
SYNC_MONTHS = int(os.environ.get("SYNC_MONTHS", "24"))  # how far back sync reaches

def _sync_since():
    """First day of the month SYNC_MONTHS ago, as YYYY-MM-DD."""
    if SYNC_MONTHS <= 0:
        return None  # 0 or negative = pull all history
    t = date.today()
    m = t.month - SYNC_MONTHS
    y = t.year + (m - 1) // 12
    m = (m - 1) % 12 + 1
    return f"{y:04d}-{m:02d}-01"

def qbo_query(entity, token, since=None, changed_since=None, each=None):
    """Query a QBO entity, following STARTPOSITION paging until exhausted.

    QuickBooks caps ANY single query at 1000 rows. The previous version sent
    MAXRESULTS 1000 with no paging, so any entity with more than 1000 records
    was silently truncated -- no error, no warning. For a reconciliation tool
    that surfaces as phantom "unrecorded" items on the bank side.

    `each`, if given, turns every page into what's kept (the raw pages are dropped as they go).
    """
    out = []
    start = 1
    conds = []
    if since:
        conds.append(f"TxnDate >= '{since}'")
    if changed_since:
        conds.append(f"MetaData.LastUpdatedTime >= '{changed_since}'")
    where = (" WHERE " + " AND ".join(conds)) if conds else ""
    for _ in range(QBO_MAX_PAGES):
        q = f"SELECT * FROM {entity}{where} STARTPOSITION {start} MAXRESULTS {QBO_PAGE_SIZE}"
        url = f"{QBO_BASE}/v3/company/{qbo_realm()}/query?query=" + urllib.parse.quote(q)
        req = urllib.request.Request(url)
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Accept", "application/json")
        with urllib.request.urlopen(req) as resp:
            batch = json.loads(resp.read()).get("QueryResponse", {}).get(entity, [])
        out.extend(each(batch) if each else batch)
        if len(batch) < QBO_PAGE_SIZE:
            break
        start += QBO_PAGE_SIZE
    return out

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
    cat = who = None
    for ln in e.get("Line", []):
        det = ln.get("DepositLineDetail")
        if det:
            cat = det.get("AccountRef", {}).get("name"); who = (det.get("Entity") or {}).get("name"); break
    return _D(e.get("TotalAmt")), who, e.get("PrivateNote"), cat

def _h_transfer(e, acct, atype):
    # Cards count what's owed: a transfer in pays the card down, one out (cash advance) adds to it.
    sign = -1 if atype == "credit_card" else 1
    if e.get("ToAccountRef", {}).get("value") == acct:
        return sign * _D(e.get("Amount")), "Transfer in", e.get("PrivateNote"), e.get("FromAccountRef", {}).get("name")
    if e.get("FromAccountRef", {}).get("value") == acct:
        return -sign * _D(e.get("Amount")), "Transfer out", e.get("PrivateNote"), e.get("ToAccountRef", {}).get("name")
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

def _h_salesreceipt(e, acct, atype):
    if e.get("DepositToAccountRef", {}).get("value") != acct: return None
    return _D(e.get("TotalAmt")), e.get("CustomerRef", {}).get("name"), e.get("PrivateNote"), None

def _h_refundreceipt(e, acct, atype):
    if e.get("DepositToAccountRef", {}).get("value") != acct: return None
    amt = _D(e.get("TotalAmt"))
    return (amt if atype == "credit_card" else -amt), e.get("CustomerRef", {}).get("name"), e.get("PrivateNote"), None

def _h_ccpayment(e, acct, atype):
    # Paying the card: money leaves the bank, and the card balance owed goes down.
    amt = _D(e.get("Amount"))
    if e.get("BankAccountRef", {}).get("value") == acct:
        return -amt, "Credit card payment", e.get("PrivateNote"), e.get("CreditCardAccountRef", {}).get("name")
    if e.get("CreditCardAccountRef", {}).get("value") == acct:
        return -amt, "Credit card payment", e.get("PrivateNote"), e.get("BankAccountRef", {}).get("name")
    return None

def _entity_ref(e):
    """'Type:Id' of the transaction's payee, so a write-back can name the same vendor/customer."""
    for key, default in (("EntityRef", "Vendor"), ("VendorRef", "Vendor"), ("CustomerRef", "Customer")):
        r = e.get(key) or {}
        if r.get("value"):
            return f"{r.get('type') or default}:{r['value']}"
    for ln in e.get("Line", []):
        r = (ln.get("DepositLineDetail") or {}).get("Entity") or {}
        if r.get("value"):
            return f"{r.get('type') or 'Customer'}:{r['value']}"
    return None

def _account_refs(etype, e):
    if etype == "Purchase":
        return [e.get("AccountRef", {}).get("value")]
    if etype in ("Deposit", "Payment", "SalesReceipt", "RefundReceipt"):
        return [e.get("DepositToAccountRef", {}).get("value")]
    if etype == "CreditCardPayment":
        return [e.get("BankAccountRef", {}).get("value"), e.get("CreditCardAccountRef", {}).get("value")]
    if etype == "Transfer":
        return [e.get("ToAccountRef", {}).get("value"), e.get("FromAccountRef", {}).get("value")]
    if etype == "BillPayment":
        return [e.get("CheckPayment", {}).get("BankAccountRef", {}).get("value"),
                e.get("CreditCardPayment", {}).get("CCAccountRef", {}).get("value")]
    if etype == "JournalEntry":
        refs = []
        for ln in e.get("Line", []):
            det = ln.get("JournalEntryLineDetail")
            if det:
                refs.append(det.get("AccountRef", {}).get("value"))
        return refs
    return []


QBO_HANDLERS = {"Purchase": _h_purchase, "Deposit": _h_deposit, "Transfer": _h_transfer,
                "BillPayment": _h_billpayment, "Payment": _h_payment, "JournalEntry": _h_journalentry,
                "SalesReceipt": _h_salesreceipt, "RefundReceipt": _h_refundreceipt,
                "CreditCardPayment": _h_ccpayment}
# Newer entity; some companies/API versions reject the query. A failure here shouldn't hold
# back the sync watermark forever.
QBO_OPTIONAL = {"CreditCardPayment"}

def _store_coa(accts):
    """Cache the chart of accounts so write-back can offer real accounts without a QBO call per page."""
    rows = [(str(a["Id"]), a.get("Name"), a.get("FullyQualifiedName") or a.get("Name"), a.get("AccountType"),
             a.get("Classification"), bool(a.get("Active", True)), (a.get("CurrencyRef") or {}).get("value"))
            for a in accts if a.get("Id")]
    if not rows:
        return
    conn = get_conn(); cur = conn.cursor()
    cur.execute("DELETE FROM qbo_coa;")
    execute_values(cur, "INSERT INTO qbo_coa (qbo_id, name, fqn, account_type, classification, active, currency) VALUES %s", rows)
    conn.commit(); cur.close(); conn.close()


def import_accounts_from_qbo(token):
    """Discover Bank and Credit Card accounts from the connected QBO company and upsert them."""
    try:
        accts = qbo_query("Account", token)
    except Exception:
        return 0
    _store_coa(accts)
    try:
        ci = qbo_query("CompanyInfo", token)
        if ci and ci[0].get("CompanyName"):
            set_config("company_name", ci[0]["CompanyName"])   # letterhead on printed reports
    except Exception:
        pass
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='account';")
    cols = {r[0] for r in cur.fetchall()}
    conn_id = None
    if "connection_id" in cols:
        try:
            cur.execute("SELECT connection_id FROM account WHERE connection_id IS NOT NULL LIMIT 1;")
            r = cur.fetchone()
            conn_id = r[0] if r else None
        except Exception:
            conn.rollback()
    created = 0
    for a in accts:
        t = {"Bank": "bank", "Credit Card": "credit_card"}.get(a.get("AccountType", ""))
        if not t:
            continue
        qid = a.get("Id")
        if not qid:
            continue
        name = a.get("Name") or ("Account " + str(qid))
        ccy = (a.get("CurrencyRef") or {}).get("value")
        try:
            cur.execute("SELECT account_id FROM account WHERE source_account_id=%s;", (qid,))
            if cur.fetchone():
                cur.execute("UPDATE account SET name=%s, type=%s WHERE source_account_id=%s;", (name, t, qid))
                conn.commit()
                continue
            fields = {"source_account_id": qid, "name": name, "type": t}
            if "currency" in cols and ccy:
                fields["currency"] = ccy
            if "org_id" in cols:
                fields["org_id"] = ORG_ID
            if "connection_id" in cols and conn_id is not None:
                fields["connection_id"] = conn_id
            colnames = list(fields.keys())
            placeholders = ["%s"] * len(colnames)
            if "account_id" in cols:
                colnames = ["account_id"] + colnames
                placeholders = ["gen_random_uuid()"] + placeholders
            sql = "INSERT INTO account (" + ", ".join(colnames) + ") VALUES (" + ", ".join(placeholders) + ");"
            cur.execute(sql, [fields[c] for c in fields])
            conn.commit()
            created += 1
        except Exception:
            conn.rollback()
    cur.close(); conn.close()
    return created


CDC_MAX_DAYS = 29          # QBO's change feed only reaches back 30 days
CDC_ENTITY_CAP = 1000      # CDC returns at most this many objects per entity, unpaged
REMOVAL_GUARD_MIN = 20     # a full resync may always remove this many rows per type...
REMOVAL_GUARD_SHARE = 0.5  # ...but never more than this share of them without a human looking


def qbo_cdc_deleted(token, entities, changed_since):
    """{entity: [ids]} of objects QBO reports deleted since `changed_since`, and whether any
    entity hit the per-call cap (in which case some deletions may be missing)."""
    def call(ents):
        q = urllib.parse.urlencode({"entities": ",".join(ents), "changedSince": changed_since})
        req = urllib.request.Request(f"{QBO_BASE}/v3/company/{qbo_realm()}/cdc?{q}")
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Accept", "application/json")
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    try:
        data = call(entities)
    except urllib.error.HTTPError:
        data = call([e for e in entities if e not in QBO_OPTIONAL])   # older API versions
    deleted, capped = {}, False
    for block in data.get("CDCResponse", []):
        for qr in block.get("QueryResponse", []):
            for etype, objs in qr.items():
                if etype not in QBO_HANDLERS or not isinstance(objs, list):
                    continue
                capped = capped or len(objs) >= CDC_ENTITY_CAP
                ids = [str(o["Id"]) for o in objs if o.get("status") == "Deleted" and o.get("Id")]
                if ids:
                    deleted.setdefault(etype, []).extend(ids)
    return deleted, capped


def _mark_removed(cur, txn_ids):
    if txn_ids:
        cur.execute("UPDATE book_txn SET is_deleted=true, updated_at=now() WHERE txn_id = ANY(%s::uuid[]);",
                    ([str(t) for t in txn_ids],))


def _apply_removals(cur, rows, cache, fetched, deleted, full_mode, since):
    """Flag synced book rows that no longer exist in QuickBooks under that account.

    Three sources: CDC deletions (incremental), changed transactions that now point at a
    different account or none, and -- on a full pull only -- rows QBO no longer returns at all.
    Rows are flagged is_deleted, never removed; CSV-imported books are never touched.
    Returns (removed txn_ids, warnings)."""
    removed, warnings = set(), []
    for etype, ids in deleted.items():
        cur.execute("""SELECT txn_id FROM book_txn WHERE source_txn_type=%s AND source_txn_id = ANY(%s)
                       AND NOT is_deleted;""", (etype, ids))
        removed.update(r[0] for r in cur.fetchall())
    current = {(str(r[1]), r[3], str(r[2])) for r in rows}   # (account, type, id) as QBO has them now
    for etype, ids in cache.items():   # cache: the ids QBO returned per type
        if not ids:
            continue
        cur.execute("""SELECT txn_id, account_id, source_txn_id FROM book_txn
                       WHERE source_txn_type=%s AND source_txn_id = ANY(%s) AND NOT is_deleted;""", (etype, ids))
        removed.update(t for t, a, i in cur.fetchall() if (str(a), etype, i) not in current)
    if full_mode:
        for etype, ids in cache.items():
            if fetched.get(etype, -1) < 0:
                continue   # a failed pull says nothing about what exists
            cur.execute("""SELECT txn_id FROM book_txn
                           WHERE source_txn_type=%s AND NOT is_deleted AND NOT (source_txn_id = ANY(%s))
                             AND (%s::date IS NULL OR posted_date >= %s::date);""", (etype, ids, since, since))
            gone = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT count(*) FROM book_txn WHERE source_txn_type=%s AND NOT is_deleted AND (%s::date IS NULL OR posted_date >= %s::date);",
                        (etype, since, since))
            live = cur.fetchone()[0]
            if len(gone) > max(REMOVAL_GUARD_MIN, REMOVAL_GUARD_SHARE * live):
                warnings.append(f"{etype}: QuickBooks no longer returns {len(gone)} of {live} synced records. "
                                f"Nothing was removed; check the right company is connected, then run a full resync again.")
                continue
            removed.update(gone)
    _mark_removed(cur, removed)
    return removed, warnings


def _removal_fallout(cur, removed):
    """Statements whose accepted matches used a removed transaction: (open ids, signed-off labels)."""
    if not removed:
        return [], []
    cur.execute("""SELECT DISTINCT s.statement_id, a.name, s.period_start, s.period_end, s.signed_off_at
                   FROM match_book_txn mbt JOIN match m ON m.match_id=mbt.match_id
                   JOIN statement s ON s.statement_id=m.statement_id JOIN account a ON a.account_id=s.account_id
                   WHERE mbt.txn_id = ANY(%s::uuid[]) AND m.status<>'rejected';""", ([str(t) for t in removed],))
    open_, signed = [], []
    for sid, nm, ps, pe, so in cur.fetchall():
        (signed.append(f"{nm} {ps} to {pe}") if so else open_.append(sid))
    return open_, signed


def _open_statement_ids():
    """Latest statement per account, where it isn't signed off yet."""
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""SELECT statement_id FROM (SELECT DISTINCT ON (account_id) statement_id, signed_off_at FROM statement
                   ORDER BY account_id, created_at DESC) t WHERE signed_off_at IS NULL;""")
    ids = [r[0] for r in cur.fetchall()]
    cur.close(); conn.close()
    return ids


def last_sync_label():
    stamp = get_config("last_sync_at")
    try:
        t = datetime.strptime(stamp[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        return t.astimezone(EAT).strftime("%d %b %Y, %H:%M")
    except Exception:
        return None


def qbo_is_connected():
    return bool(_get_stored_refresh() or os.environ.get("QBO_REFRESH_TOKEN", "")) and get_config("qbo_conn") != "disconnected"


BOOK_BALANCE_FRESH_SECS = 15 * 60   # a sync this recent is fresh enough for the book balance


def _sync_age_secs(stamp):
    try:
        t = datetime.strptime(stamp[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - t).total_seconds()
    except Exception:
        return float("inf")


def _sync_age_days(stamp):
    try:
        t = datetime.strptime(stamp[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - t).days
    except Exception:
        return 10 ** 6


def _sync_plan(full=False):
    """(changed_since, ent_sig, notes) for the next sync; changed_since None means a full pull."""
    # Watermark: only pull records QBO says changed since our last good sync.
    # A newly handled entity type needs one full pull, or its older records never arrive.
    ent_sig = ",".join(sorted(QBO_HANDLERS)) + "|refs2"   # bump the suffix to force one full re-pull (refs2: card transfer signs)
    changed_since = None if (full or get_config("sync_entities") != ent_sig or get_config("sync_force_full") == "1") \
        else get_config("last_sync_at")
    notes = []
    if changed_since and _sync_age_days(changed_since) > CDC_MAX_DAYS:
        # Deletions older than the change feed's reach can only be found by a full pull.
        changed_since = None
        notes.append(f"last sync was over {CDC_MAX_DAYS} days ago, so this was a full sync to catch deletions")
    return changed_since, ent_sig, notes


def sync_full_due():
    """True when the next sync must re-download everything. That takes minutes on a real company,
    so it's only run from the Sync button, never inside another request (an upload would time out)."""
    return _sync_plan()[0] is None


def _book_rows(etype, handler, ents, by_qbo):
    """book_txn rows for the tracked accounts these QuickBooks records touch."""
    rows = []
    for e in ents:
        seen = set()
        for ref in _account_refs(etype, e):
            if not ref:
                continue
            ref = str(ref)
            if ref in seen or ref not in by_qbo:
                continue
            seen.add(ref)
            acct_uuid, atype = by_qbo[ref]
            try:
                res = handler(e, ref, atype)
            except Exception:
                continue
            if not res:
                continue
            amount, cp, desc, cat = res
            # QBO voids by zeroing the amounts; keep the row but out of matching.
            rows.append((ORG_ID, acct_uuid, e.get("Id"), etype, e.get("TxnDate"), amount,
                         e.get("CurrencyRef", {}).get("value", "USD"), desc, cp,
                         e.get("DocNumber"), cat, "unknown",
                         e.get("MetaData", {}).get("LastUpdatedTime"), amount == 0, _entity_ref(e)))
    return rows


def sync_from_quickbooks(full=False, progress=None):
    step = progress or (lambda msg: None)
    step("Reading your chart of accounts")
    token = qbo_token()
    import_accounts_from_qbo(token)
    since = _sync_since()
    changed_since, ent_sig, notes = _sync_plan(full)
    # Stamped BEFORE fetching, so anything edited mid-sync is caught next time.
    started_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S-00:00")
    conn = get_conn(); cur = conn.cursor()
    cur.execute("ALTER TABLE book_txn ADD COLUMN IF NOT EXISTS category text;")
    conn.commit()
    cur.execute("SELECT account_id, source_account_id, name, type FROM account ORDER BY type, name;")
    by_qbo = {}
    for acct_uuid, acct_qbo, name, atype in cur.fetchall():
        if acct_qbo:
            by_qbo[str(acct_qbo)] = (acct_uuid, atype)
    cur.close(); conn.close()   # not held open through the fetch, which can take minutes
    t0 = time.time()
    # Each page becomes book rows as it arrives and only its ids are kept: a full pull of a
    # large company never holds every raw record in memory at once.
    rows, cache, fetched = [], {}, {}
    for etype, handler in QBO_HANDLERS.items():
        got, seen_n = [], [0]
        step(f"Downloading {etype} records")
        def keep(batch, etype=etype, handler=handler, got=got, seen_n=seen_n):
            got.extend(_book_rows(etype, handler, batch, by_qbo))
            seen_n[0] += len(batch)
            step(f"Downloading {etype} records ({seen_n[0]:,} so far)")
            return [str(e["Id"]) for e in batch if e.get("Id")]
        try:
            cache[etype] = qbo_query(etype, token, since=since, changed_since=changed_since, each=keep)
            fetched[etype] = len(cache[etype])
            rows.extend(got)
        except urllib.error.HTTPError:
            cache[etype] = []
            fetched[etype] = -1  # -1 marks a failed pull, distinct from a genuine zero
    deleted, cdc_ok, capped = {}, True, False
    if changed_since:
        try:
            deleted, capped = qbo_cdc_deleted(token, list(QBO_HANDLERS), changed_since)
        except Exception:
            cdc_ok = False
            notes.append("couldn't read deletions from QuickBooks; they'll be checked on the next sync")
    fetch_secs = time.time() - t0
    step(f"Saving {len(rows):,} transactions")
    conn = get_conn(); cur = conn.cursor()
    t1 = time.time()
    if rows:
        execute_values(cur, """
            INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type,
                                  posted_date, amount, currency, description, counterparty,
                                  reference, category, cleared_status, last_modified, is_void, counterparty_ref)
            VALUES %s
            ON CONFLICT (account_id, source_txn_type, source_txn_id) DO UPDATE SET
              posted_date=EXCLUDED.posted_date, amount=EXCLUDED.amount, currency=EXCLUDED.currency,
              description=EXCLUDED.description, counterparty=EXCLUDED.counterparty,
              reference=EXCLUDED.reference, category=EXCLUDED.category, last_modified=EXCLUDED.last_modified,
              is_void=EXCLUDED.is_void, is_deleted=false, updated_at=now(),
              counterparty_ref=EXCLUDED.counterparty_ref;
        """, rows, page_size=500)
    total = len(rows)
    realm, synced_realm = qbo_realm(), get_config("synced_realm")
    if synced_realm and synced_realm != realm:
        # A different company: its IDs mean nothing against rows synced from the old one.
        removed, warns = set(), ["the connected QuickBooks company changed since the last sync, so no transactions "
                                 "were flagged as deleted this time"]
    else:
        removed, warns = _apply_removals(cur, rows, cache, fetched, deleted, changed_since is None, since)
    reopen, signed_hit = _removal_fallout(cur, removed)
    conn.commit(); cur.close(); conn.close()
    # Fresh books: re-match every open reconciliation (the user's review decisions are kept).
    for sid in set(reopen) | set(_open_statement_ids()):
        step("Re-matching open reconciliations")
        run_matcher(sid)
    if removed:
        notes.append(f"flagged {len(removed)} transaction{'' if len(removed)==1 else 's'} deleted or moved in QuickBooks"
                     + (f", re-matched {len(reopen)} open reconciliation{'' if len(reopen)==1 else 's'}" if reopen else ""))
    if signed_hit:
        notes.append("WARNING: signed-off reconciliations used transactions since removed in QuickBooks: "
                     + "; ".join(signed_hit[:5]) + (" …" if len(signed_hit) > 5 else "") + ". Review and reopen them.")
    notes.extend(warns)
    insert_secs = time.time() - t1
    clean = all(v >= 0 for k, v in fetched.items() if k not in QBO_OPTIONAL) and cdc_ok
    if clean:
        set_config("last_sync_at", started_at)   # only advance if every entity pulled OK
        set_config("sync_entities", ent_sig)
        set_config("synced_realm", realm)
        # Past the CDC cap some deletions may be missing: make the next sync a full one.
        set_config("sync_force_full", "1" if capped else "0")
        if capped:
            notes.append("many changes at once, so the next sync will be a full one to be sure nothing was missed")
    detail = ", ".join(f"{k} {('unavailable' if k in QBO_OPTIONAL else 'FAILED') if v < 0 else v}" for k, v in fetched.items())
    mode = "full" if changed_since is None else f"changes since {changed_since[:16].replace('T', ' ')}"
    timing = f"fetch {fetch_secs:.0f}s, save {insert_secs:.0f}s"
    if not clean:
        detail += " \u2014 some types failed, so the next sync will re-check the same period"
    if notes:
        detail += ". " + "; ".join(notes)
    return total, detail, mode, timing


# Make sure sign-off columns exist (runs once at startup)
try:
    _c = get_conn(); _cur = _c.cursor()
    _cur.execute("ALTER TABLE statement ADD COLUMN IF NOT EXISTS signed_off_at timestamptz;")
    _cur.execute("ALTER TABLE statement ADD COLUMN IF NOT EXISTS signed_off_by text;")
    _cur.execute("ALTER TABLE account ADD COLUMN IF NOT EXISTS is_active boolean NOT NULL DEFAULT true;")
    # Balance reconciliation. A *_source of NULL means "not known yet"; otherwise one of
    # user / file / carried (previous signed-off closing) / derived (closing - movements) / qbo.
    for _col, _typ in (("opening_source", "text"), ("closing_source", "text"), ("book_balance", "numeric"),
                       ("book_balance_source", "text"), ("signoff_note", "text"),
                       ("snap_exact", "int"), ("snap_fuzzy", "int"), ("snap_m2o", "int"),
                       ("snap_exc", "int"), ("snap_diff", "numeric")):
        _cur.execute(f"ALTER TABLE statement ADD COLUMN IF NOT EXISTS {_col} {_typ};")
    _cur.execute("CREATE INDEX IF NOT EXISTS idx_book_txn_amt_date ON book_txn (amount, posted_date);")
    _cur.execute("ALTER TABLE book_txn ADD COLUMN IF NOT EXISTS counterparty_ref text;")   # e.g. 'Vendor:56'
    _cur.execute("""CREATE TABLE IF NOT EXISTS qbo_coa (qbo_id text PRIMARY KEY, name text, fqn text,
                    account_type text, classification text, active boolean, currency text);""")
    # One row per statement line ever sent to QuickBooks: stops a double click or a retry
    # from posting the same line twice to the company file.
    _cur.execute("""CREATE TABLE IF NOT EXISTS writeback_log (line_id uuid PRIMARY KEY, status text NOT NULL,
                    qbo_type text, qbo_id text, account_fqn text, payee text, error text,
                    created_by text, created_at timestamptz NOT NULL DEFAULT now());""")
    _cur.execute("""CREATE TABLE IF NOT EXISTS payee_correction (id serial PRIMARY KEY, org_id uuid NOT NULL,
                    payee text NOT NULL, category text NOT NULL, created_at timestamptz NOT NULL DEFAULT now());""")
    for _col, _typ in (("money_out", "boolean"), ("vendor", "text"), ("vendor_ref", "text"), ("currency", "text")):
        _cur.execute(f"ALTER TABLE payee_correction ADD COLUMN IF NOT EXISTS {_col} {_typ};")
    _cur.execute("CREATE TABLE IF NOT EXISTS app_config (key text PRIMARY KEY, value text);")
    _cur.execute("SELECT 1 FROM app_config WHERE key='migr_proposed_v1';")
    if not _cur.fetchone():
        # Suggestions used to be saved as confirmed. Put the ones on open reconciliations back
        # up for review; signed-off periods are left as they were signed.
        _cur.execute("""UPDATE match SET status='proposed'
                        WHERE status='confirmed' AND (match_type IN ('fuzzy','manual','many_to_one') OR confidence < 1)
                          AND statement_id IN (SELECT statement_id FROM statement WHERE signed_off_at IS NULL);""")
        _cur.execute("INSERT INTO app_config (key, value) VALUES ('migr_proposed_v1', 'done');")
    _cur.execute("CREATE INDEX IF NOT EXISTS idx_stmt_line_amt_date ON statement_line (amount, posted_date);")
    _c.commit(); _cur.close(); _c.close()
except Exception as e:
    print("startup check:", e)


# ---------------- learned categorization (memory) ----------------
_STOPWORDS = {"and","the","of","inc","llc","ltd","co","corp","company",
              "services","service","pos","debit","purchase","payment",
              "card","visa","ach","ppd","tst",
              # bank-narrative noise
              "ref","txn","trx","trn","chq","no","from","to","for","via","being","limited","pmt"}

def _normalize(name):
    if not name: return ""
    s = re.sub(r"[^a-z0-9 ]", " ", name.lower())
    return re.sub(r"\s+", " ", s).strip()

def _mtokens(name):
    # Drop reference numbers (anything with 3+ digits) along with the noise words.
    return [t for t in _normalize(name).split()
            if len(t) > 1 and sum(ch.isdigit() for ch in t) < 3 and t not in _STOPWORDS]

def _tok_match(a, b):
    return a == b or (len(a) >= 3 and len(b) >= 3 and (a.startswith(b) or b.startswith(a)))

def _coverage(known, inn):
    if not known: return 0.0
    return sum(1 for k in known if any(_tok_match(k, i) for i in inn)) / len(known)

def _money_out(amount, atype):
    """Credit cards: a charge (positive) is money out. Banks: a withdrawal (negative) is."""
    return amount > 0 if atype == "credit_card" else amount < 0


def _similarity(a, b):
    """Two-way token overlap (Dice), counting prefix matches like SUPERMKT ~ SUPERMARKET."""
    if not a or not b:
        return 0.0
    hits = sum(1 for x in a if any(_tok_match(x, y) for y in b)) + sum(1 for y in b if any(_tok_match(x, y) for x in a))
    return hits / (len(a) + len(b))


LEARN_MIN_SCORE = 0.5
TIER_TEXT = {1: "you recorded a similar line", 2: "similar bank lines were posted", 3: "this payee is usually posted"}


class PostingMemory:
    """Suggests an account and payee for a bank line from how the books treated similar lines.

    Three tiers, strongest first -- the first tier with a good match wins:
      1. lines recorded from this app (the user picked the account)
      2. bank lines matched to a QuickBooks transaction (bank wording -> how it was posted)
      3. QuickBooks history by payee name
    """
    def __init__(self, cur, currency=None):
        """currency: learn only from postings on accounts in this currency, so a USD bank
        charge is suggested from USD postings and a UGX one from UGX postings."""
        self.entries, self.index = [], {}
        ccy_ok = "(%(ccy)s::text IS NULL OR a.currency = %(ccy)s)"
        args = {"ccy": currency}
        cur.execute("""SELECT payee, category, money_out, vendor, vendor_ref FROM payee_correction
                       WHERE %(ccy)s::text IS NULL OR currency IS NULL OR currency = %(ccy)s;""", args)
        for desc, cat, out, vendor, ref in cur.fetchall():
            self._add(1, desc, out, cat, vendor, ref, 1)
        cur.execute("""SELECT sl.description, sl.amount, a.type, bt.category, bt.counterparty, bt.counterparty_ref
                       FROM match m JOIN match_statement_line msl ON msl.match_id=m.match_id
                       JOIN statement_line sl ON sl.line_id=msl.line_id
                       JOIN match_book_txn mbt ON mbt.match_id=m.match_id JOIN book_txn bt ON bt.txn_id=mbt.txn_id
                       JOIN account a ON a.account_id=bt.account_id
                       WHERE m.status='confirmed' AND (m.match_type IN ('exact','fuzzy') OR m.created_by='user')
                         AND bt.category IS NOT NULL AND NOT bt.is_deleted AND """ + ccy_ok + ";", args)
        for desc, amt, atype, cat, cp, ref in cur.fetchall():
            self._add(2, desc, _money_out(amt, atype), cat, cp, ref, 1)
        cur.execute("""SELECT bt.counterparty, bt.category, bt.counterparty_ref, a.type, bt.amount > 0, count(*)
                       FROM book_txn bt JOIN account a ON a.account_id=bt.account_id
                       WHERE bt.category IS NOT NULL AND coalesce(trim(bt.counterparty),'') <> '' AND NOT bt.is_deleted
                         AND """ + ccy_ok + " GROUP BY 1, 2, 3, 4, 5;", args)
        for cp, cat, ref, atype, pos, n in cur.fetchall():
            self._add(3, cp, pos if atype == "credit_card" else not pos, cat, cp, ref, n)

    def _add(self, tier, text, out, cat, payee, ref, n):
        toks = _mtokens(text)
        if not toks or not cat:
            return
        i = len(self.entries)
        self.entries.append((tier, toks, out, cat, payee, ref, n, text))
        for t in set(toks):
            self.index.setdefault(t[:3], []).append(i)

    def suggest(self, desc, money_out):
        toks = _mtokens(desc)
        if not toks:
            return None
        cands = set()
        for t in toks:
            cands.update(self.index.get(t[:3], ()))
        tiers = {}
        for i in cands:
            tier, etoks, out, cat, payee, ref, n, text = self.entries[i]
            if out is not None and out != money_out:
                continue
            sc = _similarity(toks, etoks)
            if sc >= LEARN_MIN_SCORE:
                tiers.setdefault(tier, []).append((sc, self.entries[i]))
        for tier in (1, 2, 3):
            hits = tiers.get(tier)
            if not hits:
                continue
            votes, best, count = Counter(), {}, Counter()
            for sc, e in hits:
                votes[e[3]] += sc * sc * e[6]
                count[e[3]] += e[6]
                if e[3] not in best or sc > best[e[3]][0]:
                    best[e[3]] = (sc, e)
            cat, v = votes.most_common(1)[0]
            sc, e = best[cat]
            payees = Counter()
            for s2, e2 in hits:
                if e2[3] == cat and e2[4]:
                    payees[(e2[4], e2[5])] += s2 * e2[6]
            (payee, ref) = payees.most_common(1)[0][0] if payees else (None, None)
            n = count[cat]
            because = (f"{TIER_TEXT[tier]} to {cat}" + (f" ({n}×)" if n > 1 else "")
                       + (f" — like '{e[7][:40]}'" if tier < 3 else f" — '{e[7][:40]}'"))
            return {"cat": cat, "conf": round(v / sum(votes.values()) * sc, 2), "payee": payee, "payee_ref": ref,
                    "because": because, "tier": tier}
        return None


POST_EXCLUDE = {"Bank", "Credit Card", "Accounts Receivable", "Accounts Payable"}


def load_coa(cur):
    """Postable accounts from the cached chart of accounts."""
    cur.execute("SELECT qbo_id, name, fqn, account_type FROM qbo_coa WHERE coalesce(active, true) ORDER BY fqn;")
    return [{"id": i, "name": n, "fqn": f or n, "type": t} for i, n, f, t in cur.fetchall() if t not in POST_EXCLUDE]


def resolve_coa(coa, category):
    """QuickBooks refs carry either the full name (Automobile:Fuel) or the leaf name."""
    if not category:
        return None
    for key in ("fqn", "name"):
        for a in coa:
            if a[key] == category:
                return a
    leaf = category.split(":")[-1]
    return next((a for a in coa if a["name"] == leaf), None)


def transfer_targets(cur, acct_qbo, atype, currency):
    """Your other bank (and card) accounts a line can be recorded as a transfer to or from.
    Same currency only -- UGX money moves between UGX accounts, USD between USD. A card is
    paid from a bank, so card lines only offer banks."""
    types = ["Bank"] if atype == "credit_card" else ["Bank", "Credit Card"]
    cur.execute("""SELECT qbo_id, name, fqn, account_type FROM qbo_coa
                   WHERE coalesce(active, true) AND account_type = ANY(%s) AND qbo_id <> %s
                     AND (%s::text IS NULL OR currency IS NULL OR currency = %s) ORDER BY fqn;""",
                (types, acct_qbo or "", currency, currency))
    return [{"id": i, "name": n, "fqn": f or n, "type": t, "xfer": True} for i, n, f, t in cur.fetchall()]


def transfer_ends(this_qbo, other_qbo, amount, atype):
    """(from, to) QuickBooks ids for a transfer that explains `amount` on this account's statement."""
    return (this_qbo, other_qbo) if _money_out(amount, atype) else (other_qbo, this_qbo)


def qbo_post(token, entity, body):
    req = urllib.request.Request(f"{QBO_BASE}/v3/company/{qbo_realm()}/{entity.lower()}",
                                 data=json.dumps(body).encode(), method="POST")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def qbo_record_line(token, acct_qbo, atype, money_out, target_id, amount_abs, txn_date, desc, payee, payee_ref):
    """Create the QuickBooks transaction for one bank line. Returns (entity, new id, payee_ref used)."""
    note = (f"{payee} — " if payee else "") + f"Recorded from bank reconciliation: {desc}"
    amt = float(amount_abs)
    if money_out:
        entity = "Purchase"
        body = {"AccountRef": {"value": acct_qbo}, "PaymentType": "CreditCard" if atype == "credit_card" else "Cash",
                "TxnDate": str(txn_date), "PrivateNote": note[:4000],
                "Line": [{"DetailType": "AccountBasedExpenseLineDetail", "Amount": amt, "Description": desc[:4000],
                          "AccountBasedExpenseLineDetail": {"AccountRef": {"value": target_id}}}]}
        if payee_ref and ":" in payee_ref:
            rtype, rid = payee_ref.split(":", 1)
            if rtype in ("Vendor", "Customer", "Employee"):
                body["EntityRef"] = {"value": rid, "type": rtype}
    else:
        entity = "Deposit"
        body = {"DepositToAccountRef": {"value": acct_qbo}, "TxnDate": str(txn_date), "PrivateNote": note[:4000],
                "Line": [{"DetailType": "DepositLineDetail", "Amount": amt, "Description": desc[:4000],
                          "DepositLineDetail": {"AccountRef": {"value": target_id}}}]}
    try:
        res = qbo_post(token, entity, body)
    except urllib.error.HTTPError as e:
        # A 400 means nothing was created. If a payee was attached (it may since have been made
        # inactive or merged), record the line without it rather than fail.
        if e.code == 400 and "EntityRef" in body:
            body.pop("EntityRef")
            res = qbo_post(token, entity, body)
        else:
            raise
    return entity, str((res.get(entity) or {}).get("Id") or ""), payee_ref if "EntityRef" in body else None


def qbo_record_transfer(token, from_qbo, to_qbo, amount_abs, txn_date, desc):
    """One QuickBooks Transfer between two of your own accounts. Returns (entity, new id, the entity)."""
    body = {"FromAccountRef": {"value": from_qbo}, "ToAccountRef": {"value": to_qbo}, "Amount": float(amount_abs),
            "TxnDate": str(txn_date), "PrivateNote": f"Recorded from bank reconciliation: {desc}"[:4000]}
    res = qbo_post(token, "Transfer", body)
    ent = res.get("Transfer") or {}
    return "Transfer", str(ent.get("Id") or ""), ent or body


def store_transfer(cur, new_id, ent, txn_date, desc, names):
    """Put a just-recorded Transfer into the books of every tracked account it touches, exactly as
    the next sync will (same key, so no duplicate). Returns {account_uuid: txn_id}."""
    ent = {**ent, "FromAccountRef": {**ent.get("FromAccountRef", {}), "name": names.get(ent["FromAccountRef"]["value"])},
           "ToAccountRef": {**ent.get("ToAccountRef", {}), "name": names.get(ent["ToAccountRef"]["value"])}}
    ids = [ent["FromAccountRef"]["value"], ent["ToAccountRef"]["value"]]
    cur.execute("SELECT account_id, source_account_id, type, currency FROM account WHERE source_account_id = ANY(%s);", (ids,))
    out = {}
    for acct_uuid, qid, atype, ccy in cur.fetchall():
        amt, who, _note, cat = _h_transfer(ent, qid, atype)
        cur.execute("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount,
                          currency, description, counterparty, category, cleared_status, last_modified)
                       VALUES (%s,%s,%s,'Transfer',%s,%s,%s,%s,%s,%s,'unknown',now())
                       ON CONFLICT (account_id, source_txn_type, source_txn_id) DO NOTHING RETURNING txn_id;""",
                    (ORG_ID, acct_uuid, new_id, txn_date, amt, ccy or "USD", desc, who, cat))
        r = cur.fetchone()
        if r:
            out[str(acct_uuid)] = str(r[0])
    return out


def rematch_open(cur, account_uuids):
    """Re-run matching on these accounts' latest statements, unless signed off (leave those alone)."""
    for a in account_uuids:
        s = _latest_statement(cur, a)
        if s and not s[3]:
            run_matcher(s[0])


DUP_WINDOW_DAYS = 45   # how far apart a bank line and its QuickBooks twin can plausibly be dated


def possible_duplicates(cur, acct_uuid, lines):
    """{line_id: [...]} QuickBooks transactions that look like the same money as these unmatched
    bank lines -- same amount within DUP_WINDOW_DAYS and not matched to anything yet. Recording
    such a line would put a duplicate in the books."""
    if not lines:
        return {}
    cur.execute("""WITH un(line_id, d, amt) AS (SELECT * FROM unnest(%s::uuid[], %s::date[], %s::numeric[]))
                   SELECT un.line_id, bt.txn_id, bt.posted_date, bt.amount, coalesce(bt.counterparty, bt.description,'')
                   FROM un JOIN book_txn bt ON bt.account_id=%s AND bt.amount=un.amt
                        AND bt.posted_date BETWEEN un.d - %s AND un.d + %s
                   WHERE NOT bt.is_deleted AND NOT bt.is_void
                     AND NOT EXISTS (SELECT 1 FROM match_book_txn mbt JOIN match m ON m.match_id=mbt.match_id
                                     WHERE mbt.txn_id=bt.txn_id AND m.status='confirmed')
                   ORDER BY abs(bt.posted_date - un.d);""",
                ([str(l[0]) for l in lines], [l[1] for l in lines], [l[2] for l in lines], acct_uuid,
                 DUP_WINDOW_DAYS, DUP_WINDOW_DAYS))
    out = {}
    for lid, tid, d, a, who in cur.fetchall():
        out.setdefault(str(lid), []).append({"txn_id": str(tid), "date": d, "amount": a, "who": who})
    return out


def match_sides(cur, match_ids):
    """({match_id: [(date, amount, who)]} for statement lines, same for book txns)."""
    sls, bts = {}, {}
    ids = [str(m) for m in match_ids]
    if ids:
        cur.execute("""SELECT msl.match_id, sl.posted_date, sl.amount, coalesce(sl.counterparty, sl.description,'')
                       FROM match_statement_line msl JOIN statement_line sl ON sl.line_id=msl.line_id
                       WHERE msl.match_id = ANY(%s::uuid[]) ORDER BY sl.posted_date;""", (ids,))
        for mid, d, a, w in cur.fetchall():
            sls.setdefault(str(mid), []).append((d, a, w))
        cur.execute("""SELECT mbt.match_id, bt.posted_date, bt.amount, coalesce(bt.counterparty, bt.description,'')
                       FROM match_book_txn mbt JOIN book_txn bt ON bt.txn_id=mbt.txn_id
                       WHERE mbt.match_id = ANY(%s::uuid[]) ORDER BY bt.posted_date;""", (ids,))
        for mid, d, a, w in cur.fetchall():
            bts.setdefault(str(mid), []).append((d, a, w))
    return sls, bts


def _claim_writeback(line_id, user):
    """Reserve a line for writing. Refuses if it's already done or an attempt is in flight --
    an interrupted attempt stays blocked until someone confirms it didn't reach QuickBooks."""
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""INSERT INTO writeback_log (line_id, status, created_by) VALUES (%s, 'pending', %s)
                   ON CONFLICT (line_id) DO UPDATE SET status='pending', error=NULL, created_at=now(),
                     created_by=EXCLUDED.created_by
                   WHERE writeback_log.status='failed' RETURNING line_id;""", (line_id, user))
    ok = cur.fetchone() is not None
    conn.commit(); cur.close(); conn.close()
    return ok


# ---------------- QuickBooks write-back (expenses + deposits) ----------------
def qbo_book_balance_at(token, acct_uuid, acct_qbo, as_of):
    """Register balance of a bank/card account at the end of `as_of`, in the account's currency.

    QuickBooks only exposes the *current* balance per account, so step back from it by the synced
    transactions dated after `as_of`. That's exact as long as sync is fresh and reaches back to
    `as_of` -- the caller syncs first, and this refuses dates before the sync window.
    """
    since = _sync_since()
    if since and str(as_of) < since:
        raise ValueError(f"The period ends before the sync window ({since}). Enter the book balance by hand, "
                         f"or raise SYNC_MONTHS.")
    q = f"SELECT * FROM Account WHERE Id = '{acct_qbo}'"
    req = urllib.request.Request(f"{QBO_BASE}/v3/company/{qbo_realm()}/query?query=" + urllib.parse.quote(q))
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/json")
    with urllib.request.urlopen(req) as resp:
        accts = json.loads(resp.read()).get("QueryResponse", {}).get("Account", [])
    if not accts:
        raise ValueError("QuickBooks didn't return this account.")
    current = _D(accts[0].get("CurrentBalance"))
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""SELECT coalesce(sum(amount),0) FROM book_txn
                   WHERE account_id=%s AND posted_date > %s AND source_txn_type <> 'CSV'
                     AND coalesce(is_void,false)=false AND coalesce(is_deleted,false)=false;""", (acct_uuid, as_of))
    later = cur.fetchone()[0]
    cur.close(); conn.close()
    return current - later


# ---------------- shared styling ----------------
CSS = """<style>
:root{
  --bg:#eef1f5;--panel:#fff;--ink:#16202e;--muted:#667085;
  --line:#e4e7ec;--line-soft:#eef0f3;
  --accent:#0f766e;--accent-soft:#d6efea;
  --ok:#047857;--ok-soft:#d7f3e3;--warn:#b45309;--warn-soft:#fbedcf;
  --bad:#b42318;--bad-soft:#fbe2de;--none:#667085;--none-soft:#ebedf0;
  --radius:14px;--shadow:0 1px 2px rgba(16,24,40,.04),0 2px 6px rgba(16,24,40,.04),0 10px 28px rgba(16,24,40,.05);
  --lift:0 14px 32px rgba(16,24,40,.13);
}
*{box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;background:radial-gradient(1100px 520px at 85% -8%,rgba(15,118,110,.07),transparent 60%),linear-gradient(180deg,#eff3f8 0%,#e6ecf3 65%,#e1e8f0 100%);background-attachment:fixed;min-height:100vh;color:var(--ink);margin:0;font-size:15px;line-height:1.5;-webkit-font-smoothing:antialiased}
a{color:inherit;text-decoration:none}
.nav{background:rgba(255,255,255,.82);backdrop-filter:saturate(180%) blur(12px);-webkit-backdrop-filter:saturate(180%) blur(12px);border-bottom:1px solid var(--line);padding:15px 24px;display:flex;justify-content:space-between;align-items:center;position:sticky;top:0;z-index:5;box-shadow:0 1px 0 rgba(16,24,40,.03)}
.nav .brand{font-weight:650;letter-spacing:-.01em;display:flex;align-items:center;gap:9px}
.nav .brand .dot{width:9px;height:9px;border-radius:50%;background:var(--accent);box-shadow:0 0 0 3px var(--accent-soft)}
.nav .links a{color:var(--muted);font-size:14px;margin-left:18px}
.nav .links a:hover{color:var(--ink)}
.wrap{max-width:1000px;margin:0 auto;padding:34px 24px 64px}
h1{font-size:26px;font-weight:680;letter-spacing:-.02em;margin:0 0 5px}
h2{font-size:15px;font-weight:650;letter-spacing:-.01em;color:var(--ink);margin:30px 0 12px;border-radius:7px;padding:2px 6px;margin-left:-6px}
.sub{color:var(--muted);margin:0 0 26px;font-size:14px}
.btn{background:linear-gradient(180deg,#243244,#16202e);color:#fff;border:none;padding:10px 18px;border-radius:10px;cursor:pointer;font-size:14px;font-weight:550;box-shadow:0 1px 2px rgba(16,24,40,.24),0 2px 8px rgba(16,24,40,.14);transition:transform .15s ease,box-shadow .15s ease,filter .15s ease}
.btn:hover{transform:translateY(-1px);box-shadow:0 3px 10px rgba(16,24,40,.28),0 6px 18px rgba(16,24,40,.16);filter:brightness(1.07)}
.btn:active{transform:translateY(0)}
.btn-go{background:linear-gradient(180deg,#059669,#047857);color:#fff;border:none;padding:9px 17px;border-radius:10px;cursor:pointer;font-size:14px;font-weight:550;box-shadow:0 1px 2px rgba(4,120,87,.3),0 2px 8px rgba(4,120,87,.18);transition:transform .15s ease,filter .15s ease}
.btn-go:hover{transform:translateY(-1px);filter:brightness(1.07)}
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
.recgrid{display:grid;grid-template-columns:1fr 1fr;gap:16px}
.recgrid table{margin:0}
.rec td{white-space:normal}
.rec tr.tot td{font-weight:650;background:#fafbfc;border-top:1px solid var(--line)}
.rec .src{color:var(--muted);font-size:12px;font-weight:400}
.recres{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;margin:14px 0 8px;padding:14px 18px;border-radius:var(--radius);font-weight:600;font-variant-numeric:tabular-nums}
.recres.balanced{background:var(--ok-soft);color:var(--ok)}
.recres.out{background:var(--bad-soft);color:var(--bad)}
.recres.incomplete{background:var(--none-soft);color:var(--none)}
.recnote{font-size:13px;padding:9px 13px;border-radius:9px;margin:8px 0;line-height:1.5}
.recnote.bad{background:var(--bad-soft);color:var(--bad)}
.recnote.warn{background:#fffbeb;color:#92400e;border:1px solid #fde68a}
.balform{display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end;margin:12px 0 4px}
.balform label{display:block;font-size:12px;color:var(--muted);margin-bottom:4px}
.balform input{width:170px;padding:7px 9px;border:1px solid var(--line);border-radius:7px;font-size:13px;font-variant-numeric:tabular-nums}
.tag.bf{background:var(--none-soft);color:var(--none)}
.tag.pending{background:var(--warn-soft);color:var(--warn)}
.hint{color:var(--muted);font-size:12px;line-height:1.4;white-space:normal}
.rectbl td{vertical-align:top}
.rectbl select,.rectbl input.payee{padding:6px 8px;border:1px solid var(--line);border-radius:7px;font-size:13px;max-width:230px;background:#fff}
.rectbl select{width:230px}.rectbl input.payee{width:150px}
.booksrc{display:flex;gap:10px;align-items:center;flex-wrap:wrap;font-size:14px}
.btnrow{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.dupwarn{margin-top:6px;padding:7px 9px;border-radius:7px;background:#fffbeb;border:1px solid #fde68a;color:#92400e;font-size:12.5px;line-height:1.45;white-space:normal}
.dupwarn .btn-sm{margin-top:5px}
.mmgrid{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.mmcol{background:var(--panel);border:1px solid var(--line);border-radius:var(--radius);box-shadow:var(--shadow);overflow:hidden}
.mmhead{padding:10px 12px;border-bottom:1px solid var(--line-soft);font-size:11px;text-transform:uppercase;letter-spacing:.04em;font-weight:600;color:var(--muted);display:flex;gap:8px;align-items:center;justify-content:space-between}
.mmsearch{padding:5px 8px;border:1px solid var(--line);border-radius:7px;font-size:12.5px;width:55%;text-transform:none;letter-spacing:0}
.mmlist{max-height:340px;overflow:auto}
.mmrow{display:grid;grid-template-columns:22px 86px 1fr auto;gap:8px;align-items:center;padding:8px 12px;border-bottom:1px solid var(--line-soft);font-size:13px;cursor:pointer}
.mmrow:hover{background:#f7f9fb}.mmrow.on{background:var(--accent-soft)}
.mmrow .mmw{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mmrow .a{font-variant-numeric:tabular-nums}
.mmbar{position:sticky;bottom:0;display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;margin:12px 0 26px;padding:12px 16px;background:var(--panel);border:1px solid var(--line);border-radius:var(--radius);box-shadow:var(--shadow);font-size:14px;font-variant-numeric:tabular-nums}
.mmbar .ok{color:var(--ok);font-weight:600}.mmbar .warn{color:var(--warn);font-weight:600}
@media (max-width:760px){.mmgrid{grid-template-columns:1fr}}
.ok{color:var(--ok)}.warn{color:var(--warn)}.bad{color:var(--bad)}.muted{color:var(--muted)}
@media (max-width:760px){
  .tiles{grid-template-columns:repeat(2,1fr)}
  .cards{grid-template-columns:repeat(2,1fr)}
  .recgrid{grid-template-columns:1fr}
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
.appfoot{max-width:1000px;margin:0 auto;padding:22px 24px 44px;color:#9ca3af;font-size:13px;text-align:center}.appfoot a{color:#6b7280;font-weight:500}.appfoot a:hover{color:var(--accent)}
.pw-wrap{position:relative}
.pw-wrap input{padding-right:42px !important}
.pw-toggle{position:absolute;right:5px;top:50%;transform:translateY(-50%);width:auto;height:auto;margin:0;padding:6px;background:none;border:none;border-radius:6px;cursor:pointer;color:var(--muted);display:flex}
.pw-toggle:hover{color:var(--ink)}
.pw-toggle:focus-visible{outline:2px solid var(--accent);outline-offset:1px}
</style>"""

# Reusable show/hide-password eye icon: EYE_ICON = "click to reveal" state, EYE_OFF_ICON = "click to hide" state.
EYE_ICON = '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M1 12s4-7 11-7 11 7 11 7-4 7-11 7-11-7-11-7Z"/><circle cx="12" cy="12" r="3"/></svg>'
EYE_OFF_ICON = '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M17.94 17.94A10.94 10.94 0 0 1 12 19c-7 0-11-7-11-7a21.27 21.27 0 0 1 5.06-5.94M9.9 4.24A10.94 10.94 0 0 1 12 4c7 0 11 7 11 7a21.27 21.27 0 0 1-4.22 5.06"/><path d="M14.12 14.12a3 3 0 1 1-4.24-4.24"/><path d="M1 1l22 22"/></svg>'
PW_TOGGLE_JS = ("function togglePw(btn,id){var i=document.getElementById(id);if(!i)return;"
                 "var showing=i.type==='text';i.type=showing?'password':'text';"
                 "btn.setAttribute('aria-pressed',showing?'false':'true');"
                 "btn.setAttribute('aria-label',showing?'Show password':'Hide password');"
                 "btn.innerHTML=showing?" + repr(EYE_ICON) + ":" + repr(EYE_OFF_ICON) + ";}")

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
.pw-wrap{position:relative}
.pw-wrap input{padding-right:42px}
.pw-toggle{position:absolute;right:5px;top:50%;transform:translateY(-50%);width:auto;margin:0;padding:6px;background:none;border:none;border-radius:6px;cursor:pointer;color:#98a2b3;display:flex}
.pw-toggle:hover{color:#475467}
.pw-toggle:focus-visible{outline:2px solid #0f766e;outline-offset:1px}
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
<label for=un>Username</label>
<input id=un type=text name=username placeholder="Your username" autocapitalize=off autofocus>
<label for=pw style="display:block;margin-top:16px">Password</label>
<div class=pw-wrap>
<input id=pw type=password name=password placeholder="Enter your password">
<button type=button class=pw-toggle onclick="togglePw(this,'pw')" aria-label="Show password" aria-pressed="false">""" + EYE_ICON + """</button>
</div>
<button type=submit>Sign in</button>
{% if error %}<div class=err>{{ error }}</div>{% endif %}
</form>
<details style="margin-top:14px"><summary style="cursor:pointer;color:#667085;font-size:13px">Forgot password?</summary><div style="color:#98a2b3;font-size:12.5px;margin-top:8px;line-height:1.55">The password originally set up for this app still works as a recovery key. Sign in with that, then change your password from the menu.</div></details>
<div class=foot>Private · access by password<br><a href="{{ url_for('terms') }}" style="color:#98a2b3">Terms</a> · <a href="{{ url_for('privacy') }}" style="color:#98a2b3">Privacy</a> · <a href="mailto:{{ contact_email }}" style="color:#98a2b3">Contact</a></div>
</div>
<script>""" + PW_TOGGLE_JS + """</script>
</body></html>"""


@app.context_processor
def _inject_contact():
    return {"contact_email": CONTACT_EMAIL, "sync_banner": sync_banner}


@app.template_filter("acct")
def _acct(x):
    """Accounting style: negatives in brackets."""
    if x is None:
        return ""
    try:
        v = Decimal(x)
    except Exception:
        return str(x)
    return f"({abs(v):,.2f})" if v < 0 else f"{v:,.2f}"


@app.template_filter("money")
def _money(x):
    if x is None:
        return ""
    try:
        return f"{Decimal(x):,.2f}"
    except Exception:
        return str(x)


LEGAL_STYLE = """<style>
*{box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;color:#1f2937;background:#f7f8fa;margin:0;line-height:1.65;font-size:16px}
.legal-nav{background:#fff;border-bottom:1px solid #e5e7eb;padding:16px 24px;font-weight:650;display:flex;align-items:center;gap:9px}
.legal-nav .dot{width:9px;height:9px;border-radius:50%;background:#0f766e;box-shadow:0 0 0 3px #d6efea}
.legal-wrap{max-width:760px;margin:0 auto;padding:40px 24px 80px}
.legal-wrap h1{font-size:30px;letter-spacing:-.02em;margin:0 0 6px}
.legal-wrap .updated{color:#6b7280;font-size:14px;margin-bottom:32px}
.legal-wrap h2{font-size:19px;margin:34px 0 10px;letter-spacing:-.01em}
.legal-wrap p,.legal-wrap li{color:#374151}
.legal-wrap ul{padding-left:20px}
.legal-wrap li{margin:6px 0}
.legal-wrap a{color:#0f766e}
.legal-foot{color:#9ca3af;font-size:13px;margin-top:40px;border-top:1px solid #e5e7eb;padding-top:20px}
</style>"""

PRIVACY_PAGE = """<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Privacy Policy · Reconciliation Tool</title>""" + LEGAL_STYLE + """</head><body>
<div class=legal-nav><span class=dot></span>Reconciliation Tool</div>
<div class=legal-wrap>
<h1>Privacy Policy</h1>
<div class=updated>Last updated: 20 July 2026</div>

<p>This Privacy Policy explains how the Reconciliation Tool (“the app”, “we”) collects, uses, stores, and protects information when you use it to reconcile bank statements against your QuickBooks Online accounting records.</p>

<h2>Information we access and collect</h2>
<ul>
<li><b>QuickBooks Online data.</b> With your explicit authorization through Intuit’s secure OAuth process, the app accesses accounting data from your connected QuickBooks Online company — such as transactions, accounts, payees, and related records — only as needed to perform reconciliation and, at your direction, to record transactions back to your books.</li>
<li><b>Bank statement data you provide.</b> Files you upload (CSV or OFX), containing dates, descriptions, and amounts.</li>
<li><b>Account access.</b> A password you set to sign in to the app.</li>
</ul>

<h2>How we use your information</h2>
<p>We use your information solely to provide the reconciliation service: matching your bank statement against your books, highlighting discrepancies, and — only when you choose to — creating corresponding transactions in your QuickBooks Online company. We do not sell, rent, or share your data with third parties for marketing, advertising, or any unrelated purpose.</p>

<h2>How your information is stored and protected</h2>
<ul>
<li>Data is stored in a hosted PostgreSQL database (Supabase) and the application runs on a hosted platform (Render).</li>
<li>Connections to QuickBooks Online and to the app are encrypted in transit (HTTPS/TLS).</li>
<li>QuickBooks access and refresh tokens are stored securely and used only to connect to the QuickBooks company you authorized.</li>
<li>Access to the app is protected by a password.</li>
</ul>

<h2>Data retention and deletion</h2>
<p>You remain in control of your data. You can clear an account’s imported statements and stored transactions at any time using the app’s built-in “Clear this account’s data” function. You may also disconnect the app from QuickBooks Online at any time from within your QuickBooks account, which revokes the app’s access. To request deletion of any remaining data, contact us at the address below.</p>

<h2>Use of Intuit / QuickBooks data</h2>
<p>The app accesses QuickBooks Online data in accordance with Intuit’s API terms and security requirements. QuickBooks data is used only to provide the reconciliation service described above and for no other purpose.</p>

<h2>Third-party services</h2>
<p>The app relies on Render (application hosting) and Supabase (database hosting) to operate. These providers process data on our behalf to run the service. The app displays no advertising and does not sell data.</p>

<h2>Changes to this policy</h2>
<p>We may update this Privacy Policy from time to time. The “last updated” date above reflects the most recent revision.</p>

<h2>Contact</h2>
<p>For any questions about this Privacy Policy or your data, contact: <a href="mailto:__EMAIL__">__EMAIL__</a></p>

<div class=legal-foot>Reconciliation Tool — a tool for reconciling bank statements with QuickBooks Online.</div>
</div></body></html>"""

TERMS_PAGE = """<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Terms of Service · Reconciliation Tool</title>""" + LEGAL_STYLE + """</head><body>
<div class=legal-nav><span class=dot></span>Reconciliation Tool</div>
<div class=legal-wrap>
<h1>Terms of Service</h1>
<div class=updated>Last updated: 20 July 2026</div>

<p>These Terms of Service govern your use of the Reconciliation Tool (“the app”). By using the app, you agree to these terms.</p>

<h2>The service</h2>
<p>The app helps you reconcile bank statement transactions against your accounting records in QuickBooks Online. It identifies matches and discrepancies and, at your direction, can record transactions back to your QuickBooks Online company.</p>

<h2>Connecting QuickBooks Online</h2>
<p>You may connect the app to your QuickBooks Online company through Intuit’s authorization process. You authorize the app to access and, where you choose, write accounting data on your behalf. You can revoke this access at any time from within your QuickBooks Online account.</p>

<h2>Your responsibilities</h2>
<ul>
<li>You are responsible for the accuracy of the data you upload and for reviewing reconciliation results before relying on them.</li>
<li>You are responsible for keeping your app password secure.</li>
<li>You agree to use the app only for lawful purposes and only with accounting data you are authorized to access.</li>
</ul>

<h2>No warranty; professional advice</h2>
<p>The app is provided “as is” and “as available”, without warranties of any kind. It is a productivity aid and is not a substitute for professional accounting, bookkeeping, audit, or tax advice. You are responsible for verifying that your books are correct.</p>

<h2>Limitation of liability</h2>
<p>To the maximum extent permitted by law, we are not liable for any indirect, incidental, or consequential damages, or for any loss arising from your use of, or inability to use, the app, including any errors in reconciliation results or data written to QuickBooks Online.</p>

<h2>Intellectual property</h2>
<p>The app and its underlying code and design are the property of their owner. These terms do not grant you any ownership rights in the app.</p>

<h2>Termination</h2>
<p>You may stop using the app and disconnect it from QuickBooks Online at any time. We may suspend or discontinue the app at our discretion.</p>

<h2>Governing law</h2>
<p>These terms are governed by the laws of the Republic of Uganda.</p>

<h2>Contact</h2>
<p>For questions about these terms, contact: <a href="mailto:__EMAIL__">__EMAIL__</a></p>

<div class=legal-foot>Reconciliation Tool — a tool for reconciling bank statements with QuickBooks Online.</div>
</div></body></html>"""


@app.route("/privacy")
def privacy():
    return render_template_string(PRIVACY_PAGE.replace("__EMAIL__", CONTACT_EMAIL))


@app.route("/terms")
def terms():
    return render_template_string(TERMS_PAGE.replace("__EMAIL__", CONTACT_EMAIL))


USERS_PAGE = """<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Users · Reconciliation Tool</title>""" + CSS + """</head><body>
<div class=nav><span class=brand><span class=dot></span>Reconciliation Tool</span><span class=links>{% if session.name %}<span style="color:var(--muted);font-size:13px;margin-right:6px">{{ session.name }}</span>{% endif %}<a href="{{ url_for('dashboard') }}">← All accounts</a><a href="{{ url_for('logout') }}">Sign out</a></span></div>
<div class=wrap style="max-width:720px">
<h1>Users</h1>
<div class=sub>People who can sign in to this reconciliation tool.</div>
{% if msg %}<div style="background:var(--accent-soft);color:var(--accent);padding:10px 14px;border-radius:9px;font-size:14px;margin-bottom:16px">{{ msg }}</div>{% endif %}
{% if error %}<div style="background:var(--bad-soft);color:var(--bad);padding:10px 14px;border-radius:9px;font-size:14px;margin-bottom:16px">{{ error }}</div>{% endif %}
<table>
<thead><tr><th>Username</th><th>Name</th><th>Role</th><th>Added</th><th class=a></th></tr></thead>
<tbody>
{% for un, nm, adm, created in users %}<tr>
<td><b>{{ un }}</b></td><td>{{ nm }}</td><td>{{ 'Admin' if adm else 'User' }}</td>
<td>{{ created.strftime('%Y-%m-%d') if created else '' }}</td>
<td class=a><a href="{{ url_for('users') }}?edit={{ un }}" class=btn-sm style="text-decoration:none;display:inline-block;margin-right:6px">Edit</a><form method=post style="display:inline;margin:0" onsubmit="return confirm('Remove user {{ un }}?');"><input type=hidden name=action value=delete><input type=hidden name=username value="{{ un }}"><button type=submit class=btn-sm style="color:var(--bad);border-color:var(--bad-soft)">Remove</button></form></td>
</tr>{% endfor %}
{% if not users %}<tr><td colspan=5 class=muted>No named users yet. Add one below.</td></tr>{% endif %}
</tbody></table>
<h2>{% if edit_user %}Edit user{% else %}Add a user{% endif %}</h2>
<form method=post style="max-width:420px">
<input type=hidden name=action value=save>
<label style="display:block;font-size:12px;font-weight:600;color:#475467;margin:12px 0 6px;text-transform:uppercase;letter-spacing:.04em">Username</label>
<input name=username autocapitalize=off {% if edit_user %}value="{{ edit_user.username }}" readonly{% endif %} style="width:100%;padding:10px 12px;border:1px solid var(--line);border-radius:9px;font-size:15px{% if edit_user %};background:#f3f4f6;color:var(--muted){% endif %}">
<label style="display:block;font-size:12px;font-weight:600;color:#475467;margin:14px 0 6px;text-transform:uppercase;letter-spacing:.04em">Display name</label>
<input name=name value="{{ edit_user.name if edit_user else '' }}" style="width:100%;padding:10px 12px;border:1px solid var(--line);border-radius:9px;font-size:15px">
<label style="display:block;font-size:12px;font-weight:600;color:#475467;margin:14px 0 6px;text-transform:uppercase;letter-spacing:.04em">Password</label>
<input id=pwfield name=password type=password placeholder="{% if edit_user %}Leave blank to keep current{% else %}At least 6 characters{% endif %}" style="width:100%;padding:10px 12px;border:1px solid var(--line);border-radius:9px;font-size:15px">
<label style="display:flex;align-items:center;gap:6px;margin:8px 0 0;font-size:13px;color:var(--muted)"><input type=checkbox onclick="document.getElementById('pwfield').type=this.checked?'text':'password'"> Show password</label>
<label style="display:flex;align-items:center;gap:8px;margin:16px 0;font-size:14px;color:var(--ink)"><input type=checkbox name=is_admin {% if edit_user and edit_user.is_admin %}checked{% endif %}> Administrator (can manage users)</label>
<button type=submit class=btn>{% if edit_user %}Update user{% else %}Create user{% endif %}</button>
{% if edit_user %}<a href="{{ url_for('users') }}" class=btn-sm style="text-decoration:none;display:inline-block;margin-left:8px">Cancel</a>{% endif %}
</form>
<div class=sub style="margin-top:18px;font-size:13px;line-height:1.5">Sign-offs are recorded under each user's display name. The recovery password from your server settings always works as an admin — so you can't be locked out.</div>
</div></body></html>"""


ACCOUNTS_TEMPLATE = """<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Accounts · Reconciliation Tool</title>""" + CSS + """</head><body>
<div class=nav><span class=brand><span class=dot></span>Reconciliation Tool</span><span class=links>{% if session.name %}<span style="color:var(--muted);font-size:13px;margin-right:6px">{{ session.name }}</span>{% endif %}<a href="{{ url_for('dashboard') }}">← All accounts</a><a href="{{ url_for('users') }}">Users</a><a href="{{ url_for('backup') }}">Backup</a><a href="{{ url_for('logout') }}">Sign out</a></span></div>
<div class=wrap><h1>Accounts</h1>
<div class=sub>QuickBooks has {{ rows|length }} bank and credit-card accounts. Tick only the ones you actually reconcile — the rest stay synced but stop loading on the dashboard.</div>
{% if msg %}<div style="background:var(--accent-soft);color:var(--accent);padding:11px 14px;border-radius:9px;font-size:14px;margin-bottom:18px;font-weight:550">{{ msg }}</div>{% endif %}
<div style="background:#fffbeb;border:1px solid #fde68a;color:#92400e;padding:10px 13px;border-radius:9px;font-size:13px;margin:0 0 18px;line-height:1.5">Hiding an account never deletes anything. Its transactions and reconciliation history stay in the database — tick it again any time to bring it back.</div>
<form method=post>
<div style="margin-bottom:12px;display:flex;gap:8px;flex-wrap:wrap">
<button type=button class=btn-sm onclick="document.querySelectorAll('.acc').forEach(c=>c.checked=true)">Select all</button>
<button type=button class=btn-sm onclick="document.querySelectorAll('.acc').forEach(c=>c.checked=false)">Select none</button>
<button type=button class=btn-sm onclick="document.querySelectorAll('.acc').forEach(c=>{c.checked=c.dataset.hastxn=='1'})">Only ones with data</button>
</div>
<table><tr><th style="width:70px">Show</th><th>Account</th><th>Type</th><th>Currency</th><th style="text-align:right">Transactions</th></tr>
{% for r in rows %}<tr>
<td><input type=checkbox class=acc name=active value="{{ r.id }}" data-hastxn="{{ '1' if r.n else '0' }}" {% if r.active %}checked{% endif %}></td>
<td>{{ r.name }}</td><td>{{ 'Credit card' if r.type=='credit_card' else 'Bank' }}</td>
<td>{{ r.currency or '' }}</td><td style="text-align:right">{{ r.n }}</td></tr>{% endfor %}
</table>
<div style="margin-top:18px"><button type=submit class=btn-go>Save</button>
<a href="{{ url_for('dashboard') }}" class=btn-sm style="text-decoration:none;display:inline-block;margin-left:8px">Cancel</a></div>
</form></div></body></html>"""


@app.route("/accounts", methods=["GET", "POST"])
def manage_accounts():
    if not session.get("is_admin"):
        return "Admins only. <a href='/'>Back</a>", 403
    conn = get_conn(); cur = conn.cursor()
    msg = None
    if request.method == "POST":
        keep = set(request.form.getlist("active"))
        cur.execute("SELECT account_id FROM account;")
        all_ids = [str(r[0]) for r in cur.fetchall()]
        on = [i for i in all_ids if i in keep]
        off = [i for i in all_ids if i not in keep]
        if on:
            cur.execute("UPDATE account SET is_active=true WHERE account_id::text = ANY(%s);", (on,))
        if off:
            cur.execute("UPDATE account SET is_active=false WHERE account_id::text = ANY(%s);", (off,))
        conn.commit()
        msg = f"Saved — {len(on)} account{'' if len(on)==1 else 's'} showing, {len(off)} hidden."
    # one grouped query for counts, rather than one per account
    cur.execute("SELECT account_id, count(*) FROM book_txn GROUP BY account_id;")
    counts = {str(a): n for a, n in cur.fetchall()}
    cur.execute("SELECT account_id, name, type, currency, coalesce(is_active,true) FROM account ORDER BY type, name;")
    rows = [{"id": str(a), "name": n, "type": t, "currency": c, "active": act,
             "n": counts.get(str(a), 0)} for a, n, t, c, act in cur.fetchall()]
    cur.close(); conn.close()
    return render_template_string(ACCOUNTS_TEMPLATE, rows=rows, msg=msg)


@app.route("/users", methods=["GET", "POST"])
def users():
    if not session.get("is_admin"):
        return "Admins only. <a href='/'>Back</a>", 403
    error = msg = None
    edit_user = None
    if request.method == "POST":
        action = request.form.get("action")
        if action == "save":
            un = (request.form.get("username") or "").strip().lower()
            nm = (request.form.get("name") or "").strip()
            pw = request.form.get("password") or ""
            adm = request.form.get("is_admin") == "on"
            if not un:
                error = "Username is required."
            elif get_user(un):
                update_user(un, nm, adm, pw or None)
                msg = f"User '{un}' updated."
            elif len(pw) < 6:
                error = "New users need a password of at least 6 characters."
            else:
                add_user(un, nm, pw, adm); msg = f"User '{un}' created."
        elif action == "delete":
            un = (request.form.get("username") or "").strip().lower()
            if un == session.get("username"):
                error = "You can't delete the account you're signed in with."
            else:
                delete_user(un); msg = f"User '{un}' removed."
    else:
        eu = (request.args.get("edit") or "").strip().lower()
        if eu:
            row = get_user(eu)
            if row:
                edit_user = {"username": row[0], "name": row[1], "is_admin": row[3]}
    return render_template_string(USERS_PAGE, users=list_users(), error=error, msg=msg, edit_user=edit_user)


BACKUP_TABLES = ["account", "statement", "statement_line", "book_txn", "match",
                 "match_statement_line", "match_book_txn", "payee_correction",
                 "settings", "app_users", "app_config", "qbo_auth"]


def _bk_default(o):
    if isinstance(o, Decimal):
        return str(o)
    if hasattr(o, "isoformat"):
        return o.isoformat()
    return str(o)


@app.route("/backup")
def backup():
    if not session.get("is_admin"):
        return "Admins only. <a href='/'>Back</a>", 403
    conn = get_conn(); cur = conn.cursor()
    out = {"exported_at": datetime.now(timezone.utc).isoformat(), "app": "reconciliation-tool", "tables": {}}
    for t in BACKUP_TABLES:
        try:
            cur.execute("SELECT * FROM " + t + ";")
            cols = [d[0] for d in cur.description]
            out["tables"][t] = [dict(zip(cols, r)) for r in cur.fetchall()]
        except Exception:
            conn.rollback()
            out["tables"][t] = None
    cur.close(); conn.close()
    data = json.dumps(out, default=_bk_default, indent=2)
    fname = "reconciliation_backup_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M") + ".json"
    return Response(data, mimetype="application/json",
                    headers={"Content-Disposition": "attachment; filename=" + fname})


@app.route("/schema-dump")
def schema_dump():
    """Admin-only, read-only introspection of the live schema — a stand-in for a migrations file."""
    if not session.get("is_admin"):
        return "Admins only. <a href='/'>Back</a>", 403
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""SELECT table_name FROM information_schema.tables
                   WHERE table_schema='public' AND table_type='BASE TABLE' ORDER BY table_name;""")
    tables = [r[0] for r in cur.fetchall()]
    out = [f"-- Schema dump generated by /schema-dump on {datetime.now(timezone.utc).isoformat()}", ""]
    for t in tables:
        cur.execute("""SELECT column_name, data_type, is_nullable, column_default
                       FROM information_schema.columns
                       WHERE table_schema='public' AND table_name=%s ORDER BY ordinal_position;""", (t,))
        cols = cur.fetchall()
        cur.execute("""SELECT kcu.column_name FROM information_schema.table_constraints tc
                       JOIN information_schema.key_column_usage kcu
                         ON tc.constraint_name=kcu.constraint_name AND tc.table_schema=kcu.table_schema
                       WHERE tc.table_schema='public' AND tc.table_name=%s AND tc.constraint_type='PRIMARY KEY'
                       ORDER BY kcu.ordinal_position;""", (t,))
        pk = [r[0] for r in cur.fetchall()]
        cur.execute("""SELECT kcu.column_name, ccu.table_name, ccu.column_name
                       FROM information_schema.table_constraints tc
                       JOIN information_schema.key_column_usage kcu
                         ON tc.constraint_name=kcu.constraint_name AND tc.table_schema=kcu.table_schema
                       JOIN information_schema.constraint_column_usage ccu
                         ON tc.constraint_name=ccu.constraint_name AND tc.table_schema=ccu.table_schema
                       WHERE tc.table_schema='public' AND tc.table_name=%s AND tc.constraint_type='FOREIGN KEY';""", (t,))
        fks = cur.fetchall()
        out.append(f"CREATE TABLE {t} (")
        lines = []
        for name, dtype, nullable, default in cols:
            bits = [f"  {name} {dtype}"]
            if nullable == "NO":
                bits.append("NOT NULL")
            if default:
                bits.append(f"DEFAULT {default}")
            lines.append(" ".join(bits))
        if pk:
            lines.append(f"  PRIMARY KEY ({', '.join(pk)})")
        out.append(",\n".join(lines))
        out.append(");")
        for col, ftable, fcol in fks:
            out.append(f"-- FK: {t}.{col} -> {ftable}.{fcol}")
        cur.execute("""SELECT indexname, indexdef FROM pg_indexes
                       WHERE schemaname='public' AND tablename=%s AND indexname NOT LIKE %s;""", (t, "%_pkey"))
        for iname, idef in cur.fetchall():
            out.append(f"-- {idef};")
        out.append("")
    cur.close(); conn.close()
    return Response("\n".join(out), mimetype="text/plain")


@app.route("/qbo-info")
def qbo_info():
    if not session.get("is_admin"):
        return "Admins only. <a href='/'>Back</a>", 403
    L = []
    L.append("QBO_BASE       = " + QBO_BASE)
    L.append("resolved realm = " + str(qbo_realm()))
    L.append("env QBO_REALM_ID = " + (os.environ.get("QBO_REALM_ID", "") or "(unset)"))
    L.append("")
    try:
        token = qbo_token()
        L.append("token: OK")
        try:
            ci = qbo_query("CompanyInfo", token)
            if ci:
                L.append("COMPANY: " + str(ci[0].get("CompanyName", "?")))
                L.append("Country: " + str(ci[0].get("Country", "?")))
            else:
                L.append("CompanyInfo: (empty)")
        except Exception as e:
            L.append("CompanyInfo error: " + str(e))
        try:
            accts = qbo_query("Account", token)
            banks = [a for a in accts if a.get("AccountType") in ("Bank", "Credit Card")]
            L.append("")
            L.append("Total accounts returned by QBO: " + str(len(accts)))
            L.append("Bank / Credit Card accounts:    " + str(len(banks)))
            for a in banks[:30]:
                L.append("  - " + str(a.get("Name")) + "  [" + str(a.get("AccountType")) + "]  id=" + str(a.get("Id")))
            # live discovery test: try to upsert one bank account and report the real error
            L.append("")
            L.append("--- discovery test ---")
            conn = get_conn(); cur = conn.cursor()
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='account';")
            cols = sorted([r[0] for r in cur.fetchall()])
            L.append("account table columns: " + ", ".join(cols))
            if banks:
                a = banks[0]
                qid = a.get("Id"); nm = a.get("Name") or ("Account " + str(qid))
                ccy = (a.get("CurrencyRef") or {}).get("value")
                t = {"Bank": "bank", "Credit Card": "credit_card"}.get(a.get("AccountType", ""))
                try:
                    cur.execute("SELECT account_id FROM account WHERE source_account_id=%s;", (qid,))
                    exists = cur.fetchone()
                    if exists:
                        L.append("first bank account ALREADY in app: " + nm)
                    else:
                        conn_id = None
                        if "connection_id" in cols:
                            cur.execute("SELECT connection_id FROM account WHERE connection_id IS NOT NULL LIMIT 1;")
                            rr = cur.fetchone(); conn_id = rr[0] if rr else None
                        fields = {"source_account_id": qid, "name": nm, "type": t}
                        if "currency" in cols and ccy: fields["currency"] = ccy
                        if "org_id" in cols: fields["org_id"] = ORG_ID
                        if "connection_id" in cols and conn_id is not None: fields["connection_id"] = conn_id
                        cn = list(fields.keys()); ph = ["%s"] * len(cn)
                        if "account_id" in cols:
                            cn = ["account_id"] + cn; ph = ["gen_random_uuid()"] + ph
                        sql = "INSERT INTO account (" + ", ".join(cn) + ") VALUES (" + ", ".join(ph) + ");"
                        L.append("insert sql: " + sql)
                        cur.execute(sql, [fields[c] for c in fields]); conn.commit()
                        L.append("INSERT SUCCEEDED for: " + nm)
                except Exception as e:
                    conn.rollback()
                    L.append("INSERT FAILED: " + repr(e))
            cur.close(); conn.close()
        except Exception as e:
            L.append("Account query error: " + str(e))
    except Exception as e:
        L.append("token error: " + str(e))
    return Response("\n".join(L), mimetype="text/plain")


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


# ---------------- CSRF protection ----------------
# Every POST must carry the session's token. It's injected into each POST form on the way out,
# so new forms are covered without remembering to add a field.
def csrf_token():
    tok = session.get("csrf")
    if not tok:
        tok = session["csrf"] = secrets.token_urlsafe(32)
    return tok


_POST_FORM = re.compile(r"""(<form\b[^>]*\bmethod\s*=\s*["']?post\b[^>]*>)""", re.IGNORECASE)


@app.after_request
def _inject_csrf(resp):
    if resp.mimetype == "text/html" and not resp.direct_passthrough:
        html = resp.get_data(as_text=True)
        if _POST_FORM.search(html):
            field = f'<input type=hidden name=_csrf value="{csrf_token()}">'
            resp.set_data(_POST_FORM.sub(lambda m: m.group(1) + field, html))
    return resp


@app.before_request
def check_csrf():
    if request.method != "POST":
        return
    good = session.get("csrf")
    sent = request.form.get("_csrf", "")
    if good and hmac.compare_digest(sent, good):
        return
    if request.endpoint == "login":
        return render_template_string(LOGIN_PAGE, error="That sign-in page had expired. Please try again."), 400
    return (f"This form had expired or didn't come from this app, so nothing was changed. "
            f"<a href='{url_for('dashboard')}'>Reload the app</a> and try again."), 400


@app.before_request
def require_login():
    if request.endpoint in ("login", "static", "health", "terms", "privacy"):
        return
    if not session.get("authed"):
        return redirect(url_for("login"))

CHANGE_PW_PAGE = """<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Change password · Reconciliation Tool</title>""" + CSS + """</head><body>
<div class=nav><span class=brand><span class=dot></span>Reconciliation Tool</span><span class=links>{% if session.name %}<span style="color:var(--muted);font-size:13px;margin-right:6px">{{ session.name }}</span>{% endif %}<a href="{{ url_for('dashboard') }}">← All accounts</a>{% if session.is_admin %}<a href="{{ url_for('manage_accounts') }}">Accounts</a><a href="{{ url_for('users') }}">Users</a><a href="{{ url_for('backup') }}">Backup</a>{% endif %}<a href="{{ url_for('change_password') }}">Change password</a><a href="{{ url_for('logout') }}">Sign out</a></span></div>
<div class=wrap style="max-width:440px">
<h1>Change password</h1>
<div class=sub>Set a new password for signing in.</div>
<form method=post>
<label style="display:block;font-size:12px;font-weight:600;color:#475467;margin:14px 0 6px;text-transform:uppercase;letter-spacing:.04em">Current password</label>
<div class=pw-wrap><input id=cpw-cur type=password name=current autofocus style="width:100%;padding:11px 12px;border:1px solid var(--line);border-radius:10px;font-size:15px">
<button type=button class=pw-toggle onclick="togglePw(this,'cpw-cur')" aria-label="Show password" aria-pressed="false">""" + EYE_ICON + """</button></div>
<label style="display:block;font-size:12px;font-weight:600;color:#475467;margin:16px 0 6px;text-transform:uppercase;letter-spacing:.04em">New password</label>
<div class=pw-wrap><input id=cpw-new type=password name=new style="width:100%;padding:11px 12px;border:1px solid var(--line);border-radius:10px;font-size:15px">
<button type=button class=pw-toggle onclick="togglePw(this,'cpw-new')" aria-label="Show password" aria-pressed="false">""" + EYE_ICON + """</button></div>
<label style="display:block;font-size:12px;font-weight:600;color:#475467;margin:16px 0 6px;text-transform:uppercase;letter-spacing:.04em">Confirm new password</label>
<div class=pw-wrap><input id=cpw-confirm type=password name=confirm style="width:100%;padding:11px 12px;border:1px solid var(--line);border-radius:10px;font-size:15px">
<button type=button class=pw-toggle onclick="togglePw(this,'cpw-confirm')" aria-label="Show password" aria-pressed="false">""" + EYE_ICON + """</button></div>
<button type=submit class=btn style="margin-top:18px;width:100%">Update password</button>
{% if error %}<div style="color:var(--bad);font-size:13px;margin-top:13px;background:var(--bad-soft);padding:9px 12px;border-radius:8px">{{ error }}</div>{% endif %}
</form>
<div class=sub style="margin-top:22px;font-size:13px;line-height:1.55">Forgot your password? The password originally set up for this app always works as a recovery key — sign in with that, then change it here.</div>
</div>
<script>""" + PW_TOGGLE_JS + """</script>
</body></html>"""


@app.route("/change-password", methods=["GET", "POST"])
def change_password():
    error = None
    uname = session.get("username")
    u = get_user(uname) if uname and uname != "admin" else None
    if request.method == "POST":
        cur_pw = request.form.get("current", "")
        new_pw = request.form.get("new", "")
        confirm = request.form.get("confirm", "")
        ok = bool(u and u[2] and check_password_hash(u[2], cur_pw)) or (APP_PASSWORD and cur_pw == APP_PASSWORD)
        if not ok:
            error = "Current password is incorrect."
        elif len(new_pw) < 6:
            error = "New password must be at least 6 characters."
        elif new_pw != confirm:
            error = "The new passwords don't match."
        elif not u:
            error = "You're signed in with the recovery password. Create a named user under Users, then set its password there."
        else:
            try:
                add_user(u[0], u[1], new_pw, u[3])
                session["sync_msg"] = "Password updated."
                return redirect(url_for("dashboard"))
            except Exception as e:
                error = f"Could not save password: {e}"
    return render_template_string(CHANGE_PW_PAGE, error=error)


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = (request.form.get("username") or "").strip().lower()
        password = request.form.get("password") or ""
        u = get_user(username) if username else None
        if u and u[2] and check_password_hash(u[2], password):
            session["csrf"] = secrets.token_urlsafe(32)
            session["authed"] = True; session["username"] = u[0]
            session["name"] = u[1] or u[0]; session["is_admin"] = bool(u[3])
            return redirect(url_for("dashboard"))
        if check_password(password):
            session["csrf"] = secrets.token_urlsafe(32)
            session["authed"] = True; session["username"] = "admin"
            session["name"] = "Admin"; session["is_admin"] = True
            return redirect(url_for("dashboard"))
        return render_template_string(LOGIN_PAGE, error="Incorrect username or password")
    return render_template_string(LOGIN_PAGE, error=None)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ---------------- statement parsing (CSV + OFX) ----------------
def _detect_dayfirst(samples):
    # Decide day-first vs month-first for slash/dash dates by scanning the column.
    for s in samples:
        s = (s or "").strip()
        s = s.split()[0] if s else s
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
    s = re.sub(r"[ T]\d{1,2}:\d{2}(:\d{2})?(\.\d+)?\s*([AaPp][Mm])?$", "", s).strip()  # drop a trailing time
    s = re.sub(r"\s+", " ", s)
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
    s = s.strip().replace(",", "").replace(" ", "")
    s = re.sub(r"^[A-Za-z]{3}(?=[-(\d.])|[$€£]", "", s)   # currency code / symbol prefix, e.g. UGX1000
    neg = False
    suffix = re.search(r"(DR|CR|D|C)\.?$", s, flags=re.IGNORECASE)
    if suffix:
        neg = suffix.group(1).upper() in ("DR", "D")
        s = s[:suffix.start()]
    if s.startswith("(") and s.endswith(")"):
        neg, s = True, s[1:-1]
    if s.endswith("-"):
        neg, s = True, s[:-1]
    v = Decimal(s)
    return -v if neg else v


def _dedupe_keys(rows):
    """Stable per-row keys. Identical rows (same date, amount and description — e.g. two
    bank charges on one day) are genuinely separate transactions, so the n-th repeat gets
    its own key instead of being collapsed into the first."""
    counts = Counter()
    for r in rows:
        if r.get("fitid"):
            yield r["fitid"]
            continue
        base = f"{r['date']}|{r['amount']}|{(r.get('desc') or '').lower()}"
        n = counts[base]; counts[base] += 1
        yield hashlib.sha256((base if n == 0 else f"{base}|{n}").encode()).hexdigest()[:32]

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

class _Rows(list):
    """Parsed rows, plus the dates of rows that had a date but couldn't be parsed,
    and any opening/closing balance the file itself states."""
    def __init__(self, *a):
        super().__init__(*a); self.skipped = []; self.opening = self.closing = None
        self.period_start = self.period_end = None


def _parse_ledger(text, want_category=False):
    reader = _ledger_reader(text)
    fns = reader.fieldnames or []
    dk = _sub_col(fns, "transaction date", "posted date", "posting date", "trans date", "date")
    ak = _amount_col(fns)
    dr = _sub_col(fns, "debit", "withdrawal", "payment")
    crk = _sub_col(fns, "credit", "deposit")
    nk = _sub_col(fns, "payee", "description", "name", "memo", "narrative")
    ck = _sub_col(fns, "split", "category") if want_category else None
    bk = None if want_category else _sub_col(fns, "running balance", "balance")
    if not dk or not (ak or dr or crk):
        raise ValueError(f"Couldn't find a Date column and an Amount (or Debit/Credit, Payment/Deposit) column. Found columns: {fns}")
    raw = list(reader)
    dayfirst = _detect_dayfirst([r.get(dk) or "" for r in raw])
    rows = _Rows()
    for r in raw:
        ds = (r.get(dk) or "").strip()
        if not ds:
            continue
        try:
            d = parse_date(ds, dayfirst=dayfirst)
        except ValueError:
            rows.skipped.append(ds); continue
        if ak and (r.get(ak) or "").strip():
            try:
                amount = parse_amount(r[ak])
            except Exception:
                rows.skipped.append(ds); continue
        else:
            try:
                deb = abs(parse_amount(r[dr])) if (dr and (r.get(dr) or "").strip()) else Decimal(0)
                cre = abs(parse_amount(r[crk])) if (crk and (r.get(crk) or "").strip()) else Decimal(0)
            except Exception:
                rows.skipped.append(ds); continue
            amount = cre - deb
        if amount == 0:
            continue
        desc = (r.get(nk) or "").strip() if nk else ""
        row = {"date": d, "amount": amount, "desc": desc}
        if bk and (r.get(bk) or "").strip():
            try:
                row["balance"] = parse_amount(r[bk])
            except Exception:
                pass
        if want_category:
            row["category"] = ((r.get(ck) or "").strip() or None) if ck else None
        rows.append(row)
    return rows

def parse_csv(text):
    rows = _parse_ledger(text, want_category=False)
    rows.opening, rows.closing = _balances_from_running(rows)
    return rows


def _balances_from_running(rows):
    """Opening/closing balance from a running-balance column, whichever way the file is sorted.

    Ascending files satisfy bal[i] = bal[i-1] + amt[i]; newest-first files satisfy
    bal[i-1] = bal[i] + amt[i-1]. Whichever holds for most adjacent pairs wins."""
    rb = [r for r in rows if "balance" in r]
    if len(rb) < 2 or len(rb) != len(rows):
        return None, None
    asc = sum(1 for a, b in zip(rb, rb[1:]) if b["balance"] == a["balance"] + b["amount"])
    desc = sum(1 for a, b in zip(rb, rb[1:]) if a["balance"] == b["balance"] + a["amount"])
    if max(asc, desc) * 2 < len(rb) - 1:
        return None, None
    if asc >= desc:
        return rb[0]["balance"] - rb[0]["amount"], rb[-1]["balance"]
    return rb[-1]["balance"] - rb[-1]["amount"], rb[0]["balance"]

def parse_ofx(text):
    rows = _Rows()
    lb = re.search(r"<LEDGERBAL>.*?<BALAMT>([^<\r\n]+)", text, flags=re.IGNORECASE | re.DOTALL)
    try:
        rows.closing = Decimal(lb.group(1).strip().replace(",", "")) if lb else None
    except Exception:
        rows.closing = None
    rows.opening = None
    for attr, tag_ in (("period_start", "DTSTART"), ("period_end", "DTEND")):
        mm = re.search(r"<BANKTRANLIST>.*?<" + tag_ + r">(\d{8})", text, flags=re.IGNORECASE | re.DOTALL)
        if mm:
            try:
                setattr(rows, attr, datetime.strptime(mm.group(1), "%Y%m%d").date())
            except ValueError:
                pass
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


# ---------------- PDF statements ----------------
# Bank PDFs have no fixed layout, so this reads positioned words, finds the table header
# (Date ... Debit / Credit / Balance), and takes each line starting with a date as a transaction.
# Nothing is guessed silently: signs come from the Debit/Credit columns or the running balance,
# and when the statement has running balances every line must agree with them, or the upload
# is refused with the lines that don't add up.

PDF_MAX_PAGES = 300


class PdfPasswordError(ValueError):
    pass


_MONEY_STRICT = re.compile(r"^\(?-?(?:\d{1,3}(?:,\d{3})+|\d+)\.\d{2}\)?-?(?:CR|DR|Cr|Dr|cr|dr)?$"
                           r"|^\(?-?\d{1,3}(?:,\d{3})+\)?-?(?:CR|DR|Cr|Dr|cr|dr)?$")
_PDF_COLS = {"debit": "debit", "debits": "debit", "withdrawal": "debit", "withdrawals": "debit", "dr": "debit",
             "credit": "credit", "credits": "credit", "deposit": "credit", "deposits": "credit", "cr": "credit",
             "lodgement": "credit", "lodgements": "credit", "receipts": "credit", "payments": "debit",
             "balance": "balance", "bal": "balance", "amount": "amount"}
_PDF_OPENING = re.compile(r"opening balance|brought forward|balance b/?f|\bb/f\b|balance at start|previous balance", re.I)
_PDF_CLOSING = re.compile(r"closing balance|carried forward|balance c/?f|\bc/f\b|ending balance|balance at end", re.I)
_PDF_SKIP = re.compile(r"^page \d+|\bpage \d+ of \d+|^total|^sub-?total|statement of account|continued", re.I)
_PDF_DATE_FMTS = ("%d %b %Y", "%d-%b-%Y", "%d %B %Y", "%d-%b-%y", "%d %b %y", "%d/%b/%Y", "%d/%b/%y", "%d.%m.%Y",
                  "%d.%m.%y", "%b %d, %Y", "%b %d %Y", "%B %d, %Y")
_PDF_DATE_TOKEN = re.compile(r"^(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}|\d{4}-\d{2}-\d{2}|\d{1,2}[/-][A-Za-z]{3,9}[/-]?\d{0,4}|\d{1,2})$")


def _pdf_one_date(s, dayfirst):
    s = s.strip().rstrip(",")
    if re.match(r"^\d{1,2}\.\d{1,2}\.\d{2,4}$", s):
        s = s.replace(".", "/")
    try:
        return parse_date(s, dayfirst=dayfirst)
    except ValueError:
        pass
    for fmt in _PDF_DATE_FMTS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


def _pdf_date(words, dayfirst):
    """(date, words used) when the line starts with a date in any common layout, else (None, 0)."""
    if not words or not _PDF_DATE_TOKEN.match(words[0]["text"]) and not re.match(r"^[A-Za-z]{3,9}$", words[0]["text"]):
        return None, 0
    for n in (3, 2, 1):
        if len(words) >= n:
            d = _pdf_one_date(" ".join(w["text"] for w in words[:n]), dayfirst)
            if d and 1990 <= d.year <= 2100:
                return d, n
    return None, 0


def _pdf_lines(page):
    """Words grouped into visual lines, top to bottom, left to right."""
    words = sorted(page.extract_words(keep_blank_chars=False, x_tolerance=1.5, y_tolerance=2),
                   key=lambda w: (round(w["top"]), w["x0"]))
    lines = []
    for w in words:
        if lines and abs(lines[-1][0]["top"] - w["top"]) <= 3:
            lines[-1].append(w)
        else:
            lines.append([w])
    return [sorted(l, key=lambda w: w["x0"]) for l in lines]


def _pdf_header(line):
    """{column: (x0, x1)} when this line is a statement table header."""
    texts = [w["text"].lower().strip(":.()") for w in line]
    if not any(t in ("date", "posted", "posting", "txn", "trans") or t.endswith("date") for t in texts):
        return None
    cols = {}
    for i, (w, t) in enumerate(zip(line, texts)):
        nxt = texts[i + 1] if i + 1 < len(texts) else ""
        prev = texts[i - 1] if i else ""
        col = _PDF_COLS.get(t)
        if t in ("out", "in") and prev in ("money", "paid"):
            col = "debit" if t == "out" else "credit"
        if t == "value" and nxt != "date":
            col = "amount"
        if col == "debit" and t == "payments" and "withdrawals" in texts:
            col = None
        if col and col not in cols:
            x0 = line[i - 1]["x0"] if t in ("out", "in") and prev in ("money", "paid") else w["x0"]
            cols[col] = (x0, w["x1"])
    if "balance" in cols and len(cols) == 1:
        return None
    return cols if cols and ("debit" in cols or "credit" in cols or "amount" in cols) else None


def _pdf_col(w, cols):
    """Which money column a number sits in: nearest header by centre or right edge (numbers are right-aligned)."""
    c = (w["x0"] + w["x1"]) / 2
    best = None
    for name, (x0, x1) in cols.items():
        dist = min(abs(c - (x0 + x1) / 2), abs(w["x1"] - x1))
        if best is None or dist < best[0]:
            best = (dist, name)
    return best[1] if best and best[0] <= 60 else None


def _pdf_money_tail(words, cols):
    """Split a line into (description words, trailing money tokens). A lone CR/DR after a number
    belongs to it. Plain integers only count when they sit under a money column."""
    money, i = [], len(words)
    while i > 0:
        t = words[i - 1]["text"]
        if t.upper() in ("CR", "DR") and i >= 2 and _MONEY_STRICT.match(words[i - 2]["text"]):
            w = dict(words[i - 2]); w["text"] += t
            money.insert(0, w); i -= 2; continue
        # A bare number ("500") is money only when it stands apart from the text before it, under a
        # money column -- not "INV 2231" at the end of a description.
        gap = words[i - 1]["x0"] - words[i - 2]["x1"] if i >= 2 else 99
        if _MONEY_STRICT.match(t) or (cols and re.match(r"^\d+$", t) and gap > 12 and _pdf_col(words[i - 1], cols)):
            money.insert(0, words[i - 1]); i -= 1; continue
        break
    return words[:i], money


def _signed(t):
    return bool(re.search(r"^\(|^-|-$|\)$|(CR|DR)$", t.strip(), re.I))


def parse_pdf(data, password=None, opening_hint=None):
    """Statement lines from a bank's PDF statement. Returns _Rows like the CSV/OFX parsers."""
    try:
        import pdfplumber
        from pdfminer.pdfdocument import PDFPasswordIncorrect
    except ImportError:
        raise ValueError("PDF import isn't installed on this server (pdfplumber). Upload the CSV or OFX instead.")
    try:
        pdf = pdfplumber.open(io.BytesIO(data), password=password or "")
        pages = pdf.pages
    except Exception as e:
        # pdfplumber wraps pdfminer's error, so look inside it too.
        if (isinstance(e, PDFPasswordIncorrect) or any(isinstance(a, PDFPasswordIncorrect) for a in e.args)
                or "password" in repr(e).lower()):
            raise PdfPasswordError("This PDF is password-protected. Enter its password and upload again." if not password
                                   else "That PDF password isn't right. Check it and upload again.")
        raise ValueError("That file couldn't be read as a PDF.")
    try:
        if len(pages) > PDF_MAX_PAGES:
            raise ValueError(f"That PDF has {len(pages)} pages; the limit is {PDF_MAX_PAGES}. Split it by month.")
        page_lines = [_pdf_lines(p) for p in pages]
    finally:
        pdf.close()
    if not any(page_lines):
        raise ValueError("This PDF has no readable text — it's probably a scan or photo. Download the statement "
                         "from online banking as a PDF, CSV or Excel file instead.")
    all_text = "\n".join(" ".join(w["text"] for w in l) for pl in page_lines for l in pl)
    dayfirst = _detect_dayfirst([l[0]["text"] for pl in page_lines for l in pl if l])

    rows, raw, cols, opening, closing = _Rows(), [], None, None, None
    for pl in page_lines:
        prev = None      # the transaction a wrapped description line belongs to (same page only)
        for line in pl:
            text = " ".join(w["text"] for w in line)
            h = _pdf_header(line) if not _pdf_date(line, dayfirst)[0] else None
            if h:
                cols, prev = h, None; continue
            d, k = _pdf_date(line, dayfirst)
            body, money = _pdf_money_tail(line[k:] if d else line, cols)
            if d:
                d2, k2 = _pdf_date(body, dayfirst)       # a value date next to the transaction date
                if d2:
                    body = body[k2:]
            desc = " ".join(w["text"] for w in body).strip()
            if money and (_PDF_OPENING.search(text) or _PDF_CLOSING.search(text)):
                v = parse_amount(money[-1]["text"])
                if _PDF_OPENING.search(text):
                    opening = v if opening is None else opening
                else:
                    closing = v
                prev = None; continue
            if not d:
                # A wrapped description: no numbers, lined up under the description, close below it.
                if (prev is not None and not money and not _PDF_SKIP.search(text)
                        and line[0]["x0"] >= prev["desc_x0"] - 4 and line[0]["top"] - prev["bottom"] < 14
                        and line[-1]["x1"] <= prev["money_x0"] + 2):
                    prev["desc"] = (prev["desc"] + " " + text).strip()
                    prev["bottom"] = line[0]["bottom"]
                else:
                    prev = None
                continue
            if not money:
                prev = None; continue
            r = {"date": d, "desc": desc, "debit": None, "credit": None, "amount": None, "balance": None,
                 "known": False, "desc_x0": body[0]["x0"] if body else money[0]["x0"],
                 "money_x0": money[0]["x0"], "bottom": line[0]["bottom"]}
            if cols:
                for w in money:
                    c = _pdf_col(w, cols)
                    if c and r[c] is None:
                        r[c] = w["text"]
            else:
                r["balance"] = money[-1]["text"] if len(money) >= 2 else None
                r["amount"] = money[-2]["text"] if len(money) >= 2 else money[0]["text"]
            try:
                if r["debit"] or r["credit"]:
                    deb = abs(parse_amount(r["debit"])) if r["debit"] else Decimal(0)
                    cre = abs(parse_amount(r["credit"])) if r["credit"] else Decimal(0)
                    r["amount"], r["known"] = cre - deb, True
                elif r["amount"]:
                    t = r["amount"]; r["known"] = bool(cols) and _signed(t)
                    r["amount"] = parse_amount(t)
                    if not r["known"]:
                        r["amount"] = abs(r["amount"])
                r["balance"] = parse_amount(r["balance"]) if r["balance"] else None
            except Exception:
                rows.skipped.append(str(d)); prev = None; continue
            if r["amount"] is None:
                prev = None; continue       # a balance-only line (e.g. a dated "balance b/f")
            raw.append(r); prev = r

    if not raw:
        raise ValueError("No transactions found in the PDF. If it's a statement, upload the CSV or OFX export instead.")

    # Signs the columns didn't settle come from the running balance.
    bals = [r["balance"] for r in raw]
    have_bal = all(b is not None for b in bals)
    if not all(r["known"] for r in raw):
        if not have_bal:
            raise ValueError("This PDF shows amounts without Debit/Credit columns or a running balance, so money in "
                             "and money out can't be told apart. Upload the CSV or OFX export instead.")
        start = opening if opening is not None else opening_hint
        def fits(order):
            seq = raw if order == "asc" else raw[::-1]
            before, ok = start, 0
            for r in seq:
                if before is not None and abs(r["balance"] - before) == abs(r["amount"]):
                    ok += 1
                before = r["balance"]
            return ok
        order = "asc" if fits("asc") >= fits("desc") else "desc"
        seq = raw if order == "asc" else raw[::-1]
        before = start
        for r in seq:
            if not r["known"]:
                if before is None:
                    raise ValueError("Couldn't tell whether the first line is money in or out. Enter the statement's "
                                     "opening balance and upload again.")
                r["amount"] = r["balance"] - before
                r["known"] = True
            before = r["balance"]

    # Every running balance must agree, whichever way round the statement is sorted.
    if have_bal and len(raw) >= 1:
        def breaks(seq, start):
            bad, before = [], start
            for r in seq:
                if before is not None and r["balance"] != before + r["amount"]:
                    bad.append(r)
                before = r["balance"]
            return bad
        asc, desc_ = breaks(raw, opening), breaks(raw[::-1], opening)
        bad = asc if len(asc) <= len(desc_) else desc_
        if bad:
            eg = "; ".join(f"{r['date']} {r['desc'][:30]} {_money(r['amount'])} (balance {_money(r['balance'])})" for r in bad[:3])
            raise ValueError(f"The running balance doesn't add up on {len(bad)} line{'' if len(bad) == 1 else 's'} of "
                             f"the PDF, so it may have been read wrongly: {eg}. Nothing was imported — upload the CSV "
                             f"or OFX export instead, or send this PDF to support.")
        rows.pdf_checked = True
    else:
        rows.pdf_checked = False

    for r in raw:
        if r["amount"] != 0:
            rows.append({"date": r["date"], "amount": r["amount"], "desc": r["desc"],
                         **({"balance": r["balance"]} if r["balance"] is not None else {})})
    run_o, run_c = _balances_from_running(rows) if have_bal and len(rows) >= 2 else (None, None)
    if have_bal and len(rows) == 1:
        run_c = rows[0]["balance"]; run_o = run_c - rows[0]["amount"]
    rows.opening = opening if opening is not None else run_o
    rows.closing = run_c if run_c is not None else closing

    # The statement's own period, when it says so ("Period: 01/01/2026 to 31/01/2026").
    first, last = min(r["date"] for r in rows), max(r["date"] for r in rows)
    dpat = r"(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}|\d{4}-\d{2}-\d{2}|\d{1,2}[ -][A-Za-z]{3,9}[ ,-]*\d{2,4}|[A-Za-z]{3,9} \d{1,2},? \d{4})"
    for m in re.finditer(r"(?:period|from|statement date|dated)[^\n]{0,20}?" + dpat + r"\s*(?:to|-|–|—|through|till)\s*" + dpat,
                         all_text, re.I):
        a, b = _pdf_one_date(m.group(1), dayfirst), _pdf_one_date(m.group(2), dayfirst)
        if a and b and a <= first and b >= last and (b - a).days <= 400:
            rows.period_start, rows.period_end = a, b
            break
    return rows


def ingest_pdf(data, account_name, password=None, opening=None, closing=None, p_start=None, p_end=None):
    rows = parse_pdf(data, password, opening)
    sid = _save_statement(rows, account_name, "pdf", opening, closing, p_start, p_end)
    return sid, len(rows), rows.skipped, rows.pdf_checked


def _prev_signed_closing(cur, acct_uuid, before, exclude_sid=None):
    """Closing balance of the latest signed-off statement ending before `before`, if it's known."""
    cur.execute("""SELECT closing_balance, period_end FROM statement
                   WHERE account_id=%s AND signed_off_at IS NOT NULL AND closing_source IS NOT NULL
                     AND period_end < %s AND statement_id IS DISTINCT FROM %s
                   ORDER BY period_end DESC LIMIT 1;""", (acct_uuid, before, exclude_sid))
    return cur.fetchone()


def _resolve_balances(cur, acct_uuid, p_start, moves, opening, o_src, closing, c_src, exclude_sid=None):
    """Fill in whatever balance wasn't supplied. Closing is never derived from movements -- that
    would make the statement add up by construction and hide a missing line."""
    if opening is None:
        prev = _prev_signed_closing(cur, acct_uuid, p_start, exclude_sid)
        if prev:
            opening, o_src = prev[0], "carried"
        elif closing is not None:
            opening, o_src = closing - moves, "derived"
    return opening, o_src, closing, c_src


def _resolve_period(cur, acct_uuid, rows, p_start=None, p_end=None):
    """The statement's own period, not just the span of its transactions -- the book balance and
    the outstanding items are measured at the statement END date, which is usually later than
    the last transaction. Explicit dates win, then what the file states, then: start the day after
    the last signed-off period, end on the last transaction date."""
    first = min(r["date"] for r in rows); last = max(r["date"] for r in rows)
    p_end = p_end or getattr(rows, "period_end", None) or last
    if not p_start:
        p_start = getattr(rows, "period_start", None)
    if not p_start:
        prev = _prev_signed_closing(cur, acct_uuid, first)
        p_start = prev[1] + timedelta(days=1) if prev else first
    if p_start > first or p_end < last:
        raise ValueError(f"The statement period {p_start} to {p_end} doesn't cover all its transactions "
                         f"({first} to {last}).")
    return p_start, p_end


def _save_statement(rows, account_name, source_format, opening=None, closing=None, p_start=None, p_end=None):
    if not rows: raise ValueError("No transactions found in the file.")
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id, currency FROM account WHERE name=%s LIMIT 1;", (account_name,))
    arow = cur.fetchone()
    if not arow:
        cur.close(); conn.close(); raise ValueError(f"Unknown account: {account_name}")
    acct_uuid, currency = arow
    p_start, p_end = _resolve_period(cur, acct_uuid, rows, p_start, p_end)
    moves = sum((r["amount"] for r in rows), Decimal(0))
    o_src = "user" if opening is not None else None
    c_src = "user" if closing is not None else None
    if opening is None and getattr(rows, "opening", None) is not None:
        opening, o_src = rows.opening, "file"
    if closing is None and getattr(rows, "closing", None) is not None:
        closing, c_src = rows.closing, "file"
    opening, o_src, closing, c_src = _resolve_balances(cur, acct_uuid, p_start, moves, opening, o_src, closing, c_src)
    cur.execute("DELETE FROM statement WHERE account_id=%s AND period_start=%s AND period_end=%s;",
                (acct_uuid, p_start, p_end))
    cur.execute("""INSERT INTO statement (org_id, account_id, period_start, period_end,
                   opening_balance, closing_balance, opening_source, closing_source, currency, source_format)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING statement_id;""",
                (ORG_ID, acct_uuid, p_start, p_end, opening or 0, closing or 0, o_src, c_src, currency, source_format))
    sid = cur.fetchone()[0]
    seen = {}
    for r, key in zip(rows, _dedupe_keys(rows)):
        if key not in seen:
            seen[key] = (ORG_ID, sid, r["date"], r["amount"], currency, r.get("desc") or "", key)
    if seen:
        execute_values(cur,
            """INSERT INTO statement_line (org_id, statement_id, posted_date, amount, currency, description, dedupe_key)
               VALUES %s ON CONFLICT (statement_id, dedupe_key) DO NOTHING;""",
            list(seen.values()))
    conn.commit(); cur.close(); conn.close()
    return sid

def ingest_file(text, filename, account_name, opening=None, closing=None, p_start=None, p_end=None):
    is_ofx = (filename or "").lower().endswith(".ofx") or "<OFX>" in text[:3000].upper()
    rows = parse_ofx(text) if is_ofx else parse_csv(text)
    sid = _save_statement(rows, account_name, "ofx" if is_ofx else "csv", opening, closing, p_start, p_end)
    return sid, len(rows), getattr(rows, "skipped", [])


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
    seen = {}
    for r, key in zip(rows, _dedupe_keys(rows)):
        desc = r.get("desc") or ""
        seen[key] = (ORG_ID, acct_uuid, key, r["date"], r["amount"], currency, desc, desc, r.get("category"))
    n = len(seen)
    if seen:
        execute_values(cur,
            """INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type,
               posted_date, amount, currency, description, counterparty, category, cleared_status, is_void, is_deleted, last_modified)
               VALUES %s
               ON CONFLICT (account_id, source_txn_type, source_txn_id) DO UPDATE SET
                 amount=EXCLUDED.amount, description=EXCLUDED.description,
                 counterparty=EXCLUDED.counterparty, category=EXCLUDED.category, last_modified=now();""",
            list(seen.values()),
            template="(%s,%s,%s,'CSV',%s,%s,%s,%s,%s,%s,'unknown',false,false,now())")
    conn.commit(); cur.close(); conn.close()
    return n, rows.skipped


def run_matcher(statement_id):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id, period_start, period_end FROM statement WHERE statement_id=%s;", (statement_id,))
    acct_uuid, p_start, p_end = cur.fetchone()
    # Keep the user's decisions across re-runs (sync, write-back, books import):
    #   confirmed suggestions are pinned first, so nothing else can claim their lines;
    #   rejected pairings are never proposed again, and stay listed as rejected.
    cur.execute("""SELECT m.match_type, m.confidence, m.status, m.amount_delta,
                          array(SELECT line_id::text FROM match_statement_line WHERE match_id=m.match_id),
                          array(SELECT txn_id::text FROM match_book_txn WHERE match_id=m.match_id),
                          m.confirmed_by, m.confirmed_at, m.created_by
                   FROM match m WHERE m.statement_id=%s AND m.status IN ('confirmed','rejected');""", (statement_id,))
    pinned, rejected = [], {}
    for mt, conf, st, delta, ls, ts, by, at, origin in cur.fetchall():
        if st == "rejected":
            rejected[(mt, frozenset(ls), frozenset(ts))] = (mt, conf, delta, ls, ts, by, at, origin)
        elif mt != "exact" or (conf is not None and conf < 1):
            pinned.append((mt, conf, delta, ls, ts, by, at, origin))   # user-confirmed, or matched by hand
    cur.execute("DELETE FROM match_statement_line WHERE match_id IN (SELECT match_id FROM match WHERE statement_id=%s);", (statement_id,))
    cur.execute("DELETE FROM match_book_txn WHERE match_id IN (SELECT match_id FROM match WHERE statement_id=%s);", (statement_id,))
    cur.execute("DELETE FROM match WHERE statement_id=%s;", (statement_id,))
    cur.execute("SELECT line_id, posted_date, amount, coalesce(counterparty, description,'') FROM statement_line WHERE statement_id=%s;", (statement_id,))
    lines = cur.fetchall()
    txns = book_pool(cur, acct_uuid, statement_id, p_start, p_end)

    used, matched_lines, matches = set(), set(), []
    def add(lids, tids, mt, conf, delta, status=None, by=None, at=None, origin="engine"):
        # Only an exact amount on (nearly) the same date is safe to accept unseen.
        status = status or ("confirmed" if mt == "exact" and conf >= 1 else "proposed")
        matches.append((str(uuid.uuid4()), mt, conf, delta, lids, tids, status, by, at, origin or "engine"))
    def ok(mt, lids, tids):
        return (mt, frozenset(map(str, lids)), frozenset(map(str, tids))) not in rejected

    line_ids = {str(l[0]) for l in lines}; pool_ids = {str(t[0]) for t in txns}
    for mt, conf, delta, ls, ts, by, at, origin in pinned:
        if set(ls) <= line_ids and set(ts) <= pool_ids and not (set(ts) & used) and not (set(ls) & matched_lines):
            add(ls, ts, mt, float(conf or 0), delta, "confirmed", by, at, origin)
            used.update(ts); matched_lines.update(ls)
    used = {str(u) for u in used}; matched_lines = {str(m) for m in matched_lines}
    lines = [(str(a), b, c, d) for a, b, c, d in lines]
    txns = [(str(a), b, c, d) for a, b, c, d in txns]

    # pass 1: exact (amount equal, date within tolerance). Take the closest date, not the
    # first hit, so two equal amounts a few days apart don't get cross-paired.
    for l_id, ld, la, lw in lines:
        best = None
        for t_id, td, ta, tw in txns:
            if t_id in used or la != ta or not ok("exact", [l_id], [t_id]):
                continue
            gap = abs((ld - td).days)
            if gap <= DATE_TOLERANCE_DAYS and (best is None or gap < best[0]):
                best = (gap, t_id)
        if best:
            add([l_id], [best[1]], "exact", 1.0, 0); used.add(best[1]); matched_lines.add(l_id)

    # pass 1b: cleared later -- same amount, bank date on/after the book date but beyond the
    # tolerance (cheques presented late, items brought forward from last period). Confidence
    # below 1 puts these in the review list.
    for l_id, ld, la, lw in lines:
        if l_id in matched_lines:
            continue
        best = None
        for t_id, td, ta, tw in txns:
            if t_id in used or la != ta or not ok("exact", [l_id], [t_id]):
                continue
            lag = (ld - td).days
            if DATE_TOLERANCE_DAYS < lag <= CLEARING_WINDOW_DAYS and (best is None or lag < best[0]):
                best = (lag, t_id)
        if best:
            add([l_id], [best[1]], "exact", 0.9, 0); used.add(best[1]); matched_lines.add(l_id)

    # pass 2: fuzzy (same payee, amount differs)
    for l_id, ld, la, lw in lines:
        if l_id in matched_lines:
            continue
        for t_id, td, ta, tw in txns:
            if t_id in used:
                continue
            if lw and tw and lw.strip().lower() == tw.strip().lower() and abs((ld - td).days) <= DATE_TOLERANCE_DAYS \
                    and ok("fuzzy", [l_id], [t_id]):
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
                    if sum((c[1] for c in combo), Decimal(0)) == la and ok("many_to_one", [l_id], [c[0] for c in combo]):
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
                    if sum((c[1] for c in combo), Decimal(0)) == ta and ok("many_to_one", [c[0] for c in combo], [t_id]):
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
            if la == -ta and abs((ld - td).days) <= DATE_TOLERANCE_DAYS and ok("manual", [l_id], [t_id]):
                add([l_id], [t_id], "manual", 0.5, 0)
                used.add(t_id); matched_lines.add(l_id); break

    # Rejected pairings stay on record (so they can be restored) but claim nothing.
    for mt, conf, delta, ls, ts, by, at, origin in rejected.values():
        if set(ls) <= line_ids and set(ts) <= pool_ids:
            add(ls, ts, mt, float(conf or 0), delta, "rejected", by, at, origin)

    # bulk insert (few round-trips instead of hundreds)
    if matches:
        execute_values(cur,
            "INSERT INTO match (match_id, org_id, statement_id, status, match_type, confidence, amount_delta, confirmed_by, confirmed_at, created_by) VALUES %s",
            [(m[0], ORG_ID, statement_id, m[6], m[1], m[2], m[3], m[7], m[8], m[9]) for m in matches])
        msl = [(m[0], lid) for m in matches for lid in m[4]]
        if msl:
            execute_values(cur, "INSERT INTO match_statement_line (match_id, line_id) VALUES %s", msl)
        mbt = [(m[0], tid) for m in matches for tid in m[5]]
        if mbt:
            execute_values(cur, "INSERT INTO match_book_txn (match_id, txn_id) VALUES %s", mbt)
    conn.commit(); cur.close(); conn.close()

    if len(unmatched) > M2O_MAX_LINES or len(unmatched_t) > M2O_MAX_LINES:
        return (f"Note: batched (many-to-one) matching was skipped — over {M2O_MAX_LINES} unmatched "
                f"items on one side, too many to combine safely. Exact and fuzzy matches are unaffected.")
    return None


def account_summary(cur, acct_uuid, name, atype, currency=None):
    s = _latest_statement(cur, acct_uuid)
    if not s: return {"name": name, "type": atype, "status": "none", "currency": currency}
    sid, ps, pe, signed = s[:4]
    cur.execute("SELECT match_type, count(*) FROM match WHERE statement_id=%s AND status='confirmed' GROUP BY match_type;", (sid,))
    mc = dict(cur.fetchall())
    rec = reconcile(cur, acct_uuid, s)
    exc = len(rec["un_lines"]) + len(rec["un_books"])
    return {"name": name, "type": atype, "currency": currency, "status": "signed" if signed else "open",
            "p_start": ps, "p_end": pe, "exact": mc.get("exact", 0), "fuzzy": mc.get("fuzzy", 0),
            "m2o": mc.get("many_to_one", 0), "exc": exc, "rec_status": rec["status"],
            "rec_diff": rec["rec_diff"], "missing": rec["missing"]}


DASH_TEMPLATE = """<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Dashboard · Reconciliation Tool</title>""" + CSS + """</head><body>
<div class=nav><span class=brand><span class=dot></span>Reconciliation Tool</span><span class=links>{% if session.name %}<span style="color:var(--muted);font-size:13px;margin-right:6px">{{ session.name }}</span>{% endif %}{% if session.is_admin %}<a href="{{ url_for('manage_accounts') }}">Accounts</a><a href="{{ url_for('users') }}">Users</a><a href="{{ url_for('backup') }}">Backup</a>{% endif %}<a href="{{ url_for('change_password') }}">Change password</a><a href="{{ url_for('logout') }}">Sign out</a></span></div>
<div class=wrap>
<h1>All accounts</h1>
<div class=sub>Updated {{ now }} EAT{% if n_hidden %} · {{ n_hidden }} account{{ '' if n_hidden==1 else 's' }} hidden{% if session.is_admin %} · <a href="{{ url_for('manage_accounts') }}" style="color:var(--accent);font-weight:600">manage</a>{% endif %}{% endif %}</div>
<div style="display:flex;gap:10px;align-items:center;margin-bottom:14px;flex-wrap:wrap">
{% if qbo_connected %}<span style="color:var(--ok);font-size:13px;font-weight:600">● Connected to QuickBooks</span>
<form method=post action="{{ url_for('check_connection') }}" style="margin:0"><button type=submit class=btn-sm>Check connection</button></form>
<form method=post action="{{ url_for('disconnect') }}" style="margin:0" onsubmit="return confirm('Disconnect from QuickBooks? You will need to reconnect before syncing again.');"><button type=submit class=btn-sm style="color:var(--bad);border-color:var(--bad-soft)">Disconnect</button></form>
{% else %}<a href="{{ url_for('connect') }}" class=btn style="text-decoration:none;display:inline-block">Connect to QuickBooks</a>
<span style="color:var(--muted);font-size:13px">Not connected</span>{% endif %}
</div>
<details style="margin-bottom:20px">
<summary style="cursor:pointer;color:var(--muted);font-size:13px">Advanced: connect with a refresh token</summary>
<form method=post action="{{ url_for('set_token') }}" style="margin-top:12px;display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end">
<div><label style="display:block;font-size:12px;color:var(--muted);margin-bottom:4px">Refresh token</label><input name=refresh_token style="width:340px;max-width:100%;padding:8px 10px;border:1px solid var(--line);border-radius:8px;font-size:13px"></div>
<div><label style="display:block;font-size:12px;color:var(--muted);margin-bottom:4px">Realm / Company ID</label><input name=realm_id style="width:190px;padding:8px 10px;border:1px solid var(--line);border-radius:8px;font-size:13px"></div>
<button type=submit class=btn-sm>Save token</button>
</form>
<div style="color:var(--muted);font-size:12px;margin-top:8px;line-height:1.5">Paste a refresh token from the Intuit OAuth Playground. This connects the app without needing the redirect URI registered.</div>
</details>
<form method=post action="{{ url_for('sync') }}" style="margin-bottom:24px" onsubmit="var b=this.querySelector('button');b.textContent='Syncing\u2026';b.disabled=true;">
<button type=submit class=btn-sm>Sync from QuickBooks</button></form>
<form method=post action="{{ url_for('sync') }}" style="display:inline" onsubmit="return confirm('Full resync re-downloads every transaction in the window, ignoring the last-sync marker. Slower, but use it if you think something was missed.');"><input type=hidden name=full value="1"><button type=submit class=btn-sm>Full resync</button></form>
{% if sync_msg %}<div class=sub style="color:var(--ok);margin-top:-16px">{{ sync_msg }}</div>{% endif %}
{{ sync_banner() }}
<div class=tiles>
<button class="tile active" data-filter="all"><div class=t-top><span class=t-ic><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3 3 8l9 5 9-5-9-5Z"/><path d="m3 13 9 5 9-5"/></svg></span><span class=t-label>Accounts</span></div><div class=t-val>{{ rows|length }}</div></button>
<button class="tile" data-filter="reconciled"><div class=t-top><span class=t-ic><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="m8.4 12 2.4 2.4L16 9"/></svg></span><span class=t-label>Reconciled</span></div><div class=t-val>{{ n_recon }}</div></button>
<button class="tile" data-filter="signed"><div class=t-top><span class=t-ic><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3 5 6v5.5c0 4 3 6.5 7 7.5 4-1 7-3.5 7-7.5V6l-7-3Z"/><path d="m9 12 2 2 4-4"/></svg></span><span class=t-label>Signed off</span></div><div class=t-val>{{ n_signed }}</div></button>
<button class="tile" data-filter="exceptions"><div class=t-top><span class=t-ic><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M12 4 2.5 20h19L12 4Z"/><path d="M12 10v4.5"/><path d="M12 17.6h.01"/></svg></span><span class=t-label>Open exceptions</span></div><div class="t-val {{ 'warn' if tot_exc else '' }}">{{ tot_exc }}</div></button>
</div>
<div class=fbar id=fbar></div>
<table>
<thead><tr><th>Account</th><th>Type</th><th>Currency</th><th>Status</th><th>Period</th><th>Matches</th><th>Exceptions</th><th class=a>Unreconciled</th></tr></thead>
<tbody>
{% for r in rows %}<tr data-status="{{ r.status }}" data-exc="{{ r.get('exc',0) }}">
<td><a href="{{ url_for('detail', name=r.name) }}"><b>{{ r.name }}</b></a></td>
<td>{{ 'bank' if r.type=='bank' else 'credit card' }}</td>
<td>{{ r.currency or '—' }}</td>
<td>{% if r.status=='none' %}<span class="pill none">Not reconciled</span>{% elif r.status=='signed' %}<span class="pill signed">Signed off</span>{% elif r.rec_status=='balanced' %}<span class="pill open">Balanced</span>{% else %}<span class="pill open">In progress</span>{% endif %}</td>
{% if r.status=='none' %}<td class=muted>—</td><td class=muted>—</td><td class=muted>—</td><td class="a muted">—</td>
{% else %}<td>{{ r.p_start }} → {{ r.p_end }}</td>
<td>{{ r.exact }} exact{% if r.fuzzy %}, {{ r.fuzzy }} fuzzy{% endif %}{% if r.m2o %}, {{ r.m2o }} batched{% endif %}</td>
<td>{{ r.exc }}</td>
<td class=a>{% if r.rec_status=='balanced' %}<span class=ok>0.00 · balanced</span>{% elif r.rec_status=='out' %}<span class=bad>{{ r.rec_diff|money }} · out</span>{% else %}<span class=muted>needs {{ r.missing }}</span>{% endif %}</td>
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
</div><div class=appfoot><a href="{{ url_for('terms') }}">Terms</a> · <a href="{{ url_for('privacy') }}">Privacy</a> · <a href="mailto:{{ contact_email }}">Contact</a></div>
<div id=loadingov><div class=spin></div><div class=msg id=loadingmsg>Loading...</div></div>
<script>(function(){
var ov=document.getElementById('loadingov'),msg=document.getElementById('loadingmsg'),timer,hideTimer;
function show(t){if(msg&&t)msg.textContent=t;if(ov)ov.classList.add('on');clearTimeout(hideTimer);hideTimer=setTimeout(function(){if(ov)ov.classList.remove('on');},40000);}
function schedule(t){clearTimeout(timer);timer=setTimeout(function(){show(t);},180);}
document.addEventListener('click',function(e){
var a=e.target.closest?e.target.closest('a'):null;if(!a)return;
var href=a.getAttribute('href')||'';if(!href)return;
if(a.target==='_blank'||a.hasAttribute('download'))return;
if(href[0]==='#'||href.indexOf('javascript:')===0||href.indexOf('mailto:')===0)return;
if(href.indexOf('.csv')>-1||href.indexOf('/template/')>-1||href.indexOf('/backup')>-1)return;
if(e.metaKey||e.ctrlKey||e.shiftKey||e.altKey)return;
schedule('Loading...');});
document.addEventListener('submit',function(e){
if(e.defaultPrevented)return;
var act=(e.target.getAttribute&&e.target.getAttribute('action'))||'';var t='Working...';
if(act.indexOf('/record')>-1)t='Recording in QuickBooks...';
else if(act.indexOf('/match')>-1||act.indexOf('/unmatch')>-1)t='Matching...';
if(act.indexOf('/upload')>-1)t='Reconciling your statement...';
else if(act.indexOf('/import_books')>-1)t='Importing your books...';
else if(act.indexOf('/sync')>-1)t='Syncing from QuickBooks...';
else if(act.indexOf('/clear')>-1)t='Clearing account data...';
else if(act.indexOf('/signoff')>-1)t='Signing off...';
else if(act.indexOf('/reopen')>-1)t='Reopening...';
else if(act.indexOf('/disconnect')>-1)t='Disconnecting...';
else if(act.indexOf('/check-connection')>-1)t='Checking connection...';
schedule(t);});
window.addEventListener('pageshow',function(){clearTimeout(timer);clearTimeout(hideTimer);if(ov)ov.classList.remove('on');});
})();</script>
</body></html>"""


@app.route("/")
def dashboard():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id, name, type, currency FROM account WHERE coalesce(is_active,true) ORDER BY type, name;")
    accts = cur.fetchall()
    rows = [account_summary(cur, a, n, t, ccy) for a, n, t, ccy in accts]
    cur.execute("SELECT count(*) FROM account WHERE NOT coalesce(is_active,true);")
    n_hidden = cur.fetchone()[0]
    cur.close(); conn.close()
    n_recon = sum(1 for r in rows if r["status"] != "none")
    n_signed = sum(1 for r in rows if r["status"] == "signed")
    tot_exc = sum(r.get("exc", 0) for r in rows)
    sync_msg = session.pop("sync_msg", None)
    qbo_connected = qbo_is_connected()
    return render_template_string(DASH_TEMPLATE, qbo_connected=qbo_connected, rows=rows, n_recon=n_recon, n_signed=n_signed, n_hidden=n_hidden,
                                  tot_exc=tot_exc, sync_msg=sync_msg, now=datetime.now(EAT).strftime("%Y-%m-%d %H:%M"))


DETAIL_TEMPLATE = """<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>{{ name }} · Reconciliation Tool</title>""" + CSS + """</head><body>
<div class=nav><span class=brand><span class=dot></span>Reconciliation Tool</span><span class=links>{% if session.name %}<span style="color:var(--muted);font-size:13px;margin-right:6px">{{ session.name }}</span>{% endif %}<a href="{{ url_for('dashboard') }}">← All accounts</a>{% if session.is_admin %}<a href="{{ url_for('manage_accounts') }}">Accounts</a><a href="{{ url_for('users') }}">Users</a><a href="{{ url_for('backup') }}">Backup</a>{% endif %}<a href="{{ url_for('change_password') }}">Change password</a><a href="{{ url_for('logout') }}">Sign out</a></span></div>
<div class=wrap><h1>{{ name }}</h1>
{% if has_results %}<div class=sub>Statement period {{ p_start }} to {{ p_end }}{% if ccy %} · {{ ccy }}{% endif %}</div>{% else %}<div class=sub>No statement yet — upload one to reconcile.</div>{% endif %}
<form method=post action="{{ url_for('set_currency', name=name) }}" style="margin:0 0 20px;display:flex;align-items:center;gap:8px"><label style="font-size:13px;color:var(--muted)">Currency</label><input name=currency value="{{ ccy or '' }}" maxlength=8 placeholder="UGX" style="width:80px;padding:6px 9px;border:1px solid var(--line);border-radius:7px;font-size:13px;text-transform:uppercase"><button type=submit class=btn-sm>Set</button></form>
<a href="{{ url_for('history', name=name) }}" class=btn-sm style="text-decoration:none;display:inline-block;margin:0 0 20px">View reconciliation history</a>
{% if atype=='credit_card' %}<div style="background:#fffbeb;border:1px solid #fde68a;color:#92400e;padding:10px 13px;border-radius:9px;font-size:13px;margin:0 0 20px;line-height:1.5">Credit-card account: enter <b>charges as positive</b> and <b>payments/refunds as negative</b>, so signs match your QuickBooks credit-card register.</div>{% endif %}
{{ sync_banner() }}
{% if detail_msg %}<div id=flash style="background:var(--accent-soft);color:var(--accent);padding:11px 14px;border-radius:9px;font-size:14px;margin-bottom:18px;font-weight:550">{{ detail_msg }}</div>
<script>// After an action the page opens at its section (#sec-record...): show the result there, not off-screen at the top.
document.addEventListener('DOMContentLoaded',function(){var f=document.getElementById('flash'),id=location.hash.slice(1),
h=id&&document.getElementById(id);if(f&&h&&h.parentNode){f.style.marginTop='8px';h.parentNode.insertBefore(f,h.nextSibling);}});</script>{% endif %}
<div class=upload>
<form action="{{ url_for('upload', name=name) }}" method=post enctype=multipart/form-data style="margin-bottom:14px">
<div class=u-label>Bank statement (PDF, CSV or OFX) · <a href="{{ url_for('template', kind='bank') }}" style="color:var(--accent);font-weight:600">download template</a></div>
<input type=file name=statement accept=.pdf,.csv,.ofx required> <button type=submit class=btn>Upload &amp; reconcile</button>
<div class=balform style="margin-top:10px">
<div><label>Opening balance <span class=muted>(optional)</span></label><input name=opening_balance inputmode=decimal placeholder="from the statement"></div>
<div><label>Closing balance</label><input name=closing_balance inputmode=decimal placeholder="from the statement"></div>
<div><label>Period start</label><input type=date name=period_start></div>
<div><label>Period end (statement date)</label><input type=date name=period_end></div>
<div><label>PDF password <span class=muted>(if it has one)</span></label><input type=password name=pdf_password autocomplete=off placeholder="only for protected PDFs"{% if request.args.get('pdfpw') %} autofocus style="border-color:var(--accent)"{% endif %}></div>
</div>
<div class=muted style="font-size:12px">Leave blank if the file has a running-balance column (PDF, CSV) or a ledger balance (OFX) — they're read automatically. A PDF must be the one downloaded from online banking, not a scan; its password is used once to open it and never stored. Opening defaults to last signed-off closing. Set the period end to the statement date: without it the period ends on the last transaction, and later book items won't show as outstanding.</div></form>
<div class=u-label>Books</div>
{% if qbo_connected and qbo_linked %}
<form method=post action="{{ url_for('sync') }}" class=booksrc><input type=hidden name=back value="{{ name }}">
<span>&#10003; Read straight from QuickBooks{% if last_sync %} · last synced {{ last_sync }}{% endif %}</span>
<button type=submit class=btn-sm>Refresh from QuickBooks</button></form>
<div class=hint style="margin-top:4px">Uploading a statement refreshes the books automatically — no export needed.</div>
<details style="margin-top:10px"><summary class=muted style="cursor:pointer;font-size:12.5px">Offline? Import a QuickBooks CSV export instead</summary>
{% else %}
<div class=hint style="margin-bottom:8px">{% if not qbo_connected %}<a href="{{ url_for('connect') }}" style="color:var(--accent);font-weight:600">Connect QuickBooks</a> to read books directly, or import a CSV export:{% else %}This account isn't linked to a QuickBooks account — import a CSV export:{% endif %}</div>
{% endif %}
<form action="{{ url_for('import_books', name=name) }}" method=post enctype=multipart/form-data style="margin-top:8px">
<div class=u-label>QuickBooks CSV export · <a href="{{ url_for('template', kind='books') }}" style="color:var(--accent);font-weight:600">download template</a></div>
<input type=file name=books accept=.csv required> <button type=submit class=btn-sm>Import books</button></form>
{% if qbo_connected and qbo_linked %}</details>{% endif %}
{% if session.is_admin %}
<div style="margin-top:15px;border-top:1px solid var(--line-soft);padding-top:13px;display:flex;align-items:center;gap:10px;flex-wrap:wrap">
<form method=post action="{{ url_for('clear_account', name=name) }}" onsubmit="return confirm('Clear ALL statements and book transactions for this account? This removes old synced or imported data so you can start fresh offline. This cannot be undone.');" style="display:inline">
<button type=submit class=btn-sm style="color:var(--bad);border-color:var(--bad-soft)">Clear this account&#39;s data</button></form>
<form method=post action="{{ url_for('delete_account', name=name) }}" onsubmit="return confirm('Delete this account entirely, including all its statements and transactions? Use this to remove old sandbox accounts. This cannot be undone.');" style="display:inline;margin-left:8px">
<button type=submit class=btn-sm style="color:var(--bad);border-color:var(--bad-soft)">Delete account</button></form>
<span style="color:var(--muted);font-size:12px">Removes old synced/imported data for a clean offline slate</span>
</div>
{% endif %}
</div>
{% if has_results %}
<div class=tiles id=dtiles>
<button class="tile" data-target="sec-matched"><div class=t-top><span class=t-ic><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="m5 12.5 4.5 4.5L19 7"/></svg></span><span class=t-label>Exact</span></div><div class=t-val>{{ n_exact }}</div></button>
<button class="tile" data-target="sec-review" data-fallback="sec-matched"><div class=t-top><span class=t-ic><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M5 9.5h14"/><path d="M5 14.5h14"/><path d="M16 4 8 20"/></svg></span><span class=t-label>To review</span></div><div class="t-val {{ 'warn' if n_pending else '' }}">{{ n_pending }}</div></button>
<button class="tile" data-target="sec-review" data-fallback="sec-matched"><div class=t-top><span class=t-ic><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3.5" width="8" height="8" rx="1.5"/><rect x="13" y="12.5" width="8" height="8" rx="1.5"/><path d="M13 7.5h3a2 2 0 0 1 2 2v3"/></svg></span><span class=t-label>Batched</span></div><div class=t-val>{{ n_m2o }}</div></button>
<button class="tile" data-target="sec-exceptions"><div class=t-top><span class=t-ic><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M12 4 2.5 20h19L12 4Z"/><path d="M12 10v4.5"/><path d="M12 17.6h.01"/></svg></span><span class=t-label>Exceptions</span></div><div class=t-val>{{ writebacks|length + deposits|length + on_stmt|length + in_books|length }}</div></button>
<button class="tile" data-target="sec-balance"><div class=t-top><span class=t-ic><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3.5v17"/><path d="M7 6.5h10"/><path d="M7 6.5 4 12.8a3 3 0 0 0 6 0L7 6.5Z"/><path d="M17 6.5l-3 6.3a3 3 0 0 0 6 0L17 6.5Z"/><path d="M8.5 20.5h7"/></svg></span><span class=t-label>Unreconciled</span></div><div class=t-val style="color:{{ '#047857' if rec.status=='balanced' else ('#b42318' if rec.status=='out' else '#667085') }}">{% if rec.rec_diff is none %}—{% else %}{{ rec.rec_diff|money }}{% endif %}</div></button>
</div>
<h2 id=sec-balance style="font-size:15px">Balance reconciliation</h2>
{% set cc = atype=='credit_card' %}
<div class=recgrid>
<table class=rec>
<tr><th colspan=2>Bank {{ 'card statement' if cc else 'statement' }}</th></tr>
<tr><td>Closing balance at {{ p_end }}{% if rec.closing_src %} <span class=src>· {{ src_label[rec.closing_src] }}</span>{% endif %}</td><td class=a>{% if rec.closing is none %}<span class=muted>not entered</span>{% else %}{{ rec.closing|money }}{% endif %}</td></tr>
<tr><td>Add: {{ 'charges' if cc else 'deposits' }} in books, not yet on statement ({{ rec.n_out_in }})</td><td class=a>{{ rec.out_in|money }}</td></tr>
<tr><td>Less: {{ 'payments & refunds' if cc else 'payments' }} in books, not yet on statement ({{ rec.n_out_out }})</td><td class=a>{{ rec.out_out|money }}</td></tr>
<tr class=tot><td>Adjusted bank balance</td><td class=a>{% if rec.adj_bank is none %}—{% else %}{{ rec.adj_bank|money }}{% endif %}</td></tr>
</table>
<table class=rec>
<tr><th colspan=2>Books</th></tr>
<tr><td>Book balance at {{ p_end }}{% if rec.book_src %} <span class=src>· {{ src_label[rec.book_src] }}</span>{% endif %}</td><td class=a>{% if rec.book is none %}<span class=muted>not entered</span>{% else %}{{ rec.book|money }}{% endif %}</td></tr>
<tr><td>Add/less: on statement, not in books ({{ rec.n_unrec }})</td><td class=a>{{ rec.unrec|money }}</td></tr>
<tr><td>Add/less: amount differences on matched items ({{ rec.n_match_adj }})</td><td class=a>{{ rec.match_adj|money }}</td></tr>
<tr class=tot><td>Adjusted book balance</td><td class=a>{% if rec.adj_book is none %}—{% else %}{{ rec.adj_book|money }}{% endif %}</td></tr>
</table>
</div>
<div class="recres {{ rec.status }}">
{% if rec.status=='balanced' %}<span>&#10003; Balanced — adjusted bank and book balances agree</span><span>0.00</span>
{% elif rec.status=='out' %}<span>Out of balance{% if rec.rec_diff == 0 %} — the statement itself doesn't add up{% endif %}</span><span>{{ rec.rec_diff|money }}</span>
{% else %}<span>Enter the {{ rec.missing }} to complete the reconciliation</span><span>—</span>{% endif %}
</div>
{% if rec.foot_diff %}<div class="recnote bad">The statement doesn't add up: opening {{ rec.opening|money }} + {{ rec.n_lines }} lines ({{ rec.moves|money }}) = {{ (rec.opening + rec.moves)|money }}, but the closing balance is {{ rec.closing|money }} (out by {{ rec.foot_diff|money }}). A line is probably missing from the upload, or a balance was mistyped.</div>
{% elif rec.foot_diff is not none and rec.opening_src != 'derived' %}<div class=sub style="margin:4px 0 8px;font-size:13px">&#10003; Statement adds up: opening {{ rec.opening|money }} + movements {{ rec.moves|money }} = closing {{ rec.closing|money }}</div>{% endif %}
{% if rec.prev_closing is not none and rec.opening is not none and rec.opening_src != 'carried' and rec.prev_closing != rec.opening %}<div class="recnote warn">This opening balance ({{ rec.opening|money }}) doesn't match the last signed-off closing balance ({{ rec.prev_closing|money }} at {{ rec.prev_end }}). Check for a missing statement between the two periods.</div>{% endif %}
{% if rec.n_gone %}<div class="recnote bad">{{ rec.n_gone }} book transaction{{ '' if rec.n_gone==1 else 's' }} matched in this reconciliation {{ 'has' if rec.n_gone==1 else 'have' }} since been deleted, voided or moved in QuickBooks.{% if signed_off %} Undo the sign-off and re-upload the statement to re-match.{% endif %}</div>{% endif %}
{% if rec.bf_count %}<div class=sub style="margin:4px 0 8px;font-size:13px">Includes {{ rec.bf_count }} item{{ '' if rec.bf_count==1 else 's' }} brought forward from earlier periods, still not cleared by the bank.</div>{% endif %}
<details {% if rec.status=='incomplete' %}open{% endif %} style="margin:10px 0 20px">
<summary style="cursor:pointer;color:var(--accent);font-size:13px;font-weight:600">Edit balances</summary>
<form method=post action="{{ url_for('balances', name=name) }}" class=balform>
<div><label>Period start</label><input type=date name=period_start value="{{ p_start }}"></div>
<div><label>Period end</label><input type=date name=period_end value="{{ p_end }}"></div>
<div><label>Opening balance</label><input name=opening inputmode=decimal value="{{ '' if rec.opening is none else rec.opening }}"></div>
<div><label>Closing balance (statement)</label><input name=closing inputmode=decimal value="{{ '' if rec.closing is none else rec.closing }}"></div>
<div><label>Book balance at {{ p_end }}</label><input name=book inputmode=decimal value="{{ '' if rec.book is none else rec.book }}"></div>
<button type=submit class=btn-sm>Save balances</button>
</form>
{% if qbo_linked %}<form method=post action="{{ url_for('balances', name=name) }}" style="margin:8px 0 0"><input type=hidden name=action value=fetch_book><button type=submit class=btn-sm>Get book balance from QuickBooks</button> <span class=muted style="font-size:12px">Syncs first, then reads the account balance as at {{ p_end }}.</span></form>{% endif %}
<div class=muted style="font-size:12px;margin-top:8px;line-height:1.5">Book balance is the account's register (or Balance Sheet) balance in QuickBooks as at the statement end date{{ ' — enter what you owe as a positive number' if cc else '' }}. Blank opening falls back to the last signed-off closing balance.</div>
</details>
<div style="margin-bottom:24px">
{% if signed_off %}<span class="pill signed">Signed off {{ signed_off }}</span>
<form method=post action="{{ url_for('reopen', name=name) }}" style="display:inline;margin-left:8px" onsubmit="return confirm('Reopen this reconciliation? You can sign it off again afterward.');"><button type=submit class=btn-sm>Undo sign-off</button></form>
{% elif rec.status=='balanced' and not n_pending %}<form method=post action="{{ url_for('signoff', name=name) }}" style="display:inline"><button type=submit class=btn-go>Sign off this reconciliation</button></form>
{% else %}<button type=button class=btn-go disabled style="opacity:.45;cursor:not-allowed" title="{{ 'Review the suggested matches first' if n_pending else 'Balance the reconciliation first' }}">Sign off this reconciliation</button>{% if n_pending %} <a href="#sec-review" class=hint style="color:var(--warn);font-weight:600">{{ n_pending }} suggested match{{ '' if n_pending==1 else 'es' }} to review first</a>{% endif %}{% endif %}
<a href="{{ url_for('exceptions_csv', name=name) }}" class=btn-sm style="display:inline-block;text-decoration:none;margin-left:8px">Download exceptions (CSV)</a>
<a href="{{ url_for('qbo_import_csv', name=name) }}" class=btn-sm style="display:inline-block;text-decoration:none;margin-left:8px">Download for QuickBooks (CSV)</a>
<a href="{{ url_for('report', name=name) }}" class=btn-sm style="display:inline-block;text-decoration:none;margin-left:8px" target=_blank rel=noopener>Print reconciliation report</a>
{% if session.is_admin and not signed_off and (rec.status!='balanced' or n_pending) %}<details style="margin-top:12px"><summary style="cursor:pointer;color:var(--muted);font-size:13px">Admin: sign off anyway</summary>
<form method=post action="{{ url_for('signoff', name=name) }}" class=balform><input type=hidden name=override value=1>
<div><label>Reason (recorded with the sign-off)</label><input name=note required style="width:340px;max-width:100%"></div>
<button type=submit class=btn-sm style="color:var(--bad);border-color:var(--bad-soft)">Sign off unbalanced</button></form></details>{% endif %}
</div>
{% if reviewable %}
<h2 id=sec-review style="font-size:15px">Suggested matches{% if n_pending %} — {{ n_pending }} to review{% endif %}</h2>
<div class=sub style="margin:-4px 0 12px">These aren't counted until you confirm them.{% if n_signflip %} {{ n_signflip }} {{ 'is an' if n_signflip==1 else 'are' }} opposite-sign pairing{{ '' if n_signflip==1 else 's' }} (same amount, flipped sign) — usually a transfer entered the wrong way round.{% endif %}</div>
{% if n_pending > 1 %}<form method=post action="{{ url_for('review_all', name=name) }}" style="margin:0 0 4px" onsubmit="return confirm('Confirm all {{ n_pending }} suggested matches?');"><button type=submit class=btn-sm>Confirm all {{ n_pending }}</button></form>{% endif %}
<table><tr><th>Why suggested</th><th>Statement side</th><th>Books side</th><th>Status</th><th></th></tr>
{% for r in reviewable %}<tr>
<td><span class="tag {{ 'fuzzy' if r.type in ('fuzzy','manual') else 'exact' }}">{{ 'same payee, amount differs' if r.type=='fuzzy' else ('opposite sign' if r.type=='manual' else ('cleared later' if r.type=='exact' else 'batched total')) }}</span></td>
<td>{% for d,a,w in r.sls %}{{ d }} · {{ a|money }} · {{ w }}<br>{% endfor %}{% if r.delta and r.delta != 0 %}<span style="color:#9a6a16">off {{ r.delta|money }}</span>{% endif %}</td>
<td>{% for d,a,w in r.bts %}{{ d }} · {{ a|money }} · {{ w }}<br>{% endfor %}</td>
<td>{% if r.status=='proposed' %}<span class="tag pending">to review</span>{% elif r.status=='rejected' %}<span style="color:#b3471f">rejected</span>{% else %}<span style="color:#3a7d44">confirmed</span>{% endif %}</td>
<td><form method=post action="{{ url_for('review_match', name=name, match_id=r.id) }}" class=btnrow>
{% if r.status=='proposed' %}<button type=submit name=status value=confirmed class=btn-sm>Confirm</button><button type=submit name=status value=rejected class=btn-sm>Reject</button>
{% else %}<button type=submit name=status value=proposed class=btn-sm>Undo</button>{% endif %}
</form></td>
</tr>{% endfor %}</table>
{% endif %}
<h2 id=sec-matched style="font-size:15px">Matched ({{ matched|length }}{% if n_m2o %} + {{ n_m2o }} batched{% endif %})</h2>
<table><tr><th>Date</th><th>Payee</th><th></th><th class=a>Statement</th><th class=a>Books</th></tr>
{% for mt, delta, d, samt, who, bamt in matched %}<tr><td>{{ d }}</td><td>{{ who }}</td>
<td><span class="tag {{ mt }}">{{ mt }}{% if delta and delta != 0 %} · off {{ delta|money }}{% endif %}</span></td>
<td class=a>{{ samt|money }}</td><td class=a>{{ bamt|money }}</td></tr>{% endfor %}</table>
{% if writebacks or deposits %}
<h2 id=sec-record style="font-size:15px">Not in QuickBooks yet — record them ({{ writebacks|length + deposits|length }})</h2>
<div class=sub style="margin:-4px 0 12px">The account and payee are suggested from how similar bank lines were posted before. Check them, then record one line, or tick several and record them together. Each becomes {{ 'a credit-card expense' if atype=='credit_card' else 'an expense (money out) or a deposit (money in)' }} in QuickBooks, dated as on the statement. Money moved between your own accounts: pick the other account under <em>Transfer</em> and it's recorded as one transfer (same currency only).</div>
<form method=post action="{{ url_for('record', name=name) }}" id=recform>
<table class=rectbl><tr><th><input type=checkbox id=selall title="Select all"></th><th>Date</th><th>Bank description</th><th class=a>Amount</th><th>Post to account</th><th>Payee</th><th></th></tr>
{% for w in writebacks + deposits %}<tr>
<td>{% if w.recordable and not w.wb %}<input type=checkbox name=sel value="{{ w.line_id }}" class=rsel data-amt="{{ w.amount }}" {% if w.acct_id and not w.dups %}checked{% endif %}>{% endif %}</td>
<td>{{ w.date }}</td>
<td style="white-space:normal;max-width:280px">{{ w.who }}{% if w.sug %}<div class=hint>&#8627; {{ w.sug.because }}</div>{% endif %}
{% if w.dups and not w.wb %}<div class=dupwarn>&#9888; QuickBooks may already have this: {% for x in w.dups %}{{ x.date }} · {{ x.amount|money }}{% if x.who %} · {{ x.who }}{% endif %}{% if not loop.last %}; {% endif %}{% endfor %}.
{% if w.dup_matchable %}<br><button type=button class="btn-sm mm-open" data-line="{{ w.line_id }}" data-txn="{{ w.dup_matchable }}">Match it instead</button>
{% else %}<br>It's dated outside this statement period. If it's the same money, don't record it again — correct its date in QuickBooks, then refresh.{% endif %}</div>{% endif %}</td>
<td class=a>{{ w.amount|money }}</td>
{% if w.wb == 'pending' %}<td colspan=3 style="white-space:normal"><span class=bad>Recording was interrupted — check QuickBooks before trying again.</span><br><button type=submit name=reset value="{{ w.line_id }}" formaction="{{ url_for('record_reset', name=name) }}" class=btn-sm style="margin-top:6px">I checked — it's not in QuickBooks</button></td>
{% elif w.wb == 'done' %}<td colspan=3 class=muted style="white-space:normal">Recorded in QuickBooks{% if w.qbo_id %} (#{{ w.qbo_id }}){% endif %} — it will match on the next refresh.</td>
{% elif not w.recordable %}<td colspan=3 class=muted style="white-space:normal">{{ w.why_not }}</td>
{% else %}
<td><select name="acct_{{ w.line_id }}" class=acct data-dir="{{ 'xfer' if w.xfer_only else ('out' if w.out else 'in') }}" data-sel="{{ w.acct_id or '' }}" aria-label="Account"></select>{% if w.xfer_only %}<div class=hint>Card payment: choose the bank it was paid from</div>{% elif w.is_xfer %}<div class=hint>Recorded as a transfer {{ 'to' if w.out else 'from' }} this account</div>{% endif %}{% if w.sug and not w.acct_id %}<div class=hint>'{{ w.sug.cat }}' isn't in your chart of accounts any more</div>{% elif w.sug %}<div class=hint>{{ "%.0f"|format(w.sug.conf*100) }}% match</div>{% endif %}</td>
<td><input name="payee_{{ w.line_id }}" value="{{ w.payee or '' }}" placeholder="optional" class=payee aria-label="Payee">{% if w.dups %}<label class=hint style="display:flex;gap:5px;align-items:center;margin-top:6px"><input type=checkbox name="dupok_{{ w.line_id }}" value=1> Not a duplicate</label>{% endif %}<input type=hidden name="psug_{{ w.line_id }}" value="{{ w.payee or '' }}"><input type=hidden name="pref_{{ w.line_id }}" value="{{ w.payee_ref or '' }}"></td>
<td><button type=submit name=only value="{{ w.line_id }}" class=btn-sm>Record</button></td>
{% endif %}
</tr>{% endfor %}</table>
<button type=submit name=bulk value=1 class=btn>Record selected in QuickBooks</button>
</form>
<script id=coa-data type=application/json>{{ coa_json }}</script>
<script>(function(){
var el=document.getElementById('coa-data');if(!el)return;var coa=[];try{coa=JSON.parse(el.textContent)}catch(e){}
var order={out:['Expense','Cost of Goods Sold','Other Expense'],'in':['Income','Other Income']};
var groups={},xfer=[];coa.forEach(function(a){if(a.x)xfer.push(a);else(groups[a.t]=groups[a.t]||[]).push(a)});
function opt(g,a,sel){var op=document.createElement('option');op.value=a.id;op.textContent=a.n;if(a.id===sel)op.selected=true;g.appendChild(op)}
document.querySelectorAll('select.acct').forEach(function(s){
  var dir=s.getAttribute('data-dir'),pref=order[dir]||[],sel=s.getAttribute('data-sel');
  var o=document.createElement('option');o.value='';o.textContent=dir==='xfer'?'— paid from which bank? —':'— choose account —';s.appendChild(o);
  if(dir!=='xfer')Object.keys(groups).sort(function(a,b){var x=pref.indexOf(a),y=pref.indexOf(b);x=x<0?99:x;y=y<0?99:y;return x-y||a.localeCompare(b)}).forEach(function(t){
    var g=document.createElement('optgroup');g.label=t;
    groups[t].forEach(function(a){opt(g,a,sel)});
    s.appendChild(g)});
  if(xfer.length){var g=document.createElement('optgroup');g.label=dir==='out'?'Transfer to your account':dir==='in'?'Transfer from your account':'Transfer from your bank';
    xfer.forEach(function(a){opt(g,a,sel)});s.appendChild(g)}
  s.addEventListener('change',function(){var cb=s.closest('tr').querySelector('.rsel');if(cb&&s.value)cb.checked=true});
});
var all=document.getElementById('selall');if(all)all.addEventListener('change',function(){document.querySelectorAll('.rsel').forEach(function(c){c.checked=all.checked})});
var f=document.getElementById('recform');
if(f)f.addEventListener('submit',function(e){var b=e.submitter;if(!b||b.name!=='bulk')return;
  var n=0,t=0;document.querySelectorAll('.rsel:checked').forEach(function(c){n++;t+=Math.abs(parseFloat(c.getAttribute('data-amt'))||0)});
  if(!n){e.preventDefault();return}
  if(!confirm('Record '+n+' transaction'+(n==1?'':'s')+' totalling '+t.toLocaleString(undefined,{minimumFractionDigits:2})+' in QuickBooks?'))e.preventDefault();});
})();</script>
{% endif %}
{% if all_unmatched or in_books or user_matches %}
<h2 id=sec-manual style="font-size:15px">Match manually</h2>
<div class=sub style="margin:-4px 0 12px">Pair bank lines with QuickBooks transactions the matcher missed — one to one, or several together (two deposits banked as one, a payment split in the books). Tick items on both sides; the QuickBooks list re-sorts to put the closest amounts first. Only QuickBooks entries dated up to {{ p_end }} can be matched here.</div>
{% if user_matches %}
<table><tr><th>Matched by you</th><th>Statement side</th><th>Books side</th><th class=a>Difference</th><th></th></tr>
{% for u in user_matches %}<tr>
<td class=hint>{{ u.by or '' }}{% if u.at %}<br>{{ u.at.strftime('%Y-%m-%d') }}{% endif %}</td>
<td>{% for d,a,w in u.sls %}{{ d }} · {{ a|money }} · {{ w }}<br>{% endfor %}</td>
<td>{% for d,a,w in u.bts %}{{ d }} · {{ a|money }} · {{ w }}<br>{% endfor %}</td>
<td class=a>{% if u.delta %}<span class=warn>{{ u.delta|money }}</span>{% else %}0.00{% endif %}</td>
<td><form method=post action="{{ url_for('unmatch', name=name, match_id=u.id) }}"><button type=submit class=btn-sm>Undo</button></form></td>
</tr>{% endfor %}</table>
{% endif %}
{% if all_unmatched and in_books %}
<form method=post action="{{ url_for('manual_match', name=name) }}" id=mmform>
<div class=mmgrid>
<div class=mmcol><div class=mmhead>Bank statement ({{ all_unmatched|length }}) <input class=mmsearch data-list=mml placeholder="Search" aria-label="Search bank lines"></div>
<div class=mmlist id=mml>{% for lid, d, a, who in all_unmatched %}<label class=mmrow data-amt="{{ a }}" data-date="{{ d }}" data-text="{{ who|lower }} {{ a }} {{ d }}"><input type=checkbox name=ml value="{{ lid }}"><span class=hint>{{ d }}</span><span class=mmw title="{{ who }}">{{ who }}</span><span class=a>{{ a|money }}</span></label>{% endfor %}</div></div>
<div class=mmcol><div class=mmhead>QuickBooks ({{ in_books|length }}) <input class=mmsearch data-list=mmb placeholder="Search" aria-label="Search QuickBooks transactions"></div>
<div class=mmlist id=mmb>{% for tid, d, a, who in in_books %}<label class=mmrow data-amt="{{ a }}" data-date="{{ d }}" data-text="{{ who|lower }} {{ a }} {{ d }}"><input type=checkbox name=mb value="{{ tid }}"><span class=hint>{{ d }}</span><span class=mmw title="{{ who }}">{{ who }}{% if d < p_start %} <span class="tag bf">brought forward</span>{% endif %}</span><span class=a>{{ a|money }}</span></label>{% endfor %}</div></div>
</div>
<div class=mmbar><span id=mmsum>Tick at least one item on each side.</span><button type=submit class=btn id=mmgo disabled>Match selected</button></div>
</form>
<script>(function(){
var f=document.getElementById('mmform');if(!f)return;
function num(x){return parseFloat(x)||0}
function rowsOf(id){return Array.prototype.slice.call(document.querySelectorAll('#'+id+' .mmrow'))}
function picked(id){return rowsOf(id).filter(function(r){return r.querySelector('input').checked})}
function fmt(v){return v.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2})}
var diff=0;
function update(rank){
  var L=picked('mml'),B=picked('mmb'),sl=0,sb=0;
  rowsOf('mml').concat(rowsOf('mmb')).forEach(function(r){r.classList.toggle('on',r.querySelector('input').checked)});
  L.forEach(function(r){sl+=num(r.getAttribute('data-amt'))});B.forEach(function(r){sb+=num(r.getAttribute('data-amt'))});
  diff=Math.round((sl-sb)*100)/100;
  var s=document.getElementById('mmsum'),go=document.getElementById('mmgo');
  if(!L.length||!B.length){s.textContent='Tick at least one item on each side.';s.className='';go.disabled=true}
  else{s.textContent='Bank '+fmt(sl)+'  ·  QuickBooks '+fmt(sb)+'  ·  Difference '+fmt(diff);s.className=diff===0?'ok':'warn';go.disabled=false}
  if(rank&&L.length){   // closest remaining amount first, then closest date
    var target=sl-sb,d0=new Date(L[0].getAttribute('data-date')).getTime(),box=document.getElementById('mmb');
    rowsOf('mmb').sort(function(a,b){
      var ca=a.querySelector('input').checked,cb=b.querySelector('input').checked;if(ca!==cb)return ca?-1:1;
      var x=Math.abs(num(a.getAttribute('data-amt'))-target)-Math.abs(num(b.getAttribute('data-amt'))-target);if(x)return x;
      return Math.abs(new Date(a.getAttribute('data-date')).getTime()-d0)-Math.abs(new Date(b.getAttribute('data-date')).getTime()-d0);
    }).forEach(function(r){box.appendChild(r)});box.scrollTop=0;
  }
}
document.getElementById('mml').addEventListener('change',function(){update(true)});
document.getElementById('mmb').addEventListener('change',function(){update(false)});
document.querySelectorAll('.mmsearch').forEach(function(inp){inp.addEventListener('input',function(){
  var q=inp.value.trim().toLowerCase().replace(/,/g,'');
  rowsOf(inp.getAttribute('data-list')).forEach(function(r){r.style.display=!q||r.getAttribute('data-text').indexOf(q)>-1?'':'none'})})});
document.querySelectorAll('.mm-open').forEach(function(b){b.addEventListener('click',function(){
  f.querySelectorAll('input[type=checkbox]').forEach(function(c){c.checked=false});
  var l=f.querySelector('input[name=ml][value="'+b.getAttribute('data-line')+'"]'),t=f.querySelector('input[name=mb][value="'+b.getAttribute('data-txn')+'"]');
  if(l)l.checked=true;if(t)t.checked=true;update(true);
  document.getElementById('sec-manual').scrollIntoView({behavior:'smooth',block:'start'});})});
f.addEventListener('submit',function(e){if(diff!==0&&!confirm('The two sides differ by '+fmt(diff)+'. Match anyway? The difference will show under amount differences.'))e.preventDefault()});
update(false);
})();</script>
{% endif %}
{% endif %}
{% if n_xfer %}<h2 id=sec-exceptions style="font-size:15px">Possible transfers between your own accounts ({{ n_xfer }})</h2>
<div style="background:#fffbeb;border:1px solid #fde68a;color:#92400e;padding:10px 13px;border-radius:9px;font-size:13px;margin:0 0 12px;line-height:1.5">Suggestions only \u2014 check each pair first. A genuine transfer is recorded once, as a Transfer between the two accounts, never as an expense on one and a deposit on the other. <em>Record as one transfer</em> does that and matches both bank lines to it.</div>
<table><tr><th>Date</th><th>On this statement</th><th class=a>Amount</th><th>Possible counterpart</th><th>Why flagged</th></tr>
{% for lid, d, a, who in all_unmatched %}{% if xfers.get(lid) %}{% for c in xfers[lid] %}
<tr><td>{{ d }}</td><td>{{ who }}</td><td class=a>{{ a|money }}</td>
<td><strong>{{ c.account }}</strong><br><span style="color:var(--muted);font-size:12px">{{ c.date }} \u00b7 {{ c.amount|money }}{% if c.who %} \u00b7 {{ c.who }}{% endif %}</span></td>
<td style="font-size:12px;color:var(--muted)">{{ c.note }}{% if c.rule == 'unrecorded' %}<br>{% if not acct_linked or not c.other_linked %}Both accounts must be linked to QuickBooks to record it here.{% elif signed_off or c.other_signed %}A statement is signed off \u2014 reopen it to record this.{% else %}<form method=post action="{{ url_for('record_transfer', name=name) }}" style="margin-top:6px" onsubmit="return confirm(this.dataset.q)" data-q="Record one transfer of {{ a|abs|money }} between {{ name }} and {{ c.account }} in QuickBooks, and match both bank lines to it?"><input type=hidden name=line value="{{ lid }}"><input type=hidden name=other value="{{ c.line_id }}"><button type=submit class=btn-sm>Record as one transfer</button></form>{% endif %}{% endif %}</td></tr>
{% endfor %}{% endif %}{% endfor %}</table>{% endif %}
<h2 id=sec-exceptions style="font-size:15px">On statement, not in books ({{ on_stmt|length }})</h2>
<table class=exc><tr><th>Date</th><th>Description</th><th class=a>Amount</th></tr>
{% for _, d, a, who in on_stmt %}<tr><td>{{ d }}</td><td>{{ who }}</td><td class=a>{{ a|money }}</td></tr>{% endfor %}</table>
<h2 style="font-size:15px">In books, not on statement ({{ in_books|length }})</h2>
<table class=exc><tr><th>Date</th><th>Description</th><th class=a>Amount</th></tr>
{% for _, d, a, who in in_books %}<tr><td>{{ d }}{% if d < p_start %} <span class="tag bf">brought forward</span>{% endif %}</td><td>{{ who }}</td><td class=a>{{ a|money }}</td></tr>{% endfor %}</table>
{% endif %}
<script>(function(){function go(btn){document.querySelectorAll('#dtiles .tile').forEach(function(t){t.classList.toggle('active',t===btn)});var el=document.getElementById(btn.getAttribute('data-target'));if(!el){var fb=btn.getAttribute('data-fallback'); if(fb) el=document.getElementById(fb);}if(el){el.scrollIntoView({behavior:'smooth',block:'start'}); el.classList.remove('flash'); void el.offsetWidth; el.classList.add('flash');}}document.querySelectorAll('#dtiles .tile').forEach(function(t){t.addEventListener('click',function(){go(t)})});})();</script>
</div><div class=appfoot><a href="{{ url_for('terms') }}">Terms</a> · <a href="{{ url_for('privacy') }}">Privacy</a> · <a href="mailto:{{ contact_email }}">Contact</a></div>
<div id=loadingov><div class=spin></div><div class=msg id=loadingmsg>Loading...</div></div>
<script>(function(){
var ov=document.getElementById('loadingov'),msg=document.getElementById('loadingmsg'),timer,hideTimer;
function show(t){if(msg&&t)msg.textContent=t;if(ov)ov.classList.add('on');clearTimeout(hideTimer);hideTimer=setTimeout(function(){if(ov)ov.classList.remove('on');},40000);}
function schedule(t){clearTimeout(timer);timer=setTimeout(function(){show(t);},180);}
document.addEventListener('click',function(e){
var a=e.target.closest?e.target.closest('a'):null;if(!a)return;
var href=a.getAttribute('href')||'';if(!href)return;
if(a.target==='_blank'||a.hasAttribute('download'))return;
if(href[0]==='#'||href.indexOf('javascript:')===0||href.indexOf('mailto:')===0)return;
if(href.indexOf('.csv')>-1||href.indexOf('/template/')>-1||href.indexOf('/backup')>-1)return;
if(e.metaKey||e.ctrlKey||e.shiftKey||e.altKey)return;
schedule('Loading...');});
document.addEventListener('submit',function(e){
if(e.defaultPrevented)return;
var act=(e.target.getAttribute&&e.target.getAttribute('action'))||'';var t='Working...';
if(act.indexOf('/record')>-1)t='Recording in QuickBooks...';
else if(act.indexOf('/match')>-1||act.indexOf('/unmatch')>-1)t='Matching...';
if(act.indexOf('/upload')>-1)t='Reconciling your statement...';
else if(act.indexOf('/import_books')>-1)t='Importing your books...';
else if(act.indexOf('/sync')>-1)t='Syncing from QuickBooks...';
else if(act.indexOf('/clear')>-1)t='Clearing account data...';
else if(act.indexOf('/signoff')>-1)t='Signing off...';
else if(act.indexOf('/reopen')>-1)t='Reopening...';
else if(act.indexOf('/disconnect')>-1)t='Disconnecting...';
else if(act.indexOf('/check-connection')>-1)t='Checking connection...';
schedule(t);});
window.addEventListener('pageshow',function(){clearTimeout(timer);clearTimeout(hideTimer);if(ov)ov.classList.remove('on');});
})();</script>
</body></html>"""


def _ensure_snapshot_cols(cur):
    for col, typ in (("snap_exact", "int"), ("snap_fuzzy", "int"), ("snap_m2o", "int"),
                     ("snap_exc", "int"), ("snap_diff", "numeric")):
        cur.execute(f"ALTER TABLE statement ADD COLUMN IF NOT EXISTS {col} {typ};")


TRANSFER_WINDOW_DAYS = 4      # how far apart the two sides of a transfer may sit
TRANSFER_EXACT_ONLY = True    # fees charged as separate debits, so amounts should tie exactly


def transfer_candidates(cur, acct_uuid, unmatched_lines, window=TRANSFER_WINDOW_DAYS):
    """Find likely own-transfer counterparts on OTHER accounts.

    Two fingerprints, deliberately kept apart because they mean different things:

      Rule 1 (opposite sign, other account's STATEMENT) -- the money visibly left one
      bank and arrived at another but was never recorded. Also catches in-transit items
      straddling a period end.

      Rule 2 (same sign, other account's BOOKS) -- the entry exists in QuickBooks but
      against the wrong bank, so that account shows a book entry its statement will
      never confirm.

    Only accounts in the same currency are compared: a UGX line and a USD line of the
    same number are a coincidence, not a transfer (each bank's charges stay on that bank).
    Suggestions only. Nothing here auto-matches or writes anything back.
    """
    if not unmatched_lines:
        return {}
    ids = [str(l[0]) for l in unmatched_lines]
    dts = [l[1] for l in unmatched_lines]
    amts = [l[2] for l in unmatched_lines]
    out = {}

    # Signs mean direction differently per account type: on a bank, positive is money in; on a
    # card, positive is a charge. So a bank paying a card shows as -X on BOTH statements.
    # `flow` turns each amount into "money into this account" before comparing.
    cur.execute("SELECT type FROM account WHERE account_id=%s;", (acct_uuid,))
    m_this = -1 if (cur.fetchone() or [None])[0] == "credit_card" else 1
    flow = "(CASE WHEN a.type = 'credit_card' THEN -1 ELSE 1 END)"

    # Rule 1: unmatched statement line on another account, money moving the opposite way
    cur.execute(f"""
        WITH un(line_id, d, amt) AS (SELECT * FROM unnest(%s::uuid[], %s::date[], %s::numeric[]))
        SELECT un.line_id, a.name, sl.posted_date, sl.amount,
               coalesce(sl.counterparty, sl.description, ''), sl.line_id, s.signed_off_at, a.source_account_id
        FROM un
        JOIN statement s ON s.account_id <> %s
        JOIN account a ON a.account_id = s.account_id
                      AND a.currency = (SELECT currency FROM account WHERE account_id = %s)
        JOIN statement_line sl ON sl.statement_id = s.statement_id
                              AND sl.amount * {flow} = -un.amt * %s
                              AND sl.posted_date BETWEEN un.d - %s AND un.d + %s
        WHERE NOT EXISTS (SELECT 1 FROM match_statement_line msl
                          JOIN match m ON m.match_id = msl.match_id
                          WHERE msl.line_id = sl.line_id AND m.status = 'confirmed');
    """, (ids, dts, amts, acct_uuid, acct_uuid, m_this, window, window))
    for lid, nm, d, a, who, other_lid, other_signed, other_qbo in cur.fetchall():
        out.setdefault(str(lid), []).append(
            {"rule": "unrecorded", "account": nm, "date": d, "amount": a, "who": who,
             "line_id": str(other_lid), "other_signed": bool(other_signed), "other_linked": bool(other_qbo),
             "note": "Opposite entry on another bank statement, not recorded in QuickBooks either side."})

    # Rule 2: unmatched BOOK transaction on another account, money moving the same way
    cur.execute(f"""
        WITH un(line_id, d, amt) AS (SELECT * FROM unnest(%s::uuid[], %s::date[], %s::numeric[]))
        SELECT un.line_id, a.name, bt.posted_date, bt.amount,
               coalesce(bt.counterparty, bt.description, '')
        FROM un
        JOIN book_txn bt ON bt.account_id <> %s
                        AND bt.posted_date BETWEEN un.d - %s AND un.d + %s
        JOIN account a ON a.account_id = bt.account_id
                      AND a.currency = (SELECT currency FROM account WHERE account_id = %s)
                      AND bt.amount * {flow} = un.amt * %s
        WHERE coalesce(bt.is_void, false) = false AND coalesce(bt.is_deleted, false) = false
          AND NOT EXISTS (SELECT 1 FROM match_book_txn mbt
                          JOIN match m ON m.match_id = mbt.match_id
                          WHERE mbt.txn_id = bt.txn_id AND m.status = 'confirmed');
    """, (ids, dts, amts, acct_uuid, window, window, acct_uuid, m_this))
    for lid, nm, d, a, who in cur.fetchall():
        out.setdefault(str(lid), []).append(
            {"rule": "wrong_account", "account": nm, "date": d, "amount": a, "who": who,
             "note": "Recorded in QuickBooks against this account instead \u2014 likely posted to the wrong bank."})
    return out


STMT_COLS = """statement_id, period_start, period_end, signed_off_at, opening_balance, closing_balance,
               opening_source, closing_source, book_balance, book_balance_source"""


def _latest_statement(cur, acct_uuid):
    cur.execute(f"SELECT {STMT_COLS} FROM statement WHERE account_id=%s ORDER BY created_at DESC LIMIT 1;", (acct_uuid,))
    return cur.fetchone()


def book_pool(cur, acct_uuid, sid, p_start, p_end):
    """Book transactions a statement can clear: those dated in its period, plus older ones still
    outstanding (brought forward) since the account's first signed-off period. Anything already
    cleared on an EARLIER signed-off statement is excluded, so it can't be matched twice -- while
    a past period still shows the items that were outstanding at its end, even if a later
    period has since cleared them."""
    cur.execute("""SELECT min(period_start) FROM statement
                   WHERE account_id=%s AND signed_off_at IS NOT NULL AND statement_id<>%s AND period_start < %s;""",
                (acct_uuid, sid, p_start))
    floor = cur.fetchone()[0] or p_start
    cur.execute("""SELECT bt.txn_id, bt.posted_date, bt.amount, coalesce(bt.counterparty, bt.description,'')
                   FROM book_txn bt
                   WHERE bt.account_id=%s AND bt.posted_date BETWEEN %s AND %s
                     AND coalesce(bt.is_void,false)=false AND coalesce(bt.is_deleted,false)=false
                     AND NOT EXISTS (SELECT 1 FROM match_book_txn mbt
                                     JOIN match m ON m.match_id=mbt.match_id
                                     JOIN statement s ON s.statement_id=m.statement_id
                                     WHERE mbt.txn_id=bt.txn_id AND m.status='confirmed'
                                       AND s.statement_id<>%s AND s.signed_off_at IS NOT NULL
                                       AND s.period_start < %s)
                   ORDER BY bt.posted_date;""", (acct_uuid, floor, p_end, sid, p_start))
    return cur.fetchall()


def reconcile(cur, acct_uuid, stmt):
    """The balance proof for one statement.

        adjusted bank = statement closing + book items not yet on the statement
        adjusted book = book balance at period end + statement items not in the books
                        + (statement - books) on every accepted match
    The two must agree. Status is 'balanced', 'out', or 'incomplete' (a balance is missing).
    """
    sid, ps, pe, signed, ob, cb, o_src, c_src, bb, b_src = stmt
    cur.execute("SELECT line_id, posted_date, amount, coalesce(counterparty, description,'') FROM statement_line WHERE statement_id=%s ORDER BY posted_date;", (sid,))
    lines = cur.fetchall()
    pool = book_pool(cur, acct_uuid, sid, ps, pe)
    cur.execute("SELECT msl.line_id FROM match m JOIN match_statement_line msl ON msl.match_id=m.match_id WHERE m.statement_id=%s AND m.status='confirmed';", (sid,))
    ml = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT mbt.txn_id FROM match m JOIN match_book_txn mbt ON mbt.match_id=m.match_id WHERE m.statement_id=%s AND m.status='confirmed';", (sid,))
    mt = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT count(*) FROM match WHERE statement_id=%s AND status='proposed';", (sid,))
    n_pending = cur.fetchone()[0]
    cur.execute("""SELECT d FROM (
                     SELECT (SELECT coalesce(sum(sl.amount),0) FROM match_statement_line msl
                               JOIN statement_line sl ON sl.line_id=msl.line_id WHERE msl.match_id=m.match_id)
                          - (SELECT coalesce(sum(bt.amount),0) FROM match_book_txn mbt
                               JOIN book_txn bt ON bt.txn_id=mbt.txn_id WHERE mbt.match_id=m.match_id) AS d
                     FROM match m WHERE m.statement_id=%s AND m.status='confirmed') q WHERE d<>0;""", (sid,))
    deltas = [r[0] for r in cur.fetchall()]
    cur.execute("""SELECT count(*) FROM match m JOIN match_book_txn mbt ON mbt.match_id=m.match_id
                   JOIN book_txn bt ON bt.txn_id=mbt.txn_id
                   WHERE m.statement_id=%s AND m.status<>'rejected' AND (bt.is_deleted OR bt.is_void);""", (sid,))
    n_gone = cur.fetchone()[0]
    un_lines = [l for l in lines if l[0] not in ml]
    un_books = [t for t in pool if t[0] not in mt]
    Z = Decimal(0)
    moves = sum((l[2] for l in lines), Z)
    opening = ob if o_src else None
    closing = cb if c_src else None
    book = bb if b_src else None
    r = {"lines": lines, "pool": pool, "ml": ml, "mt": mt, "un_lines": un_lines, "un_books": un_books,
         "opening": opening, "opening_src": o_src, "closing": closing, "closing_src": c_src,
         "book": book, "book_src": b_src, "moves": moves, "n_lines": len(lines), "p_end": pe,
         "bf_count": sum(1 for t in un_books if t[1] < ps),
         "out_in": sum((t[2] for t in un_books if t[2] > 0), Z), "n_out_in": sum(1 for t in un_books if t[2] > 0),
         "out_out": sum((t[2] for t in un_books if t[2] < 0), Z), "n_out_out": sum(1 for t in un_books if t[2] < 0),
         "unrec": sum((l[2] for l in un_lines), Z), "n_unrec": len(un_lines),
         "match_adj": sum(deltas, Z), "n_match_adj": len(deltas), "n_gone": n_gone, "n_pending": n_pending}
    r["foot_diff"] = (opening + moves - closing) if (opening is not None and closing is not None) else None
    prev = _prev_signed_closing(cur, acct_uuid, ps, sid)
    r["prev_closing"], r["prev_end"] = (prev[0], prev[1]) if prev else (None, None)
    r["adj_bank"] = closing + r["out_in"] + r["out_out"] if closing is not None else None
    r["adj_book"] = book + r["unrec"] + r["match_adj"] if book is not None else None
    missing = [w for w, v in (("closing balance", closing), ("book balance", book)) if v is None]
    if missing:
        r["status"], r["rec_diff"] = "incomplete", None
    else:
        r["rec_diff"] = r["adj_bank"] - r["adj_book"]
        r["status"] = "balanced" if r["rec_diff"] == 0 and not r["foot_diff"] else "out"
    r["missing"] = " and ".join(missing)
    return r


def compute_detail(cur, acct_uuid, atype="bank", acct_qbo=None):
    s = _latest_statement(cur, acct_uuid)
    if not s: return {"has_results": False}
    sid, ps, pe, signed = s[:4]
    rec = reconcile(cur, acct_uuid, s)
    lines, ml = rec["lines"], rec["ml"]
    cur.execute("""SELECT m.match_type, m.amount_delta, sl.posted_date, sl.amount,
                   coalesce(sl.counterparty, sl.description,''), bt.amount FROM match m
                   JOIN match_statement_line msl ON msl.match_id=m.match_id JOIN statement_line sl ON sl.line_id=msl.line_id
                   JOIN match_book_txn mbt ON mbt.match_id=m.match_id JOIN book_txn bt ON bt.txn_id=mbt.txn_id
                   WHERE m.statement_id=%s AND m.match_type IN ('exact','fuzzy') AND m.status='confirmed' ORDER BY sl.posted_date;""", (sid,))
    matched = cur.fetchall()
    cur.execute("SELECT count(*) FROM match WHERE statement_id=%s AND match_type='many_to_one' AND status='confirmed';", (sid,))
    n_m2o = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM match WHERE statement_id=%s AND match_type='manual' AND status='proposed';", (sid,))
    n_signflip = cur.fetchone()[0]
    reviewable = []
    cur.execute("""SELECT match_id, match_type, status, amount_delta FROM match WHERE statement_id=%s
                   AND (match_type IN ('fuzzy','many_to_one','manual') OR (match_type='exact' AND confidence < 1))
                   AND created_by <> 'user'
                   ORDER BY status='proposed' DESC, match_type;""", (sid,))
    rmatches = cur.fetchall()
    cur.execute("""SELECT match_id, amount_delta, confirmed_by, confirmed_at FROM match
                   WHERE statement_id=%s AND created_by='user' AND status='confirmed' ORDER BY confirmed_at;""", (sid,))
    umatches = cur.fetchall()
    sls_by, bts_by = match_sides(cur, [r[0] for r in rmatches] + [r[0] for r in umatches])
    for mid, mtype, status, delta in rmatches:
        reviewable.append({"id": mid, "type": mtype, "status": status, "delta": delta,
                           "sls": sls_by.get(str(mid), []), "bts": bts_by.get(str(mid), [])})
    user_matches = [{"id": mid, "delta": delta, "by": by, "at": at,
                     "sls": sls_by.get(str(mid), []), "bts": bts_by.get(str(mid), [])}
                    for mid, delta, by, at in umatches]
    unmatched_lines = [l for l in lines if l[0] not in ml]
    cur.execute("SELECT currency FROM account WHERE account_id=%s;", (acct_uuid,))
    acct_ccy = (cur.fetchone() or [None])[0]
    mem = PostingMemory(cur, acct_ccy) if unmatched_lines else None
    coa = load_coa(cur)
    xt = transfer_targets(cur, acct_qbo, atype, acct_ccy) if coa else []
    wb = {}
    if unmatched_lines:
        cur.execute("SELECT line_id, status, qbo_id FROM writeback_log WHERE line_id = ANY(%s::uuid[]);",
                    ([str(l[0]) for l in unmatched_lines],))
        wb = {str(r[0]): (r[1], r[2]) for r in cur.fetchall()}
    dups = possible_duplicates(cur, acct_uuid, [l for l in unmatched_lines if l[2] != 0])
    pool_ids = {str(t[0]) for t in rec["un_books"]}
    writebacks, deposits, on_stmt_in = [], [], []
    for (lid, dd, a, who) in unmatched_lines:
        if a == 0:
            on_stmt_in.append((lid, dd, a, who)); continue
        out = _money_out(a, atype)
        sug = mem.suggest(who, out)
        # A card payment can only be a transfer from a bank (refunds are recorded in QuickBooks).
        xfer_only = atype == "credit_card" and not out
        acct = resolve_coa(xt if xfer_only else coa + xt, sug["cat"]) if sug else None
        why_not = (None if acct_qbo and coa else
                   "Connect this account to QuickBooks to record it" if not acct_qbo else
                   "Sync with QuickBooks once to load your accounts")
        if xfer_only and not why_not and not xt:
            why_not = "Card payment or refund — record it in QuickBooks as a transfer or refund"
        status, qbo_id = wb.get(str(lid), (None, None))
        item = {"line_id": lid, "date": dd, "amount": a, "who": who, "out": out, "sug": sug,
                "acct_id": acct["id"] if acct else None, "payee": (sug or {}).get("payee"),
                "payee_ref": (sug or {}).get("payee_ref"), "wb": status if status in ("pending", "done") else None,
                "qbo_id": qbo_id, "recordable": why_not is None, "why_not": why_not, "xfer_only": xfer_only,
                "is_xfer": bool(acct and acct.get("xfer")),
                "dups": dups.get(str(lid), [])[:3]}
        item["dup_matchable"] = next((x["txn_id"] for x in item["dups"] if x["txn_id"] in pool_ids), None)
        (writebacks if out else deposits).append(item)
    _unmatched = [l for l in lines if l[0] not in ml]
    _all_unmatched = [(str(l[0]), l[1], l[2], l[3]) for l in _unmatched]
    try:
        xfers = transfer_candidates(cur, acct_uuid, _unmatched)
    except Exception:
        xfers = {}   # a suggestion engine must never break the reconciliation itself
    return {"xfers": xfers, "n_xfer": sum(len(v) for v in xfers.values()), "all_unmatched": _all_unmatched,
            "has_results": True, "p_start": ps, "p_end": pe,
            "signed_off": signed.strftime("%Y-%m-%d") if signed else None,
            "n_exact": sum(1 for m in matched if m[0] == "exact"),
            "n_fuzzy": sum(1 for m in matched if m[0] == "fuzzy"), "n_m2o": n_m2o, "n_signflip": n_signflip,
            "matched": matched, "reviewable": reviewable, "writebacks": writebacks, "deposits": deposits,
            "on_stmt": on_stmt_in, "in_books": rec["un_books"], "rec": rec, "diff": rec["rec_diff"],
            "n_pending": rec["n_pending"], "user_matches": user_matches,
            "acct_linked": bool(acct_qbo),
            "coa_json": Markup(json.dumps([{"id": a["id"], "n": a["fqn"], "t": a["type"], "x": 1 if a.get("xfer") else 0}
                                           for a in coa + xt]).replace("<", "\\u003c"))}


REPORT_TEMPLATE = """<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1">
<title>Bank reconciliation · {{ name }} · {{ r.p_end }}</title>
<style>
:root{--ink:#16202e;--muted:#667085;--line:#d0d5dd;--soft:#f2f4f7;--ok:#047857;--bad:#b42318;--warn:#92400e}
*{box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;color:var(--ink);background:#e9edf2;margin:0;font-size:13px;line-height:1.45}
.toolbar{max-width:210mm;margin:16px auto 0;display:flex;gap:10px;justify-content:space-between;align-items:center;padding:0 16px;flex-wrap:wrap}
.toolbar a{color:var(--muted);text-decoration:none;font-size:14px}
.toolbar button{background:#16202e;color:#fff;border:0;border-radius:9px;padding:9px 16px;font-size:14px;font-weight:600;cursor:pointer}
.sheet{background:#fff;max-width:210mm;margin:12px auto 32px;padding:15mm 14mm;box-shadow:0 2px 10px rgba(16,24,40,.12);position:relative;overflow:hidden}
.draft{position:absolute;top:38%;left:0;right:0;text-align:center;font-size:110px;font-weight:800;letter-spacing:.1em;color:rgba(180,35,24,.07);transform:rotate(-22deg);pointer-events:none}
.co{font-size:11px;text-transform:uppercase;letter-spacing:.09em;color:var(--muted);font-weight:600}
h1{font-size:21px;margin:2px 0 0;letter-spacing:-.01em}
.meta{display:grid;grid-template-columns:repeat(4,auto);justify-content:start;gap:3px 26px;margin:14px 0 18px;padding:10px 12px;background:var(--soft);border-radius:6px}
.meta dt{font-size:10px;text-transform:uppercase;letter-spacing:.05em;color:var(--muted)}.meta dd{margin:0 0 4px;font-weight:600}
table{width:100%;border-collapse:collapse}
td{padding:3px 6px;vertical-align:top}
td.a{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap;width:110px}
td.d{white-space:nowrap;width:78px;color:var(--muted)}
tr.head td{font-weight:700;padding-top:12px}
tr.item td{font-size:12px;color:#344054}
tr.item td:first-child{padding-left:22px}
tr.none td{font-size:12px;color:var(--muted);font-style:italic;padding-left:22px}
tr.sub td.a{border-top:1px solid var(--line)}
tr.tot td{font-weight:700;border-top:1px solid var(--ink);border-bottom:3px double var(--ink);padding:6px}
tr.gap td{height:10px}
.bf{font-size:10px;color:var(--muted);border:1px solid var(--line);border-radius:3px;padding:0 3px;margin-left:4px}
.result{margin:18px 0 6px;padding:10px 12px;border-radius:6px;font-weight:700;display:flex;justify-content:space-between;font-variant-numeric:tabular-nums}
.result.balanced{background:#d7f3e3;color:var(--ok)}.result.out{background:#fbe2de;color:var(--bad)}.result.incomplete{background:var(--soft);color:var(--muted)}
.note{font-size:12px;margin:6px 0;padding:7px 10px;border-radius:6px;background:#fffbeb;color:var(--warn);border:1px solid #fde68a}
.note.bad{background:#fbe2de;color:var(--bad);border-color:#f5c2bb}
.facts{font-size:12px;color:var(--muted);margin:10px 0 0}
.sign{display:grid;grid-template-columns:1fr 1fr;gap:34px;margin-top:30px;page-break-inside:avoid}
.sign .who{font-weight:600;min-height:18px}
.sign .line{border-top:1px solid var(--ink);padding-top:4px;margin-top:4px;font-size:11px;color:var(--muted)}
footer{margin-top:22px;padding-top:8px;border-top:1px solid var(--line);font-size:10.5px;color:var(--muted);display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap}
@media print{body{background:#fff}.toolbar{display:none}.sheet{margin:0;box-shadow:none;padding:0;max-width:none}tr{page-break-inside:avoid}@page{size:A4;margin:14mm}}
@media (max-width:640px){.sheet{padding:18px 14px}.meta{grid-template-columns:repeat(2,auto)}td.d{display:none}}
</style></head><body>
<div class=toolbar><a href="{{ url_for('detail', name=name) }}">&larr; Back to {{ name }}</a><button type=button onclick="window.print()">Print / Save as PDF</button></div>
<div class=sheet>
{% if not r.signed_at %}<div class=draft>DRAFT</div>{% endif %}
<div class=co>{{ company or 'Bank reconciliation' }}</div>
<h1>Bank reconciliation statement</h1>
<dl class=meta>
<div><dt>Account</dt><dd>{{ name }}</dd></div>
<div><dt>Currency</dt><dd>{{ ccy or '—' }}</dd></div>
<div><dt>Period</dt><dd>{{ r.p_start }} to {{ r.p_end }}</dd></div>
<div><dt>Status</dt><dd>{% if r.signed_at %}Signed off{% else %}Draft — not signed off{% endif %}</dd></div>
</dl>
{% set cc = atype=='credit_card' %}
<table>
<tr class=head><td colspan=2>Balance per {{ 'card' if cc else 'bank' }} statement at {{ r.p_end }}</td><td class=a></td><td class=a>{% if r.closing is none %}not entered{% else %}{{ r.closing|acct }}{% endif %}</td></tr>
<tr class=head><td colspan=4>Add: {{ 'charges in the books, not yet on the statement' if cc else 'deposits in transit (in the books, not yet on the statement)' }}</td></tr>
{% for t in r.in_items %}<tr class=item><td>{{ t[3] }}{% if t[1] < r.p_start %}<span class=bf>b/f</span>{% endif %}</td><td class=d>{{ t[1] }}</td><td class=a>{{ t[2]|acct }}</td><td class=a></td></tr>{% else %}<tr class=none><td colspan=4>None</td></tr>{% endfor %}
<tr class=sub><td colspan=2></td><td class=a></td><td class=a>{{ r.out_in|acct }}</td></tr>
<tr class=head><td colspan=4>Less: {{ 'payments and refunds in the books, not yet on the statement' if cc else 'outstanding payments (in the books, not yet presented)' }}</td></tr>
{% for t in r.out_items %}<tr class=item><td>{{ t[3] }}{% if t[1] < r.p_start %}<span class=bf>b/f</span>{% endif %}</td><td class=d>{{ t[1] }}</td><td class=a>{{ t[2]|acct }}</td><td class=a></td></tr>{% else %}<tr class=none><td colspan=4>None</td></tr>{% endfor %}
<tr class=sub><td colspan=2></td><td class=a></td><td class=a>{{ r.out_out|acct }}</td></tr>
<tr class=tot><td colspan=3>Adjusted {{ 'card' if cc else 'bank' }} balance</td><td class=a>{% if r.adj_bank is none %}—{% else %}{{ r.adj_bank|acct }}{% endif %}</td></tr>
<tr class=gap><td colspan=4></td></tr>
<tr class=head><td colspan=2>Balance per books (QuickBooks) at {{ r.p_end }}</td><td class=a></td><td class=a>{% if r.book is none %}not entered{% else %}{{ r.book|acct }}{% endif %}</td></tr>
<tr class=head><td colspan=4>Add / (less): on the statement, not yet in the books</td></tr>
{% for l in r.unrec_items %}<tr class=item><td>{{ l[3] }}</td><td class=d>{{ l[1] }}</td><td class=a>{{ l[2]|acct }}</td><td class=a></td></tr>{% else %}<tr class=none><td colspan=4>None</td></tr>{% endfor %}
<tr class=sub><td colspan=2></td><td class=a></td><td class=a>{{ r.unrec|acct }}</td></tr>
<tr class=head><td colspan=4>Add / (less): amount differences on matched items</td></tr>
{% for m in r.delta_items %}<tr class=item><td>{{ m.desc }}</td><td class=d>{{ m.date }}</td><td class=a>{{ m.delta|acct }}</td><td class=a></td></tr>{% else %}<tr class=none><td colspan=4>None</td></tr>{% endfor %}
<tr class=sub><td colspan=2></td><td class=a></td><td class=a>{{ r.match_adj|acct }}</td></tr>
<tr class=tot><td colspan=3>Adjusted book balance</td><td class=a>{% if r.adj_book is none %}—{% else %}{{ r.adj_book|acct }}{% endif %}</td></tr>
</table>
<div class="result {{ r.status }}">
{% if r.status=='balanced' %}<span>&#10003; Reconciled — adjusted {{ 'card' if cc else 'bank' }} and book balances agree</span><span>Difference 0.00</span>
{% elif r.status=='out' %}<span>Not reconciled — out of balance</span><span>Difference {{ r.rec_diff|acct }}</span>
{% else %}<span>Incomplete — the {{ r.missing }} {{ 'is' if ' and ' not in r.missing else 'are' }} not entered</span><span>—</span>{% endif %}
</div>
{% if r.foot_diff %}<div class="note bad">The statement doesn't add up: opening {{ r.opening|acct }} + movements {{ r.moves|acct }} = {{ (r.opening + r.moves)|acct }}, but the closing balance is {{ r.closing|acct }}.</div>
{% elif r.foot_diff is not none and r.opening_src != 'derived' %}<div class=facts>Statement check: opening balance {{ r.opening|acct }} + movements {{ r.moves|acct }} = closing balance {{ r.closing|acct }} &#10003;</div>{% endif %}
{% if r.n_pending %}<div class=note>{{ r.n_pending }} suggested match{{ '' if r.n_pending==1 else 'es' }} not yet reviewed; {{ 'it is' if r.n_pending==1 else 'they are' }} treated as unmatched above.</div>{% endif %}
{% if r.n_gone %}<div class="note bad">{{ r.n_gone }} matched book transaction{{ '' if r.n_gone==1 else 's' }} {{ 'has' if r.n_gone==1 else 'have' }} since been deleted, voided or moved in QuickBooks.</div>{% endif %}
{% if r.snap_diff is not none and r.rec_diff is not none and r.snap_diff != r.rec_diff %}<div class=note>Recalculated from current data. When signed off on {{ r.signed_at.strftime('%Y-%m-%d') }} the difference was {{ r.snap_diff|acct }}; the books or matches have changed since.</div>{% endif %}
{% if r.signoff_note %}<div class="note bad">Signed off while not reconciled. Reason given: {{ r.signoff_note }}</div>{% endif %}
<div class=facts>{{ r.n_lines }} statement line{{ '' if r.n_lines==1 else 's' }}: {{ r.n_auto }} matched automatically, {{ r.n_confirmed }} confirmed suggestion{{ '' if r.n_confirmed==1 else 's' }}, {{ r.n_manual }} matched by hand, {{ r.unrec_items|length }} not in the books.{% if r.bf_count %} {{ r.bf_count }} outstanding item{{ '' if r.bf_count==1 else 's' }} brought forward (b/f) from earlier periods.{% endif %}</div>
<div class=sign>
<div><div class=who>{% if r.signed_at %}{{ r.signed_by }}, {{ r.signed_at.strftime('%d %b %Y') }}{% endif %}</div><div class=line>Prepared and signed off by · date</div></div>
<div><div class=who></div><div class=line>Reviewed by · signature · date</div></div>
</div>
<footer><span>Amounts in {{ ccy or 'account currency' }}. Brackets are negative.</span><span>Generated {{ now }} EAT · Reconciliation Tool</span></footer>
</div></body></html>"""


def build_report(cur, acct_uuid, stmt):
    """Everything the reconciliation statement shows, for any statement of the account."""
    sid = stmt[0]
    r = reconcile(cur, acct_uuid, stmt)
    r.update({"p_start": stmt[1], "p_end": stmt[2],
              "in_items": [t for t in r["un_books"] if t[2] > 0],
              "out_items": [t for t in r["un_books"] if t[2] < 0],
              "unrec_items": [l for l in r["un_lines"] if l[2] != 0]})
    cur.execute("""SELECT match_id, match_type, confidence, created_by FROM match
                   WHERE statement_id=%s AND status='confirmed';""", (sid,))
    ms = cur.fetchall()
    r["n_manual"] = sum(1 for m in ms if m[3] == "user")
    r["n_auto"] = sum(1 for m in ms if m[3] != "user" and m[1] == "exact" and (m[2] or 0) >= 1)
    r["n_confirmed"] = len(ms) - r["n_manual"] - r["n_auto"]
    sls, bts = match_sides(cur, [m[0] for m in ms])
    items = []
    for m in ms:
        a, b = sls.get(str(m[0]), []), bts.get(str(m[0]), [])
        delta = sum((x[1] for x in a), Decimal(0)) - sum((x[1] for x in b), Decimal(0))
        if delta:
            items.append({"date": a[0][0] if a else None, "delta": delta,
                          "desc": (a[0][2] if a else "") + (f" — books: {b[0][2]} {_acct(b[0][1])}" if b else "")})
    r["delta_items"] = sorted(items, key=lambda x: str(x["date"]))
    cur.execute("SELECT signed_off_at, signed_off_by, signoff_note, snap_diff FROM statement WHERE statement_id=%s;", (sid,))
    r["signed_at"], r["signed_by"], r["signoff_note"], r["snap_diff"] = cur.fetchone()
    return r


@app.route("/account/<name>/report")
def report(name):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id, type, currency FROM account WHERE name=%s LIMIT 1;", (name,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close(); return "Unknown account", 404
    acct_uuid, atype, ccy = row
    want = request.args.get("s")
    if want:
        try:
            uuid.UUID(want)
        except ValueError:
            cur.close(); conn.close(); return "No such reconciliation", 404
        cur.execute(f"SELECT {STMT_COLS} FROM statement WHERE account_id=%s AND statement_id=%s;", (acct_uuid, want))
        stmt = cur.fetchone()
    else:
        stmt = _latest_statement(cur, acct_uuid)
    if not stmt:
        cur.close(); conn.close(); return "No such reconciliation", 404
    r = build_report(cur, acct_uuid, stmt)
    cur.close(); conn.close()
    return render_template_string(REPORT_TEMPLATE, name=name, atype=atype, ccy=ccy, r=r,
                                  company=get_config("company_name"),
                                  now=datetime.now(EAT).strftime("%d %b %Y, %H:%M"))


BALANCE_SOURCES = {"user": "entered", "file": "from file", "carried": "last signed-off closing",
                   "derived": "closing less movements", "qbo": "from QuickBooks"}


@app.route("/account/<name>")
def detail(name):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id, type, currency, source_account_id FROM account WHERE name=%s LIMIT 1;", (name,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close(); return "Unknown account", 404
    acct_uuid, atype, ccy, acct_qbo = row
    d = compute_detail(cur, acct_uuid, atype, acct_qbo)
    cur.close(); conn.close()
    return render_template_string(DETAIL_TEMPLATE, name=name, atype=atype, ccy=ccy, qbo_linked=bool(acct_qbo),
                                  qbo_connected=qbo_is_connected(), last_sync=last_sync_label(),
                                  src_label=BALANCE_SOURCES, detail_msg=session.pop("detail_msg", None), **d)


def _form_amount(field):
    """Optional money field from the posted form: None when blank, ValueError when unreadable."""
    v = (request.form.get(field) or "").strip()
    if not v:
        return None
    try:
        return parse_amount(v)
    except Exception:
        raise ValueError(f"Couldn't read '{v}' as an amount.")


def _form_date(field):
    v = (request.form.get(field) or "").strip()
    return datetime.strptime(v, "%Y-%m-%d").date() if v else None


def _skipped_note(skipped):
    if not skipped:
        return ""
    eg = ", ".join(skipped[:3]) + (" …" if len(skipped) > 3 else "")
    return (f" WARNING: {len(skipped)} row{'' if len(skipped)==1 else 's'} had a date or amount that couldn't "
            f"be read and {'was' if len(skipped)==1 else 'were'} skipped (dates: {eg}). Check the file before signing off.")


@app.route("/account/<name>/upload", methods=["POST"])
def upload(name):
    f = request.files.get("statement")
    if not f or not f.filename:
        return redirect(url_for("detail", name=name))
    data = f.read()
    is_pdf = data[:5] == b"%PDF-" or f.filename.lower().endswith(".pdf")
    try:
        opening, closing = _form_amount("opening_balance"), _form_amount("closing_balance")
        if is_pdf:   # read it first: a wrong password shouldn't cost a QuickBooks sync
            pdf_rows = parse_pdf(data, request.form.get("pdf_password") or None, opening)
    except ValueError as e:
        session["detail_msg"] = f"PDF not imported: {e}" if is_pdf else str(e)
        return redirect(url_for("detail", name=name) + ("?pdfpw=1" if isinstance(e, PdfPasswordError) else ""))
    try:
        checked = ""
        if is_pdf:
            sid = _save_statement(pdf_rows, name, "pdf", opening, closing, _form_date("period_start"), _form_date("period_end"))
            n, skipped = len(pdf_rows), pdf_rows.skipped
            checked = (" Read from the PDF; every running balance checks out." if pdf_rows.pdf_checked else
                       " Read from the PDF. It has no running balance to check against, so compare the totals with "
                       "the statement before signing off.")
        else:
            sid, n, skipped = ingest_file(data.decode("utf-8-sig", errors="ignore"), f.filename, name, opening, closing,
                                          _form_date("period_start"), _form_date("period_end"))
        note = run_matcher(sid)
    except Exception as e:
        return f"Could not process file: {escape(str(e))} <br><a href='{url_for('detail', name=name)}'>Back</a>"
    # Fresh books come from a background sync, never inside this request: even a quick one
    # re-matches every open reconciliation, and a slow response is cut off before the page
    # comes back. The statement is matched now; the sync re-matches it when it finishes.
    refreshed = ""
    if qbo_is_connected():
        try:
            refreshed = (" Refreshing books from QuickBooks in the background; the matches update when it finishes."
                         if start_sync(False, session.get("username")) else
                         " A QuickBooks sync is running; this statement is re-matched automatically when it finishes.")
        except Exception as e:
            refreshed = f" (Couldn't start a QuickBooks refresh: {e}. Matched against the last sync.)"
    session["detail_msg"] = (f"Loaded {n} statement lines and reconciled." + checked + _skipped_note(skipped)
                             + (f" {note}" if note else "") + refreshed)
    return redirect(url_for("detail", name=name))


@app.route("/account/<name>/import_books", methods=["POST"])
def import_books(name):
    f = request.files.get("books")
    if not f or not f.filename:
        return redirect(url_for("detail", name=name))
    try:
        n, skipped = ingest_books(f.read().decode("utf-8-sig", errors="ignore"), name)
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT account_id FROM account WHERE name=%s LIMIT 1;", (name,))
        arow = cur.fetchone()
        sid = None
        if arow:
            cur.execute("SELECT statement_id FROM statement WHERE account_id=%s ORDER BY created_at DESC LIMIT 1;", (arow[0],))
            srow = cur.fetchone(); sid = srow[0] if srow else None
        cur.close(); conn.close()
        note = None
        if sid:
            note = run_matcher(sid)
            c2 = get_conn(); cu2 = c2.cursor()
            cu2.execute("UPDATE statement SET signed_off_at=NULL, signed_off_by=NULL WHERE statement_id=%s;", (sid,))
            c2.commit(); cu2.close(); c2.close()
        session["detail_msg"] = f"Imported {n} book transactions." + _skipped_note(skipped) + (f" {note}" if note else "")
    except Exception as e:
        return f"Could not import books: {escape(str(e))} <br><a href='{url_for('detail', name=name)}'>Back</a>"
    return redirect(url_for("detail", name=name))


def _after_review(sid):
    run_matcher(sid)
    conn = get_conn(); cur = conn.cursor()
    cur.execute("UPDATE statement SET signed_off_at=NULL, signed_off_by=NULL WHERE statement_id=%s;", (sid,))
    conn.commit(); cur.close(); conn.close()


@app.route("/account/<name>/review/<match_id>", methods=["POST"])
def review_match(name, match_id):
    new_status = request.form.get("status")
    if new_status in ("confirmed", "rejected", "proposed"):
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""UPDATE match SET status=%s, confirmed_by=%s, confirmed_at=now(), updated_at=now()
                       WHERE match_id=%s AND statement_id IN (SELECT s.statement_id FROM statement s
                         JOIN account a ON a.account_id=s.account_id WHERE a.name=%s)
                       RETURNING statement_id;""", (new_status, session.get("name"), match_id, name))
        row = cur.fetchone(); conn.commit(); cur.close(); conn.close()
        if row:
            _after_review(row[0])
    return redirect(url_for("detail", name=name) + "#sec-review")


@app.route("/account/<name>/review_all", methods=["POST"])
def review_all(name):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id FROM account WHERE name=%s LIMIT 1;", (name,))
    row = cur.fetchone()
    s = _latest_statement(cur, row[0]) if row else None
    n = 0
    if s:
        cur.execute("""UPDATE match SET status='confirmed', confirmed_by=%s, confirmed_at=now(), updated_at=now()
                       WHERE statement_id=%s AND status='proposed';""", (session.get("name"), s[0]))
        n = cur.rowcount
    conn.commit(); cur.close(); conn.close()
    if s and n:
        _after_review(s[0])
        session["detail_msg"] = f"Confirmed {n} suggested match{'' if n == 1 else 'es'}."
    return redirect(url_for("detail", name=name) + "#sec-review")


@app.route("/account/<name>/match", methods=["POST"])
def manual_match(name):
    """Pair bank lines with QuickBooks transactions by hand (any shape: 1-1, many-1, 1-many)."""
    lids = list(dict.fromkeys(request.form.getlist("ml")))
    tids = list(dict.fromkeys(request.form.getlist("mb")))
    back = redirect(url_for("detail", name=name) + "#sec-manual")
    if not lids or not tids:
        session["detail_msg"] = "Pick at least one bank line and one QuickBooks transaction."
        return back
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id FROM account WHERE name=%s LIMIT 1;", (name,))
    row = cur.fetchone()
    s = _latest_statement(cur, row[0]) if row else None
    if not s:
        cur.close(); conn.close(); return back
    acct_uuid, (sid, ps, pe) = row[0], s[:3]
    cur.execute("""SELECT line_id, amount FROM statement_line sl WHERE statement_id=%s AND line_id = ANY(%s::uuid[])
                   AND NOT EXISTS (SELECT 1 FROM match_statement_line msl JOIN match m ON m.match_id=msl.match_id
                                   WHERE msl.line_id=sl.line_id AND m.status='confirmed');""", (sid, lids))
    lines = cur.fetchall()
    # Only book entries this statement can clear: dated up to its end, not cleared elsewhere.
    pool = {str(t[0]): t for t in book_pool(cur, acct_uuid, sid, ps, pe)}
    cur.execute("""SELECT mbt.txn_id FROM match m JOIN match_book_txn mbt ON mbt.match_id=m.match_id
                   WHERE m.statement_id=%s AND m.status='confirmed';""", (sid,))
    taken = {str(r[0]) for r in cur.fetchall()}
    txns = [pool[t] for t in tids if t in pool and t not in taken]
    if len(lines) != len(lids) or len(txns) != len(tids):
        cur.close(); conn.close()
        session["detail_msg"] = "Some of those items were already matched or can't be matched to this statement. Reload and try again."
        return back
    delta = sum((l[1] for l in lines), Decimal(0)) - sum((t[2] for t in txns), Decimal(0))
    mid = str(uuid.uuid4())
    cur.execute("""INSERT INTO match (match_id, org_id, statement_id, status, match_type, confidence, amount_delta,
                                      created_by, confirmed_by, confirmed_at)
                   VALUES (%s,%s,%s,'confirmed','manual',1,%s,'user',%s,now());""",
                (mid, ORG_ID, sid, delta, session.get("name") or "user"))
    execute_values(cur, "INSERT INTO match_statement_line (match_id, line_id) VALUES %s", [(mid, str(l[0])) for l in lines])
    execute_values(cur, "INSERT INTO match_book_txn (match_id, txn_id) VALUES %s", [(mid, str(t[0])) for t in txns])
    conn.commit(); cur.close(); conn.close()
    _after_review(sid)   # re-match the rest around it; suggestions that used these items are replaced
    session["detail_msg"] = (f"Matched {len(lines)} bank line{'' if len(lines) == 1 else 's'} to {len(txns)} "
                             f"QuickBooks transaction{'' if len(txns) == 1 else 's'}."
                             + (f" The difference of {_money(delta)} shows under amount differences." if delta else ""))
    return back


@app.route("/account/<name>/unmatch/<match_id>", methods=["POST"])
def unmatch(name, match_id):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""SELECT m.statement_id FROM match m JOIN statement s ON s.statement_id=m.statement_id
                   JOIN account a ON a.account_id=s.account_id
                   WHERE m.match_id=%s AND m.created_by='user' AND a.name=%s;""", (match_id, name))
    row = cur.fetchone()
    if row:
        cur.execute("DELETE FROM match_statement_line WHERE match_id=%s;", (match_id,))
        cur.execute("DELETE FROM match_book_txn WHERE match_id=%s;", (match_id,))
        cur.execute("DELETE FROM match WHERE match_id=%s;", (match_id,))
    conn.commit(); cur.close(); conn.close()
    if row:
        _after_review(row[0])
        session["detail_msg"] = "Match undone."
    return redirect(url_for("detail", name=name) + "#sec-manual")


@app.route("/account/<name>/record", methods=["POST"])
def record(name):
    """Record selected unmatched bank lines in QuickBooks: Purchase for money out, Deposit for money in,
    or a Transfer when the chosen account is another of your own bank/card accounts."""
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id, source_account_id, type, currency FROM account WHERE name=%s LIMIT 1;", (name,))
    arow = cur.fetchone()
    if not arow:
        cur.close(); conn.close(); return "Unknown account", 404
    acct_uuid, acct_qbo, atype, ccy = arow
    ids = [request.form["only"]] if request.form.get("only") else request.form.getlist("sel")
    s = _latest_statement(cur, acct_uuid)
    if not ids or not s or not acct_qbo:
        cur.close(); conn.close()
        session["detail_msg"] = "Nothing to record." if acct_qbo else "This account isn't linked to QuickBooks."
        return redirect(url_for("detail", name=name) + "#sec-record")
    coa = {a["id"]: a for a in load_coa(cur)}
    xt = {a["id"]: a for a in transfer_targets(cur, acct_qbo, atype, ccy)}
    names = {a["id"]: a["name"] for a in xt.values()}
    cur.execute("SELECT name FROM qbo_coa WHERE qbo_id=%s;", (acct_qbo,))
    names[acct_qbo] = (cur.fetchone() or [name])[0]
    cur.execute("""SELECT sl.line_id, sl.posted_date, sl.amount, coalesce(sl.description,'') FROM statement_line sl
                   WHERE sl.statement_id=%s AND sl.line_id = ANY(%s::uuid[])
                     AND NOT EXISTS (SELECT 1 FROM match_statement_line msl JOIN match m ON m.match_id=msl.match_id
                                     WHERE msl.line_id=sl.line_id AND m.status='confirmed')
                   ORDER BY sl.posted_date;""", (s[0], ids))
    lines = cur.fetchall()
    dups = possible_duplicates(cur, acct_uuid, lines)
    cur.close(); conn.close()
    done, problems, skipped = 0, [], 0
    token, touched = None, set()
    for lid, d, amt, desc in lines:
        lid = str(lid)
        label = f"{d} {desc[:30]}"
        pick = request.form.get(f"acct_{lid}") or ""
        acc = coa.get(pick) or xt.get(pick)
        out = _money_out(amt, atype)
        if not acc:
            problems.append(f"{label}: choose an account"); continue
        is_xfer = bool(acc.get("xfer"))
        if atype == "credit_card" and not out and not is_xfer:
            problems.append(f"{label}: choose the bank the card was paid from (refunds are recorded in QuickBooks)"); continue
        if dups.get(lid) and not request.form.get(f"dupok_{lid}"):
            x = dups[lid][0]
            problems.append(f"{label}: QuickBooks may already have it ({x['date']}, {_money(x['amount'])}) — "
                            f"match it instead, or tick 'Not a duplicate'"); continue
        payee = (request.form.get(f"payee_{lid}") or "").strip()
        # Only reuse the learned payee ID if the user kept the suggested payee name.
        ref = request.form.get(f"pref_{lid}") or None
        if payee != (request.form.get(f"psug_{lid}") or "").strip():
            ref = None
        if not _claim_writeback(lid, session.get("name")):
            skipped += 1; continue
        try:
            token = token or qbo_token()
            if is_xfer:
                frm, to = transfer_ends(acct_qbo, acc["id"], amt, atype)
                entity, new_id, ent = qbo_record_transfer(token, frm, to, abs(amt), d, desc)
                payee, used_ref = "", None
            else:
                entity, new_id, used_ref = qbo_record_line(token, acct_qbo, atype, out, acc["id"], abs(amt), d, desc, payee, ref)
        except Exception as e:
            err = f"HTTP {e.code}: {e.read().decode(errors='ignore')[:200]}" if isinstance(e, urllib.error.HTTPError) else str(e)
            c2 = get_conn(); k2 = c2.cursor()
            k2.execute("UPDATE writeback_log SET status='failed', error=%s WHERE line_id=%s;", (err, lid))
            c2.commit(); k2.close(); c2.close()
            problems.append(f"{label}: QuickBooks said {err}"); continue
        c2 = get_conn(); k2 = c2.cursor()
        k2.execute("""UPDATE writeback_log SET status='done', qbo_type=%s, qbo_id=%s, account_fqn=%s, payee=%s
                      WHERE line_id=%s;""", (entity, new_id or None, acc["fqn"], payee or None, lid))
        if new_id and is_xfer:
            ent = {**ent, "FromAccountRef": {"value": frm}, "ToAccountRef": {"value": to}, "Amount": abs(amt)}
            touched.update(a for a in store_transfer(k2, new_id, ent, d, desc, names) if a != str(acct_uuid))
        elif new_id:
            book_amt = abs(amt) if (atype == "credit_card" or not out) else -abs(amt)
            k2.execute("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount,
                              currency, description, counterparty, reference, category, cleared_status, last_modified,
                              counterparty_ref)
                          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,%s,'unknown',now(),%s)
                          ON CONFLICT (account_id, source_txn_type, source_txn_id) DO NOTHING;""",
                       (ORG_ID, acct_uuid, new_id, entity, d, book_amt, ccy or "USD", desc, payee or None, acc["fqn"], used_ref))
        # Every recorded line teaches the suggestion engine (strongest tier).
        k2.execute("""INSERT INTO payee_correction (org_id, payee, category, money_out, vendor, vendor_ref, currency)
                      VALUES (%s,%s,%s,%s,%s,%s,%s);""", (ORG_ID, desc, acc["fqn"], out, payee or None, used_ref, ccy))
        c2.commit(); k2.close(); c2.close()
        done += 1
    if done:
        _after_review(s[0])
    if touched:   # the other side of each transfer can clear on its own statement straight away
        c2 = get_conn(); k2 = c2.cursor(); rematch_open(k2, touched); k2.close(); c2.close()
    msg = f"Recorded {done} transaction{'' if done == 1 else 's'} in QuickBooks." if done else ""
    if skipped:
        msg += f" Skipped {skipped} already recorded or in progress."
    if problems:
        msg += " Not recorded: " + "; ".join(problems[:5]) + (" …" if len(problems) > 5 else "")
    session["detail_msg"] = msg.strip() or "Nothing to record."
    return redirect(url_for("detail", name=name) + "#sec-record")


@app.route("/account/<name>/transfer", methods=["POST"])
def record_transfer(name):
    """Money visibly left one of your accounts and arrived at another, and neither side is in
    QuickBooks: record ONE Transfer and match both bank lines to it."""
    back = redirect(url_for("detail", name=name) + "#sec-exceptions")
    lid, other = request.form.get("line") or "", request.form.get("other") or ""
    try:
        uuid.UUID(lid); uuid.UUID(other)
    except ValueError:
        session["detail_msg"] = "Nothing to record."; return back
    conn = get_conn(); cur = conn.cursor()
    q = """SELECT sl.line_id, sl.posted_date, sl.amount, coalesce(sl.description,''), s.statement_id, s.signed_off_at,
                  a.account_id, a.source_account_id, a.type, a.currency, a.name,
                  EXISTS (SELECT 1 FROM match_statement_line msl JOIN match m ON m.match_id=msl.match_id
                          WHERE msl.line_id=sl.line_id AND m.status='confirmed')
           FROM statement_line sl JOIN statement s ON s.statement_id=sl.statement_id
           JOIN account a ON a.account_id=s.account_id WHERE sl.line_id=%s;"""
    cur.execute(q, (lid,)); me = cur.fetchone()
    cur.execute(q, (other,)); them = cur.fetchone()
    problem = None
    if not me or not them or me[10] != name:
        problem = "Those bank lines weren't found. Reload and try again."
    elif me[6] == them[6] or (me[9] or "") != (them[9] or ""):
        problem = "A transfer needs two different accounts in the same currency."
    elif me[11] or them[11]:
        problem = "One of those bank lines is already matched."
    elif me[5] or them[5]:
        problem = "A statement is signed off. Reopen it to record this transfer."
    elif not me[7] or not them[7]:
        problem = "Both accounts must be linked to QuickBooks."
    elif (me[2] * (-1 if me[8] == "credit_card" else 1)) != -(them[2] * (-1 if them[8] == "credit_card" else 1)):
        problem = "The two lines aren't the same money moving in opposite directions."
    if problem:
        cur.close(); conn.close(); session["detail_msg"] = problem; return back
    frm, to = transfer_ends(me[7], them[7], me[2], me[8])
    out_line = me if frm == me[7] else them   # dated when the money left
    d, desc = out_line[1], out_line[3]
    if not _claim_writeback(str(me[0]), session.get("name")):
        cur.close(); conn.close(); session["detail_msg"] = "Already recorded or in progress."; return back
    if not _claim_writeback(str(them[0]), session.get("name")):
        cur.execute("UPDATE writeback_log SET status='failed', error='released' WHERE line_id=%s;", (str(me[0]),))
        conn.commit(); cur.close(); conn.close()
        session["detail_msg"] = "The other bank line is already recorded or in progress."; return back
    try:
        entity, new_id, ent = qbo_record_transfer(qbo_token(), frm, to, abs(me[2]), d, desc)
    except Exception as e:
        err = f"HTTP {e.code}: {e.read().decode(errors='ignore')[:200]}" if isinstance(e, urllib.error.HTTPError) else str(e)
        cur.execute("UPDATE writeback_log SET status='failed', error=%s WHERE line_id = ANY(%s::uuid[]);",
                    (err, [str(me[0]), str(them[0])]))
        conn.commit(); cur.close(); conn.close()
        session["detail_msg"] = f"Not recorded: QuickBooks said {err}"; return back
    cur.execute("SELECT qbo_id, name FROM qbo_coa WHERE qbo_id = ANY(%s);", ([frm, to],))
    names = dict(cur.fetchall())
    names.setdefault(me[7], me[10]); names.setdefault(them[7], them[10])
    cur.execute("""UPDATE writeback_log SET status='done', qbo_type=%s, qbo_id=%s, account_fqn=%s WHERE line_id=%s;""",
                (entity, new_id or None, names.get(them[7]), str(me[0])))
    cur.execute("""UPDATE writeback_log SET status='done', qbo_type=%s, qbo_id=%s, account_fqn=%s WHERE line_id=%s;""",
                (entity, new_id or None, names.get(me[7]), str(them[0])))
    booked = {}
    if new_id:
        ent = {**ent, "FromAccountRef": {"value": frm}, "ToAccountRef": {"value": to}, "Amount": abs(me[2])}
        booked = store_transfer(cur, new_id, ent, d, desc, names)
    conn.commit()
    # Match each bank line to its side of the transfer, as if the user had matched it by hand --
    # if that side is one its statement can clear (dated within reach of the period).
    matched = 0
    for row in (me, them):
        tid, sid = booked.get(str(row[6])), row[4]
        if not tid:
            continue
        cur.execute("SELECT period_start, period_end FROM statement WHERE statement_id=%s;", (sid,))
        ps, pe = cur.fetchone()
        if tid not in {str(t[0]) for t in book_pool(cur, row[6], sid, ps, pe)}:
            continue
        mid = str(uuid.uuid4())
        cur.execute("""INSERT INTO match (match_id, org_id, statement_id, status, match_type, confidence, amount_delta,
                                          created_by, confirmed_by, confirmed_at)
                       VALUES (%s,%s,%s,'confirmed','manual',1,0,'user',%s,now());""",
                    (mid, ORG_ID, sid, session.get("name") or "user"))
        cur.execute("INSERT INTO match_statement_line (match_id, line_id) VALUES (%s,%s);", (mid, str(row[0])))
        cur.execute("INSERT INTO match_book_txn (match_id, txn_id) VALUES (%s,%s);", (mid, tid))
        matched += 1
    conn.commit(); cur.close(); conn.close()
    for sid in {me[4], them[4]}:
        _after_review(sid)
    session["detail_msg"] = (f"Recorded a transfer of {_money(abs(me[2]))} from {names.get(frm)} to {names.get(to)} "
                             f"in QuickBooks" + (f" (#{new_id})" if new_id else "") +
                             (" and matched both bank lines." if matched == 2 else
                              ". It will match on the next refresh."))
    return back


@app.route("/account/<name>/record_reset", methods=["POST"])
def record_reset(name):
    """After an interrupted write-back, the user checked QuickBooks and it isn't there: allow a retry."""
    lid = request.form.get("reset")
    if lid:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""UPDATE writeback_log SET status='failed', error='cleared by ' || %s
                       WHERE line_id=%s AND status='pending';""", (session.get("name") or "user", lid))
        conn.commit(); cur.close(); conn.close()
    return redirect(url_for("detail", name=name) + "#sec-record")


@app.route("/account/<name>/balances", methods=["POST"])
def balances(name):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id, source_account_id FROM account WHERE name=%s LIMIT 1;", (name,))
    row = cur.fetchone()
    s = _latest_statement(cur, row[0]) if row else None
    if not s:
        cur.close(); conn.close(); return redirect(url_for("detail", name=name))
    acct_uuid, acct_qbo = row
    sid, ps, pe = s[:3]
    try:
        if request.form.get("action") == "fetch_book":
            cur.close(); conn.close()
            if not acct_qbo:
                raise ValueError("This account isn't linked to a QuickBooks account.")
            if sync_running():
                raise ValueError("A QuickBooks sync is running. Try again when it finishes.")
            # The calculation needs freshly synced transactions. Syncing here would outlast the
            # request, so use a sync from the last few minutes or start one in the background.
            if sync_full_due() or _sync_age_secs(get_config("last_sync_at")) > BOOK_BALANCE_FRESH_SECS:
                start_sync(False, session.get("username"))
                raise ValueError("Refreshing books from QuickBooks first (see the banner). Press Get book balance "
                                 "again when it finishes.")
            bal = qbo_book_balance_at(qbo_token(), acct_uuid, acct_qbo, pe)
            conn = get_conn(); cur = conn.cursor()
            cur.execute("UPDATE statement SET book_balance=%s, book_balance_source='qbo' WHERE statement_id=%s;", (bal, sid))
            session["detail_msg"] = f"Book balance at {pe} from QuickBooks: {_money(bal)}."
        else:
            opening, closing, book = _form_amount("opening"), _form_amount("closing"), _form_amount("book")
            new_ps, new_pe = _form_date("period_start") or ps, _form_date("period_end") or pe
            cur.execute("SELECT coalesce(sum(amount),0), min(posted_date), max(posted_date) FROM statement_line WHERE statement_id=%s;", (sid,))
            moves, first, last = cur.fetchone()
            rematch = (new_ps, new_pe) != (ps, pe)
            if rematch:
                if new_ps > first or new_pe < last:
                    raise ValueError(f"the period must cover the statement's transactions ({first} to {last})")
                cur.execute("UPDATE statement SET period_start=%s, period_end=%s WHERE statement_id=%s;", (new_ps, new_pe, sid))
                ps = new_ps
            opening, o_src, closing, c_src = _resolve_balances(
                cur, acct_uuid, ps, moves, opening, "user" if opening is not None else None,
                closing, "user" if closing is not None else None, exclude_sid=sid)
            cur.execute("""UPDATE statement SET opening_balance=%s, opening_source=%s, closing_balance=%s, closing_source=%s,
                           book_balance=%s, book_balance_source=%s WHERE statement_id=%s;""",
                        (opening or 0, o_src, closing or 0, c_src, book, "user" if book is not None else None, sid))
            session["detail_msg"] = "Balances saved."
            if rematch:
                conn.commit(); cur.close(); conn.close()
                note = run_matcher(sid)
                conn = get_conn(); cur = conn.cursor()
                session["detail_msg"] = "Balances and period saved; matching re-run." + (f" {note}" if note else "")
        cur.execute("UPDATE statement SET signed_off_at=NULL, signed_off_by=NULL WHERE statement_id=%s;", (sid,))
        conn.commit()
    except urllib.error.HTTPError as e:
        session["detail_msg"] = f"QuickBooks error (HTTP {e.code}) while fetching the book balance."
    except Exception as e:
        session["detail_msg"] = f"Couldn't update balances: {e}"
    try:
        cur.close(); conn.close()
    except Exception:
        pass
    return redirect(url_for("detail", name=name) + "#sec-balance")


@app.route("/account/<name>/signoff", methods=["POST"])
def signoff(name):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id, type FROM account WHERE name=%s LIMIT 1;", (name,))
    row = cur.fetchone()
    if row:
        acct_uuid, atype = row
        cur.execute("SELECT statement_id FROM statement WHERE account_id=%s ORDER BY created_at DESC LIMIT 1;", (acct_uuid,))
        srow = cur.fetchone()
        if srow:
            sid = srow[0]
            d = compute_detail(cur, acct_uuid, atype)
            rec = d["rec"]
            note = (request.form.get("note") or "").strip()
            override = session.get("is_admin") and request.form.get("override") == "1" and note
            if rec["n_pending"] and not override:
                session["detail_msg"] = (f"Not signed off: {rec['n_pending']} suggested match"
                                         f"{'' if rec['n_pending'] == 1 else 'es'} still need a confirm or reject.")
                cur.close(); conn.close()
                return redirect(url_for("detail", name=name) + "#sec-review")
            if rec["status"] != "balanced" and not override:
                why = (f"it needs a {rec['missing']}" if rec["status"] == "incomplete"
                       else f"the adjusted balances are out by {_money(rec['rec_diff'])}" if rec["rec_diff"]
                       else f"the statement doesn't add up (out by {_money(rec['foot_diff'])})")
                session["detail_msg"] = f"Not signed off: {why}."
                cur.close(); conn.close()
                return redirect(url_for("detail", name=name) + "#sec-balance")
            try:
                exc = len(d.get("writebacks", [])) + len(d.get("deposits", [])) + len(d.get("on_stmt", [])) + len(d.get("in_books", []))
                _ensure_snapshot_cols(cur)
                cur.execute("UPDATE statement SET snap_exact=%s, snap_fuzzy=%s, snap_m2o=%s, snap_exc=%s, snap_diff=%s WHERE statement_id=%s;",
                            (d.get("n_exact", 0), d.get("n_fuzzy", 0), d.get("n_m2o", 0), exc, rec["rec_diff"], sid))
            except Exception:
                conn.rollback()
            cur.execute("UPDATE statement SET signed_off_at=now(), signed_off_by=%s, signoff_note=%s WHERE statement_id=%s;",
                        (session.get("name") or "you", note if (rec["status"] != "balanced" or rec["n_pending"]) else None, sid))
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
        cur.close(); conn.close(); return f"No account named {escape(name)}"
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
        cur.close(); conn.close(); return f"No account named {escape(name)}"
    acct_uuid, atype = row
    d = compute_detail(cur, acct_uuid, atype)
    cur.close(); conn.close()
    buf = io.StringIO()
    wtr = csv.writer(buf)
    wtr.writerow(["Source", "Date", "Description", "Amount", "Direction", "Suggested category"])
    if d.get("has_results"):
        for wb in d.get("writebacks", []):
            wtr.writerow(["Bank statement", wb["date"], wb["who"], wb["amount"], "Money out", (wb.get("sug") or {}).get("cat") or ""])
        for dp in d.get("deposits", []):
            wtr.writerow(["Bank statement", dp["date"], dp["who"], dp["amount"], "Money in", (dp.get("sug") or {}).get("cat") or ""])
        for (lid, dd, a, who) in d.get("on_stmt", []):
            wtr.writerow(["Bank statement", dd, who, a, ("Money in" if a > 0 else "Money out"), ""])
        for (tid, dd, a, who) in d.get("in_books", []):
            wtr.writerow(["Books", dd, who, a, "In books, not on statement", ""])
    return Response(buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={name}_exceptions.csv"})


HISTORY_TEMPLATE = """<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>{{ name }} history · Reconciliation Tool</title>""" + CSS + """</head><body>
<div class=nav><span class=brand><span class=dot></span>Reconciliation Tool</span><span class=links>{% if session.name %}<span style="color:var(--muted);font-size:13px;margin-right:6px">{{ session.name }}</span>{% endif %}<a href="{{ url_for('detail', name=name) }}">← Back to {{ name }}</a><a href="{{ url_for('dashboard') }}">All accounts</a><a href="{{ url_for('logout') }}">Sign out</a></span></div>
<div class=wrap>
<h1>{{ name }} — reconciliation history</h1>
<div class=sub>Past reconciliations for this account{% if ccy %} · {{ ccy }}{% endif %}</div>
{% if stmts %}
<table>
<thead><tr><th>Period</th><th>Reconciled on</th><th>Matches</th><th>Exceptions</th><th class=a>Unreconciled</th><th>Status</th><th></th></tr></thead>
<tbody>
{% for s in stmts %}<tr>
<td>{{ s.period_start }} → {{ s.period_end }}</td>
<td>{{ s.created.strftime('%Y-%m-%d') if s.created else '—' }}</td>
<td>{% if s.exact is not none %}{{ s.exact }} exact{% if s.fuzzy %}, {{ s.fuzzy }} fuzzy{% endif %}{% if s.m2o %}, {{ s.m2o }} batched{% endif %}{% else %}—{% endif %}</td>
<td>{% if s.exc is not none %}{{ s.exc }}{% else %}—{% endif %}</td>
<td class=a>{% if s.diff is not none %}{{ s.diff|money }}{% else %}—{% endif %}</td>
<td>{% if s.signed %}<span class="pill signed">Signed off {{ s.signed.strftime('%Y-%m-%d') }}</span>{% if s.note %}<br><span class=bad style="font-size:12px;white-space:normal">Unbalanced — {{ s.note }}</span>{% endif %}{% else %}<span class="pill open">In progress</span>{% endif %}</td>
<td><a href="{{ url_for('report', name=name, s=s.id) }}" target=_blank rel=noopener style="color:var(--accent);font-weight:600;font-size:13px">Report</a></td>
</tr>{% endfor %}
</tbody></table>
<div class=sub style="font-size:12.5px;margin-top:6px">Match counts and difference are snapshots taken when each period was signed off.</div>
{% else %}
<div class=sub>No reconciliations yet for this account.</div>
{% endif %}
</div></body></html>"""


@app.route("/account/<name>/history")
def history(name):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id, type, currency FROM account WHERE name=%s LIMIT 1;", (name,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close(); return "Unknown account", 404
    acct_uuid, atype, ccy = row
    try:
        _ensure_snapshot_cols(cur); conn.commit()
    except Exception:
        pass
    cur.execute("""SELECT period_start, period_end, created_at, signed_off_at,
                          snap_exact, snap_fuzzy, snap_m2o, snap_exc, snap_diff, signoff_note, statement_id
                   FROM statement WHERE account_id=%s ORDER BY created_at DESC;""", (acct_uuid,))
    stmts = []
    for ps, pe, created, signed, ex, fz, m2, exc, diff, note, sid in cur.fetchall():
        stmts.append({"period_start": ps, "period_end": pe, "created": created, "signed": signed,
                      "exact": ex, "fuzzy": fz, "m2o": m2, "exc": exc, "diff": diff, "note": note, "id": sid})
    cur.close(); conn.close()
    return render_template_string(HISTORY_TEMPLATE, name=name, ccy=ccy, stmts=stmts)


@app.route("/account/<name>/currency", methods=["POST"])
def set_currency(name):
    ccy = (request.form.get("currency") or "").strip().upper()[:8]
    if ccy:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT account_id FROM account WHERE name=%s LIMIT 1;", (name,))
        row = cur.fetchone()
        if row:
            acct = row[0]
            cur.execute("UPDATE account SET currency=%s WHERE account_id=%s;", (ccy, acct))
            cur.execute("UPDATE statement SET currency=%s WHERE account_id=%s;", (ccy, acct))
            cur.execute("UPDATE statement_line SET currency=%s WHERE statement_id IN (SELECT statement_id FROM statement WHERE account_id=%s);", (ccy, acct))
            cur.execute("UPDATE book_txn SET currency=%s WHERE account_id=%s;", (ccy, acct))
            conn.commit()
            session["detail_msg"] = f"Currency set to {ccy}."
        cur.close(); conn.close()
    return redirect(url_for("detail", name=name))


@app.route("/account/<name>/diag")
def diag(name):
    if request.args.get("key") != os.environ.get("APP_PASSWORD"):
        return "Not found", 404
    conn = get_conn(); cur = conn.cursor()
    o = []
    cur.execute("SELECT account_id, type, currency FROM account WHERE name=%s LIMIT 1;", (name,))
    row = cur.fetchone()
    if not row:
        return f"No account named {escape(name)}"
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
    return "<pre style='font-size:13px;line-height:1.5;padding:24px;font-family:ui-monospace,monospace'>" + str(escape("\n".join(str(x) for x in o))) + "</pre>"


@app.route("/account/<name>/delete", methods=["POST"])
def delete_account(name):
    if not session.get("is_admin"):
        return "Admins only. <a href='/'>Back</a>", 403
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
        cur.execute("DELETE FROM account WHERE account_id=%s;", (acct,))
        conn.commit()
        session["sync_msg"] = "Removed account '" + name + "' and all its data."
    cur.close(); conn.close()
    return redirect(url_for("dashboard"))


@app.route("/account/<name>/clear", methods=["POST"])
def clear_account(name):
    if not session.get("is_admin"):
        return "Admins only. <a href='/'>Back</a>", 403
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


@app.route("/connect")
def connect():
    state = base64.urlsafe_b64encode(os.urandom(16)).decode().rstrip("=")
    session["oauth_state"] = state
    params = urllib.parse.urlencode({
        "client_id": os.environ.get("QBO_CLIENT_ID", ""),
        "redirect_uri": QBO_REDIRECT_URI,
        "response_type": "code",
        "scope": QBO_SCOPE,
        "state": state,
    })
    return redirect("https://appcenter.intuit.com/connect/oauth2?" + params)


@app.route("/callback")
def callback():
    if not request.args.get("state") or request.args.get("state") != session.get("oauth_state"):
        return "Security check failed (state mismatch). Please try Connect again. <a href='/'>Back</a>", 400
    if request.args.get("error"):
        return f"Connection was cancelled ({escape(request.args.get('error'))}). <a href='/'>Back</a>"
    code = request.args.get("code")
    realm = request.args.get("realmId")
    if not code:
        return "No authorization code returned. <a href='/'>Back</a>"
    data = urllib.parse.urlencode({
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": QBO_REDIRECT_URI,
    }).encode()
    req = urllib.request.Request("https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer", data=data, method="POST")
    auth = base64.b64encode(f"{os.environ.get('QBO_CLIENT_ID','')}:{os.environ.get('QBO_CLIENT_SECRET','')}".encode()).decode()
    req.add_header("Authorization", "Basic " + auth)
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    req.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(req) as resp:
            tok = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        try: body = e.read().decode()
        except Exception: body = ""
        return f"Token exchange failed: HTTP {e.code} {escape(body[:200])} <a href='/'>Back</a>"
    _store_refresh(tok.get("refresh_token"), realm)
    try: set_config("qbo_conn", "connected")
    except Exception: pass
    session["sync_msg"] = "Connected to QuickBooks successfully."
    return redirect(url_for("dashboard"))


@app.route("/set-token", methods=["POST"])
def set_token():
    rt = (request.form.get("refresh_token") or "").strip()
    realm = (request.form.get("realm_id") or "").strip() or None
    if rt:
        _store_refresh(rt, realm)
        try: set_config("qbo_conn", "connected")
        except Exception: pass
        session["sync_msg"] = "Refresh token saved. Use 'Check connection' to verify it works."
    else:
        session["sync_msg"] = "No refresh token was provided."
    return redirect(url_for("dashboard"))


@app.route("/disconnect", methods=["POST"])
def disconnect():
    rt = _get_stored_refresh() or os.environ.get("QBO_REFRESH_TOKEN", "")
    if rt:
        try:
            data = json.dumps({"token": rt}).encode()
            req = urllib.request.Request("https://developer.api.intuit.com/v2/oauth2/tokens/revoke", data=data, method="POST")
            auth = base64.b64encode(f"{os.environ.get('QBO_CLIENT_ID','')}:{os.environ.get('QBO_CLIENT_SECRET','')}".encode()).decode()
            req.add_header("Authorization", "Basic " + auth)
            req.add_header("Content-Type", "application/json")
            req.add_header("Accept", "application/json")
            urllib.request.urlopen(req, timeout=15)
        except Exception:
            pass  # best-effort revoke; we clear locally regardless
    _clear_stored_auth()
    try: set_config("qbo_conn", "disconnected")
    except Exception: pass
    session["sync_msg"] = "Disconnected from QuickBooks."
    return redirect(url_for("dashboard"))


@app.route("/check-connection", methods=["POST"])
def check_connection():
    try:
        tok = qbo_token()
        if not tok:
            raise RuntimeError("no token")
        try: set_config("qbo_conn", "connected")
        except Exception: pass
        session["sync_msg"] = "QuickBooks connection verified — you're connected."
    except Exception:
        try: set_config("qbo_conn", "disconnected")
        except Exception: pass
        session["sync_msg"] = "QuickBooks connection isn't active — please reconnect."
    return redirect(url_for("dashboard"))


@app.route("/sync", methods=["POST"])
def sync():
    """Starts the sync and returns straight away; the page banner follows it."""
    back = request.form.get("back")
    try:
        started = start_sync(request.form.get("full") == "1", session.get("username"))
        msg = None if started else "A QuickBooks sync is already running; its progress is shown at the top of the page."
    except Exception as e:
        msg = f"Couldn't start the sync: {e}"
    if back:
        if msg: session["detail_msg"] = msg
        return redirect(url_for("detail", name=back))
    if msg: session["sync_msg"] = msg
    return redirect(url_for("dashboard"))


@app.route("/sync/status")
def sync_status():
    job = sync_job()
    return {"state": job.get("state") or "none", "step": job.get("step") or "", "msg": job.get("msg") or ""}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))