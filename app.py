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
from flask import Flask, has_request_context, render_template_string, request, redirect, session, url_for, Response
from werkzeug.security import generate_password_hash, check_password_hash
from markupsafe import escape, Markup
import json, base64, urllib.request, urllib.parse, urllib.error
import threading

DB_URL = os.environ["SUPABASE_DB_URL"]
ORG_ID = "00000000-0000-0000-0000-000000000001"
DATE_TOLERANCE_DAYS = 3
CLEARING_WINDOW_DAYS = 31  # a cheque/payment can hit the bank this long after it's booked
# Defaults for the rules an admin can change under Settings (stored in app_config as rule_<key>).
RULES = {"date_days": (DATE_TOLERANCE_DAYS, 0, 10, "Days a bank line and its entry may differ and still match exactly"),
         "clear_days": (CLEARING_WINDOW_DAYS, 3, 120, "Days a payment may clear the bank after it was booked"),
         "transfer_days": (4, 0, 14, "Days apart the two sides of a transfer may be"),
         "charges_exact": (1, 0, 1, "Bank charges match on the exact date only"),
         "two_person": (1, 0, 1, "Sign-off needs a second person (admins excepted)"),
         "close_day": (10, 1, 28, "Month-end close is due on this day of the next month")}


def rule(key):
    """A rule's current value (whole number), falling back to its default."""
    default, lo, hi, _ = RULES[key]
    try:
        v = int(str(get_config("rule_" + key)).strip())
        return min(hi, max(lo, v))
    except (TypeError, ValueError):
        return default
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
    # perms: comma-separated ticks (NULL = from before permissions: everything but users & settings)
    for col, typ in (("perms", "text"), ("title", "text"), ("active", "boolean DEFAULT true"),
                     ("expires", "date"), ("last_seen", "timestamptz")):
        cur.execute(f"ALTER TABLE app_users ADD COLUMN IF NOT EXISTS {col} {typ};")


# What each tick lets a person do. Everyone can view; admins can do everything.
PERMS = [("upload", "Upload statements"), ("review", "Match & review"), ("record", "Record in QuickBooks"),
         ("undo", "Undo / delete in QuickBooks"), ("signoff", "Sign off"), ("reopen", "Reopen sign-off"),
         ("users", "Users & settings")]
PERM_PRESETS = {"assistant": ("Accounts assistant", ["upload", "review", "record"]),
                "approver": ("Approver", ["review", "signoff", "reopen"]),
                "viewer": ("Viewer", []),
                "admin": ("Admin", [k for k, _ in PERMS])}


def user_row(username):
    """(username, name, is_admin, perms or None, title, active, expires) for a named user, or None."""
    try:
        conn = get_conn(); cur = conn.cursor()
        _ensure_users(cur); conn.commit()
        cur.execute("""SELECT username, name, is_admin, perms, title, coalesce(active, true), expires
                       FROM app_users WHERE username=%s;""", (username.strip().lower(),))
        row = cur.fetchone(); cur.close(); conn.close()
        return row
    except Exception:
        return None


def _load_session_user(row):
    """Copy who the user is and what they may do into the session."""
    un, nm, adm, perms, title, active, expires = row
    session["username"], session["name"], session["is_admin"] = un, nm or un, bool(adm)
    session["perms"] = None if perms is None else [p for p in perms.split(",") if p]
    session["title"] = title or ""


def log_activity(action, account=None):
    """One line in the activity log: who did what, where."""
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS activity_log (id serial PRIMARY KEY, at timestamptz NOT NULL DEFAULT now(),
                       username text, name text, action text NOT NULL, account text);""")
        cur.execute("INSERT INTO activity_log (username, name, action, account) VALUES (%s,%s,%s,%s);",
                    (session.get("username"), session.get("name"), action[:400], account))
        conn.commit(); cur.close(); conn.close()
    except Exception:
        pass   # the log must never stop the work it records


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
    cur.execute("""SELECT username, name, is_admin, created_at, perms, title, coalesce(active, true), expires, last_seen
                   FROM app_users ORDER BY is_admin DESC, created_at;""")
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


def _store_customers(custs):
    rows = [(str(c["Id"]), c.get("DisplayName"), c.get("FullyQualifiedName") or c.get("DisplayName"),
             (c.get("CurrencyRef") or {}).get("value"), bool(c.get("Active", True)),
             (c.get("ParentRef") or {}).get("value")) for c in custs if c.get("Id")]
    if not rows:
        return
    conn = get_conn(); cur = conn.cursor()
    cur.execute("DELETE FROM qbo_customer;")
    execute_values(cur, "INSERT INTO qbo_customer (qbo_id, name, fqn, currency, active, parent_id) VALUES %s", rows)
    conn.commit(); cur.close(); conn.close()


def load_customers(cur, currency, also=None):
    """Active customers (students and families) a receipt in `currency` can be recorded against --
    QuickBooks ties each to one currency. `also`: another currency accepted with an exchange rate
    (a UGX receipt paid to a student's USD account). The bank's own currency comes first."""
    cur.execute("""SELECT qbo_id, coalesce(fqn, name), parent_id, currency FROM qbo_customer WHERE coalesce(active, true)
                     AND (%s::text IS NULL OR currency IS NULL OR currency = %s OR currency = %s)
                   ORDER BY (currency IS DISTINCT FROM %s), 2;""", (currency, currency, also, currency))
    return [{"id": i, "n": n or "", "c": c or currency or "", **({"p": p} if p else {})} for i, n, p, c in cur.fetchall()]


def cross_currency(cur, currency, home=None):
    """The other currency a home-currency bank's receipts can be paid to a student in (UGX bank -> a
    student's USD account, at a rate). A foreign-currency bank only takes its own currency."""
    home = home or qbo_home_currency(cur)
    if not currency or currency != home:
        return None
    cur.execute("""SELECT currency FROM qbo_customer WHERE currency IS NOT NULL AND currency <> %s
                   GROUP BY currency ORDER BY count(*) DESC LIMIT 1;""", (currency,))
    r = cur.fetchone()
    return r[0] if r else None


def _store_vendors(vends):
    rows = [(str(v["Id"]), v.get("DisplayName"), (v.get("CurrencyRef") or {}).get("value"), bool(v.get("Active", True)))
            for v in vends if v.get("Id")]
    if not rows:
        return
    conn = get_conn(); cur = conn.cursor()
    cur.execute("DELETE FROM qbo_vendor;")
    execute_values(cur, "INSERT INTO qbo_vendor (qbo_id, name, currency, active) VALUES %s", rows)
    conn.commit(); cur.close(); conn.close()


def load_vendors(cur, currency):
    """Active suppliers in `currency`: a payable (Rent payable, say) is paid against one of them."""
    cur.execute("""SELECT qbo_id, name FROM qbo_vendor WHERE coalesce(active, true)
                     AND (%s::text IS NULL OR currency IS NULL OR currency = %s) ORDER BY 2;""", (currency, currency))
    return [{"id": i, "n": n or ""} for i, n in cur.fetchall()]


def import_accounts_from_qbo(token):
    """Discover Bank and Credit Card accounts from the connected QBO company and upsert them."""
    try:
        accts = qbo_query("Account", token)
    except Exception:
        return 0
    _store_coa(accts)
    try:
        _store_customers(qbo_query("Customer", token))
    except Exception:
        pass   # receipts can still be recorded to income accounts
    try:
        _store_vendors(qbo_query("Vendor", token))
    except Exception:
        pass   # payables need a supplier; everything else records without
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
    ent_sig = ",".join(sorted(QBO_HANDLERS)) + "|ccy3"   # bump the suffix to force one full re-pull (ccy3: amounts in the account's currency)
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


def _in_account_ccy(amount, e, acct_ccy, home):
    """QuickBooks gives every amount in the transaction's currency (CurrencyRef), with ExchangeRate =
    home units per unit. A USD transfer into a UGX account, or a USD bill paid from a UGX bank, is
    in USD on the record but in UGX on that account."""
    tx_ccy = (e.get("CurrencyRef") or {}).get("value")
    rate = _D(e.get("ExchangeRate")) if e.get("ExchangeRate") else None
    if not tx_ccy or not acct_ccy or tx_ccy == acct_ccy or not rate:
        return amount
    if acct_ccy == home:
        return (amount * rate).quantize(Decimal("0.01"))
    if tx_ccy == home:
        return (amount / rate).quantize(Decimal("0.01"))
    return amount


def _book_rows(etype, handler, ents, by_qbo, home=None):
    """book_txn rows for the tracked accounts these QuickBooks records touch, in each account's currency."""
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
            acct_uuid, atype, acct_ccy = (by_qbo[ref] + (None,))[:3]
            try:
                res = handler(e, ref, atype)
            except Exception:
                continue
            if not res:
                continue
            amount, cp, desc, cat = res
            amount = _in_account_ccy(amount, e, acct_ccy, home)
            # QBO voids by zeroing the amounts; keep the row but out of matching.
            rows.append((ORG_ID, acct_uuid, e.get("Id"), etype, e.get("TxnDate"), amount,
                         acct_ccy or e.get("CurrencyRef", {}).get("value", "USD"), desc, cp,
                         e.get("DocNumber"), cat, "unknown",
                         e.get("MetaData", {}).get("LastUpdatedTime"), amount == 0, _entity_ref(e)))
    return rows


def sync_from_quickbooks(full=False, progress=None):
    step = progress or (lambda msg: None)
    step("Reading your chart of accounts and customers")
    token = qbo_token()
    import_accounts_from_qbo(token)
    since = _sync_since()
    changed_since, ent_sig, notes = _sync_plan(full)
    # Stamped BEFORE fetching, so anything edited mid-sync is caught next time.
    started_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S-00:00")
    conn = get_conn(); cur = conn.cursor()
    cur.execute("ALTER TABLE book_txn ADD COLUMN IF NOT EXISTS category text;")
    conn.commit()
    cur.execute("SELECT account_id, source_account_id, name, type, currency FROM account ORDER BY type, name;")
    by_qbo = {}
    for acct_uuid, acct_qbo, name, atype, ccy in cur.fetchall():
        if acct_qbo:
            by_qbo[str(acct_qbo)] = (acct_uuid, atype, ccy)
    home = qbo_home_currency(cur)
    cur.close(); conn.close()   # not held open through the fetch, which can take minutes
    t0 = time.time()
    # Each page becomes book rows as it arrives and only its ids are kept: a full pull of a
    # large company never holds every raw record in memory at once.
    rows, cache, fetched = [], {}, {}
    for etype, handler in QBO_HANDLERS.items():
        got, seen_n = [], [0]
        step(f"Downloading {etype} records")
        def keep(batch, etype=etype, handler=handler, got=got, seen_n=seen_n):
            got.extend(_book_rows(etype, handler, batch, by_qbo, home))
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
    _cur.execute("ALTER TABLE statement ADD COLUMN IF NOT EXISTS prepared_by text;")
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
    # Customers, so a receipt can be recorded against the customer's name.
    _cur.execute("""CREATE TABLE IF NOT EXISTS qbo_customer (qbo_id text PRIMARY KEY, name text, fqn text,
                    currency text, active boolean);""")
    _cur.execute("ALTER TABLE qbo_customer ADD COLUMN IF NOT EXISTS parent_id text;")   # a child's parent (sub-customer)
    # The USD leg of each forward deal recorded here, so its UGX receipt can be worked out from it.
    _cur.execute("""CREATE TABLE IF NOT EXISTS transfer_dismissal (line_id uuid NOT NULL, other text NOT NULL,
                    dismissed_by text, dismissed_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (line_id, other));""")
    _cur.execute("""CREATE TABLE IF NOT EXISTS qbo_vendor (qbo_id text PRIMARY KEY, name text, currency text,
                    active boolean);""")
    _cur.execute("""CREATE TABLE IF NOT EXISTS hedge_leg (line_id uuid PRIMARY KEY, deal text NOT NULL,
                    usd numeric NOT NULL, rate numeric NOT NULL, txn_date date, qbo_ids text,
                    created_at timestamptz NOT NULL DEFAULT now());""")
    # What the user ticked and chose in the record table, kept until recorded or discarded.
    _cur.execute("""CREATE TABLE IF NOT EXISTS record_draft (line_id uuid PRIMARY KEY, data text NOT NULL,
                    saved_by text, saved_at timestamptz NOT NULL DEFAULT now());""")
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


def load_coa(cur, currency=None):
    """Postable accounts from the cached chart of accounts. With `currency` (the bank's), only those a
    transaction in that currency can use: home-currency accounts, or ones in that same currency.
    QuickBooks refuses a second foreign currency on one transaction."""
    cur.execute("SELECT qbo_id, name, fqn, account_type, currency FROM qbo_coa WHERE coalesce(active, true) ORDER BY fqn;")
    rows = cur.fetchall()
    home = qbo_home_currency(cur) if currency else None
    return [{"id": i, "name": n, "fqn": f or n, "type": t, "ccy": c} for i, n, f, t, c in rows
            if (t not in POST_EXCLUDE and (not currency or not c or c in (home, currency)))
            or (t in ("Accounts Receivable", "Accounts Payable") and currency and c == currency)]


def split_banks(cur, acct_qbo, currency):
    """Bank and card accounts a split line can use (e.g. FX in Transit on a hedge): home currency or the bank's."""
    home = qbo_home_currency(cur)
    cur.execute("""SELECT qbo_id, name, fqn, account_type FROM qbo_coa
                   WHERE coalesce(active, true) AND account_type IN ('Bank','Credit Card') AND qbo_id <> %s
                     AND (currency IS NULL OR currency = %s OR currency = %s) ORDER BY fqn;""",
                (acct_qbo or "", home, currency))
    return [{"id": i, "name": n, "fqn": f or n, "type": t, "bank": True} for i, n, f, t in cur.fetchall()]


def qbo_home_currency(cur):
    """The company's home currency: income and expense accounts are always in it."""
    cur.execute("""SELECT currency FROM qbo_coa WHERE currency IS NOT NULL
                     AND account_type IN ('Expense','Income','Cost of Goods Sold','Other Expense','Other Income')
                   GROUP BY currency ORDER BY count(*) DESC LIMIT 1;""")
    r = cur.fetchone()
    return r[0] if r else None


def qbo_exchange_rate(token, currency, as_of):
    """QuickBooks' own rate for `currency` on `as_of` (home-currency units per unit)."""
    url = (f"{QBO_BASE}/v3/company/{qbo_realm()}/exchangerate?sourcecurrencycode={urllib.parse.quote(currency)}"
           f"&asofdate={as_of}")
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/json")
    with urllib.request.urlopen(req) as resp:
        rate = (json.loads(resp.read()).get("ExchangeRate") or {}).get("Rate")
    if not rate or float(rate) <= 0:
        raise ValueError(f"QuickBooks has no {currency} exchange rate for {as_of}")
    return float(rate)


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


def qbo_read(token, entity, ent_id):
    """One QuickBooks record by id (for its SyncToken). None when QuickBooks has no such record."""
    req = urllib.request.Request(f"{QBO_BASE}/v3/company/{qbo_realm()}/{entity.lower()}/{ent_id}", method="GET")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read()).get(entity)
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="ignore")
        if e.code in (400, 404) and ("Object Not Found" in body or "610" in body):
            return None
        raise urllib.error.HTTPError(e.url, e.code, body[:300], e.hdrs, None)


def qbo_delete(token, entity, ent_id, sync_token):
    req = urllib.request.Request(f"{QBO_BASE}/v3/company/{qbo_realm()}/{entity.lower()}?operation=delete",
                                 data=json.dumps({"Id": str(ent_id), "SyncToken": str(sync_token)}).encode(), method="POST")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def qbo_record_line(token, acct_qbo, atype, money_out, target_id, amount_abs, txn_date, desc, payee, payee_ref,
                    currency=None, rate=None):
    """Create the QuickBooks transaction for one bank line. Returns (entity, new id, payee_ref used).

    A line on a foreign-currency bank (USD in a UGX company) is recorded in that currency at `rate`;
    without CurrencyRef QuickBooks takes it as home currency and refuses the USD bank on it."""
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
    if currency:
        body["CurrencyRef"] = {"value": currency}
        if rate:
            body["ExchangeRate"] = rate
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


def _fx(body, currency, rate):
    if currency:
        body["CurrencyRef"] = {"value": currency}
        if rate:
            body["ExchangeRate"] = rate
    return body


HEDGE_RE = re.compile(r"FXPL[A-Z]*[ ~]*(\d{5,})[ ~]*(FWD|SPOT)[ ~]*(BUY|SELL)[ ~]*([A-Z]{3})/([A-Z]{3})[ ~]*([\d][\d, ]*(?:\.\d+)?)", re.I)


def hedge_info(desc):
    """A forward deal's number and rate from the bank's text: 'FXPLOU~1110179~FWD~BUY~USD/UGX~3,840.0000'."""
    m = HEDGE_RE.search((desc or "").replace("\n", " "))
    if not m or m.group(2).upper() != "FWD":
        return None
    try:
        fwd = Decimal(re.sub(r"[ ,]", "", m.group(6)).rstrip("."))
    except Exception:
        return None
    return {"deal": m.group(1), "fwd": fwd, "ccy": m.group(4).upper(), "home": m.group(5).upper()}


def hedge_accounts(cur, foreign):
    """FX in Transit (foreign), FX in Transit (home) and Forex Gain, found by name in the chart of accounts."""
    home = qbo_home_currency(cur)
    cur.execute("SELECT qbo_id, name, fqn, account_type, currency FROM qbo_coa WHERE coalesce(active, true);")
    rows = cur.fetchall()
    def find(pred):
        return next(({"id": i, "name": n, "fqn": f or n} for i, n, f, t, c in rows if pred(n or "", t, c)), None)
    return {"transit": find(lambda n, t, c: t == "Bank" and c == foreign and n.lower().startswith("fx in transit")),
            "transit_home": find(lambda n, t, c: t == "Bank" and c == home and n.lower().startswith("fx in transit")),
            "gain": find(lambda n, t, c: "forex gain" in n.lower()), "home": home}


def month_rate(cur, ccy, d):
    cur.execute("SELECT value FROM app_config WHERE key=%s;", (f"fx_rate:{ccy}:{str(d)[:7]}",))
    r = cur.fetchone()
    return r[0] if r else ""


def qbo_record_hedge_receipt(token, acct_qbo, transit_home, gain, amount_abs, principal, txn_date, desc):
    """The home-currency receipt of a forward deal: principal (USD x the month's rate) clears FX in
    Transit; the rest is the gain (or, negative, the loss) to Forex Gain."""
    diff = Decimal(amount_abs) - principal
    lines = [{"DetailType": "DepositLineDetail", "Amount": float(principal), "Description": desc[:4000],
              "DepositLineDetail": {"AccountRef": {"value": transit_home}}}]
    if diff:
        lines.append({"DetailType": "DepositLineDetail", "Amount": float(diff),
                      "Description": ("Gain" if diff > 0 else "Loss") + f" on forward deal: {desc}"[:3990],
                      "DepositLineDetail": {"AccountRef": {"value": gain}}})
    body = {"DepositToAccountRef": {"value": acct_qbo}, "TxnDate": str(txn_date), "Line": lines,
            "PrivateNote": f"Recorded from bank reconciliation: {desc}"[:4000]}
    res = qbo_post(token, "Deposit", body)
    return "Deposit", str((res.get("Deposit") or {}).get("Id") or "")


def _pay_amount(bank_amt, x_rate, foreign, rate, pay_ccy):
    """(amount, currency, rate) for a customer Payment. Paid to a student's account in another currency
    (UGX received for their USD account): that currency's amount, at the rate that turns it back into
    exactly the amount the bank received."""
    if not x_rate:
        return bank_amt, foreign, rate
    amt = (Decimal(bank_amt) / x_rate).quantize(Decimal("0.01"))
    return amt, pay_ccy, round(float(Decimal(bank_amt) / amt), 10)


def qbo_record_payment(token, acct_qbo, customer_id, amount_abs, currency=None, rate=None, d=None, desc=""):
    """Money received against a customer's name: an unapplied QuickBooks Payment into this bank."""
    body = _fx({"CustomerRef": {"value": customer_id}, "TotalAmt": float(amount_abs), "TxnDate": str(d),
                "DepositToAccountRef": {"value": acct_qbo},
                "PrivateNote": f"Recorded from bank reconciliation: {desc}"[:4000]}, currency, rate)
    res = qbo_post(token, "Payment", body)
    return "Payment", str((res.get("Payment") or {}).get("Id") or "")


def qbo_record_journal(token, acct_qbo, money_out, parts, txn_date, desc, currency=None, rate=None):
    """One bank line split across several accounts (an FX hedge and its gain or loss, say), as one
    journal entry. `parts` are (account id, amount) in the line's direction; a negative amount goes
    the other way. They add up to the bank amount, so debits equal credits."""
    bank_side = "Credit" if money_out else "Debit"
    other = "Debit" if money_out else "Credit"
    total = sum((a for _, a in parts), Decimal(0))
    note = f"Recorded from bank reconciliation: {desc}"[:4000]
    lines = [{"DetailType": "JournalEntryLineDetail", "Amount": float(total), "Description": desc[:4000],
              "JournalEntryLineDetail": {"PostingType": bank_side, "AccountRef": {"value": acct_qbo}}}]
    for acc_id, a in parts:
        lines.append({"DetailType": "JournalEntryLineDetail", "Amount": float(abs(a)), "Description": desc[:4000],
                      "JournalEntryLineDetail": {"PostingType": other if a > 0 else bank_side,
                                                 "AccountRef": {"value": acc_id}}})
    res = qbo_post(token, "JournalEntry", _fx({"TxnDate": str(txn_date), "PrivateNote": note, "Line": lines},
                                              currency, rate))
    return "JournalEntry", str((res.get("JournalEntry") or {}).get("Id") or ""), note


def qbo_record_transfer(token, from_qbo, to_qbo, amount_abs, txn_date, desc, currency=None, rate=None):
    """One QuickBooks Transfer between two of your own accounts. Returns (entity, new id, the entity).
    Between USD accounts (or USD into a UGX one) it's in USD at `rate`."""
    body = _fx({"FromAccountRef": {"value": from_qbo}, "ToAccountRef": {"value": to_qbo}, "Amount": float(amount_abs),
                "TxnDate": str(txn_date), "PrivateNote": f"Recorded from bank reconciliation: {desc}"[:4000]},
               currency, rate)
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
    accts = cur.fetchall()
    out = {}
    home = qbo_home_currency(cur)
    for acct_uuid, qid, atype, ccy in accts:
        amt, who, _note, cat = _h_transfer(ent, qid, atype)
        amt = _in_account_ccy(amt, ent, ccy, home)
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
CHARGE_RE = re.compile(r"\b(charges?|chgs?|fees?|excise|duty|commission|levy)\b", re.I)
NOT_CHARGE_RE = re.compile(r"^\s*chq|\beft:", re.I)   # a cheque or payment to someone, not the bank's charge


TYPE_XFER_RE = re.compile(r"\b(transfer|trf|tfr|xfer|sweep|own\s*a/?c|inter[- ]?account|funds?\s*trans)", re.I)
TYPE_CUST_RE = re.compile(r"\b(fees?|tuition|pupil|student|admission|term\s*[1-3i]+|school\s*pay|schoolpay)\b", re.I)
TYPE_AP_RE = re.compile(r"\b(rent|landlord)\b", re.I)


def guess_type(who, out, amount, sug_kind=None, conf=None, pair=None):
    """The likely kind of a bank line for the record list's Type box: 'xfer', 'cust' (a student or
    customer payment), 'ap' (a supplier paid against a payable) or 'gl' (an expense or deposit),
    with why, so the user can judge how far to trust it. (kind, why) or ('', '')."""
    if pair:
        days = abs((pair["date"] - pair["d"]).days) if pair.get("d") else None
        when = "" if days is None else " the same day" if days == 0 else f", {days} day{'' if days == 1 else 's'} apart"
        return "xfer", f"Transfer: the same amount moves the other way on {pair['account']}'s statement{when}"
    pct = f"{conf * 100:.0f}% match" if conf is not None else ""
    if sug_kind and conf is not None and conf >= 0.6:
        return sug_kind, f"From how similar lines were posted before ({pct})"
    if is_bank_charge(who, amount):
        return "gl", "Bank charges, from the description"
    text = who or ""
    if TYPE_XFER_RE.search(text):
        return "xfer", "The description mentions a transfer"
    if not out and TYPE_CUST_RE.search(text):
        return "cust", "The description mentions fees or a student"
    if out and TYPE_AP_RE.search(text):
        return "ap", "The description mentions rent"
    if sug_kind:
        return sug_kind, f"From similar lines posted before ({pct}, a weak match)"
    return ("gl", "Most money out is an expense") if out else ("cust", "Most money in is a student or customer payment")


def is_bank_charge(text, amount):
    """A bank charge (fee, commission, excise duty...). The bank takes the same small amounts again and
    again, so a charge only ever pairs with a QuickBooks entry on exactly the same date."""
    return amount is not None and amount < 0 and bool(CHARGE_RE.search(text or "")) \
        and not NOT_CHARGE_RE.search(text or "")


def possible_duplicates(cur, acct_uuid, lines):
    """{line_id: [...]} QuickBooks transactions that look like the same money as these unmatched
    bank lines -- same amount within DUP_WINDOW_DAYS and not matched to anything yet. Recording
    such a line would put a duplicate in the books."""
    if not lines:
        return {}
    cur.execute("""WITH un(line_id, d, amt, win) AS (SELECT * FROM unnest(%s::uuid[], %s::date[], %s::numeric[], %s::int[]))
                   SELECT un.line_id, bt.txn_id, bt.posted_date, bt.amount, coalesce(bt.counterparty, bt.description,'')
                   FROM un JOIN book_txn bt ON bt.account_id=%s AND bt.amount=un.amt
                        AND bt.posted_date BETWEEN un.d - un.win AND un.d + un.win
                   WHERE NOT bt.is_deleted AND NOT bt.is_void
                     AND NOT EXISTS (SELECT 1 FROM match_book_txn mbt JOIN match m ON m.match_id=mbt.match_id
                                     WHERE mbt.txn_id=bt.txn_id AND m.status='confirmed')
                   ORDER BY abs(bt.posted_date - un.d);""",
                ([str(l[0]) for l in lines], [l[1] for l in lines], [l[2] for l in lines],
                 [0 if len(l) > 3 and is_bank_charge(l[3], l[2]) else DUP_WINDOW_DAYS for l in lines], acct_uuid))
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


def match_items(cur, match_ids):
    """{match_id: ([line_id], [txn_id])}: the items behind each match, for editing it by hand."""
    out = {}
    ids = [str(m) for m in match_ids]
    if ids:
        cur.execute("SELECT match_id, line_id FROM match_statement_line WHERE match_id = ANY(%s::uuid[]);", (ids,))
        for mid, lid in cur.fetchall():
            out.setdefault(str(mid), ([], []))[0].append(str(lid))
        cur.execute("SELECT match_id, txn_id FROM match_book_txn WHERE match_id = ANY(%s::uuid[]);", (ids,))
        for mid, tid in cur.fetchall():
            out.setdefault(str(mid), ([], []))[1].append(str(tid))
    return out


def taken_elsewhere(cur, acct_uuid, line_ids):
    """Lines logged as recorded whose QuickBooks entry is matched to a different bank line (an identical
    line took it), while they themselves are unmatched: they still need recording."""
    if not line_ids:
        return set()
    cur.execute("""SELECT w.line_id::text FROM writeback_log w
                   JOIN book_txn bt ON bt.account_id=%s AND bt.source_txn_id=w.qbo_id AND bt.source_txn_type=w.qbo_type
                   WHERE w.line_id = ANY(%s::uuid[]) AND w.status='done'
                     AND EXISTS (SELECT 1 FROM match_book_txn mbt JOIN match m ON m.match_id=mbt.match_id
                                 JOIN match_statement_line msl ON msl.match_id=m.match_id
                                 WHERE mbt.txn_id=bt.txn_id AND m.status='confirmed' AND msl.line_id<>w.line_id)
                     AND NOT EXISTS (SELECT 1 FROM match_statement_line msl JOIN match m ON m.match_id=msl.match_id
                                     WHERE msl.line_id=w.line_id AND m.status='confirmed');""",
                (acct_uuid, [str(x) for x in line_ids]))
    return {r[0] for r in cur.fetchall()}


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
CSS = """<link rel=preconnect href="https://fonts.googleapis.com"><link rel=preconnect href="https://fonts.gstatic.com" crossorigin>
<link rel=stylesheet href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=IBM+Plex+Serif:wght@600&family=IBM+Plex+Mono:wght@500&display=swap">
<style>
/* ReconBook: navy sidebar + compact working area. Navy for structure and primary actions, gold for the
   brand and the one decisive action (Sign off); orange/red/green only for state. */
:root{
  --navy:#13213b;--navy-2:#1c2e4f;--navy-line:#2a3d61;--navy-text:#b9c3d6;
  --gold:#c8a23a;--gold-soft:#f6eed6;
  --bg:#f3f4f7;--panel:#fff;--ink:#18202f;--muted:#5d6779;--faint:#8a93a3;
  --line:#e1e4ea;--line-soft:#eef0f4;--row:#f8f9fb;
  --accent:#2b5797;--accent-soft:#e5ecf7;
  --ok:#1f7a4d;--ok-soft:#e1f2e8;--warn:#c2570c;--warn-soft:#fdebdc;
  --bad:#b42318;--bad-soft:#fbe4e1;--none:#5d6779;--none-soft:#eef0f4;
  --radius:8px;--shadow:0 1px 2px rgba(19,33,59,.05);
  --lift:0 10px 28px rgba(19,33,59,.14);
  --f-ui:'IBM Plex Sans',system-ui,-apple-system,'Segoe UI',Roboto,sans-serif;
  --f-brand:'IBM Plex Serif',Georgia,serif;
  --f-num:'IBM Plex Mono',ui-monospace,Consolas,monospace;
}
*{box-sizing:border-box}
html{color-scheme:light}
[hidden]{display:none!important}
body{font-family:var(--f-ui);background:var(--bg);min-height:100vh;color:var(--ink);margin:0;font-size:13px;line-height:1.45;-webkit-font-smoothing:antialiased}
button,input,select,textarea{font-family:inherit}
a{color:inherit;text-decoration:none}
:focus-visible{outline:2px solid var(--gold);outline-offset:2px}
.wrap{max-width:1240px;margin:0;padding:18px 24px 60px}
h1{font-size:20px;font-weight:600;letter-spacing:-.01em;margin:0 0 3px;text-wrap:balance}
h2{font-size:14px;font-weight:600;color:var(--ink);margin:22px 0 10px}
.sub{color:var(--muted);margin:0 0 16px;font-size:13px}
.btn{display:inline-flex;align-items:center;gap:6px;background:var(--navy);color:#fff;border:1px solid var(--navy);padding:6px 13px;border-radius:6px;cursor:pointer;font-size:13px;font-weight:500;white-space:nowrap}
.btn:hover{background:var(--navy-2)}
.btn-go{display:inline-flex;align-items:center;gap:6px;background:var(--gold);color:var(--navy);border:1px solid var(--gold);padding:6px 14px;border-radius:6px;cursor:pointer;font-size:13px;font-weight:600}
.btn-go:hover{filter:brightness(1.05)}
.btn-go[disabled],.btn[disabled],.btn-sm[disabled]{opacity:.45;cursor:not-allowed;filter:none}
.btn-sm{display:inline-flex;align-items:center;gap:5px;background:var(--panel);border:1px solid var(--line);padding:3px 10px;border-radius:6px;cursor:pointer;font-size:12.5px;font-weight:500;color:var(--ink);white-space:nowrap}
.btn-sm:hover{border-color:#c9ced8;background:#fbfbfc}
.btn-sm.pri{background:var(--navy);border-color:var(--navy);color:#fff}
.btn-sm.danger,.danger{color:var(--bad)}
.btn-sm.danger{border-color:#efc6c0}
.icon-btn{border:1px solid var(--line);background:var(--panel);border-radius:6px;width:26px;height:24px;display:inline-grid;place-items:center;cursor:pointer;color:var(--muted);padding:0;vertical-align:middle}
.icon-btn:hover{color:var(--ink);border-color:#c9ced8}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:0;margin:0 0 14px;background:var(--panel);border:1px solid var(--line);border-radius:var(--radius);overflow:hidden}
.tile{appearance:none;text-align:left;background:none;border:0;border-right:1px solid var(--line-soft);padding:11px 14px;cursor:pointer;font:inherit;color:inherit}
.tile:last-child{border-right:0}
.tile:hover{background:var(--row)}
.tile .t-label{font-size:11px;color:var(--faint);text-transform:uppercase;letter-spacing:.06em;font-weight:600}
.tile .t-val{font:500 18px/1.2 var(--f-num);margin-top:4px;font-variant-numeric:tabular-nums}
.tile.active{box-shadow:inset 0 -2px 0 var(--gold)}
.tile .t-val.warn{color:var(--warn)}
.t-top{display:flex;align-items:center;gap:6px}
.t-ic{color:var(--faint);display:inline-flex}.t-ic svg{width:14px;height:14px;display:block}
.flash{animation:flashbg 1.3s ease}@keyframes flashbg{0%{background:var(--gold-soft)}100%{background:transparent}}
.fbar{font-size:13px;color:var(--muted);margin:10px 0 0;display:none}
.fbar a{color:var(--accent);font-weight:600;cursor:pointer}
.cards{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:18px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:var(--radius);padding:12px 14px}
.card .label{font-size:11px;color:var(--faint);text-transform:uppercase;letter-spacing:.05em;font-weight:600}
.card .val{font:500 18px var(--f-num);margin-top:5px;font-variant-numeric:tabular-nums}
table{width:100%;border-collapse:separate;border-spacing:0;background:var(--panel);border:1px solid var(--line);border-radius:var(--radius);margin:8px 0 18px}
table tr:first-child>th:first-child{border-top-left-radius:var(--radius)}table tr:first-child>th:last-child{border-top-right-radius:var(--radius)}
th,td{text-align:left;padding:6px 12px;border-bottom:1px solid var(--line-soft);font-size:13px;white-space:nowrap}
th{background:#fbfbfc;color:var(--faint);font-size:11px;text-transform:uppercase;letter-spacing:.06em;font-weight:600;border-bottom-color:var(--line)}
tbody tr:last-child td,table tr:last-child td{border-bottom:none}
tr:hover>td{background:var(--row)}
.a{text-align:right;font-variant-numeric:tabular-nums}
.pill{font-size:11.5px;padding:1px 9px;border-radius:999px;font-weight:600;display:inline-flex;align-items:center;gap:5px;white-space:nowrap}
.pill.none{background:var(--none-soft);color:var(--none)}
.pill.open,.pill.attn{background:var(--warn-soft);color:var(--warn)}
.pill.signed,.pill.ok{background:var(--ok-soft);color:var(--ok)}
.pill.bad{background:var(--bad-soft);color:var(--bad)}
.pill.info{background:var(--accent-soft);color:var(--accent)}
.pill.gold{background:var(--gold-soft);color:#7a5d0e}
.tag{font-size:11px;padding:1px 7px;border-radius:4px;font-weight:500;background:var(--line-soft);color:var(--muted)}
.tag.exact{background:var(--accent-soft);color:var(--accent)}
.tag.fuzzy{background:#fff4e5;color:#9a5b00}
.upload{background:var(--panel);border:1px solid var(--line);border-radius:var(--radius);padding:14px;margin-bottom:18px}
.u-label{font-size:12px;color:var(--muted);margin-bottom:6px;font-weight:600}
.upload input[type=file]{font-size:12.5px}
.exc th{color:var(--bad)}
.recgrid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.recgrid table{margin:0}
.rec td{white-space:normal}
.rec tr.tot td{font-weight:600;background:#fbfbfc;border-top:1px solid var(--line)}
.rec .src{color:var(--faint);font-size:11.5px;font-weight:400}
.recres{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;margin:10px 0 8px;padding:9px 12px;border-radius:6px;font-weight:600;font-variant-numeric:tabular-nums}
.recres.balanced{background:var(--ok-soft);color:var(--ok)}
.recres.out{background:var(--bad-soft);color:var(--bad)}
.recres.incomplete{background:var(--none-soft);color:var(--none)}
.recnote{font-size:12.5px;padding:8px 12px;border-radius:6px;margin:6px 0;line-height:1.5}
.recnote.bad{background:var(--bad-soft);color:var(--bad)}
.recnote.warn{background:var(--warn-soft);color:#8a3d08}
.balform{display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end;margin:10px 0 4px}
.balform label{display:block;font-size:12px;color:var(--muted);margin-bottom:4px;font-weight:600}
.balform input{width:170px;padding:5px 8px;border:1px solid var(--line);border-radius:6px;font-size:13px;font-variant-numeric:tabular-nums}
.tag.bf{background:var(--none-soft);color:var(--none)}
.tag.pending{background:var(--warn-soft);color:var(--warn)}
.hint{color:var(--faint);font-size:11.5px;line-height:1.4;white-space:normal}
.rectbl td{vertical-align:top}
/* Bank wording with no spaces (FXPLOU~1110179~FWD~BUY~USD/UGX~3,840.0000) wraps inside its cell instead of
   running into the amount beside it; dates and amounts stay on one line. */
td.desc{white-space:normal;overflow-wrap:anywhere;word-break:break-word;min-width:140px;max-width:300px}
td.a{white-space:nowrap}
.rectbl select,.rectbl input.payee{padding:3px 7px;border:1px solid var(--line);border-radius:5px;font-size:12.5px;max-width:220px;background:#fff}
.rectbl select{width:220px}.rectbl input.payee{width:150px}
.acctbox{position:relative;width:220px;max-width:100%}
.ttype-why{margin:-2px 0 5px;max-width:220px;white-space:normal}
.ttype{display:block;width:220px;max-width:100%;margin:0 0 4px;padding:2px 5px;border:1px solid var(--line);border-radius:5px;font-size:12px;color:var(--ink);background:#fbfbfc}
.acctbox .acct-q{width:100%;box-sizing:border-box;padding:3px 24px 3px 7px;border:1px solid var(--line);border-radius:5px;font-size:12.5px;background:#fff}
.acctbox .acct-q.bad{border-color:#d97706;background:#fffbeb}
.acctbox .acct-x{position:absolute;right:2px;top:1px;border:0;background:none;color:var(--faint);font-size:16px;line-height:1;cursor:pointer;padding:2px 5px}
.acctbox .acct-x:hover{color:var(--bad)}
.acct-list{position:absolute;z-index:30;top:100%;left:0;width:min(340px,86vw);max-height:280px;overflow:auto;background:#fff;border:1px solid var(--line);border-radius:8px;box-shadow:var(--lift);margin-top:3px}
.acct-list .ao{padding:5px 10px;font-size:12.5px;cursor:pointer;white-space:normal;display:flex;justify-content:space-between;gap:10px}
.acct-list .ao .at{color:var(--faint);font-size:11px;white-space:nowrap}
.acct-list .ao.hi{background:var(--accent-soft)}
.acct-list .ag{padding:7px 10px 3px;font-size:10.5px;color:var(--faint);text-transform:uppercase;letter-spacing:.05em}
.acct-list .none{padding:9px 10px;font-size:12px;color:var(--muted);white-space:normal}
.acctbox.off{opacity:.45}
.custbox{position:relative;width:160px;max-width:100%}
.custbox .acct-q{width:100%;box-sizing:border-box;padding:3px 24px 3px 7px}
.custbox .acct-q.bad{border-color:#d97706;background:#fffbeb}
.custbox .acct-x{position:absolute;right:2px;top:1px;border:0;background:none;color:var(--faint);font-size:16px;line-height:1;cursor:pointer;padding:2px 5px}
.rowtools{display:flex;gap:6px;align-items:center;margin-top:5px;flex-wrap:wrap}
.rowtools .split-btn.on,.split-btn.on{background:var(--accent-soft);color:var(--accent)}
.rectbl input.rate{width:118px;padding:3px 7px;border:1px solid var(--line);border-radius:5px;font-size:12px;background:#fff}
.splitrow td{background:#f7f9fd;white-space:normal}
.splithead{font-size:12px;color:var(--muted);margin:2px 0 6px}
.splitline{display:flex;gap:8px;align-items:center;margin:5px 0;flex-wrap:wrap}
.splitamt{width:130px;padding:4px 8px;border:1px solid var(--line);border-radius:5px;font-size:12.5px;text-align:right;font-variant-numeric:tabular-nums}
.splitfoot{display:flex;gap:10px;align-items:center;margin-top:6px;flex-wrap:wrap}
.splitrem{font-size:12.5px;font-weight:600}.splitrem.ok{color:var(--ok)}.splitrem.warn{color:var(--warn)}
.recbar{position:sticky;bottom:0;z-index:5;display:flex;gap:10px;align-items:center;margin-top:0;flex-wrap:wrap;padding:9px 14px;background:#f7f8fa;border:1px solid var(--line);border-radius:0 0 var(--radius) var(--radius)}
.hedgerow td,.kidsrow td{background:#fbfaf3;white-space:normal}
.hedgeins{display:flex;gap:14px;align-items:flex-end;flex-wrap:wrap;font-size:12px;color:var(--muted)}
.hedgeins label{display:flex;flex-direction:column;gap:4px}
.hedgeins input{width:150px;padding:4px 8px;border:1px solid var(--line);border-radius:5px;font-size:12.5px;text-align:right}
.hedgecalc{font-size:12.5px;color:var(--ink);font-weight:500}
.hedge-btn.on{background:var(--accent-soft);color:var(--accent)}
.kidline{display:flex;gap:10px;align-items:center;margin:5px 0;font-size:12.5px}
.kidline span{min-width:220px}
.savedsel{background:var(--accent-soft);border:1px solid #c8d6ec;color:#24406b;padding:7px 12px;border-radius:6px;font-size:12.5px;margin:0 0 10px}
.booksrc{display:flex;gap:10px;align-items:center;flex-wrap:wrap;font-size:13px}
.btnrow{display:flex;gap:6px;flex-wrap:wrap;align-items:center}
.btnrow form{margin:0}
.dupwarn{margin-top:5px;padding:6px 9px;border-radius:6px;background:var(--warn-soft);color:#8a3d08;font-size:12px;line-height:1.45;white-space:normal}
.dupwarn .btn-sm{margin-top:5px}
.mmgrid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.mmcol{background:var(--panel);border:1px solid var(--line);border-radius:var(--radius);overflow:hidden}
.mmhead{padding:8px 12px;border-bottom:1px solid var(--line-soft);font-size:11px;text-transform:uppercase;letter-spacing:.05em;font-weight:600;color:var(--faint);display:flex;gap:8px;align-items:center;justify-content:space-between}
.mmsearch{padding:4px 8px;border:1px solid var(--line);border-radius:5px;font-size:12.5px;width:55%;text-transform:none;letter-spacing:0}
.mmlist{max-height:340px;overflow:auto}
.mmrow{display:grid;grid-template-columns:22px 86px minmax(0,1fr) auto;gap:8px;align-items:center;padding:6px 12px;border-bottom:1px solid var(--line-soft);font-size:12.5px;cursor:pointer}
.mmrow:hover{background:var(--row)}.mmrow.on{background:var(--accent-soft)}
.mmrow .mmw{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mmrow .a{font-variant-numeric:tabular-nums}
.mmbar{position:sticky;bottom:0;display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;margin:10px 0 18px;padding:9px 14px;background:var(--panel);border:1px solid var(--line);border-radius:var(--radius);font-size:13px;font-variant-numeric:tabular-nums}
.mmbar .ok{color:var(--ok);font-weight:600}.mmbar .warn{color:var(--warn);font-weight:600}
@media (max-width:760px){.mmgrid{grid-template-columns:1fr}}
.ok{color:var(--ok)}.warn{color:var(--warn)}.bad{color:var(--bad)}.muted{color:var(--muted)}.faint{color:var(--faint)}
.num{font-variant-numeric:tabular-nums}
#loadingov{position:fixed;inset:0;background:rgba(243,244,247,.78);display:none;align-items:center;justify-content:center;flex-direction:column;gap:14px;z-index:9999}
#loadingov.on{display:flex}
#loadingov .spin{width:38px;height:38px;border:3px solid var(--line);border-top-color:var(--navy);border-radius:50%;animation:spin .8s linear infinite}
#loadingov .msg{color:var(--muted);font-size:13px;font-weight:600}
@keyframes spin{to{transform:rotate(360deg)}}
/* The footer sits at the bottom of the window (or below the content when it's longer), centred. */
.appfoot{margin-top:auto;padding:14px 24px 18px;border-top:1px solid var(--line);background:var(--panel);color:var(--faint);font-size:12px;text-align:center}.appfoot a{color:var(--muted)}.appfoot a:hover{color:var(--ink)}
.pw-wrap{position:relative}
.pw-wrap input{padding-right:42px !important}
.pw-toggle{position:absolute;right:5px;top:50%;transform:translateY(-50%);width:auto;height:auto;margin:0;padding:6px;background:none;border:none;border-radius:6px;cursor:pointer;color:var(--faint);display:flex}
.pw-toggle:hover{color:var(--ink)}
/* ---- shell ---- */
.app{display:grid;grid-template-columns:236px minmax(0,1fr);min-height:100vh}
.side{background:var(--navy);color:var(--navy-text);display:flex;flex-direction:column;position:sticky;top:0;height:100vh;overflow:auto;z-index:50}
.side .brand{display:flex;gap:10px;align-items:center;padding:16px 18px 13px;border-bottom:1px solid var(--navy-line);color:#fff}
.side .brand svg{flex:none;color:var(--gold);width:28px;height:28px}
.side .brand b{display:block;font:600 19px/1.1 var(--f-brand);color:#fff;letter-spacing:.2px}
.side .brand small{display:block;font-size:10px;letter-spacing:.9px;text-transform:uppercase;color:var(--navy-text);margin-top:3px;line-height:1.3}
.snav{padding:8px;display:flex;flex-direction:column;gap:1px}
.snav .lbl{font-size:10px;text-transform:uppercase;letter-spacing:1px;color:#7f8ba3;padding:12px 10px 5px}
.snav a{display:flex;align-items:center;gap:9px;padding:6px 10px;border-radius:6px;color:var(--navy-text);font-weight:500;min-width:0}
.snav a:hover{background:var(--navy-2);color:#fff}
.snav a.on{background:var(--navy-2);color:#fff;box-shadow:inset 3px 0 0 var(--gold)}
.snav a svg{flex:none;opacity:.85;width:16px;height:16px}
.snav a .nm{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.snav a .cnt{margin-left:auto;font-size:11px;color:#8d99b1;font-variant-numeric:tabular-nums}
.sdot{width:7px;height:7px;border-radius:50%;flex:none;background:#6b7790}
.sdot.attn{background:#f08a3c}.sdot.ok{background:#4cc38a}
.sfoot{margin-top:auto;padding:12px;border-top:1px solid var(--navy-line);display:flex;flex-direction:column;gap:10px}
.qbo{background:var(--navy-2);border-radius:8px;padding:9px 10px;font-size:12px}
.qbo .st{display:flex;align-items:center;gap:6px;color:#fff;font-weight:600}
.qbo .st i{width:7px;height:7px;border-radius:50%;background:#4cc38a}
.qbo .st i.off{background:#e06b5a}
.qbo .when{color:#8d99b1;margin:2px 0 7px}
.qbo .row{display:flex;gap:6px;align-items:center}
.qbo form{margin:0}
.btn-dk{background:transparent;border:1px solid var(--navy-line);color:#dfe5f0;border-radius:6px;padding:3px 9px;cursor:pointer;font-size:12px;font-family:inherit}
.btn-dk:hover{border-color:var(--gold);color:#fff}
.me{display:flex;align-items:center;gap:9px;padding:4px 2px;cursor:pointer;border-radius:6px;width:100%;background:none;border:0;text-align:left;color:inherit;font:inherit}
.me .av{width:28px;height:28px;border-radius:50%;background:var(--gold);color:var(--navy);display:grid;place-items:center;font-weight:700;font-size:12px;flex:none}
.me b{color:#fff;font-weight:600;display:block;font-size:12.5px}.me small{font-size:11px;color:#8d99b1}
.main{min-width:0;display:flex;flex-direction:column;min-height:100vh}
.topbar{display:flex;align-items:center;gap:12px;padding:9px 24px;background:var(--panel);border-bottom:1px solid var(--line);position:sticky;top:0;z-index:20;min-height:46px}
.crumb{color:var(--muted);font-size:12.5px;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.crumb b{color:var(--ink);font-weight:600}
.crumb a:hover{color:var(--ink)}
.topbar .sp{flex:1}
.tsearch{display:flex;align-items:center;gap:6px;border:1px solid var(--line);border-radius:6px;padding:4px 9px;color:var(--faint);width:280px;max-width:40vw;background:var(--bg);margin:0}
.tsearch input{border:0;background:none;outline:none;width:100%;color:var(--ink);font-size:12.5px}
.menu-btn{display:none}
.kebab{position:relative;display:inline-block;vertical-align:middle}
.dd{position:absolute;right:0;top:calc(100% + 4px);background:var(--panel);border:1px solid var(--line);border-radius:8px;box-shadow:var(--lift);min-width:200px;padding:4px;z-index:45;color:var(--ink);text-align:left;white-space:normal}
.dd.up{top:auto;bottom:calc(100% + 4px)}.dd.left{left:0;right:auto}
.dd form{margin:0;display:block}
.dd button,.dd a{display:flex;width:100%;text-align:left;background:none;border:0;padding:6px 10px;border-radius:5px;cursor:pointer;gap:8px;align-items:center;font:inherit;font-size:12.5px;color:inherit}
.dd button:hover,.dd a:hover{background:var(--bg)}
.dd .sep{height:1px;background:var(--line-soft);margin:4px 2px}
.dd .dh{font-size:10.5px;color:var(--faint);padding:5px 10px 2px;text-transform:uppercase;letter-spacing:.6px}
.dd .danger{color:var(--bad)}
.ph{display:flex;align-items:flex-end;gap:14px;flex-wrap:wrap;margin:0 0 14px}
.ph h1{margin:0}
.ph .meta{color:var(--muted);margin-top:4px;display:flex;gap:8px;align-items:center;flex-wrap:wrap;font-size:12.5px}
.ph .acts{margin-left:auto;display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:var(--radius)}
.panel-h{display:flex;align-items:center;gap:10px;padding:9px 14px;border-bottom:1px solid var(--line-soft)}
.panel-h h2{font-size:13.5px;margin:0}
.panel-h .r{margin-left:auto;display:flex;gap:8px;align-items:center}
.panel table{border:0;border-radius:0;margin:0}
.help{color:var(--muted);font-size:12.5px;margin:0 0 8px}
details.how{display:inline}
details.how>summary{display:inline;cursor:pointer;color:var(--accent);list-style:none}
details.how>summary::-webkit-details-marker{display:none}
details.how[open]>summary{color:var(--faint)}
details.how>div{display:block;margin:6px 0 4px;padding:9px 12px;border-radius:6px;background:var(--accent-soft);color:#24406b;line-height:1.55;max-width:860px}
.bar{height:6px;border-radius:99px;background:var(--line-soft);overflow:hidden;min-width:70px}
.bar i{display:block;height:100%;background:var(--ok);border-radius:99px}
.bar.attn i{background:#e08a3e}
.scrim{position:fixed;inset:0;background:rgba(19,33,59,.35);z-index:60}
.drawer{position:fixed;top:0;right:0;bottom:0;width:min(460px,100%);background:var(--panel);z-index:61;box-shadow:-12px 0 40px rgba(19,33,59,.18);display:flex;flex-direction:column}
.drawer-h{display:flex;align-items:center;gap:10px;padding:13px 18px;border-bottom:1px solid var(--line)}
.drawer-h h2{font-size:15px;margin:0}
.drawer-b{padding:16px 18px;overflow:auto;display:flex;flex-direction:column;gap:12px}
.drawer-f{margin-top:auto;padding:12px 18px;border-top:1px solid var(--line);display:flex;gap:8px;justify-content:flex-end;align-items:center}
.drawer h3{font-size:12px;text-transform:uppercase;letter-spacing:.06em;color:var(--faint);margin:6px 0 0}
.fld{display:flex;flex-direction:column;gap:4px}
.fld label{font-size:12px;font-weight:600;color:var(--muted)}
.fld input,.fld select{border:1px solid var(--line);border-radius:6px;padding:6px 9px;background:var(--panel);font-size:13px}
.fld small{color:var(--faint);font-size:11.5px}
.two{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
.rb-dlg{position:fixed;left:50%;top:20%;transform:translateX(-50%);width:min(440px,calc(100% - 32px));background:var(--panel);border-radius:10px;z-index:61;box-shadow:0 24px 60px rgba(19,33,59,.28);overflow:hidden}
.rb-dlg .dlg-b{padding:18px 20px 6px}
.rb-dlg h3{font-size:15px;margin:0 0 6px}
.rb-dlg p{margin:0;color:var(--muted);line-height:1.5}
.rb-dlg .dlg-f{display:flex;justify-content:flex-end;gap:8px;padding:14px 20px}
.rb-dlg .btn.danger{background:var(--bad);border-color:var(--bad);color:#fff}
#flash{position:fixed;right:16px;bottom:16px;z-index:80;background:var(--navy);color:#fff;border-radius:8px;padding:10px 38px 10px 14px;box-shadow:0 10px 26px rgba(19,33,59,.28);max-width:min(420px,calc(100% - 32px));font-size:13px;line-height:1.45;animation:tin .2s ease}
#flash.err{background:#7a1a12}
#flash .x{position:absolute;right:6px;top:6px;background:none;border:0;color:#cfd7e6;cursor:pointer;font-size:16px;line-height:1;padding:3px 6px}
#flash a{color:var(--gold);font-weight:600}
@keyframes tin{from{transform:translateY(8px);opacity:0}to{transform:none;opacity:1}}
@media (prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
@media (max-width:860px){
  .app{grid-template-columns:minmax(0,1fr)}
  .side{position:fixed;left:0;top:0;bottom:0;width:250px;transform:translateX(-100%);transition:transform .2s}
  .side.open{transform:none;box-shadow:12px 0 40px rgba(0,0,0,.3)}
  .menu-btn{display:inline-grid}
  .tsearch{display:none}
  .wrap{padding:14px 16px 50px}
  .topbar{padding:8px 16px}
  .recgrid,.two{grid-template-columns:1fr}
  .cards{grid-template-columns:repeat(2,1fr)}
}
@media print{.side,.topbar,#flash{display:none}.app{display:block}}
</style>"""

# The page frame every signed-in page shares: the navy sidebar (brand, bank accounts, QuickBooks
# status, the user menu) and the top bar (where you are, search). SHELL_END closes it and adds the
# confirmation dialog, the loading overlay and the small scripts every page uses.
SCALE_ICON = ('<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" '
              'stroke-linejoin="round" aria-hidden="true"><path d="M12 3.5v17"/><path d="M7 6.5h10"/>'
              '<path d="M7 6.5 4 12.8a3 3 0 0 0 6 0L7 6.5Z"/><path d="M17 6.5l-3 6.3a3 3 0 0 0 6 0L17 6.5Z"/><path d="M8.5 20.5h7"/></svg>')
DOTS_ICON = '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><circle cx="5" cy="12" r="1.7"/><circle cx="12" cy="12" r="1.7"/><circle cx="19" cy="12" r="1.7"/></svg>'
SHELL_TOP = """{% set S = shell_data() %}<div class=app>
<aside class=side id=side aria-label="Main menu">
<a class=brand href="{{ url_for('dashboard') }}">""" + SCALE_ICON + """<div><b>ReconBook</b>{% if S.company %}<small>{{ S.company }}</small>{% endif %}</div></a>
<nav class=snav>
<a href="{{ url_for('dashboard') }}" class="{{ 'on' if S.page=='dashboard' else '' }}"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M4 11 12 4l8 7"/><path d="M6 10v9h12v-9"/></svg>Dashboard</a>
{% if S.accounts %}<div class=lbl>Bank accounts</div>{% endif %}
{% for a in S.accounts %}<a href="{{ url_for('detail', name=a.name) }}" class="{{ 'on' if a.name==S.acct else '' }}" data-busy="Loading {{ a.name }}..." title="{{ a.name }}"><span class="sdot {{ a.state }}"></span><span class=nm>{{ a.name }}</span>{% if a.n %}<span class=cnt>{{ a.n }}</span>{% endif %}</a>{% endfor %}
<div class=lbl>Manage</div>
<a href="{{ url_for('reports') }}" class="{{ 'on' if S.page=='reports' else '' }}"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M6 3.5h8l4 4v13H6z"/><path d="M14 3.5v4h4"/><path d="M9 13h6M9 16.5h6"/></svg>Reports</a>
{% if can('users') %}<a href="{{ url_for('users') }}" class="{{ 'on' if S.page=='users' else '' }}"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"><circle cx="9" cy="8" r="3.2"/><path d="M3.5 19c.6-3.2 3-5 5.5-5s4.9 1.8 5.5 5"/><circle cx="17" cy="9" r="2.4"/><path d="M15.5 14.2c2.2.1 4.3 1.6 5 4.3"/></svg>Users &amp; permissions</a>{% endif %}
{% if can('settings') %}<a href="{{ url_for('settings') }}" class="{{ 'on' if S.page in ('settings','manage_accounts') else '' }}"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M12 3v2.5M12 18.5V21M3 12h2.5M18.5 12H21M5.6 5.6l1.8 1.8M16.6 16.6l1.8 1.8M5.6 18.4l1.8-1.8M16.6 7.4l1.8-1.8"/></svg>Settings</a>{% endif %}
</nav>
<div class=sfoot>
<div class=qbo>{% if S.qbo %}<div class=st><i></i>QuickBooks connected</div><div class=when>{% if S.synced %}Synced {{ S.synced }}{% else %}Not synced yet{% endif %}</div>
<div class=row><form method=post action="{{ url_for('sync') }}"><button type=submit class=btn-dk>Sync now</button></form>
<span class=kebab><button type=button class=btn-dk data-dd aria-label="More sync options" aria-expanded=false>&#9662;</button><div class="dd up left" hidden>
<form method=post action="{{ url_for('sync') }}" data-confirm="Full resync? It re-reads every transaction in the window, ignoring the last-sync marker. Slower, but use it if you think something was missed."><input type=hidden name=full value="1"><button type=submit>Full resync</button></form>
{% if can('settings') %}<a href="{{ url_for('settings') }}">Connection settings</a>{% endif %}</div></span></div>
{% else %}<div class=st><i class=off></i>QuickBooks not connected</div><div class=when>Books can't refresh</div>{% if can('settings') %}<div class=row><a class=btn-dk href="{{ url_for('settings') }}">Connect</a></div>{% endif %}{% endif %}</div>
<span class=kebab style="display:block"><button type=button class=me data-dd aria-expanded=false><span class=av>{{ S.initials }}</span><span><b>{{ session.name or 'Signed in' }}</b><small>{{ S.role }}</small></span></button>
<div class="dd up left" hidden><a href="{{ url_for('change_password') }}">Change password</a><div class=sep></div><a href="{{ url_for('logout') }}">Sign out</a></div></span>
</div>
</aside>
<div class=main>
<header class=topbar><button type=button class="icon-btn menu-btn" id=menubtn aria-label="Menu"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M4 7h16M4 12h16M4 17h16"/></svg></button>
<div class=crumb>{{ S.crumb|safe }}</div><span class=sp></span>
<form class=tsearch method=get action="{{ url_for('search') }}" role=search><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="11" cy="11" r="6"/><path d="m20 20-4.5-4.5"/></svg><input name=q value="{{ request.args.get('q','') if S.page=='search' else '' }}" placeholder="Find an amount, payee or reference" aria-label="Search"></form>
</header>
"""
SHELL_END = """<div class=appfoot><a href="{{ url_for('terms') }}">Terms</a> · <a href="{{ url_for('privacy') }}">Privacy</a> · <a href="mailto:{{ contact_email }}">Contact</a></div>
</div></div>
<div id=loadingov><div class=spin></div><div class=msg id=loadingmsg>Loading...</div></div>
<div class=scrim id=rb-scrim hidden></div>
<div class=rb-dlg id=rb-dlg hidden role=dialog aria-modal=true aria-labelledby=rb-dlg-t><div class=dlg-b><h3 id=rb-dlg-t>Please confirm</h3><p id=rb-dlg-msg></p></div>
<div class=dlg-f><button type=button class=btn-sm id=rb-no>Cancel</button><button type=button class=btn id=rb-yes>Confirm</button></div></div>
<script>(function(){
var ov=document.getElementById('loadingov'),msg=document.getElementById('loadingmsg'),timer,hideTimer;
function show(t){if(msg&&t)msg.textContent=t;if(ov)ov.classList.add('on');clearTimeout(hideTimer);hideTimer=setTimeout(function(){if(ov)ov.classList.remove('on');},40000);}
function schedule(t){clearTimeout(timer);timer=setTimeout(function(){show(t);},180);}
// "Confirm" -> "Confirming...", "Save balances" -> "Saving balances...": say what's being done.
var VERB={Confirm:'Confirming',Reject:'Rejecting',Undo:'Undoing',Edit:'Opening',Record:'Recording',Save:'Saving',Get:'Getting',
  Sign:'Signing',Match:'Matching',Import:'Importing',Clear:'Clearing',Delete:'Deleting',Set:'Setting',Update:'Updating',
  Create:'Creating',Remove:'Removing',Check:'Checking',Disconnect:'Disconnecting',Refresh:'Refreshing',Discard:'Discarding',
  Upload:'Uploading',Add:'Adding',Sync:'Syncing',Full:'Starting a full resync'};
function busy(label){var w=label.split(' '),v=VERB[w[0]];if(!v)return '';var rest=label.slice(w[0].length).trim();
  return v+(rest?' '+rest.charAt(0).toLowerCase()+rest.slice(1):'')+'...';}
document.addEventListener('click',function(e){
var a=e.target.closest?e.target.closest('a'):null;if(!a)return;
var href=a.getAttribute('href')||'';if(!href)return;
if(a.target==='_blank'||a.hasAttribute('download'))return;
if(href[0]==='#'||href.indexOf('javascript:')===0||href.indexOf('mailto:')===0)return;
if(href.indexOf('.csv')>-1||href.indexOf('/template/')>-1||href.indexOf('/backup')>-1||href.indexOf('/csv')>-1)return;
if(e.metaKey||e.ctrlKey||e.shiftKey||e.altKey)return;
schedule(a.getAttribute('data-busy')||'Loading...');});
document.addEventListener('submit',function(e){
if(e.defaultPrevented)return;
var f=e.target,act=(f.getAttribute&&f.getAttribute('action'))||'';var t='Please wait...';
if((f.getAttribute('method')||'get').toLowerCase()!=='post'&&act.indexOf('/search')>-1)t='Searching...';
var sb=e.submitter,bt=sb&&(sb.getAttribute('data-busy')||busy((sb.textContent||sb.value||'').trim()));
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
if(bt&&act.indexOf('/upload')<0&&act.indexOf('/import_books')<0&&act.indexOf('/sync')<0)t=bt;
schedule(t);});
window.addEventListener('pageshow',function(){clearTimeout(timer);clearTimeout(hideTimer);if(ov)ov.classList.remove('on');});
// Menus: a button with data-dd opens the .dd beside it; a click elsewhere or Escape closes it.
function closeDD(except){[].forEach.call(document.querySelectorAll('.dd'),function(d){if(d!==except&&!d.hidden){d.hidden=true;var b=d.parentNode.querySelector('[data-dd]');if(b)b.setAttribute('aria-expanded','false')}})}
document.addEventListener('click',function(e){var b=e.target.closest&&e.target.closest('[data-dd]');
  if(b){e.preventDefault();var d=b.parentNode.querySelector('.dd');if(!d)return;var open=d.hidden;closeDD(d);d.hidden=!open;b.setAttribute('aria-expanded',String(open));if(open)place(b,d);return}
  var inDD=e.target.closest&&e.target.closest('.dd');
  if(!inDD)closeDD();else if(e.target.closest('button,a')&&!e.target.closest('input,select,label'))setTimeout(function(){closeDD()},0);});
// A menu opens beside its button even inside a table or a scrolling box (fixed to the window).
function place(b,d){if(!b.getBoundingClientRect)return;var r=b.getBoundingClientRect();if(!r.width&&!r.height)return;
  var W=window.innerWidth,Hh=window.innerHeight;d.style.position='fixed';d.style.zIndex='70';
  var up=d.classList.contains('up')||(r.bottom+260>Hh&&r.top>260);
  if(up){d.style.top='auto';d.style.bottom=(Hh-r.top+4)+'px'}else{d.style.bottom='auto';d.style.top=(r.bottom+4)+'px'}
  if(d.classList.contains('left')){d.style.left=Math.max(8,r.left)+'px';d.style.right='auto'}else{d.style.right=Math.max(8,W-r.right)+'px';d.style.left='auto'}}
window.addEventListener('scroll',function(e){if(e.target&&e.target.closest&&e.target.closest('.dd'))return;closeDD()},true);
window.addEventListener('resize',function(){closeDD()});
var mb=document.getElementById('menubtn'),side=document.getElementById('side');
if(mb&&side)mb.addEventListener('click',function(){side.classList.toggle('open')});
// Confirmations in a page dialog instead of the browser's box. A form with data-confirm asks first;
// scripts call rbAsk(message, onYes).
var dlg=document.getElementById('rb-dlg'),scrim=document.getElementById('rb-scrim'),yes=document.getElementById('rb-yes'),
    no=document.getElementById('rb-no'),onYes=null,back=null;
function closeAsk(){dlg.hidden=true;scrim.hidden=true;onYes=null;if(back&&back.focus)back.focus();back=null}
window.rbAsk=function(m,cb,opts){opts=opts||{};var s=String(m||''),i=s.search(/[?.!](\\s|$)/);
  var head=i>-1&&i<110?s.slice(0,i+1):s,rest=i>-1&&i<110?s.slice(i+1).trim():'';
  document.getElementById('rb-dlg-t').textContent=head;document.getElementById('rb-dlg-msg').textContent=rest;
  var danger=opts.danger!=null?opts.danger:/\\b(delete|deleted|remove|disconnect|clear|undo)\\b/i.test(s);
  yes.textContent=opts.yes||(danger?(s.match(/\\b(Delete|Remove|Disconnect|Clear|Undo)\\b/i)||['Confirm'])[0].replace(/^./,function(c){return c.toUpperCase()}):'Confirm');
  yes.className='btn'+(danger?' danger':'');onYes=cb;back=document.activeElement;dlg.hidden=false;scrim.hidden=false;yes.focus()};
window.rbResubmit=function(f,sb){f._rbok=true;if(f.requestSubmit){try{f.requestSubmit(sb||undefined);return}catch(err){}}
  if(sb&&sb.name){var h=document.createElement('input');h.type='hidden';h.name=sb.name;h.value=sb.value;f.appendChild(h)}f.submit()};
yes.addEventListener('click',function(){var cb=onYes;closeAsk();if(cb)cb()});
no.addEventListener('click',closeAsk);scrim.addEventListener('click',closeAsk);
document.addEventListener('keydown',function(e){if(e.key==='Escape'){if(!dlg.hidden)closeAsk();closeDD()}});
document.addEventListener('submit',function(e){var f=e.target,q=f.getAttribute&&f.getAttribute('data-confirm');
  if(!q)return;if(f._rbok){f._rbok=false;return}
  e.preventDefault();e.stopImmediatePropagation();var sb=e.submitter;rbAsk(q,function(){rbResubmit(f,sb)})},true);
// Side panels: a button with data-drawer="x" opens #dr-x; data-close, the shade or Escape closes it.
var dscrim=document.createElement('div');dscrim.className='scrim';dscrim.hidden=true;document.body.appendChild(dscrim);
function drawers(){return [].slice.call(document.querySelectorAll('.drawer'))}
function closeDrawers(){drawers().forEach(function(d){d.hidden=true});dscrim.hidden=true}
window.rbOpenDrawer=function(n){var d=document.getElementById('dr-'+n);if(!d)return;closeDrawers();d.hidden=false;dscrim.hidden=false;
  var f=d.querySelector('input:not([type=hidden]),select,button');if(f&&f.focus)f.focus()};
document.addEventListener('click',function(e){var o=e.target.closest&&e.target.closest('[data-drawer]');if(o){e.preventDefault();rbOpenDrawer(o.getAttribute('data-drawer'));return}
  if(e.target.closest&&e.target.closest('[data-close]')){e.preventDefault();closeDrawers()}});
dscrim.addEventListener('click',closeDrawers);
document.addEventListener('keydown',function(e){if(e.key==='Escape'&&dlg.hidden)closeDrawers()});
drawers().forEach(function(d){if(!d.hidden)dscrim.hidden=false});
// The result of the last action: a message in the corner that fades (problems stay until closed).
var fl=document.getElementById('flash');
if(fl){var bad=/(^|\\s)(not |nothing |couldn|can't|failed|refused|error|expired|isn't|wasn't)/i.test(fl.textContent);if(bad)fl.classList.add('err');
  var x=document.createElement('button');x.type='button';x.className='x';x.setAttribute('aria-label','Close');x.textContent='\\u00d7';
  x.addEventListener('click',function(){fl.hidden=true});fl.appendChild(x);if(!bad)setTimeout(function(){fl.hidden=true},6000)}
})();</script>
"""


# Reusable show/hide-password eye icon: EYE_ICON = "click to reveal" state, EYE_OFF_ICON = "click to hide" state.
EYE_ICON = '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M1 12s4-7 11-7 11 7 11 7-4 7-11 7-11-7-11-7Z"/><circle cx="12" cy="12" r="3"/></svg>'
EYE_OFF_ICON = '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M17.94 17.94A10.94 10.94 0 0 1 12 19c-7 0-11-7-11-7a21.27 21.27 0 0 1 5.06-5.94M9.9 4.24A10.94 10.94 0 0 1 12 4c7 0 11 7 11 7a21.27 21.27 0 0 1-4.22 5.06"/><path d="M14.12 14.12a3 3 0 1 1-4.24-4.24"/><path d="M1 1l22 22"/></svg>'
PW_TOGGLE_JS = ("function togglePw(btn,id){var i=document.getElementById(id);if(!i)return;"
                 "var showing=i.type==='text';i.type=showing?'password':'text';"
                 "btn.setAttribute('aria-pressed',showing?'false':'true');"
                 "btn.setAttribute('aria-label',showing?'Show password':'Hide password');"
                 "btn.innerHTML=showing?" + repr(EYE_ICON) + ":" + repr(EYE_OFF_ICON) + ";}")

LOGIN_PAGE = """<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Sign in · ReconBook</title>
<link rel=preconnect href="https://fonts.googleapis.com"><link rel=preconnect href="https://fonts.gstatic.com" crossorigin>
<link rel=stylesheet href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Serif:wght@600&display=swap">
<style>
*{box-sizing:border-box}
html{color-scheme:light}
body{font-family:'IBM Plex Sans',system-ui,-apple-system,'Segoe UI',sans-serif;margin:0;min-height:100vh;display:flex;flex-direction:column;align-items:center;justify-content:center;padding:24px 16px;color:#18202f;font-size:14px;
  background:radial-gradient(700px 380px at 80% 10%,rgba(200,162,58,.16),transparent 70%),radial-gradient(600px 420px at 10% 100%,rgba(76,195,138,.07),transparent 70%),#13213b}
.card{background:#fff;border-radius:12px;width:100%;max-width:380px;padding:30px 28px 24px;box-shadow:0 30px 70px rgba(0,0,0,.35)}
.brand{display:flex;align-items:center;gap:10px}
.brand svg{width:32px;height:32px;color:#c8a23a;flex:none}
.brand b{font:600 23px/1 'IBM Plex Serif',Georgia,serif;color:#13213b}
.co{font-size:11px;letter-spacing:.9px;text-transform:uppercase;color:#8a93a3;margin:6px 0 22px}
label{display:block;font-size:12px;font-weight:600;color:#5d6779;margin:0 0 5px}
input{width:100%;padding:9px 11px;border:1px solid #d9dde5;border-radius:7px;font-size:14px;outline:none;font-family:inherit}
input:focus{border-color:#13213b;box-shadow:0 0 0 3px rgba(200,162,58,.25)}
.f+.f{margin-top:14px}
.pw-wrap{position:relative}
.pw-wrap input{padding-right:42px}
.pw-toggle{position:absolute;right:4px;top:50%;transform:translateY(-50%);width:auto;margin:0;padding:6px;background:none;border:none;border-radius:6px;cursor:pointer;color:#8a93a3;display:flex}
.pw-toggle:hover{color:#18202f}
.pw-toggle:focus-visible{outline:2px solid #c8a23a;outline-offset:1px}
button.go{width:100%;margin-top:18px;padding:10px;background:#13213b;color:#fff;border:none;border-radius:7px;cursor:pointer;font-size:14px;font-weight:600;font-family:inherit}
button.go:hover{background:#1c2e4f}
.err{color:#b42318;font-size:13px;margin-top:12px;background:#fbe4e1;padding:8px 11px;border-radius:7px}
details{margin-top:14px}summary{cursor:pointer;color:#5d6779;font-size:12.5px}
details div{color:#8a93a3;font-size:12.5px;margin-top:6px;line-height:1.55}
.foot{text-align:center;color:#8d99b1;font-size:12px;margin-top:18px}.foot a{color:#cfd7e6}
</style></head><body>
<div class=card>
<div class=brand>""" + SCALE_ICON + """<b>ReconBook</b></div>
<div class=co>{{ company or 'Bank reconciliation for QuickBooks' }}</div>
<form method=post>
<div class=f><label for=un>Username</label><input id=un type=text name=username placeholder="Your username" autocapitalize=off autocomplete=username autofocus></div>
<div class=f><label for=pw>Password</label><div class=pw-wrap><input id=pw type=password name=password placeholder="Your password" autocomplete=current-password>
<button type=button class=pw-toggle onclick="togglePw(this,'pw')" aria-label="Show password" aria-pressed="false" title="Show or hide the password">""" + EYE_ICON + """</button></div></div>
<button type=submit class=go>Sign in</button>
{% if error %}<div class=err role=alert>{{ error }}</div>{% endif %}
</form>
<details><summary>Forgot password?</summary><div>Ask an admin to set a new one under Users &amp; permissions. The password originally set up for this app also works as a recovery key.</div></details>
</div>
<div class=foot><a href="{{ url_for('terms') }}">Terms</a> · <a href="{{ url_for('privacy') }}">Privacy</a> · <a href="mailto:{{ contact_email }}">Contact</a></div>
<script>""" + PW_TOGGLE_JS + """</script>
</body></html>"""


ADMIN_PERMS = ("users", "settings")


def can(perm):
    """Whether the signed-in user may do `perm`. Admins may do everything."""
    if session.get("is_admin"):
        return True
    perm = {"settings": "users"}.get(perm, perm)
    perms = session.get("perms")
    if perms is None:            # users from before permissions: everything but the admin pages
        return perm not in ADMIN_PERMS
    return perm in perms


CRUMBS = {"dashboard": "<b>Dashboard</b>", "users": "Manage / <b>Users &amp; permissions</b>",
          "settings": "Manage / <b>Settings</b>", "manage_accounts": "Settings / <b>Bank accounts shown</b>",
          "reports": "Manage / <b>Reports</b>", "search": "<b>Search</b>", "change_password": "<b>Change password</b>"}


def shell_data():
    """What the sidebar and top bar show: the company, each bank account with a status dot and the
    number of its bank lines still unmatched, the QuickBooks connection, and where you are."""
    ep = request.endpoint or ""
    name = (request.view_args or {}).get("name")
    who = (session.get("name") or session.get("username") or "?").strip()
    d = {"page": ep, "acct": name, "company": "", "accounts": [], "qbo": False, "synced": None,
         "initials": "".join(w[0] for w in who.split()[:2]).upper() or "?",
         "role": "Admin" if session.get("is_admin") else (session.get("title") or "User")}
    if ep in ("detail", "history"):
        d["crumb"] = (f'Bank accounts / <b>{escape(name)}</b>' if ep == "detail" else
                      f'Bank accounts / <a href="{url_for("detail", name=name)}">{escape(name)}</a> / <b>History</b>')
    else:
        d["crumb"] = CRUMBS.get(ep, "")
    try:
        d["company"] = get_config("company_name") or ""
        d["qbo"] = qbo_is_connected()
        age = _sync_age_secs(get_config("last_sync_at"))
        if age != float("inf"):
            d["synced"] = _ago(age) if age < 5400 else last_sync_label()
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""SELECT a.name, s.statement_id IS NOT NULL, s.signed_off_at IS NOT NULL,
                              (SELECT count(*) FROM statement_line sl WHERE sl.statement_id = s.statement_id AND sl.amount <> 0
                                 AND NOT EXISTS (SELECT 1 FROM match_statement_line msl JOIN match m ON m.match_id = msl.match_id
                                                 WHERE msl.line_id = sl.line_id AND m.status = 'confirmed'))
                       FROM account a
                       LEFT JOIN LATERAL (SELECT statement_id, signed_off_at FROM statement WHERE account_id = a.account_id
                                          ORDER BY created_at DESC LIMIT 1) s ON true
                       WHERE coalesce(a.is_active, true)
                       -- no reconciliation yet first, then work in progress, then signed off (most recent last)
                       ORDER BY (s.statement_id IS NOT NULL)::int + (s.signed_off_at IS NOT NULL)::int,
                                s.signed_off_at NULLS FIRST, a.name;""")
        for nm, has, signed, n in cur.fetchall():
            d["accounts"].append({"name": nm, "n": n if has and not signed else 0,
                                  "state": "none" if not has else "ok" if signed or not n else "attn"})
        cur.close(); conn.close()
    except Exception:
        pass   # the frame must never break the page
    return d


@app.context_processor
def _inject_contact():
    return {"contact_email": CONTACT_EMAIL, "sync_banner": sync_banner, "shell_data": shell_data, "can": can}


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
.legal-nav .dot{width:9px;height:9px;border-radius:50%;background:#c8a23a;box-shadow:0 0 0 3px #f6eed6}
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

PRIVACY_PAGE = """<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Privacy Policy · ReconBook</title>""" + LEGAL_STYLE + """</head><body>
<div class=legal-nav><span class=dot></span>ReconBook</div>
<div class=legal-wrap>
<h1>Privacy Policy</h1>
<div class=updated>Last updated: 20 July 2026</div>

<p>This Privacy Policy explains how ReconBook (“the app”, “we”) collects, uses, stores, and protects information when you use it to reconcile bank statements against your QuickBooks Online accounting records.</p>

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

<div class=legal-foot>ReconBook — a tool for reconciling bank statements with QuickBooks Online.</div>
</div></body></html>"""

TERMS_PAGE = """<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Terms of Service · ReconBook</title>""" + LEGAL_STYLE + """</head><body>
<div class=legal-nav><span class=dot></span>ReconBook</div>
<div class=legal-wrap>
<h1>Terms of Service</h1>
<div class=updated>Last updated: 20 July 2026</div>

<p>These Terms of Service govern your use of ReconBook (“the app”). By using the app, you agree to these terms.</p>

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

<div class=legal-foot>ReconBook — a tool for reconciling bank statements with QuickBooks Online.</div>
</div></body></html>"""


@app.route("/privacy")
def privacy():
    return render_template_string(PRIVACY_PAGE.replace("__EMAIL__", CONTACT_EMAIL))


@app.route("/terms")
def terms():
    return render_template_string(TERMS_PAGE.replace("__EMAIL__", CONTACT_EMAIL))


USERS_PAGE = """<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Users · ReconBook</title>""" + CSS + """</head><body>
""" + SHELL_TOP + """<div class=wrap>
<div class=ph><div><h1>Users &amp; permissions</h1><div class=meta>{{ users|length }} user{{ '' if users|length == 1 else 's' }} · tick what each person may do</div></div>
<div class=acts><button type=button class=btn data-drawer=adduser>Add user</button></div></div>
{% if msg %}<div id=flash role=status>{{ msg }}</div>{% elif error %}<div id=flash role=alert>{{ error }}</div>{% endif %}
<div class=panel style="margin-bottom:14px"><div class=tw><table class=perm>
<thead><tr><th>User</th><th class=p>View &amp; reports</th>{% for k, lab in perms %}<th class=p>{{ lab }}</th>{% endfor %}<th></th></tr></thead><tbody>
{% for u in users %}<tr{% if not u.active %} class=off{% endif %}>
<td><div class=who><span class="av{{ ' gold' if u.admin else '' }}">{{ u.initials }}</span><div><b>{{ u.name }}</b><span class=sub2>{{ u.username }}{% if u.title %} · {{ u.title }}{% endif %}{% if not u.active %} · <span class=bad>switched off</span>{% elif u.expires %} · access until {{ u.expires.strftime('%d %b %Y') }}{% endif %}{% if u.seen %} · seen {{ u.seen }}{% endif %}</span></div></div></td>
<td class=p><span class="tick lock" title="Everyone can view and print reports">&#10003;</span></td>
{% for k, lab in perms %}<td class=p>{% if u.admin %}<span class="tick lock gold" title="Admins may do everything">&#10003;</span>{% else %}<form method=post class=tf><input type=hidden name=action value=perm><input type=hidden name=username value="{{ u.username }}"><input type=hidden name=perm value="{{ k }}"><input type=hidden name=on value="{{ '0' if k in u.perms else '1' }}"><button type=submit class="tick{{ ' on' if k in u.perms else '' }}" aria-label="{{ lab }} for {{ u.name }}" aria-pressed="{{ 'true' if k in u.perms else 'false' }}" data-busy="Saving...">{% if k in u.perms %}&#10003;{% endif %}</button></form>{% endif %}</td>{% endfor %}
<td class=a>{% if u.admin %}<span class="pill gold">Admin</span> {% endif %}<span class=kebab><button type=button class=icon-btn data-dd aria-label="More for {{ u.name }}" aria-expanded=false>""" + DOTS_ICON + """</button><div class=dd hidden>
<a href="{{ url_for('users') }}?edit={{ u.username }}">Edit details or password</a>
{% if u.username != session.username %}<form method=post><input type=hidden name=action value=admin><input type=hidden name=username value="{{ u.username }}"><input type=hidden name=on value="{{ '0' if u.admin else '1' }}"><button type=submit>{{ 'Remove admin' if u.admin else 'Make admin' }}</button></form>
<form method=post><input type=hidden name=action value=active><input type=hidden name=username value="{{ u.username }}"><input type=hidden name=on value="{{ '0' if u.active else '1' }}"><button type=submit>{{ 'Switch off sign-in' if u.active else 'Switch sign-in back on' }}</button></form>
<div class=sep></div><form method=post data-confirm="Remove user {{ u.username }}? Their past sign-offs keep their name."><input type=hidden name=action value=delete><input type=hidden name=username value="{{ u.username }}"><button type=submit class=danger>Remove user</button></form>{% endif %}
</div></span></td></tr>
{% else %}<tr><td colspan=9 class=muted>No named users yet. Add one with the button above.</td></tr>{% endfor %}
</tbody></table></div>
<div class=rule><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3.5 5 6v5.5c0 4.3 2.9 7.7 7 9 4.1-1.3 7-4.7 7-9V6z"/><path d="m9 12 2.2 2.2L15.5 10"/></svg>
<div>{% if two_person %}<b>Sign-off needs a second person.</b> The person who prepared a reconciliation can't sign it off, unless they're an admin. {% endif %}Every sign-off records who prepared and who approved it, and both names print on the report. The recovery password from your server settings always works as an admin, so you can't be locked out.</div></div>
</div>
<div class=panel><div class=panel-h><h2>Recent activity</h2><span class="r faint">Last {{ activity|length }}</span></div>
<table><tbody>{% for a in activity %}<tr><td class="faint num" style="width:120px">{{ a.at }}</td><td style="white-space:normal"><b>{{ a.name or a.username or 'Someone' }}</b> {{ a.action }}{% if a.account %} · <a href="{{ url_for('detail', name=a.account) }}">{{ a.account }}</a>{% endif %}</td></tr>
{% else %}<tr><td class=muted>Nothing yet. Uploads, recordings, undos and sign-offs appear here.</td></tr>{% endfor %}</tbody></table></div>

<aside class=drawer id=dr-adduser {% if not open_add %}hidden{% endif %} aria-label="Add a user"><form method=post style="display:contents">
<div class=drawer-h><h2>Add a user</h2><button type=button class=icon-btn data-close style="margin-left:auto" aria-label="Close">&times;</button></div>
<div class=drawer-b><input type=hidden name=action value=save><input type=hidden name=new value=1>
<div class=two><div class=fld><label for=au-name>Full name</label><input id=au-name name=name placeholder="e.g. Grace Nabirye" required></div>
<div class=fld><label for=au-un>Username</label><input id=au-un name=username autocapitalize=off placeholder="grace.nabirye" required></div></div>
<div class=fld><label for=au-title>Job title</label><input id=au-title name=title placeholder="e.g. Accounts assistant"></div>
<div class=fld><label for=au-pw>Password</label><div class=pw-wrap><input id=au-pw name=password type=password placeholder="At least 6 characters" style="width:100%"><button type=button class=pw-toggle onclick="togglePw(this,'au-pw')" aria-label="Show password" aria-pressed="false">""" + EYE_ICON + """</button></div><small>Share it with them privately; they can change it after signing in.</small></div>
<div class=fld><label for=au-preset>Start from</label><select id=au-preset name=preset>{% for k, p in presets %}<option value="{{ k }}">{{ p[0] }}{% if p[1] %} ({{ p[1]|join(', ') }}){% else %} (view and reports){% endif %}</option>{% endfor %}</select><small>A starting set of ticks; change any of them afterwards.</small></div>
<div class=fld><label for=au-exp>Access until</label><input id=au-exp name=expires type=date><small>Leave empty for no end date. Useful for auditors.</small></div>
</div>
<div class=drawer-f><button type=button class=btn-sm data-close>Cancel</button><button type=submit class=btn>Add user</button></div></form></aside>
{% if edit_user %}<aside class=drawer id=dr-edituser aria-label="Edit user"><form method=post style="display:contents">
<div class=drawer-h><h2>{{ edit_user.name }}</h2><a href="{{ url_for('users') }}" class=icon-btn style="margin-left:auto" aria-label="Close">&times;</a></div>
<div class=drawer-b><input type=hidden name=action value=save><input type=hidden name=username value="{{ edit_user.username }}">
<div class=fld><label>Username</label><input value="{{ edit_user.username }}" readonly style="background:var(--bg);color:var(--muted)"></div>
<div class=fld><label for=eu-name>Full name</label><input id=eu-name name=name value="{{ edit_user.name }}"></div>
<div class=fld><label for=eu-title>Job title</label><input id=eu-title name=title value="{{ edit_user.title or '' }}"></div>
<div class=fld><label for=eu-pw>New password</label><div class=pw-wrap><input id=eu-pw name=password type=password placeholder="Leave empty to keep the current one" style="width:100%"><button type=button class=pw-toggle onclick="togglePw(this,'eu-pw')" aria-label="Show password" aria-pressed="false">""" + EYE_ICON + """</button></div></div>
<div class=fld><label for=eu-exp>Access until</label><input id=eu-exp name=expires type=date value="{{ edit_user.expires or '' }}"></div>
</div>
<div class=drawer-f><a href="{{ url_for('users') }}" class=btn-sm>Cancel</a><button type=submit class=btn>Save</button></div></form></aside>{% endif %}
<style>
.tw{overflow-x:auto}
.perm th.p{text-align:center;white-space:normal;min-width:82px;line-height:1.25;vertical-align:bottom}
.perm td.p{text-align:center}.perm tr.off td{opacity:.55}
.tf{margin:0;display:inline}
.tick{width:18px;height:18px;border-radius:4px;border:1.5px solid #c3c9d4;display:inline-grid;place-items:center;cursor:pointer;background:var(--panel);color:#fff;padding:0;font-size:11px;line-height:1;font-weight:700}
.tick.on{background:var(--navy);border-color:var(--navy)}
.tick.lock{background:var(--line-soft);border-color:var(--line);color:var(--muted);cursor:default}
.tick.lock.gold{background:var(--gold);border-color:var(--gold);color:var(--navy)}
.who{display:flex;gap:9px;align-items:center}
.who .av{width:26px;height:26px;border-radius:50%;display:grid;place-items:center;font-weight:700;font-size:11px;background:var(--accent-soft);color:var(--accent);flex:none}
.who .av.gold{background:var(--gold-soft);color:#7a5d0e}
.sub2{display:block;color:var(--faint);font-size:11.5px;white-space:normal}
.rule{display:flex;gap:10px;padding:10px 14px;border-top:1px solid var(--line-soft);color:var(--muted);font-size:12.5px;align-items:flex-start}
.rule svg{flex:none;color:var(--gold);margin-top:1px}.rule b{color:var(--ink)}
.drawer-f{margin-top:auto;padding:12px 18px;border-top:1px solid var(--line);display:flex;gap:8px;justify-content:flex-end}
</style>
<script>""" + PW_TOGGLE_JS + """</script>
</div>""" + SHELL_END + """</body></html>"""


ACCOUNTS_TEMPLATE = """<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Accounts · ReconBook</title>""" + CSS + """</head><body>
""" + SHELL_TOP + """<div class=wrap><h1>Accounts</h1>
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
</form></div>""" + SHELL_END + """</body></html>"""


SETTINGS_TEMPLATE = """<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Settings · ReconBook</title>""" + CSS + """</head><body>
""" + SHELL_TOP + """<div class=wrap>
<div class=ph><div><h1>Settings</h1><div class=meta>QuickBooks connection, matching rules, sign-off and backups</div></div></div>
{% if msg %}<div id=flash role=status>{{ msg }}</div>{% endif %}
{{ sync_banner() }}
<div class=setgrid>
<div class=panel>
<div class=panel-h><h2>QuickBooks Online</h2><span class=r>{% if qbo_connected %}<span class="pill ok">Connected</span>{% else %}<span class="pill bad">Not connected</span>{% endif %}</span></div>
<dl class=kv>
<dt>Company</dt><dd>{{ company or '—' }}</dd>
<dt>Home currency</dt><dd>{{ home or '—' }}</dd>
<dt>Last sync</dt><dd>{{ last_sync or 'Not synced yet' }}</dd>
<dt>Bank accounts shown</dt><dd>{{ n_active }} of {{ n_accts }} · <a href="{{ url_for('manage_accounts') }}" class=lnk>choose</a></dd>
</dl>
<div class=ptools>
{% if qbo_connected %}<form method=post action="{{ url_for('sync') }}"><input type=hidden name=to value=settings><button type=submit class="btn-sm pri">Sync now</button></form>
<form method=post action="{{ url_for('sync') }}" data-confirm="Full resync? It re-reads every transaction in the window, ignoring the last-sync marker. Slower, but use it if you think something was missed."><input type=hidden name=to value=settings><input type=hidden name=full value="1"><button type=submit class=btn-sm>Full resync</button></form>
<form method=post action="{{ url_for('check_connection') }}"><input type=hidden name=to value=settings><button type=submit class=btn-sm>Check connection</button></form>
<span style="flex:1"></span>
<form method=post action="{{ url_for('disconnect') }}" data-confirm="Disconnect QuickBooks? Syncing and recording stop until someone connects again. Nothing in QuickBooks is changed."><input type=hidden name=to value=settings><button type=submit class="btn-sm danger">Disconnect</button></form>
{% else %}<a href="{{ url_for('connect') }}" class=btn>Connect to QuickBooks</a>{% endif %}
</div>
<details class=adv><summary>Advanced: connect with a refresh token</summary>
<form method=post action="{{ url_for('set_token') }}" class=advf><input type=hidden name=to value=settings>
<div class=fld><label for=st-rt>Refresh token</label><input id=st-rt name=refresh_token></div>
<div class=fld><label for=st-realm>Realm / Company ID</label><input id=st-realm name=realm_id></div>
<button type=submit class=btn-sm>Save token</button></form>
<div class=hint style="margin-top:6px">For support use: a refresh token from the Intuit OAuth Playground connects the app without the redirect URI.</div></details>
</div>

<form method=post class=panel action="{{ url_for('settings') }}"><input type=hidden name=action value=rules>
<div class=panel-h><h2>Matching rules</h2><span class=r><button type=submit class="btn-sm pri">Save rules</button></span></div>
<label class=opt><span><b>Bank charges match on the exact date</b><small>Excise duty, ledger fees, commissions</small></span><input type=checkbox name=charges_exact value=1 {% if rules.charges_exact %}checked{% endif %}></label>
<label class=opt><span><b>Days a bank line and its entry may differ</b><small>Still counted as an exact match</small></span><input class=n name=date_days value="{{ rules.date_days }}" inputmode=numeric></label>
<label class=opt><span><b>Days a payment may clear late</b><small>Cheques and transfers booked before they reach the bank</small></span><input class=n name=clear_days value="{{ rules.clear_days }}" inputmode=numeric></label>
<label class=opt><span><b>Days apart for transfer suggestions</b><small>Same amount, opposite direction, another account</small></span><input class=n name=transfer_days value="{{ rules.transfer_days }}" inputmode=numeric></label>
<div class=hint style="padding:8px 14px">Open reconciliations are re-matched with the new rules the next time they're opened or refreshed.</div>
</form>

<form method=post class=panel action="{{ url_for('settings') }}"><input type=hidden name=action value=signoff>
<div class=panel-h><h2>Sign-off and month end</h2><span class=r><button type=submit class="btn-sm pri">Save</button></span></div>
<label class=opt><span><b>Sign-off needs a second person</b><small>The person who prepared it can't sign it off, unless they're an admin</small></span><input type=checkbox name=two_person value=1 {% if rules.two_person %}checked{% endif %}></label>
<label class=opt><span><b>Month-end close due on day</b><small>Of the following month, shown on the dashboard checklist</small></span><input class=n name=close_day value="{{ rules.close_day }}" inputmode=numeric></label>
</form>

<div class=panel>
<div class=panel-h><h2>Backup</h2></div>
<div style="padding:12px 14px" class=muted>A copy of every reconciliation, statement, match and user, as one file.</div>
<div class=ptools><a href="{{ url_for('backup') }}" class=btn-sm>Download a backup now</a></div>
</div>
</div>
<style>.setgrid{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:12px;align-items:start}
.kv{display:grid;grid-template-columns:150px minmax(0,1fr);gap:7px 14px;padding:12px 14px;margin:0}.kv dt{color:var(--muted)}.kv dd{margin:0;font-weight:500}
.lnk{color:var(--accent);font-weight:600}
.ptools{display:flex;gap:8px;align-items:center;flex-wrap:wrap;padding:10px 14px;border-top:1px solid var(--line-soft)}.ptools form{margin:0}
.opt{display:flex;gap:12px;align-items:center;padding:9px 14px;border-bottom:1px solid var(--line-soft);cursor:pointer}
.opt span{flex:1;min-width:0}.opt small{display:block;color:var(--faint)}
.opt input.n{width:60px;border:1px solid var(--line);border-radius:5px;padding:3px 6px;text-align:right;font-variant-numeric:tabular-nums}
.opt input[type=checkbox]{width:16px;height:16px;accent-color:var(--navy)}
details.adv{padding:8px 14px 12px;border-top:1px solid var(--line-soft)}details.adv summary{cursor:pointer;color:var(--muted);font-size:12.5px}
.advf{display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end;margin-top:8px}.advf .fld{flex:1;min-width:160px}
@media (max-width:1000px){.setgrid{grid-template-columns:minmax(0,1fr)}.kv{grid-template-columns:minmax(0,1fr)}}</style>
</div>""" + SHELL_END + """</body></html>"""


@app.route("/settings", methods=["GET", "POST"])
def settings():
    if not can("settings"):
        return "Admins only. <a href='/'>Back</a>", 403
    msg = session.pop("sync_msg", None)
    if request.method == "POST":
        keys = {"rules": ("charges_exact", "date_days", "clear_days", "transfer_days"),
                "signoff": ("two_person", "close_day")}.get(request.form.get("action"), ())
        for k in keys:
            default, lo, hi, _ = RULES[k]
            if hi == 1 and lo == 0:      # a tick box
                v = 1 if request.form.get(k) == "1" else 0
            else:
                try:
                    v = min(hi, max(lo, int((request.form.get(k) or "").strip())))
                except ValueError:
                    v = rule(k)
            set_config("rule_" + k, str(v))
        if keys:
            log_activity("changed the " + ("matching rules" if "date_days" in keys else "sign-off settings"))
            session["sync_msg"] = "Settings saved."
        return redirect(url_for("settings"))
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT count(*), count(*) FILTER (WHERE coalesce(is_active,true)) FROM account;")
    n_accts, n_active = cur.fetchone()
    try:
        home = qbo_home_currency(cur)
    except Exception:
        home = None
    cur.close(); conn.close()
    return render_template_string(SETTINGS_TEMPLATE, msg=msg, qbo_connected=qbo_is_connected(),
                                  company=get_config("company_name"), home=home, last_sync=last_sync_label(),
                                  n_accts=n_accts, n_active=n_active, rules={k: rule(k) for k in RULES})


REPORTS_TEMPLATE = """<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Reports · ReconBook</title>""" + CSS + """</head><body>
""" + SHELL_TOP + """<div class=wrap>
<div class=ph><div><h1>Reports</h1><div class=meta>Every reconciliation, newest period first. Open one to print or save it as a PDF.</div></div>
<div class=acts><form method=get class=btnrow><select name=status class=btn-sm onchange="this.form.submit()" aria-label="Show">
<option value="">All reconciliations</option><option value=signed {% if status=='signed' %}selected{% endif %}>Signed off</option><option value=open {% if status=='open' %}selected{% endif %}>In progress</option></select></form></div></div>
<div class=panel><table>
<thead><tr><th>Period</th><th>Account</th><th>Currency</th><th>Status</th><th>Prepared by</th><th>Approved by</th><th></th></tr></thead><tbody>
{% for r in rows %}<tr><td>{{ r.ps.strftime('%d %b') }} – {{ r.pe.strftime('%d %b %Y') }}</td><td><a href="{{ url_for('detail', name=r.name) }}"><b>{{ r.name }}</b></a></td><td>{{ r.ccy }}</td>
<td>{% if r.signed %}<span class="pill ok">Signed off {{ r.signed.strftime('%d %b %Y') }}</span>{% else %}<span class="pill attn">In progress</span>{% endif %}</td>
<td>{{ r.prep or '—' }}</td><td>{{ r.by or '—' }}</td>
<td class=a><a class=btn-sm href="{{ url_for('report', name=r.name, s=r.id) }}" target=_blank rel=noopener>Open report</a> <a class=btn-sm href="{{ url_for('history', name=r.name) }}">History</a></td></tr>
{% else %}<tr><td colspan=7 class=muted>No reconciliations yet. Upload a statement on a bank account to start.</td></tr>{% endfor %}
</tbody></table></div>
</div>""" + SHELL_END + """</body></html>"""


@app.route("/reports")
def reports():
    status = request.args.get("status") or ""
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""SELECT s.statement_id, a.name, a.currency, s.period_start, s.period_end, s.signed_off_at, s.signed_off_by,
                          s.prepared_by
                   FROM statement s JOIN account a ON a.account_id = s.account_id
                   WHERE coalesce(a.is_active, true)
                     AND (%s = '' OR (%s = 'signed') = (s.signed_off_at IS NOT NULL))
                   ORDER BY s.period_end DESC, a.name LIMIT 500;""", (status, status))
    rows = [{"id": str(i), "name": n, "ccy": (c or "").strip(), "ps": ps, "pe": pe, "signed": so, "by": by, "prep": pr}
            for i, n, c, ps, pe, so, by, pr in cur.fetchall()]
    cur.close(); conn.close()
    return render_template_string(REPORTS_TEMPLATE, rows=rows, status=status)


SEARCH_TEMPLATE = """<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Search · ReconBook</title>""" + CSS + """</head><body>
""" + SHELL_TOP + """<div class=wrap>
<div class=ph><div><h1>{% if q %}Results for “{{ q }}”{% else %}Search{% endif %}</h1><div class=meta>{% if amount is not none %}Amounts of {{ amount|money }} either way, and text containing “{{ q }}”{% elif q %}Bank lines and QuickBooks entries whose description or payee contains “{{ q }}”{% else %}Type an amount (e.g. 731,000), a payee or a reference in the box above.{% endif %}</div></div></div>
<form method=get class=btnrow style="margin:0 0 14px"><input name=q value="{{ q }}" class=sbig placeholder="Amount, payee or reference" aria-label="Search" autofocus><button type=submit class=btn>Search</button></form>
{% if q %}
<div class=panel style="margin-bottom:14px"><div class=panel-h><h2>On bank statements</h2><span class=r class=faint>{{ lines|length }}{% if lines|length >= 100 %}+{% endif %}</span></div>
<table><thead><tr><th>Date</th><th>Account</th><th>Bank description</th><th class=a>Amount</th><th>Status</th></tr></thead><tbody>
{% for r in lines %}<tr><td>{{ r.d }}</td><td><a href="{{ url_for('detail', name=r.acct) }}"><b>{{ r.acct }}</b></a></td><td class=desc>{{ r.who }}</td><td class=a>{{ r.amt|money }}</td>
<td>{% if r.matched %}<span class="pill ok">Matched</span>{% else %}<span class="pill attn">Not matched</span>{% endif %}</td></tr>
{% else %}<tr><td colspan=5 class=muted>Nothing on any statement.</td></tr>{% endfor %}</tbody></table></div>
<div class=panel><div class=panel-h><h2>In QuickBooks</h2><span class=r>{{ books|length }}{% if books|length >= 100 %}+{% endif %}</span></div>
<table><thead><tr><th>Date</th><th>Account</th><th>Type</th><th>Description</th><th class=a>Amount</th><th>Status</th></tr></thead><tbody>
{% for r in books %}<tr><td>{{ r.d }}</td><td><a href="{{ url_for('detail', name=r.acct) }}"><b>{{ r.acct }}</b></a></td><td>{{ r.typ }}{% if r.ref %} #{{ r.ref }}{% endif %}</td><td class=desc>{{ r.who }}</td><td class=a>{{ r.amt|money }}</td>
<td>{% if r.matched %}<span class="pill ok">Matched</span>{% else %}<span class="pill none">Not on a statement</span>{% endif %}</td></tr>
{% else %}<tr><td colspan=6 class=muted>Nothing in QuickBooks.</td></tr>{% endfor %}</tbody></table></div>
{% endif %}
<style>.sbig{width:min(420px,100%);padding:6px 10px;border:1px solid var(--line);border-radius:6px;font-size:13px}</style>
</div>""" + SHELL_END + """</body></html>"""


def _search_amount(q):
    t = q.replace(",", "").replace(" ", "").lstrip("+-")
    try:
        return Decimal(t) if re.fullmatch(r"\d+(\.\d{1,2})?", t) else None
    except Exception:
        return None


@app.route("/search")
def search():
    q = (request.args.get("q") or "").strip()[:80]
    amount = _search_amount(q) if q else None
    lines, books = [], []
    if q:
        like = "%" + q.replace("%", "").replace("_", "") + "%"
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""SELECT sl.posted_date, a.name, coalesce(sl.description, sl.counterparty, ''), sl.amount,
                              EXISTS (SELECT 1 FROM match_statement_line msl JOIN match m ON m.match_id = msl.match_id
                                      WHERE msl.line_id = sl.line_id AND m.status = 'confirmed')
                       FROM statement_line sl JOIN statement s ON s.statement_id = sl.statement_id
                       JOIN account a ON a.account_id = s.account_id
                       WHERE (abs(sl.amount) = %s OR coalesce(sl.description,'') || ' ' || coalesce(sl.counterparty,'') ILIKE %s)
                       ORDER BY sl.posted_date DESC LIMIT 100;""", (amount if amount is not None else -1, like))
        lines = [{"d": d, "acct": n, "who": w, "amt": a, "matched": m} for d, n, w, a, m in cur.fetchall()]
        cur.execute("""SELECT bt.posted_date, a.name, bt.source_txn_type, bt.reference,
                              coalesce(bt.counterparty, bt.description, ''), bt.amount,
                              EXISTS (SELECT 1 FROM match_book_txn mbt JOIN match m ON m.match_id = mbt.match_id
                                      WHERE mbt.txn_id = bt.txn_id AND m.status = 'confirmed')
                       FROM book_txn bt JOIN account a ON a.account_id = bt.account_id
                       WHERE NOT coalesce(bt.is_deleted, false) AND NOT coalesce(bt.is_void, false)
                         AND (abs(bt.amount) = %s OR coalesce(bt.description,'') || ' ' || coalesce(bt.counterparty,'')
                              || ' ' || coalesce(bt.reference,'') ILIKE %s)
                       ORDER BY bt.posted_date DESC LIMIT 100;""", (amount if amount is not None else -1, like))
        books = [{"d": d, "acct": n, "typ": t, "ref": r, "who": w, "amt": a, "matched": m}
                 for d, n, t, r, w, a, m in cur.fetchall()]
        cur.close(); conn.close()
    return render_template_string(SEARCH_TEMPLATE, q=q, amount=amount, lines=lines, books=books)


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
    if not can("users"):
        return "Admins only. <a href='/'>Back</a>", 403
    error = msg = None
    edit_user = None
    open_add = False
    if request.method == "POST":
        action = request.form.get("action")
        un = (request.form.get("username") or "").strip().lower()
        row = user_row(un) if un else None
        conn = get_conn(); cur = conn.cursor(); _ensure_users(cur)
        if action == "save":
            nm = (request.form.get("name") or "").strip()
            pw = request.form.get("password") or ""
            title = (request.form.get("title") or "").strip()[:60] or None
            try:
                exp = date.fromisoformat(request.form.get("expires")) if request.form.get("expires") else None
            except ValueError:
                exp = None
            if not un or not re.fullmatch(r"[a-z0-9._@-]{2,60}", un):
                error, open_add = "Choose a username of letters, numbers, dots or dashes.", True
            elif request.form.get("new") and row:
                error, open_add = f"There is already a user called {un}.", True
            elif row:
                update_user(un, nm or row[1], row[2], pw or None)
                cur.execute("UPDATE app_users SET title=%s, expires=%s WHERE username=%s;", (title, exp, un))
                msg = f"Saved {nm or un}."
                log_activity(f"changed the details of {nm or un}")
            elif len(pw) < 6:
                error, open_add = "New users need a password of at least 6 characters.", True
            else:
                preset = PERM_PRESETS.get(request.form.get("preset") or "", PERM_PRESETS["assistant"])
                add_user(un, nm, pw, request.form.get("preset") == "admin")
                cur.execute("UPDATE app_users SET perms=%s, title=%s, expires=%s, active=true WHERE username=%s;",
                            (",".join(preset[1]), title or (preset[0] if request.form.get("preset") != "admin" else None), exp, un))
                msg = f"Added {nm or un}. Share their username and password with them privately."
                log_activity(f"added the user {nm or un} ({preset[0]})")
        elif action == "perm" and row and not row[2]:
            k = request.form.get("perm")
            if k in dict(PERMS):
                cur_p = set(p for p in row[3].split(",") if p) if row[3] is not None else {p for p, _ in PERMS if p != "users"}
                (cur_p.add if request.form.get("on") == "1" else cur_p.discard)(k)
                cur.execute("UPDATE app_users SET perms=%s WHERE username=%s;", (",".join(p for p, _ in PERMS if p in cur_p), un))
                msg = f"Saved permissions for {row[1] or un}."
                log_activity(f"{'allowed' if request.form.get('on') == '1' else 'removed'} ‘{dict(PERMS)[k].lower()}’ for {row[1] or un}")
        elif action == "admin" and row and un != session.get("username"):
            on = request.form.get("on") == "1"
            cur.execute("UPDATE app_users SET is_admin=%s WHERE username=%s;", (on, un))
            msg = f"{row[1] or un} is {'now' if on else 'no longer'} an admin."
            log_activity(f"{'made' if on else 'removed'} {row[1] or un} {'an admin' if on else 'as admin'}")
        elif action == "active" and row and un != session.get("username"):
            on = request.form.get("on") == "1"
            cur.execute("UPDATE app_users SET active=%s WHERE username=%s;", (on, un))
            msg = f"{row[1] or un} can {'sign in again' if on else 'no longer sign in'}."
            log_activity(f"switched {'on' if on else 'off'} the sign-in of {row[1] or un}")
        elif action == "delete":
            if un == session.get("username"):
                error = "You can't remove the account you're signed in with."
            elif row:
                delete_user(un); msg = f"Removed {row[1] or un}."
                log_activity(f"removed the user {row[1] or un}")
        conn.commit(); cur.close(); conn.close()
        if msg and not error:
            session["sync_msg"] = msg
            return redirect(url_for("users"))
    else:
        msg = session.pop("sync_msg", None)
        eu = (request.args.get("edit") or "").strip().lower()
        row = user_row(eu) if eu else None
        if row:
            edit_user = {"username": row[0], "name": row[1] or row[0], "title": row[4], "expires": row[6]}
    now = datetime.now(timezone.utc)
    rows = []
    for un, nm, adm, created, perms, title, active, expires, seen in list_users():
        nm = nm or un
        ago = None
        if seen:
            secs = (now - seen).total_seconds()
            ago = "just now" if secs < 120 else f"{int(secs // 60)} min ago" if secs < 3600 else \
                  f"{int(secs // 3600)} h ago" if secs < 86400 else seen.astimezone(EAT).strftime("%d %b")
        rows.append({"username": un, "name": nm, "admin": bool(adm), "title": title, "active": active, "expires": expires,
                     "seen": ago, "initials": "".join(w[0] for w in nm.split()[:2]).upper(),
                     "perms": set(p for p in perms.split(",") if p) if perms is not None else {p for p, _ in PERMS if p != "users"}})
    activity = []
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS activity_log (id serial PRIMARY KEY, at timestamptz NOT NULL DEFAULT now(),
                       username text, name text, action text NOT NULL, account text);""")
        cur.execute("SELECT at, username, name, action, account FROM activity_log ORDER BY at DESC LIMIT 40;")
        activity = [{"at": a.astimezone(EAT).strftime("%d %b %H:%M"), "username": u, "name": n, "action": ac, "account": acc}
                    for a, u, n, ac, acc in cur.fetchall()]
        conn.commit(); cur.close(); conn.close()
    except Exception:
        pass
    return render_template_string(USERS_PAGE, users=rows, error=error, msg=msg, edit_user=edit_user, open_add=open_add,
                                  perms=PERMS, presets=list(PERM_PRESETS.items()), activity=activity,
                                  two_person=rule("two_person"))


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
        return render_template_string(LOGIN_PAGE, company=get_config("company_name"), error="That sign-in page had expired. Please try again."), 400
    return (f"This form had expired or didn't come from this app, so nothing was changed. "
            f"<a href='{url_for('dashboard')}'>Reload the app</a> and try again."), 400


@app.before_request
def require_login():
    if request.endpoint in ("login", "static", "health", "terms", "privacy"):
        return
    if not session.get("authed"):
        return redirect(url_for("login"))
    un = session.get("username")
    if un and un != "admin":
        row = user_row(un)
        if row is not None:
            if not row[5] or (row[6] and row[6] < date.today()):
                session.clear()
                return redirect(url_for("login"))
            _load_session_user(row)
            if time.time() - session.get("seen_at", 0) > 60:
                session["seen_at"] = time.time()
                try:
                    conn = get_conn(); cur = conn.cursor()
                    cur.execute("UPDATE app_users SET last_seen=now() WHERE username=%s;", (un,))
                    conn.commit(); cur.close(); conn.close()
                except Exception:
                    pass
    need = PERM_BY_ENDPOINT.get(request.endpoint)
    if need and not can(need):
        what = dict(PERMS).get({"settings": "users"}.get(need, need), need).lower()
        if request.method == "POST":
            name = (request.view_args or {}).get("name")
            session["detail_msg" if name else "sync_msg"] = f"Not done: your sign-in doesn't allow ‘{what}’. Ask an admin."
            return redirect(url_for("detail", name=name) if name else url_for("dashboard"))
        return render_template_string(DENIED_PAGE, what=what), 403


# Which tick each action needs (anything not listed: any signed-in user).
PERM_BY_ENDPOINT = {
    "upload": "upload", "import_books": "upload", "balances": "upload",
    "review_match": "review", "review_all": "review", "manual_match": "review", "unmatch": "review",
    "transfer_dismiss": "review", "transfer_restore": "review",
    "record": "record", "record_save": "record", "record_discard": "record", "record_transfer": "record",
    "transfer_change": "record", "record_reset": "record",
    "transfer_undo": "undo", "clear_account": "users", "delete_account": "users", "set_currency": "users",
    "signoff": "signoff", "reopen": "reopen",
    "users": "users", "settings": "users", "manage_accounts": "users", "backup": "users",
    "set_token": "users", "disconnect": "users", "check_connection": "users", "connect": "users",
    "schema_dump": "users", "qbo_info": "users"}

DENIED_PAGE = """<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Not allowed · ReconBook</title>""" + CSS + """</head><body>
""" + SHELL_TOP + """<div class=wrap><div class=ph><div><h1>Not allowed</h1><div class=meta>Your sign-in doesn't allow ‘{{ what }}’. An admin can change this under Users &amp; permissions.</div></div></div>
<a class=btn href="{{ url_for('dashboard') }}">Back to the dashboard</a></div>""" + SHELL_END + """</body></html>"""

CHANGE_PW_PAGE = """<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Change password · ReconBook</title>""" + CSS + """</head><body>
""" + SHELL_TOP + """<div class=wrap style="max-width:440px">
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
<script>""" + PW_TOGGLE_JS + """</script>""" + SHELL_END + """
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
            row = user_row(u[0])
            if row and (not row[5] or (row[6] and row[6] < date.today())):
                return render_template_string(LOGIN_PAGE, company=get_config("company_name"), error="This sign-in has been switched off or has expired. Ask an admin.")
            session["csrf"] = secrets.token_urlsafe(32)
            session["authed"] = True
            if row:
                _load_session_user(row)
            else:
                session["username"] = u[0]; session["name"] = u[1] or u[0]; session["is_admin"] = bool(u[3])
            return redirect(url_for("dashboard"))
        if check_password(password):
            session["csrf"] = secrets.token_urlsafe(32)
            session["authed"] = True; session["username"] = "admin"
            session["name"] = "Admin"; session["is_admin"] = True; session["perms"] = None; session["title"] = ""
            return redirect(url_for("dashboard"))
        return render_template_string(LOGIN_PAGE, company=get_config("company_name"), error="Incorrect username or password")
    return render_template_string(LOGIN_PAGE, company=get_config("company_name"), error=None)

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
        self.account_number = None     # as the statement prints it, when it does


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


def _pdf_join(desc, more):
    """A wrapped description line added to the text above it. A number split across lines
    ("~3,720" then ".00") is put back together."""
    if not desc:
        return more
    if more.startswith(".") or (desc.endswith(".") and desc[-2:-1].isdigit() and more[:1].isdigit()):
        return desc + more
    return desc + " " + more


def _signed(t):
    return bool(re.search(r"^\(|^-|-$|\)$|(CR|DR)$", t.strip(), re.I))


def _pdf_day_order(group, before):
    """The lines of one day in an order where every balance follows from the one before, or None.

    Some banks (DFCU) print a day's lines in a different order from the one they were posted in,
    so each balance only chains with the right predecessor. From a given balance, the next line
    must have balance == before + amount; lines that qualify share their amount and balance,
    so taking any of them is as good as another."""
    starts = [None] if before is not None else range(len(group))
    for first in starts:
        left, out, cur = list(group), [], before
        if first is not None:
            cur = left[first]["balance"]; out.append(left.pop(first))
        while left:
            k = next((i for i, r in enumerate(left) if r["balance"] == cur + r["amount"]), None)
            if k is None:
                break
            cur = left[k]["balance"]; out.append(left.pop(k))
        if not left:
            return out
    return None


def _pdf_breaks(seq, start):
    """(lines whose running balance doesn't add up, the lines in chained order).

    Lines on the same date may be in any order, as long as they chain exactly."""
    bad, out, before, i = [], [], start, 0
    while i < len(seq):
        j = i
        while j < len(seq) and seq[j]["date"] == seq[i]["date"]:
            j += 1
        group = seq[i:j]
        ordered = _pdf_day_order(group, before)
        if ordered is None:
            ordered = group
            for r in group:
                if before is not None and r["balance"] != before + r["amount"]:
                    bad.append(r)
                before = r["balance"]
        out.extend(ordered)
        before = ordered[-1]["balance"]
        i = j
    return bad, out


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
    desc_col = None      # where descriptions start, from the last line that had one
    for pl in page_lines:
        prev = None      # the transaction a wrapped description line belongs to (same page only)
        above = None     # the last text-only line: it may start the next line's description (DFCU)
        for line in pl:
            text = " ".join(w["text"] for w in line)
            h = _pdf_header(line) if not _pdf_date(line, dayfirst)[0] else None
            if h:
                cols, prev, above = h, None, None; continue
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
                prev = above = None; continue
            if not d:
                # A wrapped description: no numbers, lined up under the description, close below it.
                owner = None
                if (prev is not None and not money and not _PDF_SKIP.search(text)
                        and line[0]["x0"] >= prev["desc_x0"] - 4 and line[0]["top"] - prev["bottom"] < 14
                        and line[-1]["x1"] <= prev["money_x0"] + 2):
                    prev["desc"] = _pdf_join(prev["desc"], text)
                    prev["bottom"] = line[0]["bottom"]
                    owner = prev
                else:
                    prev = None
                above = ({"text": text, "x0": line[0]["x0"], "bottom": line[0]["bottom"], "owner": owner}
                         if not money and not _PDF_SKIP.search(text) else None)
                continue
            if not money:
                prev = above = None; continue
            if not desc and above and line[0]["top"] - above["bottom"] < 14 and (
                    desc_col is None or abs(above["x0"] - desc_col) <= 4):
                # The description starts on the line above the date (DFCU centres the date on a
                # wrapped description): it was taken as the end of the line before; it's this one's.
                o = above["owner"]
                if o is not None and o["desc"].endswith(above["text"]):
                    o["desc"] = o["desc"][:-len(above["text"])].rstrip()
                desc = above["text"]
            above = None
            if body:
                desc_col = body[0]["x0"]
            r = {"date": d, "desc": desc, "debit": None, "credit": None, "amount": None, "balance": None,
                 "known": False, "desc_x0": body[0]["x0"] if body else (desc_col or money[0]["x0"]),
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
        asc, desc_ = _pdf_breaks(raw, opening), _pdf_breaks(raw[::-1], opening)
        (bad, seq), is_asc = (asc, True) if len(asc[0]) <= len(desc_[0]) else (desc_, False)
        raw = seq if is_asc else seq[::-1]      # same-day lines in the order the balances chain
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
    rows.account_number = _statement_account_number(all_text)
    return rows


def _statement_account_number(text):
    """The account number a statement prints ("Account Number : 02183656112477", "A/C No. 9030 0123 4567")."""
    m = re.search(r"\b(?:account|a/c|acct)\.?\s*(?:number|no\.?|num|#)\s*[:.]?\s*(\d[\d -]{4,}\d)", text, re.I)
    return re.sub(r"\D", "", m.group(1)) if m else None


def _name_digits(name):
    m = re.search(r"(\d{4,})\s*$", name or "")
    return m.group(1) if m else None


def account_mismatch(name, number):
    """Why a statement for account `number` doesn't belong in account `name`, or None.

    Accounts carry no account number of their own, but their names end with its last digits
    ("DFCU USD 12477"). A statement is refused only when its number ends with another
    account's digits and not with this one's; a number nothing recognises is let through."""
    if not number:
        return None
    own = _name_digits(name)
    if own and number.endswith(own):
        return None
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT name FROM account;")
    others = [n for (n,) in cur.fetchall() if n != name and _name_digits(n) and number.endswith(_name_digits(n))]
    cur.close(); conn.close()
    if not others:
        return None
    return (f"This statement is for account number {number}, which looks like {others[0]}, not {name}. "
            f"Nothing was imported. Open {others[0]} and upload it there.")


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
                   opening_balance, closing_balance, opening_source, closing_source, currency, source_format, prepared_by)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING statement_id;""",
                (ORG_ID, acct_uuid, p_start, p_end, opening or 0, closing or 0, o_src, c_src, currency, source_format,
                 session.get("name") if has_request_context() else None))
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
    cur.execute("""SELECT line_id, posted_date, amount, coalesce(counterparty, description,''),
                          coalesce(description,'') || ' ' || coalesce(counterparty,'')
                   FROM statement_line WHERE statement_id=%s;""", (statement_id,))
    rows = cur.fetchall()
    charges = ({str(r[0]) for r in rows if is_bank_charge(r[4], r[2])}   # these pair on the exact date only
               if rule("charges_exact") else set())
    date_days, clear_days = rule("date_days"), rule("clear_days")
    lines = [r[:4] for r in rows]
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

    def tol(l_id):
        return 0 if l_id in charges else date_days

    # pass 1: exact (amount equal, date within tolerance). Take the closest date, not the
    # first hit, so two equal amounts a few days apart don't get cross-paired.
    for l_id, ld, la, lw in lines:
        best = None
        for t_id, td, ta, tw in txns:
            if t_id in used or la != ta or not ok("exact", [l_id], [t_id]):
                continue
            gap = abs((ld - td).days)
            if gap <= tol(l_id) and (best is None or gap < best[0]):
                best = (gap, t_id)
        if best:
            add([l_id], [best[1]], "exact", 1.0, 0); used.add(best[1]); matched_lines.add(l_id)

    # pass 1b: cleared later -- same amount, bank date on/after the book date but beyond the
    # tolerance (cheques presented late, items brought forward from last period). Confidence
    # below 1 puts these in the review list.
    for l_id, ld, la, lw in lines:
        if l_id in matched_lines or l_id in charges:
            continue
        best = None
        for t_id, td, ta, tw in txns:
            if t_id in used or la != ta or not ok("exact", [l_id], [t_id]):
                continue
            lag = (ld - td).days
            if date_days < lag <= clear_days and (best is None or lag < best[0]):
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
            if lw and tw and lw.strip().lower() == tw.strip().lower() and abs((ld - td).days) <= tol(l_id) \
                    and ok("fuzzy", [l_id], [t_id]):
                add([l_id], [t_id], "fuzzy", 0.6, la - ta); used.add(t_id); matched_lines.add(l_id); break

    # pass 3: many-to-one, BOTH directions (bounded; candidates sorted by date-closeness)
    # 3a forward: one statement line = sum of several book txns
    unmatched = [(l_id, ld, la) for (l_id, ld, la, lw) in lines if l_id not in matched_lines]
    # A parent's lump sum is usually booked as payments for their children: try each family first.
    cur.execute("""SELECT bt.txn_id::text, coalesce(c.parent_id, c.qbo_id) FROM book_txn bt
                   JOIN qbo_customer c ON bt.counterparty_ref = 'Customer:' || c.qbo_id
                   WHERE bt.txn_id = ANY(%s::uuid[]);""", ([str(t[0]) for t in txns],))
    family = dict(cur.fetchall())
    if len(unmatched) <= M2O_MAX_LINES and family:
        for l_id, ld, la in unmatched:
            if l_id in matched_lines:
                continue
            groups = {}
            for (t, d, a, w) in txns:
                if t not in used and str(t) in family and abs((ld - d).days) <= GROUP_WINDOW_DAYS:
                    groups.setdefault(family[str(t)], []).append((t, a))
            found = None
            for fam_txns in groups.values():
                if len(fam_txns) < 2 or len(fam_txns) > 12:
                    continue
                for k in range(len(fam_txns), 1, -1):
                    for combo in itertools.combinations(fam_txns, k):
                        if sum((c[1] for c in combo), Decimal(0)) == la and ok("many_to_one", [l_id], [c[0] for c in combo]):
                            found = combo; break
                    if found:
                        break
                if found:
                    break
            if found:
                tids = [c[0] for c in found]
                add([l_id], tids, "many_to_one", 0.85, 0)
                matched_lines.add(l_id); used.update(tids)
    if len(unmatched) <= M2O_MAX_LINES:
        for l_id, ld, la in unmatched:
            if l_id in matched_lines:
                continue
            win = 0 if l_id in charges else GROUP_WINDOW_DAYS
            cands = sorted([(t, a, d) for (t, d, a, w) in txns
                            if t not in used and abs((ld - d).days) <= win],
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
                            if l not in matched_lines and abs((td - d).days) <= (0 if l in charges else GROUP_WINDOW_DAYS)],
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
            if la == -ta and abs((ld - td).days) <= tol(l_id) and ok("manual", [l_id], [t_id]):
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


def account_summary(cur, acct_uuid, name, atype, currency=None, stmt=False):
    s = _latest_statement(cur, acct_uuid) if stmt is False else stmt
    if not s: return {"name": name, "type": atype, "status": "none", "currency": currency}
    sid, ps, pe, signed = s[:4]
    cur.execute("""SELECT count(*) FILTER (WHERE sl.amount <> 0),
                          count(*) FILTER (WHERE sl.amount <> 0 AND EXISTS (
                              SELECT 1 FROM match_statement_line msl JOIN match m ON m.match_id = msl.match_id
                              WHERE msl.line_id = sl.line_id AND m.status = 'confirmed'))
                   FROM statement_line sl WHERE sl.statement_id = %s;""", (sid,))
    n_lines, n_matched = cur.fetchone()
    cur.execute("SELECT prepared_by, signed_off_by FROM statement WHERE statement_id=%s;", (sid,))
    prep, by = cur.fetchone() or (None, None)
    cur.execute("SELECT match_type, count(*) FROM match WHERE statement_id=%s AND status='confirmed' GROUP BY match_type;", (sid,))
    mc = dict(cur.fetchall())
    rec = reconcile(cur, acct_uuid, s)
    exc = len(rec["un_lines"]) + len(rec["un_books"])
    return {"name": name, "type": atype, "currency": currency, "status": "signed" if signed else "open",
            "p_start": ps, "p_end": pe, "exact": mc.get("exact", 0), "fuzzy": mc.get("fuzzy", 0),
            "m2o": mc.get("many_to_one", 0), "exc": exc, "rec_status": rec["status"],
            "rec_diff": rec["rec_diff"], "missing": rec["missing"], "n_lines": n_lines, "n_matched": n_matched,
            "n_unmatched": n_lines - n_matched, "n_pending": rec["n_pending"], "prep": prep, "by": by,
            "signed_at": signed, "pct": int(n_matched * 100 / n_lines) if n_lines else 100}


DASH_TEMPLATE = """<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Dashboard · ReconBook</title>""" + CSS + """</head><body>
""" + SHELL_TOP + """<div class=wrap>
<div class=ph><div><h1>{{ month_label }} close</h1><div class=meta>{{ rows|length }} bank account{{ '' if rows|length == 1 else 's' }} · {{ n_signed }} signed off{% if n_hidden %} · {{ n_hidden }} hidden{% if can('settings') %} (<a href="{{ url_for('manage_accounts') }}" class=lnk>choose</a>){% endif %}{% endif %} · updated {{ now }}</div></div>
{% if months|length > 1 %}<div class=acts><form method=get><select name=m class=btn-sm onchange="this.form.submit()" aria-label="Month">{% for v, lab in months %}<option value="{{ v }}" {% if v == month %}selected{% endif %}>{{ lab }}</option>{% endfor %}</select></form></div>{% endif %}</div>
{% if sync_msg %}<div id=flash role=status>{{ sync_msg }}</div>{% endif %}
{{ sync_banner() }}
{% if not qbo_connected %}<div class=recnote style="background:var(--accent-soft);color:#24406b;margin:0 0 14px">QuickBooks isn't connected, so books can't refresh.{% if can('settings') %} <a href="{{ url_for('settings') }}" class=lnk>Connect it in Settings</a>.{% endif %}</div>{% endif %}
{% if totals %}<div class=ccys>{% for t in totals %}<div class="panel ccy"><div class=code>{{ t.ccy }}</div>
<span class=k>Unreconciled</span><span class=k>Open items</span><span class=k>Not matched</span>
<span class="v {{ 'bad' if t.diff else '' }}">{{ t.diff|money }}</span><span class=v>{{ t.exc }}</span><span class=v>{{ t.unmatched }}</span></div>{% endfor %}</div>{% endif %}
<div class=dash2>
<div class=panel><div class=panel-h><h2>Needs your attention</h2>{% if attention %}<span class="pill attn">{{ attention|length }}</span>{% endif %}</div>
<ul class=attn>{% for a in attention %}<li><span class="sev {{ a.sev }}"></span><div><b>{{ a.name }}</b> {{ a.what }}{% if a.sub %}<span class=sub2>{{ a.sub }}</span>{% endif %}</div>
<a class=btn-sm href="{{ url_for('detail', name=a.name) }}{{ a.hash }}" data-busy="Loading {{ a.name }}...">{{ a.btn }}</a></li>
{% else %}<li class=calm><span class="sev ok"></span><div><b>All clear.</b> Every account for {{ month_label }} is reconciled and signed off.</div></li>{% endfor %}</ul></div>
<div class=panel><div class=panel-h><h2>Month-end checklist</h2><span class="r faint">{{ n_signed }} of {{ rows|length }} done</span></div>
<ul class=check>{% for r in rows %}<li><span class="bx {{ 'done' if r.status=='signed' else 'half' if r.status!='none' else '' }}">{% if r.status=='signed' %}&#10003;{% endif %}</span><a href="{{ url_for('detail', name=r.name) }}" data-busy="Loading {{ r.name }}...">{{ r.name }}</a>
<span class=who>{% if r.status=='signed' %}Signed off{% if r.by %} by {{ r.by }}{% endif %}<br>{{ r.signed_at.astimezone(eat).strftime('%d %b') }}{% elif r.status=='none' %}Not started{% else %}Prepared {{ r.pct }}%{% if r.prep %}<br>by {{ r.prep }}{% endif %}{% endif %}</span></li>{% endfor %}</ul>
<div class="due{{ ' late' if overdue else '' }}">{{ 'Overdue: was due' if overdue else 'Due' }} {{ due.strftime('%d %b %Y') }}<div class="bar{{ '' if n_signed == rows|length else ' attn' }}"><i style="width:{{ (n_signed * 100 / rows|length)|int if rows else 0 }}%"></i></div>{{ (n_signed * 100 / rows|length)|int if rows else 0 }}%</div></div>
</div>
<div class=panel><div class=panel-h><h2>Bank accounts</h2>{% if can('settings') %}<span class=r><a class=btn-sm href="{{ url_for('manage_accounts') }}">Choose accounts</a></span>{% endif %}</div>
<div class=tw><table>
<thead><tr><th>Account</th><th>Currency</th><th>Statement period</th><th style="min-width:170px">Progress</th><th>Status</th><th class=a>Difference</th><th>Prepared / approved</th><th></th></tr></thead>
<tbody>{% for r in rows %}<tr data-status="{{ r.status }}">
<td><a href="{{ url_for('detail', name=r.name) }}" data-busy="Loading {{ r.name }}: its statement, matches and lines to record..."><b>{{ r.name }}</b></a></td>
<td>{{ r.currency or '—' }}</td>
{% if r.status=='none' %}<td class=faint>{{ 'No ' ~ month_label ~ ' statement' }}</td><td></td><td><span class="pill none">Not started</span></td><td class="a faint">—</td><td class=faint>—</td>
<td class=a><a class=btn-sm href="{{ url_for('detail', name=r.name) }}?upload=1" data-busy="Loading {{ r.name }}...">Upload</a></td>
{% else %}<td>{{ r.p_start.strftime('%d %b') }} – {{ r.p_end.strftime('%d %b %Y') }}</td>
<td><div class=prog><div class="bar{{ '' if r.pct == 100 else ' attn' }}"><i style="width:{{ r.pct }}%"></i></div><span class="faint num">{{ r.n_matched }}/{{ r.n_lines }}</span></div></td>
<td>{% if r.status=='signed' %}<span class="pill ok">Signed off</span>{% elif r.rec_status=='balanced' %}<span class="pill info">Balanced</span>{% elif r.rec_status=='out' %}<span class="pill bad">Out of balance</span>{% else %}<span class="pill attn">In progress</span>{% endif %}</td>
<td class=a>{% if r.rec_status=='balanced' %}0.00{% elif r.rec_status=='out' %}<span class=bad>{{ r.rec_diff|money }}</span>{% else %}<span class=faint>needs {{ r.missing }}</span>{% endif %}</td>
<td class=muted>{{ r.prep or '—' }}{% if r.by %} / {{ r.by }}{% endif %}</td>
<td class=a><a class=btn-sm href="{{ url_for('detail', name=r.name) }}" data-busy="Loading {{ r.name }}...">{{ 'Open' if r.status=='signed' else 'Continue' }}</a></td>{% endif %}</tr>
{% else %}<tr><td colspan=8 class=muted>No bank accounts yet.{% if can('settings') %} Connect QuickBooks in <a href="{{ url_for('settings') }}" class=lnk>Settings</a>; its bank and card accounts appear here.{% endif %}</td></tr>{% endfor %}</tbody></table></div></div>
<style>.lnk{color:var(--accent);font-weight:600}
.ccys{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:12px;margin:0 0 12px}
.ccy{padding:11px 14px;display:grid;grid-template-columns:auto repeat(3,minmax(0,1fr));gap:3px 18px;align-items:center}
.ccy .code{font:600 21px/1 var(--f-num);color:var(--navy);grid-row:span 2;padding-right:12px;border-right:1px solid var(--line-soft)}
.ccy .k{font-size:10.5px;color:var(--faint);text-transform:uppercase;letter-spacing:.06em;font-weight:600}
.ccy .v{font:500 16px/1.2 var(--f-num);font-variant-numeric:tabular-nums}.ccy .v.bad{color:var(--bad)}
.dash2{display:grid;grid-template-columns:minmax(0,1.7fr) minmax(0,1fr);gap:12px;align-items:start;margin:0 0 12px}
ul.attn{list-style:none;margin:0;padding:0}
ul.attn li{display:grid;grid-template-columns:4px minmax(0,1fr) auto;gap:12px;align-items:center;padding:8px 14px 8px 0;border-bottom:1px solid var(--line-soft)}
ul.attn li:last-child{border-bottom:0}
.sev{align-self:stretch;border-radius:0 3px 3px 0;min-height:30px}.sev.bad{background:var(--bad)}.sev.attn{background:#e08a3e}.sev.info{background:#7d9bc9}.sev.gold{background:var(--gold)}.sev.ok{background:#4cb07e}
.sub2{display:block;color:var(--faint);font-size:11.5px}
ul.check{list-style:none;margin:0;padding:6px 0}
ul.check li{display:flex;gap:10px;align-items:center;padding:5px 14px}
.bx{width:16px;height:16px;border-radius:4px;border:1.5px solid #c3c9d4;display:grid;place-items:center;flex:none;font-size:10px;color:#fff;font-weight:700}
.bx.done{background:var(--ok);border-color:var(--ok)}.bx.half{border-color:#e08a3e;background:var(--warn-soft)}
.check .who{margin-left:auto;font-size:11.5px;color:var(--faint);text-align:right;line-height:1.3}
.due{display:flex;align-items:center;gap:8px;padding:8px 14px;border-top:1px solid var(--line-soft);font-size:12px;color:var(--muted)}.due .bar{flex:1}
.due.late{color:var(--bad);font-weight:600}
.prog{display:flex;gap:8px;align-items:center}.prog .bar{flex:1}
.tw{overflow-x:auto}
@media (max-width:1000px){.dash2{grid-template-columns:minmax(0,1fr)}}
@media (max-width:600px){.ccy{grid-template-columns:repeat(3,minmax(0,1fr))}.ccy .code{grid-row:auto;grid-column:1/-1;border:0}}</style>
</div>""" + SHELL_END + """</body></html>"""


@app.route("/")
def dashboard():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id, name, type, currency FROM account WHERE coalesce(is_active,true) ORDER BY type, name;")
    accts = cur.fetchall()
    # The month being closed: the one asked for, else the latest statement period end.
    cur.execute("""SELECT DISTINCT date_trunc('month', s.period_end)::date FROM statement s JOIN account a USING (account_id)
                   WHERE coalesce(a.is_active,true) ORDER BY 1 DESC LIMIT 18;""")
    months = [r[0] for r in cur.fetchall()]
    try:
        month = date.fromisoformat((request.args.get("m") or "") + "-01") if request.args.get("m") else None
    except ValueError:
        month = None
    if month not in months:
        month = months[0] if months else date.today().replace(day=1)
    rows = []
    for a, n, t, ccy in accts:
        cur.execute(f"""SELECT {STMT_COLS} FROM statement WHERE account_id=%s AND date_trunc('month', period_end)::date=%s
                        ORDER BY created_at DESC LIMIT 1;""", (a, month))
        rows.append(account_summary(cur, a, n, t, (ccy or "").strip() or None, cur.fetchone()))
    # No statement yet first, then in progress, then signed off with the most recent sign-off last.
    rows.sort(key=lambda r: ({"none": 0, "open": 1, "signed": 2}[r["status"]],
                             r["signed_at"].timestamp() if r.get("signed_at") else 0, r["name"].lower()))
    cur.execute("SELECT count(*) FROM account WHERE NOT coalesce(is_active,true);")
    n_hidden = cur.fetchone()[0]
    cur.close(); conn.close()
    label = month.strftime("%B %Y")
    attention = []
    for r in rows:
        if r["status"] == "none":
            attention.append({"sev": "info", "name": r["name"], "what": f"has no {label} statement yet", "hash": "?upload=1",
                              "btn": "Upload", "rank": 4})
            continue
        if r["status"] == "signed":
            continue
        if r["rec_status"] == "out":
            attention.append({"sev": "bad", "name": r["name"], "what": f"is out of balance by {r['currency'] or ''} {_money(r['rec_diff'])}",
                              "sub": "Recording the lines not in QuickBooks usually closes the gap", "hash": "#sec-balance",
                              "btn": "Continue", "rank": 0})
        if r["n_pending"]:
            attention.append({"sev": "attn", "name": r["name"], "what": f"has {r['n_pending']} suggested match"
                              f"{'' if r['n_pending'] == 1 else 'es'} to review", "sub": "Sign-off waits for these",
                              "hash": "#sec-review", "btn": "Review", "rank": 1})
        if r["n_unmatched"]:
            attention.append({"sev": "attn", "name": r["name"], "what": f"has {r['n_unmatched']} bank line"
                              f"{'' if r['n_unmatched'] == 1 else 's'} not matched", "hash": "#sec-record",
                              "btn": "Record", "rank": 2})
        if r["rec_status"] == "incomplete":
            attention.append({"sev": "attn", "name": r["name"], "what": f"needs its {r['missing']}", "hash": "#sec-balance",
                              "btn": "Enter", "rank": 3})
        if r["rec_status"] == "balanced" and not r["n_pending"]:
            attention.append({"sev": "gold", "name": r["name"], "what": "is balanced and ready to sign off", "hash": "",
                              "btn": "Sign off", "rank": 1})
    attention.sort(key=lambda x: x["rank"])
    totals = {}
    for r in rows:
        if r["status"] == "none" or r["status"] == "signed":
            continue
        t = totals.setdefault(r["currency"] or "—", {"ccy": r["currency"] or "—", "diff": Decimal(0), "exc": 0, "unmatched": 0})
        if r["rec_status"] == "out" and r["rec_diff"]:
            t["diff"] += abs(r["rec_diff"])
        t["exc"] += r.get("exc", 0); t["unmatched"] += r.get("n_unmatched", 0)
    nxt = (month.replace(day=28) + timedelta(days=4)).replace(day=1)
    due = nxt.replace(day=min(rule("close_day"), 28))
    n_signed = sum(1 for r in rows if r["status"] == "signed")
    sync_msg = session.pop("sync_msg", None)
    return render_template_string(DASH_TEMPLATE, qbo_connected=qbo_is_connected(), rows=rows, n_signed=n_signed,
                                  n_hidden=n_hidden, attention=attention, totals=sorted(totals.values(), key=lambda t: t["ccy"]),
                                  month=month.strftime("%Y-%m"), month_label=label, due=due, eat=EAT,
                                  overdue=date.today() > due and n_signed < len(rows),
                                  months=[(m.strftime("%Y-%m"), m.strftime("%B %Y")) for m in months],
                                  sync_msg=sync_msg, now=datetime.now(EAT).strftime("%d %b, %H:%M"))


DETAIL_TEMPLATE = """<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>{{ name }} · ReconBook</title>""" + CSS + """</head><body>
""" + SHELL_TOP + """<style>
.secnav{position:sticky;top:var(--navh,53px);z-index:4;display:flex;gap:6px;overflow-x:auto;scrollbar-width:none;padding:8px max(24px,calc(50% - 476px));background:rgba(255,255,255,.82);backdrop-filter:saturate(180%) blur(12px);-webkit-backdrop-filter:saturate(180%) blur(12px);border-bottom:1px solid var(--line)}
.secnav[hidden]{display:none}
.secnav::-webkit-scrollbar{display:none}
.secnav a{flex:none;white-space:nowrap;font-size:13px;color:var(--muted);padding:4px 11px;border:1px solid var(--line);border-radius:999px;background:var(--panel);transition:background .15s,color .15s,border-color .15s}
.secnav a:hover{color:var(--ink)}
.secnav a .n{opacity:.75;font-variant-numeric:tabular-nums}
.secnav a.on{background:var(--accent-soft);color:var(--accent);border-color:transparent;font-weight:600}
.secnav a.attn::before{content:'';display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--warn);margin:0 6px 1px 0;vertical-align:middle}
@media (max-width:760px){.secnav{padding:7px 15px}}
@media (min-width:99999px){
  .secnav{flex-direction:column}
  .secnav a{border:0;background:none;border-radius:calc(var(--radius) - 6px);padding:5px 9px;white-space:normal;line-height:1.35}
  .secnav a.on{background:var(--accent-soft)}
}
@media print{.secnav{display:none}}
</style>
<div class=pagecols><nav id=secnav class=secnav aria-label="Page sections" hidden></nav>
<div class=wrap>
<div class=ph><div><h1>{{ name }}</h1>
<div class=meta>{% if not has_results %}<span class="pill none">No statement yet</span>{% elif signed_off %}<span class="pill ok">Signed off</span>{% elif rec.status=='balanced' %}<span class="pill info">Balanced</span>{% elif rec.status=='out' %}<span class="pill bad">Out of balance</span>{% else %}<span class="pill attn">In progress</span>{% endif %}
{% if ccy %}<span>{{ ccy }}</span>{% endif %}{% if atype=='credit_card' %}<span class=faint>·</span><span>Credit card</span>{% endif %}
{% if has_results %}<span class=faint>·</span><span>Statement {{ p_start }} to {{ p_end }}</span>{% endif %}
{% if prep %}<span class=faint>·</span><span>Prepared by {{ prep }}</span>{% endif %}{% if signed_by %}<span class=faint>·</span><span>Approved by {{ signed_by }}{% if signed_off %} on {{ signed_off }}{% endif %}</span>{% endif %}</div></div>
<div class=acts>
{% if can('upload') %}<button type=button class=btn-sm data-drawer=upload><svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 16V4"/><path d="m7 9 5-5 5 5"/><path d="M4 16v4h16v-4"/></svg>Upload statement</button>{% endif %}
<a href="{{ url_for('history', name=name) }}" class=btn-sm>History</a>
{% if has_results %}<a href="{{ url_for('report', name=name) }}" class=btn-sm target=_blank rel=noopener title="Print reconciliation report">Report</a>{% endif %}
<span class=kebab><button type=button class=icon-btn data-dd aria-label="More for this account" aria-expanded=false>""" + DOTS_ICON + """</button><div class=dd hidden>
{% if has_results %}<a href="{{ url_for('exceptions_csv', name=name) }}">Download exceptions (CSV)</a><a href="{{ url_for('qbo_import_csv', name=name) }}">Download for QuickBooks (CSV)</a><div class=sep></div>{% endif %}
{% if qbo_connected and qbo_linked %}<form method=post action="{{ url_for('sync') }}"><input type=hidden name=back value="{{ name }}"><button type=submit>Refresh books from QuickBooks</button></form>{% endif %}
{% if can('upload') %}<button type=button data-drawer=books>Import books from a CSV</button>{% endif %}
{% if can('settings') %}<div class=sep></div><div class=dh>Admin</div>
<form method=post action="{{ url_for('set_currency', name=name) }}" class=ccyf><label for=ccy-in>Currency</label><input id=ccy-in name=currency value="{{ ccy or '' }}" maxlength=8 placeholder="UGX"><button type=submit class=btn-sm>Set</button></form>
<form method=post action="{{ url_for('clear_account', name=name) }}" data-confirm="Clear this account's data? All statements and book transactions for it are removed here, to start fresh. QuickBooks is not changed. This cannot be undone."><button type=submit class=danger>Clear this account's data</button></form>
<form method=post action="{{ url_for('delete_account', name=name) }}" data-confirm="Delete this account entirely? It and all its statements and transactions are removed here (for old sandbox accounts). QuickBooks is not changed. This cannot be undone."><button type=submit class=danger>Delete account</button></form>{% endif %}
</div></span>
</div></div>
{% if atype=='credit_card' %}<div class=help>Credit card: charges are positive and payments or refunds negative, as in your QuickBooks card register.</div>{% endif %}
{{ sync_banner() }}
{% if detail_msg %}<div id=flash role=status>{{ detail_msg }}</div>{% endif %}
{% if not has_results %}<div class="panel empty"><svg width="34" height="34" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M6 3.5h8l4 4v13H6z"/><path d="M14 3.5v4h4"/><path d="M12 11v6"/><path d="m9.5 13.5 2.5-2.5 2.5 2.5"/></svg>
<div><b>No statement yet.</b> Upload this account's bank statement (PDF, CSV or OFX) to start the reconciliation.</div>
{% if can('upload') %}<button type=button class=btn data-drawer=upload>Upload statement</button>{% endif %}</div>{% endif %}

<aside class=drawer id=dr-upload {% if not open_upload %}hidden{% endif %} aria-label="Upload a statement"><form action="{{ url_for('upload', name=name) }}" method=post enctype=multipart/form-data style="display:contents">
<div class=drawer-h><h2>Upload a statement</h2><button type=button class=icon-btn data-close style="margin-left:auto" aria-label="Close">&times;</button></div>
<div class=drawer-b>
<label class=drop for=up-file><b>Choose the bank statement</b><span>PDF from online banking, CSV or OFX · <a href="{{ url_for('template', kind='bank') }}">CSV template</a></span><input id=up-file type=file name=statement accept=.pdf,.csv,.ofx required></label>
<div class=two>
<div class=fld><label for=up-ps>Period start</label><input id=up-ps type=date name=period_start></div>
<div class=fld><label for=up-pe>Statement date (period end)</label><input id=up-pe type=date name=period_end></div>
<div class=fld><label for=up-ob>Opening balance</label><input id=up-ob name=opening_balance inputmode=decimal placeholder="read from the statement"><small>Empty: the last signed-off closing</small></div>
<div class=fld><label for=up-cb>Closing balance</label><input id=up-cb name=closing_balance inputmode=decimal placeholder="read from the statement"></div>
</div>
<div class=fld><label for=up-pw>PDF password</label><input id=up-pw type=password name=pdf_password autocomplete=off placeholder="only for protected PDFs"{% if request.args.get('pdfpw') %} autofocus style="border-color:var(--warn)"{% endif %}><small>Used once to open the file; never stored.</small></div>
<details class=how><summary>How this works</summary><div>Balances can stay empty when the file has a running-balance column (PDF, CSV) or a ledger balance (OFX): they're read automatically. A PDF must be the one downloaded from online banking, not a scan. Set the statement date as the period end: without it the period ends on the last transaction, and later book items won't show as outstanding.</div></details>
<div class=help style="margin:0">{% if qbo_connected and qbo_linked %}Books refresh from QuickBooks automatically when you upload{% if last_sync %} (last synced {{ last_sync }}){% endif %}.{% else %}{% if not qbo_connected %}QuickBooks isn't connected, so import the books as a CSV (⋯ menu).{% else %}This account isn't linked to a QuickBooks account: import the books as a CSV (⋯ menu).{% endif %}{% endif %}</div>
</div>
<div class=drawer-f><button type=button class=btn-sm data-close>Cancel</button><button type=submit class=btn>Upload &amp; reconcile</button></div></form></aside>
<aside class=drawer id=dr-books hidden aria-label="Import books from a CSV"><form action="{{ url_for('import_books', name=name) }}" method=post enctype=multipart/form-data style="display:contents">
<div class=drawer-h><h2>Import books from a CSV</h2><button type=button class=icon-btn data-close style="margin-left:auto" aria-label="Close">&times;</button></div>
<div class=drawer-b><div class=help style="margin:0">For working offline: a QuickBooks register exported as CSV. When QuickBooks is connected the books are read directly, with no export needed.</div>
<label class=drop for=bk-file><b>Choose the QuickBooks CSV export</b><span><a href="{{ url_for('template', kind='books') }}">CSV template</a></span><input id=bk-file type=file name=books accept=.csv required></label></div>
<div class=drawer-f><button type=button class=btn-sm data-close>Cancel</button><button type=submit class=btn>Import books</button></div></form></aside>
<style>
.sumstrip{display:grid;grid-template-columns:minmax(0,1.4fr) repeat(4,minmax(0,.6fr)) minmax(0,1.1fr);background:var(--panel);border:1px solid var(--line);border-radius:var(--radius);margin:0 0 6px}
.sumstrip .tile{border-right:1px solid var(--line-soft);display:flex;flex-direction:column;gap:3px;justify-content:center;padding:10px 14px;border-radius:0}
.sumstrip .tile:first-child{border-radius:var(--radius) 0 0 var(--radius)}
.sumstrip .t-val small{font:400 12px var(--f-ui);color:var(--faint)}
.sumstrip .t-val.bad{color:var(--bad)}
.sumstrip .prog .bar{height:7px;display:block;margin-top:3px}
.sumstrip .so{display:flex;flex-direction:column;align-items:flex-start;justify-content:center;gap:4px;padding:10px 14px}
.sumstrip .so form{margin:0}.sumstrip .so small{color:var(--faint);font-size:11.5px;line-height:1.3}
.panel.empty{display:flex;gap:14px;align-items:center;padding:18px;margin:0 0 14px;flex-wrap:wrap}.panel.empty svg{color:var(--faint);flex:none}.panel.empty div{flex:1;min-width:200px}
.drop{display:flex;flex-direction:column;gap:4px;border:1.5px dashed #c3c9d4;border-radius:8px;padding:16px;text-align:center;color:var(--muted);background:#fbfbfc;cursor:pointer;align-items:center}
.drop b{color:var(--ink)}.drop a{color:var(--accent)}.drop input{font-size:12.5px;margin-top:6px;max-width:100%}
.ccyf{display:flex;gap:6px;align-items:center;padding:4px 10px}.ccyf label{font-size:12px;color:var(--muted)}.ccyf input{width:64px;padding:3px 6px;border:1px solid var(--line);border-radius:5px;text-transform:uppercase;font-size:12.5px}
.ccyf .btn-sm{width:auto;display:inline-flex;padding:3px 9px}
@media (max-width:1100px){.sumstrip{grid-template-columns:repeat(3,minmax(0,1fr))}.sumstrip .tile,.sumstrip .so{border-bottom:1px solid var(--line-soft)}}
@media (max-width:600px){.sumstrip{grid-template-columns:repeat(2,minmax(0,1fr))}}
</style>
{% if has_results %}
<div class=sumstrip id=dtiles>
<button type=button class="tile prog" data-target="sec-matched"><span class=t-label>Matched</span><span class=t-val>{{ n_matched_lines }} <small>of {{ n_lines }} lines</small></span><span class="bar{{ '' if n_matched_lines == n_lines else ' attn' }}"><i style="width:{{ (n_matched_lines * 100 / n_lines)|int if n_lines else 100 }}%"></i></span></button>
<button type=button class=tile data-target="sec-review" data-fallback="sec-matched"><span class=t-label>To review</span><span class="t-val {{ 'warn' if n_pending else '' }}">{{ n_pending }}</span></button>
<button type=button class=tile data-target="sec-record" data-fallback="sec-exceptions"><span class=t-label>To record</span><span class="t-val {{ 'warn' if n_to_record else '' }}">{{ n_to_record }}</span></button>
<button type=button class=tile data-target="sec-transfers" data-fallback="sec-record"><span class=t-label>Transfers</span><span class="t-val {{ 'warn' if n_xfer else '' }}">{{ n_xfer }}</span></button>
<button type=button class=tile data-target="sec-balance"><span class=t-label>Difference</span><span class="t-val {{ 'bad' if rec.status=='out' else '' }}">{% if rec.rec_diff is none %}—{% else %}{{ rec.rec_diff|money }}{% endif %}</span></button>
<div class=so>{% if signed_off %}<span class="pill ok">Signed off {{ signed_off }}</span>{% if can('reopen') %}<form method=post action="{{ url_for('reopen', name=name) }}" data-confirm="Reopen this reconciliation? You can sign it off again afterwards."><button type=submit class=btn-sm>Undo sign-off</button></form>{% endif %}
{% elif rec.status=='balanced' and not n_pending and can('signoff') and not self_prepared %}<form method=post action="{{ url_for('signoff', name=name) }}" data-confirm="Sign off this reconciliation? It is locked as reconciled for {{ p_start }} to {{ p_end }}."><button type=submit class=btn-go>Sign off</button></form><small>Balanced and reviewed</small>
{% else %}<button type=button class=btn-go disabled title="{{ signoff_why }}">Sign off</button><small>{{ signoff_why }}</small>{% endif %}</div>
</div>
<h2 id=sec-balance style="font-size:15px" data-sec data-state="{{ 'done' if signed_off else ('ready' if rec.status=='balanced' else 'attn') }}" data-note="{{ ('Signed off ' ~ signed_off) if signed_off else ('Balanced' if rec.status=='balanced' else ('Out of balance' if rec.status=='out' else 'Balances needed')) }}">Balance reconciliation</h2>
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
<div style="margin-bottom:8px">
{% if session.is_admin and not signed_off and (rec.status!='balanced' or n_pending) %}<details style="margin-top:12px"><summary style="cursor:pointer;color:var(--muted);font-size:13px">Admin: sign off anyway</summary>
<form method=post action="{{ url_for('signoff', name=name) }}" class=balform><input type=hidden name=override value=1>
<div><label>Reason (recorded with the sign-off)</label><input name=note required style="width:340px;max-width:100%"></div>
<button type=submit class=btn-sm style="color:var(--bad);border-color:var(--bad-soft)">Sign off unbalanced</button></form></details>{% endif %}
</div>
{% if reviewable %}
<h2 id=sec-review style="font-size:15px" data-sec data-state="{{ 'attn' if n_pending else 'done' }}" data-note="{{ (n_pending ~ ' to review') if n_pending else 'All reviewed' }}">Suggested matches{% if n_pending %} — {{ n_pending }} to review{% endif %}</h2>
<div class=help>Close but not exact pairs. They only count once you confirm them. <details class=how><summary>How this works</summary><div>Suggested when the payee matches but the amount differs slightly, a bank line cleared later than it was booked, or several bank lines add up to one QuickBooks entry (batched).{% if n_signflip %} {{ n_signflip }} {{ 'is an' if n_signflip==1 else 'are' }} opposite-sign pairing{{ '' if n_signflip==1 else 's' }} (same amount, flipped sign): usually a transfer entered the wrong way round.{% endif %} Confirm the right ones and reject the rest; Edit pairing changes which items are paired.</div></details></div>
{% if n_pending > 1 %}<form method=post action="{{ url_for('review_all', name=name) }}" style="margin:0 0 4px" data-confirm="Confirm all {{ n_pending }} suggested matches? They count as matched; you can undo each one afterwards."><button type=submit class=btn-sm>Confirm all {{ n_pending }}</button></form>{% endif %}
<table><tr><th>Why suggested</th><th>Statement side</th><th>Books side</th><th>Status</th><th></th></tr>
{% for r in reviewable %}<tr>
<td><span class="tag {{ 'fuzzy' if r.type in ('fuzzy','manual') else 'exact' }}">{{ 'same payee, amount differs' if r.type=='fuzzy' else ('opposite sign' if r.type=='manual' else ('cleared later' if r.type=='exact' else 'batched total')) }}</span></td>
<td class=desc>{% for d,a,w in r.sls %}{{ d }} · {{ a|money }} · {{ w }}<br>{% endfor %}{% if r.delta and r.delta != 0 %}<span style="color:#9a6a16">off {{ r.delta|money }}</span>{% endif %}</td>
<td class=desc>{% for d,a,w in r.bts %}{{ d }} · {{ a|money }} · {{ w }}<br>{% endfor %}</td>
<td>{% if r.status=='proposed' %}<span class="tag pending">to review</span>{% elif r.status=='rejected' %}<span style="color:#b3471f">rejected</span>{% else %}<span style="color:#3a7d44">confirmed</span>{% endif %}</td>
<td><form method=post action="{{ url_for('review_match', name=name, match_id=r.id) }}" class=btnrow>
{% if r.status=='proposed' %}<button type=submit name=status value=confirmed class="btn-sm pri">Confirm</button>
{% else %}<button type=submit name=status value=proposed class=btn-sm>Undo</button>{% endif %}
<span class=kebab><button type=button class=icon-btn data-dd aria-label="More" aria-expanded=false>""" + DOTS_ICON + """</button><div class=dd hidden>
{% if r.status=='proposed' %}<button type=submit name=status value=rejected>Reject</button>{% endif %}
{% if r.status=='confirmed' %}<button type=submit name=status value=edit title="Change which items this match pairs">Edit pairing</button>
{% else %}<button type=button class=mm-edit data-lines="{{ r.lids|join(',') }}" data-txns="{{ r.tids|join(',') }}" title="Change which items this match pairs">Edit pairing</button>{% endif %}
</div></span>
</form></td>
</tr>{% endfor %}</table>
{% endif %}
<h2 id=sec-matched style="font-size:15px" data-sec data-state=done data-note="Matched">Matched ({{ matched|length }}{% if n_m2o %} + {{ n_m2o }} batched{% endif %})</h2>
<table><tr><th>Date</th><th>Payee</th><th></th><th class=a>Statement</th><th class=a>Books</th></tr>
{% for mt, delta, d, samt, who, bamt in matched %}<tr><td>{{ d }}</td><td class=desc>{{ who }}</td>
<td><span class="tag {{ mt }}">{{ mt }}{% if delta and delta != 0 %} · off {{ delta|money }}{% endif %}</span></td>
<td class=a>{{ samt|money }}</td><td class=a>{{ bamt|money }}</td></tr>{% endfor %}</table>
{% if writebacks or deposits %}
<h2 id=sec-record style="font-size:15px" data-sec data-state="{{ 'attn' if n_to_record else 'done' }}" data-note="{{ (n_to_record ~ ' to record') if n_to_record else 'Recorded \u2014 matches on the next refresh' }}">Not in QuickBooks yet — record them ({{ writebacks|length + deposits|length }})</h2>
<div class=help>Type and account are guessed from the description and how similar lines were posted. Check them, then record one line or tick several. <details class=how><summary>How this works</summary><div>The account and payee are suggested from how similar bank lines were posted before. Check them, then record one line, or tick several and record them together. Each becomes {{ 'a credit-card expense' if atype=='credit_card' else 'an expense (money out) or a deposit (money in)' }} in QuickBooks, dated as on the statement. <em>Type</em> narrows the accounts to one kind: an expense or deposit, a customer or student payment, a supplier payment, or a transfer. A transfer between your own accounts is recorded as one QuickBooks Transfer (same currency only); a student payment is recorded against the student. <em>Split</em> records one bank line across several accounts, such as an FX hedge and its gain or loss.{% if fx_ccy %} Amounts are in {{ fx_ccy }}; each line uses QuickBooks' {{ fx_ccy }} rate for its date unless you type one ({{ home_ccy }} per {{ fx_ccy }}).{% endif %}</div></details></div>
{% if rec_job %}<div id=recjob class=savedsel data-url="{{ url_for('record_status', name=name) }}">Recording in QuickBooks: <b class=rj-n>{{ rec_job.n }}</b> of {{ rec_job.total }} lines done ({{ rec_job.done }} recorded so far). The list updates when it finishes.</div>
<script>(function(){var b=document.getElementById('recjob');if(!b||!window.fetch)return;
function tick(){fetch(b.getAttribute('data-url'),{credentials:'same-origin'}).then(function(r){return r.json()}).then(function(j){
  if(j.state==='running'){b.querySelector('.rj-n').textContent=j.n;setTimeout(tick,3000)}
  else{location.hash='sec-record';location.reload()}}).catch(function(){setTimeout(tick,6000)})}
setTimeout(tick,3000)})();</script>{% endif %}
<form method=post action="{{ url_for('record', name=name) }}" id=recform data-ccy="{{ acct_ccy or '' }}">
{% if draft_meta %}<div class=savedsel>Showing the selection saved by {{ draft_meta.by or 'a user' }} on {{ draft_meta.at.strftime('%Y-%m-%d %H:%M') }}. <button type=submit formaction="{{ url_for('record_discard', name=name) }}" class=btn-sm data-busy="Discarding the saved selection...">Discard it</button></div>{% endif %}
<table class=rectbl><tr><th><input type=checkbox id=selall title="Select all"></th><th>Date</th><th>Bank description</th><th class=a>Amount</th><th>Type and account</th><th>Payee</th><th></th></tr>
{% for w in record_rows %}<tr data-amt="{{ w.amount }}">
<td>{% if w.recordable and not w.wb %}<input type=checkbox name=sel value="{{ w.line_id }}" class=rsel data-amt="{{ w.amount }}" {% if w.sel %}checked{% endif %}><input type=hidden name=rowid value="{{ w.line_id }}">{% endif %}</td>
<td>{{ w.date }}</td>
<td class=desc>{{ w.who }}{% if w.sug %}<div class=hint>&#8627; {{ w.sug.because }}</div>{% endif %}
{% if w.dups and not w.wb %}<div class=dupwarn>&#9888; QuickBooks may already have this: {% for x in w.dups %}{{ x.date }} · {{ x.amount|money }}{% if x.who %} · {{ x.who }}{% endif %}{% if not loop.last %}; {% endif %}{% endfor %}.
{% if w.dup_matchable %}<br><button type=button class="btn-sm mm-open" data-line="{{ w.line_id }}" data-txn="{{ w.dup_matchable }}">Match it instead</button>
{% else %}<br>It's dated outside this statement period. If it's the same money, don't record it again — correct its date in QuickBooks, then refresh.{% endif %}</div>{% endif %}</td>
<td class=a>{{ w.amount|money }}</td>
{% if w.wb == 'pending' %}<td colspan=3 style="white-space:normal"><span class=bad>Recording was interrupted — check QuickBooks before trying again.</span><br><button type=submit name=reset value="{{ w.line_id }}" formaction="{{ url_for('record_reset', name=name) }}" class=btn-sm style="margin-top:6px" data-busy="Resetting...">I checked — it's not in QuickBooks</button></td>
{% elif w.wb == 'taken' %}<td colspan=3 style="white-space:normal"><span class=bad>The entry recorded for this line{% if w.qbo_id %} (#{{ w.qbo_id }}){% endif %} was paired with another identical line, so this one isn't in QuickBooks yet.</span><br><button type=submit name=reset value="{{ w.line_id }}" formaction="{{ url_for('record_reset', name=name) }}" class=btn-sm style="margin-top:6px" data-busy="Resetting...">Record this one again</button></td>
{% elif w.wb == 'done' %}<td colspan=3 class=muted style="white-space:normal">Recorded in QuickBooks{% if w.qbo_id %} (#{{ w.qbo_id }}){% endif %} — it will match on the next refresh.</td>
{% elif not w.recordable %}<td colspan=3 class=muted style="white-space:normal">{{ w.why_not }}</td>
{% else %}
<td>{% if not w.xfer_only %}<select class=ttype aria-label="Transaction type" data-guess="{{ w.ttype or '' }}" title="{{ w.ttype_why or 'Narrow the accounts to one kind of transaction' }}"></select>{% if w.ttype_why %}<div class="hint ttype-why">{{ w.ttype_why }}</div>{% endif %}{% endif %}<div class="acctbox main" data-dir="{{ 'xfer' if w.xfer_only else ('out' if w.out else 'in') }}" data-sel="{{ w.acct_id or '' }}"><input type=text class=acct-q placeholder="{{ 'Type the bank it was paid from' if w.xfer_only else 'Type to search accounts' }}" autocomplete=off aria-label="Account" role=combobox aria-expanded=false><button type=button class=acct-x title="Clear the account (the line won't be recorded)" aria-label="Clear account">&times;</button><input type=hidden name="acct_{{ w.line_id }}" class=acct-v value=""><div class=acct-list role=listbox hidden></div></div>
{% if not w.xfer_only %}<div class=rowtools>{% if w.hedge %}<button type=button class="btn-sm hedge-btn" title="Forward deal {{ w.hedge.deal }}: record it the way hedges are booked">Hedge</button>{% endif %}<button type=button class="btn-sm split-btn" title="Record this line across several accounts">Split</button>{% if fx_ccy %}<input name="rate_{{ w.line_id }}" value="{{ w.rate }}" class=rate inputmode=decimal placeholder="Rate (QuickBooks')" aria-label="{{ fx_ccy }} rate" title="{{ home_ccy }} per {{ fx_ccy }}. Leave empty to use QuickBooks' rate for {{ w.date }}.">{% endif %}</div>{% endif %}
<input type=hidden name="split_{{ w.line_id }}" class=split-v value="{{ w.split }}">
<input type=hidden name="kids_{{ w.line_id }}" class=kids-v value="{{ w.kids }}">
{% if x_ccy and not w.out %}<input name="rate_{{ w.line_id }}" value="{{ w.rate }}" class="rate xrate" inputmode=decimal hidden placeholder="{{ acct_ccy }} per {{ x_ccy }}" aria-label="{{ x_ccy }} rate" title="The rate the {{ acct_ccy }} received converts to the student's {{ x_ccy }} account at ({{ acct_ccy }} per {{ x_ccy }}). Leave empty to use QuickBooks' rate for {{ w.date }}.">{% endif %}
{% if w.hedge %}<input type=hidden name="hedge_{{ w.line_id }}" class=hedge-on value="{{ '1' if w.hedge_on else '' }}">{% endif %}
{% if w.xfer_only %}<div class=hint>Card payment: choose the bank it was paid from</div>{% elif w.is_xfer %}<div class=hint>Recorded as a transfer {{ 'to' if w.out else 'from' }} this account</div>{% endif %}{% if w.sug and not w.acct_id and not w.saved %}<div class=hint>'{{ w.sug.cat }}' isn't in your chart of accounts any more</div>{% elif w.sug and not w.saved %}<div class=hint>{{ "%.0f"|format(w.sug.conf*100) }}% match</div>{% endif %}</td>
<td><div class=custbox data-sel="{{ w.vend }}"><input name="payee_{{ w.line_id }}" value="{{ w.payee or '' }}" placeholder="optional" class="payee acct-q" autocomplete=off aria-label="Payee"><button type=button class=acct-x title="Clear the customer" aria-label="Clear customer">&times;</button><input type=hidden name="cust_{{ w.line_id }}" class=acct-v value=""><div class=acct-list role=listbox hidden></div></div>{% if w.dups %}<label class=hint style="display:flex;gap:5px;align-items:center;margin-top:6px"><input type=checkbox name="dupok_{{ w.line_id }}" value=1 {% if w.dupok %}checked{% endif %}> Not a duplicate</label>{% endif %}<input type=hidden name="psug_{{ w.line_id }}" value="{{ w.payee or '' }}"><input type=hidden name="pref_{{ w.line_id }}" value="{{ w.payee_ref or '' }}"></td>
<td><button type=submit name=only value="{{ w.line_id }}" class=btn-sm data-busy="Recording this line in QuickBooks...">Record</button></td>
{% endif %}
</tr>{% if w.recordable and not w.wb and not w.xfer_only %}<tr class=splitrow hidden><td></td><td colspan=6 class=splitcell></td></tr>
{% if w.hedge %}{% set h = w.hedge %}<tr class=hedgerow hidden data-leg="{{ h.leg }}" data-fwd="{{ h.fwd }}"><td></td><td colspan=6 class=hedgecell>
<div class=splithead><b>Forward deal {{ h.deal }}</b> at {{ h.fwd|money }} {{ home_ccy }} per {{ h.ccy }}.
{% if h.leg == 'out' %}Records it as your hedges are booked: a transfer from this account to <b>{{ hedge_accts.transit.fqn if hedge_accts.transit else 'FX in Transit (missing)' }}</b>, then on to <b>{{ hedge_accts.transit_home.fqn if hedge_accts.transit_home else 'FX in Transit ' ~ home_ccy ~ ' (missing)' }}</b> at this month's transaction rate.
{% else %}Records the receipt as a deposit: {{ h.ccy }} amount &times; this month's transaction rate clears <b>{{ hedge_accts.transit_home.fqn if hedge_accts.transit_home else 'FX in Transit ' ~ home_ccy ~ ' (missing)' }}</b>, and the difference is the gain (or loss) on <b>{{ hedge_accts.gain.fqn if hedge_accts.gain else 'Forex Gain (missing)' }}</b>.{% if h.known %} The {{ h.ccy }} amount and rate come from the {{ h.ccy }} leg recorded here.{% endif %}{% endif %}</div>
<div class=hedgeins>{% if h.leg == 'in' %}<label>{{ h.ccy }} amount <input name="hedge_usd_{{ w.line_id }}" class=hedge-usd inputmode=decimal value="{{ h.usd }}"></label>{% endif %}
<label>This month's transaction rate ({{ home_ccy }} per {{ h.ccy }}) <input name="hedge_rate_{{ w.line_id }}" class=hedge-rate inputmode=decimal value="{{ h.rate }}" placeholder="e.g. 3,720"></label>
<span class=hedgecalc></span><button type=button class="btn-sm hedge-cancel">Not a hedge</button></div></td></tr>{% endif %}
{% if not w.out %}<tr class=kidsrow hidden><td></td><td colspan=6 class=kidscell></td></tr>{% endif %}{% endif %}{% endfor %}</table>
<div class=recbar><button type=submit name=bulk value=1 class=btn>Record selected in QuickBooks</button>
<button type=submit formaction="{{ url_for('record_save', name=name) }}" class=btn-sm data-busy="Saving your selection..." title="Keep what's ticked and chosen, to carry on later">Save selection</button>
<span id=selcount class=hint></span></div>
</form>
<script id=coa-data type=application/json>{{ coa_json }}</script>
<script id=cust-data type=application/json>{{ cust_json }}</script>
<script id=vend-data type=application/json>{{ vend_json }}</script>
<script>(function(){
var el=document.getElementById('coa-data');if(!el)return;var coa=[],custs=[];try{coa=JSON.parse(el.textContent)}catch(e){}
try{custs=JSON.parse(document.getElementById('cust-data').textContent)}catch(e){}
var vends=[];try{vends=JSON.parse(document.getElementById('vend-data').textContent)}catch(e){}
var AR='Accounts Receivable',AP='Accounts Payable',rf=document.getElementById('recform'),CCY=rf?rf.getAttribute('data-ccy'):'';
var order={out:['Expense','Cost of Goods Sold','Other Expense'],'in':['Income','Other Income']};
var xferLabel={out:'Transfer to your account','in':'Transfer from your account',xfer:'Transfer from your bank'};
function typeLabel(a,dir){return a.x===1?xferLabel[dir]:a.x===2?'Bank and card accounts':a.t===AP?'Payables (choose the supplier)':a.t}
// Students and families: a receipt is paid to one of them (their UGX or USD account), not to
// Accounts Receivable in general. The bank's currency first; the other needs a rate.
var studentList=custs.map(function(c,i){var other=CCY&&c.c&&c.c!==CCY;
  return {id:'cust:'+c.id,cid:c.id,c:c.c,other:!!other,cust:true,k:'cust',n:c.n,p:c.p||'',rank:-100000+i,low:c.n.toLowerCase(),
          t:'Students and families'+(other?' ('+c.c+', at a rate)':c.c?' ('+c.c+')':'')}});
// Each picker's accounts: students first for money in, then the usual types for its direction, then
// the rest, then your own accounts (transfers). A split line can use any bank in the home currency
// or this one's; payables need a supplier, so they aren't offered in a split.
function accountsFor(dir,split){
  var pref=order[dir]||[],out=[];
  if(dir!=='xfer')coa.forEach(function(a){if(!a.x&&a.t!==AR&&(a.t!==AP||(dir==='out'&&!split)))out.push(a)});
  out.sort(function(a,b){var x=pref.indexOf(a.t),y=pref.indexOf(b.t);x=x<0?99:x;y=y<0?99:y;return x-y||a.t.localeCompare(b.t)||a.n.localeCompare(b.n)});
  coa.forEach(function(a){if(split?a.x===2:a.x===1)out.push(a)});
  var list=out.map(function(a,i){return {id:a.id,n:a.n,t:typeLabel(a,dir),ap:a.t===AP,k:a.x===1?'xfer':a.x===2?'bank':a.t===AP?'ap':'gl',rank:i,low:a.n.toLowerCase()}});
  return dir==='in'&&!split?studentList.concat(list):list;
}
var custList=studentList,vendList=vends.map(function(v,i){return {id:v.id,n:v.n,t:'Supplier',rank:i,low:v.n.toLowerCase()}});
function childrenOf(cid){return custList.filter(function(c){return c.p===cid})}
// The panel rows (split, hedge, children) that follow a record row, up to the next record row.
function panel(tr,cls){var n=tr.nextElementSibling;while(n&&!n.hasAttribute('data-amt')){if(n.classList.contains(cls))return n;n=n.nextElementSibling}return null}
function lev(a,b){   // edit distance, for typos ("stationary" finds "Stationery")
  var m=a.length,n=b.length,p=[],i,j;for(j=0;j<=n;j++)p[j]=j;
  for(i=1;i<=m;i++){var prev=p[0];p[0]=i;for(j=1;j<=n;j++){var t=p[j];p[j]=Math.min(p[j]+1,p[j-1]+1,prev+(a[i-1]===b[j-1]?0:1));prev=t}}
  return p[n];
}
// Closest first: starts with what you typed, then a word starting with it, then containing it,
// then containing every word typed, then letters in order, then near-misses (typos).
function ranked(list,q){
  q=q.trim().toLowerCase();if(!q)return list;
  var words=q.split(/ +/),flat=q.replace(/ +/g,''),hits=[];
  list.forEach(function(a){
    var n=a.low,parts=n.split(/[^a-z0-9]+/).filter(Boolean),sc=null;
    if(n.indexOf(q)===0)sc=0;
    else if(parts.some(function(p){return p.indexOf(q)===0}))sc=1;
    else if(n.indexOf(q)>-1)sc=2;
    else if(words.every(function(w){return n.indexOf(w)>-1}))sc=3;
    else{var k=0;for(var i=0;i<n.length&&k<flat.length;i++)if(n[i]===flat[k])k++;
      if(k===flat.length&&flat.length>=3)sc=4;
      else if(flat.length>=4){var best=99;words.forEach(function(w){parts.forEach(function(p){best=Math.min(best,lev(w,p.slice(0,Math.max(w.length,p.length))))})});
        if(best<=Math.max(1,Math.floor(words[0].length/4)))sc=5+best}}
    if(sc!==null)hits.push({a:a,sc:sc});
  });
  hits.sort(function(x,y){return x.sc-y.sc||x.a.rank-y.a.rank});
  return hits.map(function(h){return h.a});
}
var fmt=function(v){return v.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2})};
var bulk=document.querySelector('#recform button[name=bulk]'),cnt=document.getElementById('selcount');
// "Selected 3 of 25 transactions" next to the button, and on the button's progress message.
function count(){
  var boxes=document.querySelectorAll('.rsel'),n=0,t=0;
  boxes.forEach(function(c){if(c.checked){n++;t+=Math.abs(parseFloat(c.getAttribute('data-amt'))||0)}});
  if(cnt)cnt.textContent='Selected '+n+' of '+boxes.length+' transaction'+(boxes.length==1?'':'s')+(n?' · total '+fmt(t):'');
  if(bulk)bulk.setAttribute('data-busy','Recording '+n+' transaction'+(n==1?'':'s')+' in QuickBooks...');
}
// A search box over `list`. `active()` false leaves it a plain text field (a payee that isn't a customer).
function initBox(box,list,onPick,active){
  var q=box.querySelector('.acct-q'),v=box.querySelector('.acct-v'),pop=box.querySelector('.acct-list'),x=box.querySelector('.acct-x'),
      shown=[],hi=0,chosen=null,on=active||function(){return true};
  var none=box.getAttribute('data-none')||'No account matches. Clear it to leave this line unrecorded, or add the account in QuickBooks and refresh.';
  function vis(){x.style.visibility=on()&&q.value?'visible':'hidden'}
  function set(a,byUser){chosen=a;v.value=a.id;q.value=a.n;q.classList.remove('bad');q.title='';vis();onPick(a,byUser)}
  function clear(){chosen=null;v.value='';q.value='';q.classList.remove('bad');q.title='';vis();onPick(null,true)}
  function close(){pop.hidden=true;q.setAttribute('aria-expanded','false')}
  function render(){
    if(!on()){close();return}
    var L=typeof list==='function'?list():list;
    if(q.value.trim())shown=ranked(L,q.value).slice(0,40);
    else{var ns=0;shown=L.filter(function(a){return !a.cust||ns++<25}).slice(0,400)}
    hi=0;pop.innerHTML='';
    if(!shown.length){var e=document.createElement('div');e.className='none';e.textContent=none;pop.appendChild(e)}
    var lastT=null,typed=!!q.value.trim();
    shown.forEach(function(a,i){
      if(!typed&&a.t!==lastT){var g=document.createElement('div');g.className='ag';g.textContent=a.t+(a.cust?' \u2014 type a name to find one':'');pop.appendChild(g);lastT=a.t}
      var o=document.createElement('div');o.className='ao'+(i===0?' hi':'');o.setAttribute('role','option');
      var nm=document.createElement('span');nm.textContent=a.n;o.appendChild(nm);
      if(typed){var t=document.createElement('span');t.className='at';t.textContent=a.t;o.appendChild(t)}
      o.addEventListener('mousedown',function(ev){ev.preventDefault();set(a,true);close()});
      pop.appendChild(o)});
    pop.hidden=false;q.setAttribute('aria-expanded','true');
  }
  function move(d){var os=pop.querySelectorAll('.ao');if(!os.length)return;os[hi].classList.remove('hi');hi=(hi+d+os.length)%os.length;os[hi].classList.add('hi');
    if(os[hi].scrollIntoView)os[hi].scrollIntoView({block:'nearest'})}
  q.addEventListener('focus',function(){if(!on())return;if(q.select)q.select();render()});
  q.addEventListener('input',function(){if(!on())return;if(chosen&&q.value!==chosen.n){chosen=null;v.value=''}vis();render()});
  q.addEventListener('keydown',function(ev){if(!on())return;
    if(ev.key==='ArrowDown'){ev.preventDefault();if(pop.hidden)render();else move(1)}
    else if(ev.key==='ArrowUp'){ev.preventDefault();move(-1)}
    else if(ev.key==='Enter'){if(!pop.hidden){ev.preventDefault();if(shown[hi])set(shown[hi],true);close()}}
    else if(ev.key==='Escape'){close()}});
  q.addEventListener('blur',function(){close();if(!on())return;
    var t=q.value.trim();
    if(!t){clear();return}
    if(chosen&&q.value===chosen.n)return;
    var exact=(typeof list==='function'?list():list).filter(function(a){return a.low===t.toLowerCase()})[0];
    if(exact){set(exact,true);return}
    chosen=null;v.value='';q.classList.add('bad');q.title="Not in the list: pick one from it, or clear it. This line won't be recorded as it is.";
    onPick(null,true)});
  x.addEventListener('click',function(){clear();q.focus()});
  var LL=typeof list==='function'?list():list,start=LL.filter(function(a){return a.id===box.getAttribute('data-sel')})[0];
  if(!start&&q.value.trim())start=LL.filter(function(a){return a.low===q.value.trim().toLowerCase()})[0];
  if(start)set(start,false);else vis();
  return {get:function(){return chosen},clear:clear,refresh:function(){vis();if(!on()){v.value='';q.classList.remove('bad')}}};
}
function boxHtml(){return '<input type=text class=acct-q placeholder="Type to search accounts" autocomplete=off aria-label="Account"><button type=button class=acct-x aria-label="Clear account">&times;</button><input type=hidden class=acct-v value=""><div class=acct-list role=listbox hidden></div>'}
document.querySelectorAll('.acctbox.main').forEach(function(box){
  var tr=box.closest('tr'),dir=box.getAttribute('data-dir'),cb=tr.querySelector('.rsel'),total=Math.abs(parseFloat(tr.getAttribute('data-amt'))||0);
  var cbox=tr.querySelector('.custbox'),cust=null,isAr=false,isAp=false,kidsRow=panel(tr,'kidsrow'),kidsV=tr.querySelector('.kids-v'),
      xr=tr.querySelector('.xrate');
  // A parent's lump sum: split it between the parent and some or all of their children.
  function kids(parent,restore){
    if(!kidsRow||!kidsV)return;
    var ch=parent&&isAr?childrenOf(parent.cid):[];
    if(!ch.length){kidsRow.hidden=true;kidsV.value='';return}
    var saved={};try{(restore&&kidsV.value?JSON.parse(kidsV.value):[]).forEach(function(k){saved[k.c]=k.v})}catch(e){}
    var cell=kidsRow.querySelector('.kidscell');
    cell.innerHTML='<div class=splithead>'+'Paid for their children? Enter how much is for each (leave the others empty). It must add up to '+fmt(total)+'; each becomes its own payment.'+'</div>';
    var ins=[];
    [parent].concat(ch).forEach(function(c,i){c={id:c.cid,n:c.n};
      var d=document.createElement('div');d.className='kidline';
      var nm=document.createElement('span');nm.textContent=c.n+(i===0?' (the parent)':'');d.appendChild(nm);
      var inp=document.createElement('input');inp.className='splitamt';inp.setAttribute('inputmode','decimal');inp.placeholder='0.00';inp.value=saved[c.id]||'';inp.setAttribute('data-c',c.id);d.appendChild(inp);
      cell.appendChild(d);ins.push(inp);inp.addEventListener('input',function(){ksync(true)});
    });
    var rem=document.createElement('div');rem.className='splitrem';cell.appendChild(rem);
    function ksync(byUser){
      var sum=0,data=[];ins.forEach(function(i){var v=parseFloat(i.value.replace(/,/g,''));if(!isNaN(v)&&v){sum+=v;data.push({c:i.getAttribute('data-c'),v:i.value.replace(/,/g,'').trim()})}});
      var left=Math.round((total-sum)*100)/100;
      rem.textContent=!data.length?'Not split: the whole amount goes to '+parent.n+'.':left===0?'Adds up to '+fmt(total):'Left to allocate: '+fmt(left);
      rem.className='splitrem '+(!data.length||left===0?'ok':'warn');
      kidsV.value=data.length?JSON.stringify(data):'';
      if(byUser&&cb)cb.checked=!data.length||left===0;count();
    }
    kidsRow.hidden=false;ksync(false);
  }
  var first=true,all=accountsFor(dir,false),tsel=tr.querySelector('.ttype'),TYPES={
    out:[['gl','Expense (or other account)'],['ap','Supplier payment (a payable)'],['xfer','Transfer to your account']],
    'in':[['cust','Customer / student payment'],['gl','Deposit (income or other)'],['xfer','Transfer from your account']]};
  // Type: narrows the account list to one kind (only kinds this line can use are offered).
  if(tsel){var o0=document.createElement('option');o0.value='';o0.textContent='Any type';tsel.appendChild(o0);
    (TYPES[dir]||[]).forEach(function(t){if(!all.some(function(a){return a.k===t[0]}))return;
      var o=document.createElement('option');o.value=t[0];o.textContent=t[1];tsel.appendChild(o)});
    if(tsel.options.length<3)tsel.hidden=true;
    var g=tsel.getAttribute('data-guess');if(g&&[].some.call(tsel.options,function(o){return o.value===g}))tsel.value=g;
    tsel.addEventListener('change',function(){var a=main.get(),why=tr.querySelector('.ttype-why');if(why)why.hidden=true;
      if(a&&tsel.value&&a.k!==tsel.value){main.clear();var q=box.querySelector('.acct-q');if(q&&q.focus)q.focus()}})}
  function listed(){var k=tsel&&tsel.value;return k?all.filter(function(a){return a.k===k}):all}
  var main=initBox(box,listed,function(a,byUser){
    isAr=!!(a&&a.cust);isAp=!!(a&&a.ap);
    if(tsel&&a&&[].some.call(tsel.options,function(o){return o.value===a.k}))tsel.value=a.k;
    var ph=tr.querySelector('.pickhint');if(ph&&a)ph.remove();
    if(byUser&&cb)cb.checked=!!a&&(!isAp||!!(cust&&cust.get()));
    // A student's account in the other currency: the amount converts at the rate typed here.
    if(xr){xr.hidden=!(a&&a.other);if(xr.hidden&&!first)xr.value=''}
    if(cbox){cbox.classList.toggle('ar',isAp);cbox.classList.toggle('off',isAr);var pq=cbox.querySelector('.acct-q');
      pq.disabled=isAr;pq.placeholder=isAp?"Type the supplier's name":isAr?'the student':'optional';if(cust)cust.refresh()}
    kids(isAr?a:null,first);first=false;
    count();
  });
  if(cbox){cbox.setAttribute('data-none','No supplier matches. Check the name, or add the supplier in QuickBooks and refresh.');
    cust=initBox(cbox,function(){return vendList},function(a,byUser){if(byUser&&cb&&isAp)cb.checked=!!(a&&main.get());count()},function(){return isAp});
    cbox.classList.toggle('ar',isAp);cust.refresh()}
  // Hedge: a forward deal booked via FX in Transit, with its gain or loss.
  var hbtn=tr.querySelector('.hedge-btn'),hrow=panel(tr,'hedgerow'),hon=tr.querySelector('.hedge-on');
  if(hbtn&&hrow&&hon){
    var hq=box.querySelector('.acct-q'),hr=hrow.querySelector('.hedge-rate'),hu=hrow.querySelector('.hedge-usd'),hc=hrow.querySelector('.hedgecalc');
    var hleg=hrow.getAttribute('data-leg'),hfwd=parseFloat(hrow.getAttribute('data-fwd'));
    var num2=function(i){return i?parseFloat(i.value.replace(/,/g,'')):NaN};
    var hsync=function(byUser){
      var r=num2(hr),u=hleg==='in'?num2(hu):total,ok=r>0&&u>0;
      if(!ok)hc.textContent=hleg==='in'?'Type the amount and the rate.':'Type the rate.';
      else if(hleg==='in'){var p=Math.round(u*r*100)/100,g=Math.round((total-p)*100)/100;
        hc.textContent='FX in Transit '+fmt(p)+'  ·  '+(g>=0?'gain ':'loss ')+fmt(Math.abs(g))+' to Forex Gain';}
      else hc.textContent=fmt(u)+' at '+fmt(r)+' = '+fmt(Math.round(u*r*100)/100)+' into FX in Transit (the deal pays '+fmt(Math.round(u*hfwd*100)/100)+')';
      if(byUser&&cb)cb.checked=ok;count();
    };
    var hopen=function(){hrow.hidden=false;hon.value='1';box.classList.add('off');hq.disabled=true;if(tsel)tsel.disabled=true;hbtn.classList.add('on');hsync(false)};
    var hclose=function(){hrow.hidden=true;hon.value='';box.classList.remove('off');hq.disabled=false;if(tsel)tsel.disabled=false;hbtn.classList.remove('on');if(cb)cb.checked=!!main.get();count()};
    hbtn.addEventListener('click',function(){if(hrow.hidden){hopen();hsync(true)}else hclose()});
    hrow.querySelector('.hedge-cancel').addEventListener('click',hclose);
    [hr,hu].forEach(function(i){if(i)i.addEventListener('input',function(){hsync(true)})});
    if(hon.value==='1')hopen();
  }
  // Split: several accounts for this one line; they must add up to its amount.
  var srow=panel(tr,'splitrow'),btn=tr.querySelector('.split-btn'),hid=tr.querySelector('.split-v');
  if(!srow||!srow.classList.contains('splitrow')||!btn)return;
  var cell=srow.querySelector('.splitcell'),lines=[],slist=accountsFor(dir,true);
  cell.innerHTML='<div class=splithead>Split this line across accounts. Amounts go the same way as the bank line; a negative amount goes the other way (for example a loss on a hedge). They must add up to '+fmt(total)+'.</div><div class=splitlines></div><div class=splitfoot><button type=button class=btn-sm data-add>+ Add line</button><span class=splitrem></span><button type=button class=btn-sm data-cancel>Remove split</button></div>';
  var wrap=cell.querySelector('.splitlines'),rem=cell.querySelector('.splitrem');
  function num(t){return parseFloat(String(t).replace(/,/g,''))}
  function sync(byUser){
    var sum=0,ok=lines.length>0,data=[];
    lines.forEach(function(l){var a=l.api.get(),v=num(l.amt.value);if(!isNaN(v))sum+=v;if(!a||isNaN(v))ok=false;
      data.push({a:a?a.id:'',v:l.amt.value.replace(/,/g,'').trim()})});
    var left=Math.round((total-sum)*100)/100;
    rem.textContent=left===0?'Adds up to '+fmt(total):'Left to allocate: '+fmt(left);rem.className='splitrem '+(left===0?'ok':'warn');
    hid.value=lines.length?JSON.stringify(data):'';
    if(byUser&&cb)cb.checked=ok&&left===0;
    count();
  }
  function add(accId,amount){
    var d=document.createElement('div');d.className='splitline';
    var b=document.createElement('div');b.className='acctbox';b.setAttribute('data-sel',accId||'');b.innerHTML=boxHtml();d.appendChild(b);
    var amt=document.createElement('input');amt.className='splitamt';amt.setAttribute('inputmode','decimal');amt.placeholder='Amount';amt.value=amount||'';d.appendChild(amt);
    var rm=document.createElement('button');rm.type='button';rm.className='btn-sm';rm.innerHTML='&times;';rm.title='Remove this line';d.appendChild(rm);
    wrap.appendChild(d);
    var l={amt:amt,el:d};l.api=initBox(b,slist,function(){sync(true)});lines.push(l);
    amt.addEventListener('input',function(){sync(true)});
    rm.addEventListener('click',function(){lines.splice(lines.indexOf(l),1);d.remove();sync(true)});
    return l;
  }
  function open(){srow.hidden=false;box.classList.add('off');box.querySelector('.acct-q').disabled=true;btn.classList.add('on')}
  function cancel(){lines=[];wrap.innerHTML='';srow.hidden=true;box.classList.remove('off');box.querySelector('.acct-q').disabled=false;btn.classList.remove('on');hid.value='';if(cb)cb.checked=!!main.get();count()}
  btn.addEventListener('click',function(){
    if(!srow.hidden){srow.querySelector('.acct-q')&&srow.querySelector('.acct-q').focus();return}
    open();if(!lines.length){var m=main.get();add(m&&!m.cust&&!m.ap?m.id:'',String(total));add('','')}sync(true)});
  cell.querySelector('[data-add]').addEventListener('click',function(){add('','');sync(true)});
  cell.querySelector('[data-cancel]').addEventListener('click',cancel);
  var saved=[];try{saved=hid.value?JSON.parse(hid.value):[]}catch(e){}
  if(saved.length){open();saved.forEach(function(p){add(p.a,p.v)});sync(false)}
});
var all=document.getElementById('selall');if(all)all.addEventListener('change',function(){document.querySelectorAll('.rsel').forEach(function(c){c.checked=all.checked});count()});
var f=document.getElementById('recform');
if(f){f.addEventListener('change',function(e){if(e.target.classList&&e.target.classList.contains('rsel'))count()});
// Enter in a text box never sends the form: it would press the first button on the page and act on
// another line. (Enter in an open list still picks the highlighted item.)
f.addEventListener('keydown',function(e){var t=e.target;
  if(e.key==='Enter'&&t.tagName==='INPUT'&&t.type!=='submit'&&t.type!=='button'&&t.type!=='checkbox')e.preventDefault()});
// A line's own Record button: its account must be picked from the list, not just typed.
f.addEventListener('submit',function(e){var b=e.submitter;if(!b||b.name!=='only')return;
  var tr=b.closest('tr'),box=tr&&tr.querySelector('.acctbox.main'),q=box&&box.querySelector('.acct-q'),v=box&&box.querySelector('.acct-v');
  var split=tr&&tr.querySelector('.split-v'),hedge=tr&&tr.querySelector('.hedge-on');
  if(!box||(split&&split.value)||(hedge&&hedge.value==='1')||(v&&v.value))return;
  e.preventDefault();q.classList.add('bad');
  q.title=q.value.trim()?'Pick it from the list (click it, or use the arrow keys and Enter), then Record.':'Choose an account first.';
  var h=tr.querySelector('.pickhint');if(!h){h=document.createElement('div');h.className='hint pickhint bad';box.parentNode.insertBefore(h,box.nextSibling)}
  h.textContent=q.title;q.focus()},true);
f.addEventListener('submit',function(e){var b=e.submitter;if(!b||b.name!=='bulk')return;
  var n=0,t=0;document.querySelectorAll('.rsel:checked').forEach(function(c){n++;t+=Math.abs(parseFloat(c.getAttribute('data-amt'))||0)});
  if(!n){e.preventDefault();return}
  if(f._rbok){f._rbok=false;return}
  e.preventDefault();rbAsk('Record '+n+' transaction'+(n==1?'':'s')+' totalling '+t.toLocaleString(undefined,{minimumFractionDigits:2})+' in QuickBooks? Each is posted dated as on the statement.',function(){rbResubmit(f,b)},{yes:'Record'});});}
count();
})();</script>
{% endif %}
{% if all_unmatched or in_books or user_matches %}
<h2 id=sec-manual style="font-size:15px" data-sec data-state="{{ 'attn' if all_unmatched and in_books else 'done' }}" data-note="{{ (all_unmatched|length ~ ' bank line' ~ ('' if all_unmatched|length == 1 else 's') ~ ' unmatched') if all_unmatched and in_books else 'Nothing to pair' }}">Match manually</h2>
<div class=help>Pair bank lines with QuickBooks entries the matcher missed, one to one or several together. <details class=how><summary>How this works</summary><div>Pair bank lines with QuickBooks transactions the matcher missed — one to one, or several together (two deposits banked as one, a payment split in the books). Tick items on both sides; the QuickBooks list re-sorts to put the closest amounts first. Only QuickBooks entries dated up to {{ p_end }} can be matched here.</div></details></div>
{% if user_matches %}
<table><tr><th>Matched by you</th><th>Statement side</th><th>Books side</th><th class=a>Difference</th><th></th></tr>
{% for u in user_matches %}<tr>
<td class=hint>{{ u.by or '' }}{% if u.at %}<br>{{ u.at.strftime('%Y-%m-%d') }}{% endif %}</td>
<td class=desc>{% for d,a,w in u.sls %}{{ d }} · {{ a|money }} · {{ w }}<br>{% endfor %}</td>
<td class=desc>{% for d,a,w in u.bts %}{{ d }} · {{ a|money }} · {{ w }}<br>{% endfor %}</td>
<td class=a>{% if u.delta %}<span class=warn>{{ u.delta|money }}</span>{% else %}0.00{% endif %}</td>
<td><form method=post action="{{ url_for('unmatch', name=name, match_id=u.id) }}"><button type=submit class=btn-sm>Undo</button></form></td>
</tr>{% endfor %}</table>
{% endif %}
{% if all_unmatched and in_books %}
<form method=post action="{{ url_for('manual_match', name=name) }}" id=mmform>
<input type=hidden name=orig id=mmorig value="">
<div id=mmnote style="display:none;background:#eef4ff;border:1px solid #c7d7f5;color:#1e3a6e;padding:8px 12px;border-radius:9px;font-size:13px;margin:0 0 10px">Editing a suggested match: its items are ticked below. Untick the ones that don't belong, tick the right ones, then press <b>Match selected</b>. The old suggestion is then marked rejected.</div>
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
  else{s.textContent='Bank '+fmt(sl)+'  ·  QuickBooks '+fmt(sb)+'  ·  Difference '+fmt(diff)+'  ·  selected '+L.length+' of '+rowsOf('mml').length+' bank lines, '+B.length+' of '+rowsOf('mmb').length+' QuickBooks entries';s.className=diff===0?'ok':'warn';go.disabled=false}
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
// Tick these items (and only these), show them at the top of each list, and go to the form.
function pick(ls,ts,orig){
  f.querySelectorAll('input[type=checkbox]').forEach(function(c){c.checked=false});
  document.querySelectorAll('.mmsearch').forEach(function(inp){inp.value=''});
  rowsOf('mml').concat(rowsOf('mmb')).forEach(function(r){r.style.display=''});
  [['ml',ls,'mml'],['mb',ts,'mmb']].forEach(function(s){
    var box=document.getElementById(s[2]);
    s[1].slice().reverse().forEach(function(id){var c=f.querySelector('input[name='+s[0]+'][value="'+id+'"]');
      if(c){c.checked=true;box.insertBefore(c.parentNode,box.firstChild)}});box.scrollTop=0});
  document.getElementById('mmorig').value=orig||'';
  document.getElementById('mmnote').style.display=orig?'':'none';
  update(true);
  var sec=document.getElementById('sec-manual');if(sec&&sec.scrollIntoView)sec.scrollIntoView({behavior:'smooth',block:'start'});
}
function ids(b,a){return (b.getAttribute(a)||'').split(',').filter(Boolean)}
document.querySelectorAll('.mm-open').forEach(function(b){b.addEventListener('click',function(){
  pick([b.getAttribute('data-line')],[b.getAttribute('data-txn')],'');})});
document.querySelectorAll('.mm-edit').forEach(function(b){b.addEventListener('click',function(){
  var ls=ids(b,'data-lines'),ts=ids(b,'data-txns');pick(ls,ts,ls.join(',')+'|'+ts.join(','));})});
var E={{ mm_edit|tojson }};
if(E){window.__mmOpen=1;}
if(E)pick(E.l,E.t,E.l.join(',')+'|'+E.t.join(','));
f.addEventListener('submit',function(e){if(f._rbok){f._rbok=false;return}if(diff===0)return;var sb=e.submitter;e.preventDefault();
  rbAsk('The two sides differ by '+fmt(diff)+'. Match anyway? The difference will show under amount differences.',function(){rbResubmit(f,sb)},{yes:'Match anyway'})});
update(false);
})();</script>
{% endif %}
{% endif %}
{% if n_xfer or xfer_recorded or xfer_dismissed %}<h2 id=sec-transfers style="font-size:15px" data-sec data-state="{{ 'attn' if n_xfer else 'done' }}" data-note="{{ (n_xfer ~ ' to check') if n_xfer else 'None to check' }}">Possible transfers between your own accounts ({{ n_xfer }})</h2>
<div class=help>Suggestions only. A real transfer is recorded once, as one QuickBooks Transfer. <details class=how><summary>How this works</summary><div>Suggestions only \u2014 check each pair first. A genuine transfer is recorded once, as a Transfer between the two accounts, never as an expense on one and a deposit on the other. <em>Record as one transfer</em> does that and matches both bank lines to it. <em>Edit</em> picks a different counterpart (or just the other account, when its statement isn't uploaded); <em>Not a transfer</em> hides a wrong suggestion.</div></details></div>
{% if n_xfer %}<form method=post action="{{ url_for('transfer_dismiss', name=name) }}" id=xferbulk class=xferbar>
<span class=xb-n>Tick the suggestions that aren't transfers</span><button type=submit class=btn-sm data-busy="Hiding the suggestions..." disabled>Not a transfer (selected)</button></form>
<table class=xfertbl><tr><th><input type=checkbox class=xb-all title="Select all" aria-label="Select all suggestions"></th><th>Date</th><th>On this statement</th><th class=a>Amount</th><th>Possible counterpart</th><th>Why flagged</th></tr>
{% for lid, d, a, who in all_unmatched %}{% if xfers.get(lid) %}{% for c in xfers[lid] %}
<tr><td><input type=checkbox name=pick value="{{ lid }}|{{ c.key }}" form=xferbulk class=xb-pick aria-label="Not a transfer"></td><td>{{ d }}</td><td class=desc>{{ who }}</td><td class=a>{{ a|money }}</td>
<td class=desc><strong>{{ c.account }}</strong><br><span style="color:var(--muted);font-size:12px">{{ c.date }} \u00b7 {{ c.amount|money }}{% if c.who %} \u00b7 {{ c.who }}{% endif %}</span></td>
<td style="font-size:12px;color:var(--muted);white-space:normal;min-width:220px">{{ c.note }}<div class=btnrow style="margin-top:6px;flex-wrap:wrap">
{% if c.rule == 'unrecorded' %}{% if not acct_linked or not c.other_linked %}<span>Both accounts must be linked to QuickBooks to record it here.</span>{% elif signed_off or c.other_signed %}<span>A statement is signed off \u2014 reopen it to record this.</span>{% else %}<form method=post action="{{ url_for('record_transfer', name=name) }}" data-confirm="Record one transfer of {{ a|abs|money }} between {{ name }} and {{ c.account }} in QuickBooks, and match both bank lines to it?"><input type=hidden name=line value="{{ lid }}"><input type=hidden name=other value="{{ c.line_id }}"><button type=submit class="btn-sm pri">Record as one transfer</button></form>{% endif %}{% endif %}
<span class=kebab><button type=button class=icon-btn data-dd aria-label="More" aria-expanded=false>""" + DOTS_ICON + """</button><div class=dd hidden>
{% if loop.first and not signed_off and acct_linked %}<button type=button class=xfer-edit data-line="{{ lid }}" aria-expanded=false>Edit counterpart</button>{% endif %}
<form method=post action="{{ url_for('transfer_dismiss', name=name) }}"><input type=hidden name=line value="{{ lid }}"><input type=hidden name=other value="{{ c.key }}"><button type=submit data-busy="Hiding this suggestion...">Not a transfer</button></form>
</div></span>
</div></td></tr>
{% if loop.last and not signed_off and acct_linked %}<tr class=xferedit id="xe-{{ lid }}" hidden><td></td><td colspan=5 style="white-space:normal;background:#f8faff">
<form method=post action="{{ url_for('record_transfer', name=name) }}" class=xe-form data-amt="{{ a|abs|money }}"><input type=hidden name=line value="{{ lid }}">
<div class=splithead>Record {{ a|abs|money }} as a transfer {{ 'to' if a < 0 else 'from' }} another of your accounts. Pick the matching line on its statement, or just the account if its statement isn't uploaded (only this line is matched then).</div>
{% set ch = xfer_choices.get(lid, []) %}{% for o in ch %}<label class=xe-opt><input type=radio name=other value="{{ o.line_id }}"{% if not o.linked %} disabled{% endif %}> <b>{{ o.account }}</b> \u00b7 {{ o.date }} \u00b7 {{ o.amount|money }}{% if o.who %} \u00b7 {{ o.who }}{% endif %}{% if not o.linked %} <span class=hint>(not linked to QuickBooks)</span>{% endif %}</label>
{% else %}<div class=hint style="margin:4px 0">No other statement has this amount moving the other way within {{ 14 }} days.</div>{% endfor %}
{% if xfer_accounts %}<label class=xe-opt><input type=radio name=other value="" class=xe-acct-r> Only the account:
<select name=other_acct class=xe-acct><option value="">choose\u2026</option>{% for t in xfer_accounts %}<option value="{{ t.id }}">{{ t.fqn }}</option>{% endfor %}</select></label>{% endif %}
<div class=btnrow style="margin-top:8px"><button type=submit class=btn-sm data-busy="Recording the transfer in QuickBooks...">Record transfer</button><button type=button class="btn-sm xe-cancel">Cancel</button></div>
</form></td></tr>{% endif %}
{% endfor %}{% endif %}{% endfor %}</table>{% endif %}
{% if xfer_recorded %}<details class=xferrec id=xferrec style="margin:4px 0 14px"{% if not n_xfer %} open{% endif %}><summary style="cursor:pointer;font-size:13.5px;font-weight:600">Transfers recorded from this statement ({{ xfer_recorded|length }}) <span class=hint style="font-weight:400">— Edit changes the other side; Undo deletes it in QuickBooks</span></summary>
<table class=xferdone><tr><th>Date</th><th>On this statement</th><th class=a>Amount</th><th>Other account</th><th></th></tr>
{% for t in xfer_recorded %}<tr><td>{{ t.date }}</td><td class=desc>{{ t.who }}</td><td class=a>{{ t.amount|money }}</td><td class=desc>{{ t.other or '' }}<br><span class=hint>QuickBooks #{{ t.qbo_id }}{% if t.by %} \u00b7 {{ t.by }}{% endif %}</span></td>
<td>{% if not signed_off %}<div class=btnrow><button type=button class="btn-sm xfer-edit" data-line="r{{ t.qbo_id }}" aria-expanded=false>Edit</button><span class=kebab><button type=button class=icon-btn data-dd aria-label="More" aria-expanded=false>""" + DOTS_ICON + """</button><div class=dd hidden><form method=post action="{{ url_for('transfer_undo', name=name) }}" data-confirm="Undo transfer #{{ t.qbo_id }}? It is deleted in QuickBooks, and its bank lines go back to the list to record again."><input type=hidden name=qbo_id value="{{ t.qbo_id }}"><button type=submit class=danger data-busy="Undoing the transfer (deleting it in QuickBooks)...">Undo (delete in QuickBooks)</button></form></div></span></div>{% else %}<span class=hint>signed off</span>{% endif %}</td></tr>
{% if not signed_off %}<tr class=xferedit id="xe-r{{ t.qbo_id }}" hidden><td></td><td colspan=4 style="white-space:normal;background:#f8faff">
<form method=post action="{{ url_for('transfer_change', name=name) }}" class=xe-form data-amt="{{ t.amount|abs|money }}" data-verb="Change transfer #{{ t.qbo_id }} of"><input type=hidden name=qbo_id value="{{ t.qbo_id }}">
<div class=splithead>Change the other side of transfer #{{ t.qbo_id }} (now {{ t.other or 'unknown' }}). It's updated in QuickBooks, keeping its number; the old counterpart line goes back to its list.</div>
{% for o in xfer_rec_choices.get(t.line_id, []) %}<label class=xe-opt><input type=radio name=other value="{{ o.line_id }}"{% if not o.linked %} disabled{% endif %}> <b>{{ o.account }}</b> · {{ o.date }} · {{ o.amount|money }}{% if o.who %} · {{ o.who }}{% endif %}{% if not o.linked %} <span class=hint>(not linked to QuickBooks)</span>{% endif %}</label>
{% else %}<div class=hint style="margin:4px 0">No unmatched line on another statement has this amount moving the other way within {{ 14 }} days.</div>{% endfor %}
{% if xfer_accounts %}<label class=xe-opt><input type=radio name=other value="" class=xe-acct-r> Only the account:
<select name=other_acct class=xe-acct><option value="">choose…</option>{% for a2 in xfer_accounts %}<option value="{{ a2.id }}">{{ a2.fqn }}</option>{% endfor %}</select></label>{% endif %}
<div class=btnrow style="margin-top:8px"><button type=submit class=btn-sm data-busy="Changing the transfer in QuickBooks...">Save change</button><button type=button class="btn-sm xe-cancel">Cancel</button></div>
</form></td></tr>{% endif %}{% endfor %}</table></details>
<script>(function(){var d=document.getElementById('xferrec');if(!d)return;var k='xferrec:'+location.pathname;
try{var v=sessionStorage.getItem(k);if(v!==null)d.open=v==='1'}catch(e){}
d.addEventListener('toggle',function(){try{sessionStorage.setItem(k,d.open?'1':'0')}catch(e){}})})();</script>{% endif %}
{% if xfer_dismissed %}<details class=xferhid style="margin:0 0 22px"><summary class=hint style="cursor:pointer">{{ xfer_dismissed|length }} suggestion{{ '' if xfer_dismissed|length == 1 else 's' }} marked \u2018Not a transfer\u2019</summary>
<table><tr><th>Date</th><th>On this statement</th><th class=a>Amount</th><th>Suggested counterpart</th><th></th></tr>
{% for x in xfer_dismissed %}<tr><td>{{ x.date }}</td><td class=desc>{{ x.who }}</td><td class=a>{{ x.amount|money }}</td><td class=desc>{{ x.c.account }} \u00b7 {{ x.c.date }} \u00b7 {{ x.c.amount|money }}{% if x.by %}<br><span class=hint>hidden by {{ x.by }}</span>{% endif %}</td>
<td><form method=post action="{{ url_for('transfer_restore', name=name) }}"><input type=hidden name=line value="{{ x.line_id }}"><input type=hidden name=other value="{{ x.c.key }}"><button type=submit class=btn-sm data-busy="Restoring the suggestion...">Restore</button></form></td></tr>{% endfor %}</table></details>{% endif %}
<style>.xe-opt{display:block;padding:5px 0;font-size:13px}.xe-opt select{margin-left:6px;padding:4px 6px;border:1px solid var(--line);border-radius:7px;font-size:13px;max-width:100%}
.xfertbl .btnrow form{margin:0}
.xferbar{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin:0 0 6px;font-size:13px;color:var(--muted)}</style>
<script>(function(){
// Bulk 'Not a transfer': tick rows (or the header box), then hide them all at once.
var bf=document.getElementById('xferbulk');if(!bf)return;
var picks=[].slice.call(document.querySelectorAll('.xb-pick')),all=document.querySelector('.xb-all'),
    btn=bf.querySelector('button'),lab=bf.querySelector('.xb-n');
function upd(){var n=picks.filter(function(p){return p.checked}).length;btn.disabled=!n;
  btn.textContent=n?'Not a transfer ('+n+')':'Not a transfer (selected)';
  lab.textContent=n?n+' suggestion'+(n==1?'':'s')+' ticked':'Tick the suggestions that aren’t transfers';
  if(all){all.checked=n&&n===picks.length;all.indeterminate=n>0&&n<picks.length}}
picks.forEach(function(p){p.addEventListener('change',upd)});
if(all)all.addEventListener('change',function(){picks.forEach(function(p){p.checked=all.checked});upd()});
bf.addEventListener('submit',function(e){if(bf._rbok){bf._rbok=false;return}var n=picks.filter(function(p){return p.checked}).length;
  e.preventDefault();if(!n)return;var sb=e.submitter;
  rbAsk('Mark '+n+' suggestion'+(n==1?'':'s')+' as not a transfer? They can be restored from the hidden list.',function(){rbResubmit(bf,sb)},{yes:'Not a transfer'})});
upd();
})();</script>
<script>(function(){
// Edit: open the chooser under the line; picking an account ticks "Only the account".
document.querySelectorAll('.xfer-edit').forEach(function(b){var row=document.getElementById('xe-'+b.getAttribute('data-line'));if(!row)return;
  b.addEventListener('click',function(){row.hidden=!row.hidden;b.setAttribute('aria-expanded',String(!row.hidden))});
  row.querySelector('.xe-cancel').addEventListener('click',function(){row.hidden=true;b.setAttribute('aria-expanded','false')});
  var sel=row.querySelector('.xe-acct'),r=row.querySelector('.xe-acct-r'),f=row.querySelector('.xe-form');
  if(sel&&r)sel.addEventListener('change',function(){if(sel.value)r.checked=true});
  f.addEventListener('submit',function(e){if(f._rbok)return;var pick=f.querySelector('input[name=other]:checked');
    var acct=pick&&pick.classList.contains('xe-acct-r')?sel.options[sel.selectedIndex]:null;
    if(!pick||(acct&&!sel.value)){e.preventDefault();var h=f.querySelector('.xe-err');if(!h){h=document.createElement('div');h.className='hint bad xe-err';f.appendChild(h)}
      h.textContent=!pick?'Pick the matching line or an account first.':'Choose the account.';return}
    var verb=f.getAttribute('data-verb')||'Record a transfer of';
    if(f._rbok){f._rbok=false;return}var sb=e.submitter;e.preventDefault();
    rbAsk(verb+' '+f.getAttribute('data-amt')+(acct?' with '+acct.textContent:' between these two lines')+' in QuickBooks?',function(){rbResubmit(f,sb)},{yes:'Save'})});
});})();</script>
{% endif %}
<h2 id=sec-exceptions style="font-size:15px" data-sec data-state="{{ 'attn' if on_stmt else 'done' }}" data-note="{{ (on_stmt|length ~ ' to clear') if on_stmt else 'None' }}">On statement, not in books ({{ on_stmt|length }})</h2>
<table class=exc><tr><th>Date</th><th>Description</th><th class=a>Amount</th></tr>
{% for _, d, a, who in on_stmt %}<tr><td>{{ d }}</td><td class=desc>{{ who }}</td><td class=a>{{ a|money }}</td></tr>{% endfor %}</table>
<h2 id=sec-inbooks style="font-size:15px" data-sec data-state="{{ 'attn' if in_books and rec.status!='balanced' else 'done' }}" data-note="{{ (in_books|length ~ ' outstanding') if in_books else 'None' }}">In books, not on statement ({{ in_books|length }})</h2>
<table class=exc><tr><th>Date</th><th>Description</th><th class=a>Amount</th></tr>
{% for _, d, a, who in in_books %}<tr><td>{{ d }}{% if d < p_start %} <span class="tag bf">brought forward</span>{% endif %}</td><td class=desc>{{ who }}</td><td class=a>{{ a|money }}</td></tr>{% endfor %}</table>
<div id=sec-end></div>
{% endif %}
<style>th.sortable{cursor:pointer;user-select:none;white-space:nowrap}th.sortable:hover{color:var(--accent)}
th.sortable .sortind{font-size:11px;margin-left:4px;color:var(--accent);font-weight:600}</style>
<script>(function(){
// Click a column heading to sort: Date and amounts sort as dates and numbers; "Statement side" and
// "Books side" sort by date, then (clicking on) by amount. A line's panels (split, hedge, children)
// move with it. The order is kept for this page until the tab is closed.
var PANEL=/(^| )(splitrow|hedgerow|kidsrow|xferedit)( |$)/;
function key(td,kind){var t=td?td.textContent.replace(/\\s+/g,' ').trim():'',m;
  if(kind==='date'){m=t.match(/\\d{4}-\\d{2}-\\d{2}/);return m?m[0]:''}
  if(kind==='amt'){m=t.replace(/,/g,'').match(/\\d{4}-\\d{2}-\\d{2} \\u00b7 (-?\\d+(?:\\.\\d+)?)/);return m?parseFloat(m[1]):-Infinity}
  if(kind==='num'){m=t.replace(/,/g,'').match(/-?\\d+(?:\\.\\d+)?/);return m?parseFloat(m[0]):-Infinity}
  return t.toLowerCase()}
var NAMES={date:'date',amt:'amount',num:'',text:''};
function store(k,v){try{if(v===undefined)return sessionStorage.getItem(k);sessionStorage.setItem(k,v)}catch(e){return null}}
document.querySelectorAll('.wrap table').forEach(function(tbl,ti){
  var head=tbl.rows[0];if(!head||!head.querySelector('th')||tbl.classList.contains('rec'))return;
  var cols=[];
  [].forEach.call(head.cells,function(th,ci){
    var lab=th.textContent.trim(),kinds=null;
    if(lab==='Date')kinds=['date'];
    else if(/^(Amount|Statement|Books|Difference)$/.test(lab))kinds=['num'];
    else if(/ side$/.test(lab))kinds=['date','amt'];
    else if(/^(Payee|Description|Bank description|On this statement)$/.test(lab))kinds=['text'];
    if(!kinds)return;
    var modes=[];kinds.forEach(function(k){modes.push([k,1],[k,-1])});
    var ind=document.createElement('span');ind.className='sortind';th.appendChild(ind);th.classList.add('sortable');
    th.title='Sort by '+(kinds[1]?'date, then amount':lab.toLowerCase());th.setAttribute('role','button');th.tabIndex=0;
    cols.push({th:th,ci:ci,modes:modes,ind:ind,at:-1});
  });
  if(!cols.length)return;
  var sk='sort:'+location.pathname+':'+ti;
  function apply(c,at,save){
    cols.forEach(function(o){if(o!==c){o.at=-1;o.ind.textContent=''}});
    c.at=at;var m=c.modes[at],k=m[0],dir=m[1];
    c.ind.textContent=(NAMES[k]?NAMES[k]+' ':'')+(dir>0?'\\u2191':'\\u2193');
    var groups=[],cur=null;
    [].slice.call(tbl.rows,1).forEach(function(r){if(PANEL.test(r.className)&&cur)cur.push(r);else groups.push(cur=[r])});
    groups.forEach(function(g,i){g.i=i;g.k=key(g[0].cells[c.ci],k)});
    groups.sort(function(a,b){var x=a.k,y=b.k;return (x<y?-1:x>y?1:0)*dir||a.i-b.i});
    var parent=head.parentNode;groups.forEach(function(g){g.forEach(function(r){parent.appendChild(r)})});
    if(save)store(sk,c.ci+':'+at);
  }
  cols.forEach(function(c){
    function next(){apply(c,(c.at+1)%c.modes.length,true)}
    c.th.addEventListener('click',function(e){if(e.target.tagName!=='INPUT')next()});
    c.th.addEventListener('keydown',function(e){if(e.key==='Enter'||e.key===' '){e.preventDefault();next()}});
  });
  var saved=(store(sk)||'').split(':');
  var sc=cols.filter(function(o){return String(o.ci)===saved[0]})[0];
  if(sc&&sc.modes[+saved[1]])apply(sc,+saved[1],false);
});
})();</script>
<style>.dsecbar{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin:14px 2px 8px;font-size:12px;color:var(--muted)}
.dsecbar b{color:var(--ink)}.dsecbar button{background:none;border:0;padding:0;color:var(--accent);font:inherit;font-weight:600;cursor:pointer}
.dsec{background:var(--panel);border:1px solid var(--line);border-radius:var(--radius);margin:0 0 10px}
.dsec>h2.dsec-h{display:flex;align-items:center;gap:9px;cursor:pointer;user-select:none;margin:0;padding:9px 14px;font-size:13.5px;border-left:3px solid transparent;border-radius:var(--radius)}
.dsec>h2.dsec-h:hover{background:var(--row)}
.dsec.attn>h2.dsec-h{border-left-color:#e08a3e}.dsec.ready>h2.dsec-h{border-left-color:var(--gold)}.dsec.done>h2.dsec-h{border-left-color:#4cb07e;color:#3d4757;font-weight:600}
.dsec:not(.closed)>h2.dsec-h{border-radius:var(--radius) var(--radius) 0 0}
.dsec-chev{display:inline-block;width:12px;font-size:9px;color:var(--faint);transition:transform .15s}.dsec.closed .dsec-chev{transform:rotate(-90deg)}
.dsec-badge{margin-left:auto;font-size:11.5px;font-weight:600;padding:1px 9px;border-radius:999px;white-space:nowrap}
.dsec.attn .dsec-badge{background:var(--warn-soft);color:var(--warn)}.dsec.ready .dsec-badge{background:var(--gold-soft);color:#7a5d0e}.dsec.done .dsec-badge{background:var(--ok-soft);color:var(--ok)}
.dsec-body{border-top:1px solid var(--line-soft);padding:10px 14px 4px}
.dsec-body>table:first-child,.dsec-body>.help:first-child{margin-top:0}</style>
<script>(function(){
// Each section heading folds its section. Sections that need work come first and start open; finished
// ones go to the bottom, folded. A section you open or fold stays that way (this tab) until its state changes.
var hs=[].slice.call(document.querySelectorAll('h2[data-sec]'));if(!hs.length)return;
var parent=hs[0].parentNode;hs=hs.filter(function(h){return h.parentNode===parent});
var end=document.getElementById('sec-end'),RANK={attn:0,ready:1,done:2};
function store(k,v){try{if(v===undefined)return sessionStorage.getItem(k);sessionStorage.setItem(k,v)}catch(e){return null}}
var anchor=document.createComment('sections');parent.insertBefore(anchor,hs[0]);
var secs=hs.map(function(h,i){
  var st=RANK.hasOwnProperty(h.getAttribute('data-state'))?h.getAttribute('data-state'):'done';
  var el=document.createElement('section'),body=document.createElement('div');
  el.className='dsec '+st;body.className='dsec-body';parent.insertBefore(el,h);
  var n=h.nextSibling;
  while(n&&n!==end&&n!==anchor&&!(n.nodeType===1&&hs.indexOf(n)>=0)){var nx=n.nextSibling;body.appendChild(n);n=nx}
  el.appendChild(h);el.appendChild(body);
  h.setAttribute('data-title',h.textContent.replace(/ +/g,' ').trim());h.classList.add('dsec-h');h.setAttribute('role','button');h.tabIndex=0;
  var chev=document.createElement('span');chev.className='dsec-chev';chev.textContent='\\u25bc';h.insertBefore(chev,h.firstChild);
  var badge=document.createElement('span');badge.className='dsec-badge';
  badge.textContent=(st==='done'?'\\u2713 ':'')+(h.getAttribute('data-note')||(st==='done'?'Done':'Needs attention'));h.appendChild(badge);
  var s={el:el,h:h,body:body,st:st,i:i,key:'sec:'+location.pathname+':'+h.id+':'+st};
  var saved=store(s.key);set(s,saved===null?st!=='done':saved==='1',false);
  h.addEventListener('click',function(){set(s,el.classList.contains('closed'),true)});
  h.addEventListener('keydown',function(e){if(e.key==='Enter'||e.key===' '){e.preventDefault();set(s,el.classList.contains('closed'),true)}});
  return s});
function set(s,open,save){s.el.classList.toggle('closed',!open);s.body.hidden=!open;s.h.setAttribute('aria-expanded',String(open));if(save)store(s.key,open?'1':'0')}
secs.slice().sort(function(a,b){return RANK[a.st]-RANK[b.st]||a.i-b.i}).forEach(function(s){parent.insertBefore(s.el,anchor)});
// summary line with expand / collapse all
var n=secs.filter(function(s){return s.st==='attn'}).length,bar=document.createElement('div');bar.className='dsecbar';
var t=document.createElement('span');
if(n){t.appendChild(document.createElement('b')).textContent=n+' section'+(n==1?' needs':'s need')+' attention';t.appendChild(document.createTextNode(' \\u2014 shown first; finished sections are folded below.'))}
else t.textContent=secs.some(function(s){return s.st==='ready'})?'Nothing left to fix \\u2014 ready to sign off.':'Nothing needs attention.';
bar.appendChild(t);
[['Expand all',true],['Collapse all',false]].forEach(function(p){var b=document.createElement('button');b.type='button';b.textContent=p[0];
  b.addEventListener('click',function(){secs.forEach(function(s){set(s,p[1],true)})});bar.appendChild(b)});
parent.insertBefore(bar,secs.slice().sort(function(a,b){return RANK[a.st]-RANK[b.st]||a.i-b.i})[0].el);
// anything that jumps into a folded section opens it first
function reveal(id){var el=id&&document.getElementById(id);if(!el)return;
  secs.forEach(function(s){if(s.el.contains(el)&&s.el.classList.contains('closed'))set(s,true,false)})}
document.addEventListener('click',function(e){var t=e.target.closest&&e.target.closest('#dtiles .tile,.mm-open,.mm-edit,a[href^="#"]');if(!t)return;
  if(t.classList.contains('tile')){var id=t.getAttribute('data-target');reveal(document.getElementById(id)?id:t.getAttribute('data-fallback'))}
  else if(t.tagName==='A')reveal(t.getAttribute('href').slice(1));else reveal('sec-manual')},true);
function fromHash(){reveal(decodeURIComponent(location.hash.slice(1)))}
window.addEventListener('hashchange',fromHash);
if(location.hash){fromHash();var el=document.getElementById(decodeURIComponent(location.hash.slice(1)));if(el&&el.scrollIntoView)el.scrollIntoView()}
if(window.__mmOpen){reveal("sec-manual");var mm=document.getElementById("sec-manual");if(mm&&mm.scrollIntoView)mm.scrollIntoView()}
})();</script>
<script>(function(){function go(btn){document.querySelectorAll('#dtiles .tile').forEach(function(t){t.classList.toggle('active',t===btn)});var el=document.getElementById(btn.getAttribute('data-target'));if(!el){var fb=btn.getAttribute('data-fallback'); if(fb) el=document.getElementById(fb);}if(el){el.scrollIntoView({behavior:'smooth',block:'start'}); el.classList.remove('flash'); void el.offsetWidth; el.classList.add('flash');}}document.querySelectorAll('#dtiles .tile').forEach(function(t){t.addEventListener('click',function(){go(t)})});})();</script>
</div></div>""" + SHELL_END + """<script>// Section menu: built from the page's own h2[id^=sec-] headings, so conditional sections take care of themselves.
(function(){
var menu=document.getElementById('secnav'),topnav=document.querySelector('.topbar');
if(!menu||menu.getAttribute('data-built'))return;
var heads=[].slice.call(document.querySelectorAll('h2[id^="sec-"]'));
if(heads.length<2)return;
menu.setAttribute('data-built','1');
var NAMES={'sec-balance':'Balance','sec-review':'Suggested','sec-matched':'Matched','sec-record':'Record','sec-manual':'Match manually',
  'sec-transfers':'Transfers','sec-exceptions':'Not in books','sec-inbooks':'Not on statement'};
function label(h){
  var t=(h.getAttribute('data-title')||h.textContent||'').replace(/ +/g,' ').trim(),name=NAMES[h.id],n=null,m;
  if(!name){name=t.split(' (')[0].split(' —')[0];if(name.length>22)name=name.slice(0,21)+'…';}
  if((m=t.match(/([0-9]+) to review/)))n=+m[1];
  else if((m=t.match(/[(]([^)]*)[)]/))){var ds=m[1].match(/[0-9][0-9,]*/g);if(ds){n=0;ds.forEach(function(x){n+=+x.split(',').join('');});}}
  return {name:name,n:n};
}
var cur=null,offset=70;
function wide(){return false;}
function setOn(a){
  if(a===cur)return;if(cur)cur.classList.remove('on');cur=a;if(!a)return;a.classList.add('on');
  if(!wide()&&menu.scrollWidth>menu.clientWidth){var l=a.offsetLeft-menu.offsetLeft;menu.scrollLeft=Math.max(0,l-(menu.clientWidth-a.offsetWidth)/2);}
}
var items=heads.map(function(h){
  var l=label(h),a=document.createElement('a');a.href='#'+h.id;a.textContent=l.name;
  if(h.getAttribute('data-state')==='attn'){a.classList.add('attn');a.title='Needs attention'}
  if(l.n!==null){var s=document.createElement('span');s.className='n';s.textContent=' ('+l.n+')';a.appendChild(s);}
  a.addEventListener('click',function(e){
    if(e.metaKey||e.ctrlKey||e.shiftKey||e.altKey)return;
    e.preventDefault();
    var reduce=window.matchMedia&&window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    if(h.scrollIntoView)h.scrollIntoView({behavior:reduce?'auto':'smooth',block:'start'});
    if(window.history&&history.pushState){if(location.hash!=='#'+h.id)history.pushState(null,'','#'+h.id);}else location.hash=h.id;
    setOn(a);});
  menu.appendChild(a);return a;});
menu.hidden=false;
function layout(){
  var nh=(topnav&&topnav.offsetHeight)||53;menu.style.setProperty('--navh',nh+'px');
  offset=nh+(wide()?0:menu.offsetHeight)+12;
  heads.forEach(function(h){h.style.scrollMarginTop=offset+'px';});
}
function spy(){
  var i=0,d=document.documentElement;
  heads.forEach(function(h,k){if(h.getBoundingClientRect().top<=offset+8)i=k;});
  if(d.scrollHeight>window.innerHeight&&window.innerHeight+(window.pageYOffset||d.scrollTop)>=d.scrollHeight-2)i=heads.length-1;
  setOn(items[i]);
}
layout();spy();
window.addEventListener('resize',function(){layout();spy();});
if('IntersectionObserver' in window){
  var io=new IntersectionObserver(function(){spy();},{rootMargin:'0px 0px -40% 0px',threshold:[0,1]});
  heads.forEach(function(h){io.observe(h);});
  window.addEventListener('scroll',function(){var d=document.documentElement;if(window.innerHeight+window.pageYOffset>=d.scrollHeight-2)spy();},{passive:true});
}else{
  var busy=false;window.addEventListener('scroll',function(){if(busy)return;busy=true;(window.requestAnimationFrame||setTimeout)(function(){busy=false;spy();});},{passive:true});
}
})();</script>
</body></html>"""


def _ensure_snapshot_cols(cur):
    for col, typ in (("snap_exact", "int"), ("snap_fuzzy", "int"), ("snap_m2o", "int"),
                     ("snap_exc", "int"), ("snap_diff", "numeric")):
        cur.execute(f"ALTER TABLE statement ADD COLUMN IF NOT EXISTS {col} {typ};")


TRANSFER_WINDOW_DAYS = 4      # how far apart the two sides of a transfer may sit
TRANSFER_EXACT_ONLY = True    # fees charged as separate debits, so amounts should tie exactly


def transfer_candidates(cur, acct_uuid, unmatched_lines, window=None):
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
    if window is None:
        window = rule("transfer_days")
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
            {"rule": "unrecorded", "account": nm, "date": d, "amount": a, "who": who, "key": f"line:{other_lid}",
             "line_id": str(other_lid), "other_signed": bool(other_signed), "other_linked": bool(other_qbo),
             "other_qbo": other_qbo,
             "note": "Opposite entry on another bank statement, not recorded in QuickBooks either side."})

    # Rule 2: unmatched BOOK transaction on another account, money moving the same way
    cur.execute(f"""
        WITH un(line_id, d, amt) AS (SELECT * FROM unnest(%s::uuid[], %s::date[], %s::numeric[]))
        SELECT un.line_id, a.name, bt.posted_date, bt.amount,
               coalesce(bt.counterparty, bt.description, ''), bt.txn_id
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
    for lid, nm, d, a, who, tid in cur.fetchall():
        out.setdefault(str(lid), []).append(
            {"rule": "wrong_account", "account": nm, "date": d, "amount": a, "who": who, "key": f"book:{tid}",
             "note": "Recorded in QuickBooks against this account instead \u2014 likely posted to the wrong bank."})
    return out


EDIT_WINDOW_DAYS = 14    # Edit offers counterparts this far apart (the suggestions use TRANSFER_WINDOW_DAYS)


def transfer_dismissed(cur, line_ids):
    """{line_id: [(key, by, at)]} pairs marked 'Not a transfer'."""
    if not line_ids:
        return {}
    cur.execute("""SELECT line_id::text, other, dismissed_by, dismissed_at FROM transfer_dismissal
                   WHERE line_id = ANY(%s::uuid[]) ORDER BY dismissed_at;""", ([str(x) for x in line_ids],))
    out = {}
    for lid, other, by, at in cur.fetchall():
        out.setdefault(lid, []).append((other, by, at))
    return out


def transfer_choices(cur, acct_uuid, lines, window=EDIT_WINDOW_DAYS):
    """For Edit: every unmatched line on another account's statement (same currency, not signed off)
    with the same money moving the other way, within `window` days. {line_id: [...]}, closest date first."""
    if not lines:
        return {}
    cur.execute("SELECT type FROM account WHERE account_id=%s;", (acct_uuid,))
    m_this = -1 if (cur.fetchone() or [None])[0] == "credit_card" else 1
    flow = "(CASE WHEN a.type = 'credit_card' THEN -1 ELSE 1 END)"
    cur.execute(f"""
        WITH un(line_id, d, amt) AS (SELECT * FROM unnest(%s::uuid[], %s::date[], %s::numeric[]))
        SELECT un.line_id, sl.line_id, a.name, sl.posted_date, sl.amount, coalesce(sl.counterparty, sl.description, ''),
               a.source_account_id IS NOT NULL
        FROM un
        JOIN statement s ON s.account_id <> %s AND s.signed_off_at IS NULL
        JOIN account a ON a.account_id = s.account_id AND a.currency = (SELECT currency FROM account WHERE account_id = %s)
        JOIN statement_line sl ON sl.statement_id = s.statement_id AND sl.amount * {flow} = -un.amt * %s
                              AND sl.posted_date BETWEEN un.d - %s AND un.d + %s
        WHERE NOT EXISTS (SELECT 1 FROM match_statement_line msl JOIN match m ON m.match_id = msl.match_id
                          WHERE msl.line_id = sl.line_id AND m.status = 'confirmed')
        ORDER BY abs(sl.posted_date - un.d), a.name;
    """, ([str(l[0]) for l in lines], [l[1] for l in lines], [l[2] for l in lines], acct_uuid, acct_uuid, m_this,
          window, window))
    out = {}
    for lid, other, nm, d, a, who, linked in cur.fetchall():
        out.setdefault(str(lid), []).append({"line_id": str(other), "account": nm, "date": d, "amount": a, "who": who,
                                             "linked": linked})
    return out


def recorded_transfers(cur, sid):
    """Transfers the app recorded for this statement's lines (from Possible transfers or the record list)."""
    cur.execute("""SELECT w.qbo_id, sl.line_id::text, sl.posted_date, sl.amount, coalesce(sl.description,''),
                          w.account_fqn, w.created_by, w.created_at
                   FROM writeback_log w JOIN statement_line sl ON sl.line_id = w.line_id
                   WHERE sl.statement_id=%s AND w.status='done' AND w.qbo_type='Transfer' AND coalesce(w.qbo_id,'') <> ''
                   ORDER BY sl.posted_date;""", (sid,))
    return [{"qbo_id": q, "line_id": l, "date": d, "amount": a, "who": w, "other": o, "by": by, "at": at}
            for q, l, d, a, w, o, by, at in cur.fetchall()]


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


def _record_rank(w):
    """Order of the 'record them' list: problems first (interrupted, taken), then lines to record,
    then lines that can't be recorded here, and lines already recorded (awaiting a refresh) last."""
    if w["wb"] in ("pending", "taken"):
        return 0
    if w["wb"] == "done":
        return 3
    return 1 if w["recordable"] else 2


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
                   ORDER BY status<>'proposed', match_type='exact', match_type;""", (sid,))
    rmatches = cur.fetchall()
    cur.execute("""SELECT match_id, amount_delta, confirmed_by, confirmed_at FROM match
                   WHERE statement_id=%s AND created_by='user' AND status='confirmed' ORDER BY confirmed_at;""", (sid,))
    umatches = cur.fetchall()
    sls_by, bts_by = match_sides(cur, [r[0] for r in rmatches] + [r[0] for r in umatches])
    items = match_items(cur, [r[0] for r in rmatches])
    for mid, mtype, status, delta in rmatches:
        lids, tids = items.get(str(mid), ([], []))
        reviewable.append({"id": mid, "type": mtype, "status": status, "delta": delta,
                           "sls": sls_by.get(str(mid), []), "bts": bts_by.get(str(mid), []),
                           "lids": lids, "tids": tids})
    user_matches = [{"id": mid, "delta": delta, "by": by, "at": at,
                     "sls": sls_by.get(str(mid), []), "bts": bts_by.get(str(mid), [])}
                    for mid, delta, by, at in umatches]
    unmatched_lines = [l for l in lines if l[0] not in ml]
    cur.execute("SELECT currency FROM account WHERE account_id=%s;", (acct_uuid,))
    acct_ccy = (cur.fetchone() or [None])[0]
    mem = PostingMemory(cur, acct_ccy) if unmatched_lines else None
    coa = load_coa(cur, acct_ccy)
    xt = transfer_targets(cur, acct_qbo, atype, acct_ccy) if coa else []
    sb = split_banks(cur, acct_qbo, acct_ccy) if coa else []
    home = qbo_home_currency(cur)
    hedge_legs = {}
    if unmatched_lines:
        cur.execute("SELECT deal, usd, rate, txn_date FROM hedge_leg ORDER BY txn_date;")
        for deal, usd, rate_, dd in cur.fetchall():
            hedge_legs.setdefault(deal, []).append((usd, rate_, dd))
    also = cross_currency(cur, acct_ccy, home) if coa and unmatched_lines else None
    custs = load_customers(cur, acct_ccy, also) if coa and unmatched_lines else []
    cust_names = {c["id"]: c["n"] for c in custs}
    vends = load_vendors(cur, acct_ccy) if coa and unmatched_lines else []
    vend_ids = {v["id"] for v in vends}
    vend_by_name = {}
    for v in vends:
        vend_by_name.setdefault(v["n"].strip().lower(), v["id"])
    coa_type = {a["id"]: a["type"] for a in coa}
    drafts, draft_meta = {}, None
    if unmatched_lines:
        cur.execute("""SELECT line_id, data, saved_by, saved_at FROM record_draft WHERE line_id = ANY(%s::uuid[])
                       ORDER BY saved_at;""", ([str(l[0]) for l in unmatched_lines],))
        for l_id, data, by, at in cur.fetchall():
            try:
                drafts[str(l_id)] = json.loads(data)
            except ValueError:
                continue
            draft_meta = {"by": by, "at": at, "n": len(drafts)}
    wb = {}
    if unmatched_lines:
        cur.execute("SELECT line_id, status, qbo_id FROM writeback_log WHERE line_id = ANY(%s::uuid[]);",
                    ([str(l[0]) for l in unmatched_lines],))
        wb = {str(r[0]): (r[1], r[2]) for r in cur.fetchall()}
        for l_id in taken_elsewhere(cur, acct_uuid, [k for k, v in wb.items() if v[0] == "done"]):
            wb[l_id] = ("taken", wb[l_id][1])
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
                "payee_ref": (sug or {}).get("payee_ref"), "wb": status if status in ("pending", "done", "taken") else None,
                "qbo_id": qbo_id, "recordable": why_not is None, "why_not": why_not, "xfer_only": xfer_only,
                "is_xfer": bool(acct and acct.get("xfer")),
                "dups": dups.get(str(lid), [])[:3]}
        item["dup_matchable"] = next((x["txn_id"] for x in item["dups"] if x["txn_id"] in pool_ids), None)
        item["sel"] = bool(item["acct_id"] and not item["dups"])
        pref = item["payee_ref"] or ""
        item["cust"] = pref.split(":", 1)[1] if pref.startswith("Customer:") and pref.split(":", 1)[1] in cust_names else ""
        item["vend"] = pref.split(":", 1)[1] if pref.startswith("Vendor:") and pref.split(":", 1)[1] in vend_ids else ""
        if not item["vend"] and item["payee"]:   # suggested by name only (older entries carry no supplier ID)
            item["vend"] = vend_by_name.get(item["payee"].strip().lower(), "")
        item["rate"] = item["split"] = item["kids"] = ""
        item["dupok"] = False
        # A forward deal: its foreign leg (money out of the USD account) or its home receipt (UGX in).
        hi = hedge_info(who) if coa else None
        item["hedge"] = None
        if hi and hi["ccy"] == (acct_ccy if acct_ccy != home else hi["ccy"]) and home and hi["home"] == home:
            leg = "out" if (acct_ccy == hi["ccy"] and out) else "in" if (acct_ccy == home and not out) else None
            if leg:
                legs = hedge_legs.get(hi["deal"], [])
                near = min(legs, key=lambda x: abs(((x[2] or dd) - dd).days)) if legs else None
                usd = near[0] if near else (abs(a) / hi["fwd"]).quantize(Decimal("0.01"))
                item["hedge"] = {**hi, "leg": leg, "usd": usd, "known": bool(near),
                                 "rate": (near[1] if near and leg == "in" else "") or month_rate(cur, hi["ccy"], dd)}
        item["hedge_on"] = False
        dr = drafts.get(str(lid))
        if dr:   # the user's saved choices win over suggestions
            item.update(sel=bool(dr.get("sel")), acct_id=dr.get("acct") or None, payee=dr.get("payee") or "",
                        cust=dr.get("cust") or "", rate=dr.get("rate") or "", split=dr.get("split") or "",
                        kids=dr.get("kids") or "", dupok=bool(dr.get("dupok")), saved=True)
            if item["hedge"] and dr.get("hedge"):
                item["hedge_on"] = True
                item["hedge"] = {**item["hedge"], "rate": dr.get("hedge_rate") or item["hedge"]["rate"],
                                 "usd": dr.get("hedge_usd") or item["hedge"]["usd"]}
        # A receipt goes to the student (or family) itself, never to Accounts Receivable in general.
        if coa_type.get(item["acct_id"]) == "Accounts Receivable" or (item["acct_id"] or "").startswith("cust:"):
            c_ = item["cust"] or (item["acct_id"] or "")[5:]
            item["acct_id"] = f"cust:{c_}" if c_ in cust_names and not out else None
            item["sel"] = item["sel"] and bool(item["acct_id"])
        if dr and coa_type.get(dr.get("acct")) == "Accounts Payable":
            item["vend"] = dr.get("cust") or ""
        (writebacks if out else deposits).append(item)
    _unmatched = [l for l in lines if l[0] not in ml]
    _all_unmatched = [(str(l[0]), l[1], l[2], l[3]) for l in _unmatched]
    try:
        xfers = transfer_candidates(cur, acct_uuid, _unmatched)
        gone = transfer_dismissed(cur, list(xfers))
        xfer_dismissed = []
        for l_id, items in list(xfers.items()):
            keys = {k for k, _, _ in gone.get(l_id, [])}
            kept = [c for c in items if c.get("key") not in keys]
            line = next(l for l in _unmatched if str(l[0]) == l_id)
            xfer_dismissed += [{"line_id": l_id, "date": line[1], "amount": line[2], "who": line[3], "c": c,
                                "by": next(b for k, b, _ in gone[l_id] if k == c.get("key"))}
                               for c in items if c.get("key") in keys]
            if kept:
                xfers[l_id] = kept
            else:
                del xfers[l_id]
        choices = transfer_choices(cur, acct_uuid, [l for l in _unmatched if str(l[0]) in xfers])
    except Exception:
        xfers, xfer_dismissed, choices = {}, [], {}   # a suggestion engine must never break the reconciliation itself
    xrec = recorded_transfers(cur, sid)
    try:
        xrec_choices = transfer_choices(cur, acct_uuid, [(t["line_id"], t["date"], t["amount"]) for t in xrec])
    except Exception:
        xrec_choices = {}
    xt_ids = {a["id"] for a in xt if a.get("xfer")}
    for item in writebacks + deposits:
        aid = item["acct_id"] or ""
        kind = ("cust" if aid.startswith("cust:") or coa_type.get(aid) == "Accounts Receivable" else
                "xfer" if aid in xt_ids else "ap" if coa_type.get(aid) == "Accounts Payable" else "gl" if aid else None)
        pairs = [c for c in xfers.get(str(item["line_id"]), []) if c.get("rule") == "unrecorded"]
        pair = {**pairs[0], "d": item["date"]} if len(pairs) == 1 else None
        conf = (item["sug"] or {}).get("conf")
        item["ttype"], item["ttype_why"] = guess_type(item["who"], item["out"], item["amount"], kind,
                                                      conf if kind else None, pair)
        if item.get("saved") and kind:
            item["ttype"], item["ttype_why"] = kind, "Your saved choice"
        # A clear transfer pair with no account chosen yet: suggest the other account (not ticked).
        if pair and not aid and not item["xfer_only"] and pair.get("other_qbo") in xt_ids:
            item["acct_id"] = pair["other_qbo"]
    return {"xfers": xfers, "n_xfer": sum(len(v) for v in xfers.values()), "all_unmatched": _all_unmatched,
            "xfer_choices": choices, "xfer_dismissed": xfer_dismissed, "xfer_recorded": xrec,
            "xfer_rec_choices": xrec_choices,
            "xfer_accounts": [a for a in xt if a.get("xfer")] if acct_qbo else [],
            "has_results": True, "p_start": ps, "p_end": pe,
            "signed_off": signed.strftime("%Y-%m-%d") if signed else None,
            "n_exact": sum(1 for m in matched if m[0] == "exact"),
            "n_fuzzy": sum(1 for m in matched if m[0] == "fuzzy"), "n_m2o": n_m2o, "n_signflip": n_signflip,
            "matched": matched, "reviewable": reviewable, "writebacks": writebacks, "deposits": deposits,
            "record_rows": sorted(writebacks + deposits, key=_record_rank),
            "n_to_record": sum(1 for w in writebacks + deposits if w["wb"] != "done"),
            "on_stmt": on_stmt_in, "in_books": rec["un_books"], "rec": rec, "diff": rec["rec_diff"],
            "n_pending": rec["n_pending"], "user_matches": user_matches,
            "acct_linked": bool(acct_qbo),
            "coa_json": Markup(json.dumps([{"id": a["id"], "n": a["fqn"], "t": a["type"],
                                            "x": 1 if a.get("xfer") else 2 if a.get("bank") else 0}
                                           for a in coa + xt + sb]).replace("<", "\\u003c")),
            "cust_json": Markup(json.dumps(custs).replace("<", "\\u003c")),
            "vend_json": Markup(json.dumps(vends).replace("<", "\\u003c")), "acct_ccy": acct_ccy, "x_ccy": also,
            "fx_ccy": acct_ccy if acct_ccy and home and acct_ccy != home else None, "home_ccy": home,
            "hedge_accts": hedge_accounts(cur, acct_ccy if acct_ccy != home else "USD") if coa else {},
            "draft_meta": draft_meta}


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


def account_switch_list(cur):
    """Active accounts for the switcher on an account page, grouped by where their reconciliation stands
    (latest statement): in progress, signed off, or no statement yet."""
    cur.execute("""SELECT a.name, a.currency, s.period_end, s.signed_off_at IS NOT NULL
                   FROM account a LEFT JOIN LATERAL (SELECT period_end, signed_off_at FROM statement
                       WHERE account_id=a.account_id ORDER BY created_at DESC LIMIT 1) s ON true
                   WHERE coalesce(a.is_active,true) ORDER BY a.name;""")
    groups = {"open": [], "signed": [], "none": []}
    for n, ccy, pe, signed in cur.fetchall():
        groups["none" if pe is None else "signed" if signed else "open"].append(
            {"name": n, "ccy": (ccy or "").strip(), "p_end": pe.strftime("%d %b %Y") if pe else None})
    return [("Reconciliation in progress", groups["open"]), ("Signed off", groups["signed"]),
            ("No statement yet", groups["none"])]


BALANCE_SOURCES = {"user": "entered", "file": "from file", "carried": "last signed-off closing",
                   "derived": "closing less movements", "qbo": "from QuickBooks"}


@app.route("/switch")
def switch_account():
    """Account switcher without JavaScript: the form posts the name here."""
    return redirect(url_for("detail", name=request.args.get("name", "")))


@app.route("/account/<name>")
def detail(name):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id, type, currency, source_account_id FROM account WHERE name=%s LIMIT 1;", (name,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close(); return "Unknown account", 404
    acct_uuid, atype, ccy, acct_qbo = row
    d = compute_detail(cur, acct_uuid, atype, acct_qbo)
    switch = account_switch_list(cur)
    cur.close(); conn.close()
    rec_job = record_job(name)
    if rec_job and rec_job.get("state") in ("done", "failed", "stalled") and \
            rec_job.get("started") != session.get("rec_seen:" + name):   # each job's result shown once
        extra = rec_job.get("msg") if rec_job["state"] != "stalled" else (
            f"Recording stopped before finishing ({rec_job.get('n')} of {rec_job.get('total')} lines done). "
            f"Lines already recorded show as recorded; record the rest again.")
        session["detail_msg"] = " ".join(x for x in (extra, session.get("detail_msg")) if x)
        session["rec_seen:" + name] = rec_job.get("started")
    d["rec_job"] = rec_job if rec_job and rec_job.get("state") == "running" else None
    conn = get_conn(); cur = conn.cursor()
    st = _latest_statement(cur, acct_uuid)
    prep = signed_by = None
    n_lines = n_matched_lines = 0
    if st:
        cur.execute("SELECT prepared_by, signed_off_by FROM statement WHERE statement_id=%s;", (st[0],))
        prep, signed_by = cur.fetchone() or (None, None)
        cur.execute("""SELECT count(*) FILTER (WHERE sl.amount <> 0),
                              count(*) FILTER (WHERE sl.amount <> 0 AND EXISTS (
                                  SELECT 1 FROM match_statement_line msl JOIN match m ON m.match_id = msl.match_id
                                  WHERE msl.line_id = sl.line_id AND m.status = 'confirmed'))
                       FROM statement_line sl WHERE sl.statement_id = %s;""", (st[0],))
        n_lines, n_matched_lines = cur.fetchone()
    cur.close(); conn.close()
    self_prepared = bool(rule("two_person") and prep and prep == session.get("name") and not session.get("is_admin"))
    rec = d.get("rec") or {}
    signoff_why = ("Your sign-in can't sign off" if not can("signoff") else
                   f"Review the {d.get('n_pending')} suggested match{'' if d.get('n_pending') == 1 else 'es'} first" if d.get("n_pending") else
                   "Balance the reconciliation first" if rec.get("status") != "balanced" else
                   "You prepared it: a second person (or an admin) signs off" if self_prepared else "")
    open_upload = bool(request.args.get("upload") or request.args.get("pdfpw")) and can("upload")
    return render_template_string(DETAIL_TEMPLATE, name=name, atype=atype, ccy=ccy, qbo_linked=bool(acct_qbo),
                                  prep=prep, signed_by=signed_by, n_lines=n_lines, n_matched_lines=n_matched_lines,
                                  self_prepared=self_prepared, signoff_why=signoff_why, open_upload=open_upload,
                                  qbo_connected=qbo_is_connected(), last_sync=last_sync_label(),
                                  src_label=BALANCE_SOURCES, detail_msg=session.pop("detail_msg", None),
                                  mm_edit=session.pop("mm_edit", None), switch=switch, **d)


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
        acctid = None if is_pdf else re.search(rb"<ACCTID>\s*([^<\r\n]+)", data, re.I)
        wrong = account_mismatch(name, pdf_rows.account_number if is_pdf else
                                 (re.sub(r"\D", "", acctid.group(1).decode("ascii", "ignore")) if acctid else None))
        if wrong:
            session["detail_msg"] = wrong
            return redirect(url_for("detail", name=name))
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
    log_activity(f"uploaded a statement ({n} lines)", name)
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
    edit = new_status == "edit"      # a confirmed suggestion to change: back to review, then pick its items
    if edit:
        new_status = "proposed"
    if new_status in ("confirmed", "rejected", "proposed"):
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""UPDATE match SET status=%s, confirmed_by=%s, confirmed_at=now(), updated_at=now()
                       WHERE match_id=%s AND statement_id IN (SELECT s.statement_id FROM statement s
                         JOIN account a ON a.account_id=s.account_id WHERE a.name=%s)
                       RETURNING statement_id;""", (new_status, session.get("name"), match_id, name))
        row = cur.fetchone()
        if row and edit:
            lids, tids = match_items(cur, [match_id]).get(str(match_id), ([], []))
            session["mm_edit"] = {"l": lids, "t": tids}
        conn.commit(); cur.close(); conn.close()
        if row:
            _after_review(row[0])
    return redirect(url_for("detail", name=name) + ("#sec-manual" if edit else "#sec-review"))


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
    replaced = False
    orig = request.form.get("orig") or ""
    if "|" in orig:
        ol, ot = (set(filter(None, x.split(","))) for x in orig.split("|", 1))
        if (ol, ot) != (set(lids), set(tids)):
            cur.execute("""SELECT m.match_id, array(SELECT line_id::text FROM match_statement_line WHERE match_id=m.match_id),
                                  array(SELECT txn_id::text FROM match_book_txn WHERE match_id=m.match_id)
                           FROM match m WHERE m.statement_id=%s AND m.status='proposed' AND m.created_by<>'user';""", (sid,))
            old = [m for m, ls, ts in cur.fetchall() if (set(ls), set(ts)) == (ol, ot)]
            if old:
                cur.execute("""UPDATE match SET status='rejected', confirmed_by=%s, confirmed_at=now(), updated_at=now()
                               WHERE match_id = ANY(%s::uuid[]);""", (session.get("name"), [str(m) for m in old]))
                replaced = True
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
                             + (f" The difference of {_money(delta)} shows under amount differences." if delta else "")
                             + (" The suggestion you edited is marked rejected." if replaced else ""))
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


RECORD_BG_MIN = 4          # this many lines or more are recorded in the background
RECORD_STALE_SECS = 600    # a recording job silent this long was lost (a restart); say so
TWIN_DAYS = 3              # an entry QuickBooks already has: same amount, dated this close


def _record_key(name):
    return f"record_job:{name}"


def record_job(name):
    """The account's latest background recording, or None. A running one gone silent is 'stalled'."""
    try:
        job = json.loads(get_config(_record_key(name)) or "null")
    except ValueError:
        return None
    if job and job.get("state") == "running" and time.time() - (job.get("beat") or 0) > RECORD_STALE_SECS:
        job["state"] = "stalled"
    return job


def start_record_job(name, form, ids, user):
    """Record these lines in a background thread; the page shows progress and then the result."""
    job = {"state": "running", "by": user, "started": time.time(), "beat": time.time(), "total": len(ids),
           "n": 0, "done": 0, "msg": ""}
    set_config(_record_key(name), json.dumps(job))

    def progress(n, total, done):
        job.update(n=n, total=total, done=done, beat=time.time())
        set_config(_record_key(name), json.dumps(job))

    def run():
        try:
            msg = _record_run(name, form, ids, user, progress)
            job.update(state="done", msg=msg, n=job["total"], finished=time.time())
        except Exception as e:
            job.update(state="failed", finished=time.time(),
                       msg=f"Recording stopped: {e}. Lines already recorded show as recorded; check the rest.")
        set_config(_record_key(name), json.dumps(job))

    threading.Thread(target=run, daemon=True, name="qbo-record").start()


def _words(t):
    stop = {"the", "and", "for", "from", "with", "ltd", "limited", "payment", "charges", "charge", "fee", "fees"}
    return {w for w in re.findall(r"[a-z0-9]{3,}", (t or "").lower()) if w not in stop}


def same_text(a, b):
    """Same description or payee, allowing for the bank's extra words: most of the shorter one's words
    appear in the other (or the two are equal)."""
    if (a or "").strip().lower() == (b or "").strip().lower() and (a or "").strip():
        return True
    wa, wb = _words(a), _words(b)
    if not wa or not wb:
        return False
    return len(wa & wb) / min(len(wa), len(wb)) >= 0.5


@app.route("/account/<name>/record", methods=["POST"])
def record(name):
    """Record selected unmatched bank lines in QuickBooks. Four lines or more go to a background job,
    so a long batch isn't cut off by the server's time limit (the page shows its progress)."""
    form = request.form.to_dict(flat=True)
    ids = [form["only"]] if form.get("only") else list(dict.fromkeys(request.form.getlist("sel")))
    ids = [i for i in ids if _is_uuid(i)]       # only real bank lines (a tampered id is simply ignored)
    user = session.get("name")
    back = redirect(url_for("detail", name=name) + "#sec-record")
    job = record_job(name)
    if job and job.get("state") == "running":
        session["detail_msg"] = (f"Still recording {job.get('total')} lines for this account "
                                 f"({job.get('n')} done). Wait for it to finish, then record the rest.")
        return back
    if SYNC_IN_BACKGROUND and len(ids) >= RECORD_BG_MIN:
        start_record_job(name, form, ids, user)
        return back
    session["detail_msg"] = _record_run(name, form, ids, user)
    return back


def _is_uuid(v):
    try:
        uuid.UUID(str(v))
        return True
    except ValueError:
        return False


@app.route("/account/<name>/record_status")
def record_status(name):
    job = record_job(name) or {}
    return {k: job.get(k) for k in ("state", "n", "total", "done", "msg")}


def _record_run(name, form, ids, user, progress=None):
    """Record these bank lines in QuickBooks: Purchase for money out, Deposit for money in, a Transfer
    to another of your own accounts, a customer Payment, a split or a hedge. A line QuickBooks already
    has is matched to it instead. Returns the message saying how it went."""
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id, source_account_id, type, currency FROM account WHERE name=%s LIMIT 1;", (name,))
    arow = cur.fetchone()
    if not arow:
        cur.close(); conn.close(); return "Unknown account."
    acct_uuid, acct_qbo, atype, ccy = arow
    s = _latest_statement(cur, acct_uuid)
    if not ids or not s or not acct_qbo:
        cur.close(); conn.close()
        return "Nothing to record." if acct_qbo else "This account isn't linked to QuickBooks."
    coa = {a["id"]: a for a in load_coa(cur, ccy)}
    home = qbo_home_currency(cur)
    foreign = ccy if ccy and home and ccy != home else None     # e.g. USD in a UGX company
    rates = {}
    xt = {a["id"]: a for a in transfer_targets(cur, acct_qbo, atype, ccy)}
    split_ok = {**{k: a for k, a in coa.items() if a["type"] not in ("Accounts Receivable", "Accounts Payable")},
                **{a["id"]: a for a in split_banks(cur, acct_qbo, ccy)}}
    x_ccy = cross_currency(cur, ccy, home)
    cust_rows = load_customers(cur, ccy, x_ccy)
    customers = {c["id"]: c["n"] for c in cust_rows}
    cust_ccy = {c["id"]: c["c"] for c in cust_rows}
    vendors = {v["id"]: v["n"] for v in load_vendors(cur, ccy)}
    cur.execute("""SELECT currency, qbo_id, coalesce(fqn, name) FROM qbo_coa
                   WHERE account_type='Accounts Receivable' AND coalesce(active, true) ORDER BY qbo_id;""")
    ar_by_ccy = {}
    for c_, i_, f_ in cur.fetchall():
        ar_by_ccy.setdefault(c_, {"id": i_, "fqn": f_, "type": "Accounts Receivable", "name": f_})
    parent_of = {c["id"]: c.get("p") for c in cust_rows}
    hacc = hedge_accounts(cur, ccy if ccy != home else "USD")
    hnames = {}
    for key in ("transit", "transit_home"):
        if hacc.get(key):
            hnames[hacc[key]["id"]] = hacc[key]["name"]
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
    pool = book_pool(cur, acct_uuid, s[0], s[1], s[2])
    cur.execute("""SELECT mbt.txn_id::text FROM match m JOIN match_book_txn mbt ON mbt.match_id=m.match_id
                   WHERE m.statement_id=%s AND m.status='confirmed';""", (s[0],))
    taken = {r[0] for r in cur.fetchall()}
    cur.close(); conn.close()
    done, problems, skipped, no_acct, twins = 0, [], 0, 0, 0
    token, touched = None, set()
    for n_line, (lid, d, amt, desc) in enumerate(lines):
        if progress:
            progress(n_line, len(lines), done)
        lid = str(lid)
        label = f"{d} {desc[:30]}"
        out = _money_out(amt, atype)
        if form.get(f"hedge_{lid}") == "1":
            r_ = _record_hedge(lid, d, amt, desc, label, out, acct_uuid, acct_qbo, ccy, home, hacc, hnames, names,
                               dups, touched, token, form, user)
            token = r_.get("token") or token
            if r_.get("problem"):
                problems.append(r_["problem"])
            if r_.get("done"):
                done += 1
            continue
        # A split: several accounts for one bank line (an FX hedge with its gain or loss).
        parts = None
        raw_split = (form.get(f"split_{lid}") or "").strip()
        if raw_split:
            try:
                parts = [(str(p["a"]), Decimal(str(p["v"]).replace(",", ""))) for p in json.loads(raw_split)
                         if str(p.get("a") or "") or str(p.get("v") or "").strip()]
            except Exception:
                problems.append(f"{label}: the split couldn't be read; open it and check each line"); continue
            if not parts:
                parts = None
            elif any(a not in split_ok for a, _ in parts):
                problems.append(f"{label}: every split line needs an account from the list"); continue
            elif sum((v for _, v in parts), Decimal(0)) != abs(amt):
                problems.append(f"{label}: the split lines add up to {_money(sum((v for _, v in parts), Decimal(0)))}, "
                                f"not {_money(abs(amt))}"); continue
        pick = form.get(f"acct_{lid}") or ""
        picked_cust = None
        if pick.startswith("cust:") and not parts:
            # A student or family: a payment into their account (Accounts Receivable in that currency).
            picked_cust = pick[5:]
            acc = ar_by_ccy.get(cust_ccy.get(picked_cust)) or {"id": "", "fqn": "Accounts Receivable",
                                                               "type": "Accounts Receivable"}
        else:
            acc = None if parts else (coa.get(pick) or xt.get(pick))
        if not acc and not parts:
            if form.get("only"):
                problems.append(f"{label}: choose an account")
            else:
                no_acct += 1     # left empty on purpose: not recorded, stays in the list
            continue
        is_xfer = bool(acc and acc.get("xfer"))
        is_ar = bool(acc and acc["type"] == "Accounts Receivable")
        if atype == "credit_card" and not out and not is_xfer:
            problems.append(f"{label}: choose the bank the card was paid from (refunds are recorded in QuickBooks)"); continue
        if is_ar and out:
            problems.append(f"{label}: money out can't be a customer payment; record refunds in QuickBooks"); continue
        payee = (form.get(f"payee_{lid}") or "").strip()
        # Only reuse the learned payee ID if the user kept the suggested payee name.
        ref = form.get(f"pref_{lid}") or None
        if payee != (form.get(f"psug_{lid}") or "").strip():
            ref = None
        cust = picked_cust or form.get(f"cust_{lid}") or ""
        kids = None
        is_ap = bool(acc and acc["type"] == "Accounts Payable")
        if is_ap:
            # A payable (Rent payable, say) is paid against the supplier QuickBooks owes.
            vend = cust if cust in vendors else (ref or "").split(":", 1)[1] if (ref or "").startswith("Vendor:") else ""
            if vend not in vendors and payee:
                vend = next((i for i, n in vendors.items() if n.strip().lower() == payee.lower()), "")
            if vend not in vendors:
                problems.append(f"{label}: choose the supplier this {acc['name'] if acc.get('name') else 'payable'} is paid to"); continue
            payee, ref = vendors[vend], f"Vendor:{vend}"
        if is_ar:
            if cust not in customers and ref and ref.startswith("Customer:") and ref.split(":", 1)[1] in customers:
                cust = ref.split(":", 1)[1]
            if cust not in customers:
                problems.append(f"{label}: choose the customer it was received from"); continue
            payee = customers[cust]
            # A parent's lump sum split between some or all of their children (sub-customers).
            raw_kids = (form.get(f"kids_{lid}") or "").strip()
            if raw_kids:
                try:
                    kids = [(str(k["c"]), Decimal(str(k["v"]).replace(",", ""))) for k in json.loads(raw_kids)
                            if str(k.get("v") or "").strip() not in ("", "0", "0.00")]
                except Exception:
                    problems.append(f"{label}: the split between children couldn't be read"); continue
                family = {cust} | {c for c, p in parent_of.items() if p == cust}
                if not kids:
                    kids = None
                elif any(c not in family for c, _ in kids) or any(v <= 0 for _, v in kids):
                    problems.append(f"{label}: split only between {payee} and their children, with amounts above zero"); continue
                elif sum((v for _, v in kids), Decimal(0)) != abs(amt):
                    problems.append(f"{label}: the children's amounts add up to "
                                    f"{_money(sum((v for _, v in kids), Decimal(0)))}, not {_money(abs(amt))}"); continue
        rate = None
        pay_ccy = cust_ccy.get(cust) if is_ar else None
        if is_ar and pay_ccy and ccy and pay_ccy != ccy:
            # UGX received for a student's USD account: paid in USD at the rate, deposited as the UGX.
            typed = (form.get(f"rate_{lid}") or "").replace(",", "").strip()
            try:
                if typed:
                    x_rate = Decimal(typed); assert x_rate > 0
                else:
                    token = token or qbo_token()
                    x_rate = Decimal(str(rates[(pay_ccy, d)] if (pay_ccy, d) in rates else qbo_exchange_rate(token, pay_ccy, d)))
                    rates[(pay_ccy, d)] = x_rate
            except Exception:
                problems.append(f"{label}: type the rate ({ccy} per {pay_ccy}) to pay it to the student's {pay_ccy} account"
                                if not typed else f"{label}: the rate '{typed}' isn't a number"); continue
        else:
            x_rate = None
        if foreign:
            typed = (form.get(f"rate_{lid}") or "").replace(",", "").strip()
            if typed:
                try:
                    rate = float(typed)
                    assert rate > 0
                except (ValueError, AssertionError):
                    problems.append(f"{label}: the rate '{typed}' isn't a number"); continue
            else:
                try:
                    token = token or qbo_token()
                    rate = rates[d] if d in rates else qbo_exchange_rate(token, foreign, d)
                    rates[d] = rate
                except Exception:
                    problems.append(f"{label}: QuickBooks has no {foreign} rate for {d}. Type the rate "
                                    f"({home} per {foreign}) in the Rate box and record again"); continue
        if not kids:
            twin_days = 0 if is_bank_charge(desc, amt) else TWIN_DAYS    # a bank charge: the same day only
            twin = next((t for t in pool if str(t[0]) not in taken and t[2] == amt and abs((t[1] - d).days) <= twin_days
                         and (same_text(t[3], desc) or (payee and same_text(t[3], payee)))), None)
            if twin:
                # QuickBooks already has it: pair the line with that entry; nothing is created or changed.
                c2 = get_conn(); k2 = c2.cursor()
                _confirm_match(k2, s[0], [lid], [str(twin[0])], user)
                k2.execute("DELETE FROM record_draft WHERE line_id=%s;", (lid,))
                c2.commit(); k2.close(); c2.close()
                taken.add(str(twin[0])); twins += 1
                continue
        if dups.get(lid) and not form.get(f"dupok_{lid}"):
            x = dups[lid][0]
            problems.append(f"{label}: QuickBooks may already have it ({x['date']}, {_money(x['amount'])}) — "
                            f"match it instead, or tick 'Not a duplicate'"); continue
        if not _claim_writeback(lid, user):
            skipped += 1; continue
        used_ref, fqn = None, (acc or {}).get("fqn")
        try:
            token = token or qbo_token()
            if parts:
                entity, new_id, jnote = qbo_record_journal(token, acct_qbo, out, parts, d, desc, foreign, rate)
                fqn = " + ".join(split_ok[a]["fqn"] for a, _ in parts)
                payee = ""
            elif is_xfer:
                frm, to = transfer_ends(acct_qbo, acc["id"], amt, atype)
                entity, new_id, ent = qbo_record_transfer(token, frm, to, abs(amt), d, desc, foreign, rate)
                payee = ""
            elif is_ar and kids:
                paid = []
                for c_id, v in kids:
                    _e, pid = qbo_record_payment(token, acct_qbo, c_id, *_pay_amount(v, x_rate, foreign, rate, pay_ccy),
                                                 d=d, desc=desc)
                    paid.append((c_id, v, pid))
                entity, new_id, used_ref = "Payment", ",".join(p for _, _, p in paid), f"Customer:{cust}"
            elif is_ar:
                entity, new_id = qbo_record_payment(token, acct_qbo, cust, *_pay_amount(abs(amt), x_rate, foreign, rate, pay_ccy),
                                                    d=d, desc=desc)
                used_ref = f"Customer:{cust}"
            else:
                entity, new_id, used_ref = qbo_record_line(token, acct_qbo, atype, out, acc["id"], abs(amt), d, desc,
                                                           payee, ref, foreign, rate)
        except Exception as e:
            err = f"HTTP {e.code}: {e.read().decode(errors='ignore')[:200]}" if isinstance(e, urllib.error.HTTPError) else str(e)
            c2 = get_conn(); k2 = c2.cursor()
            k2.execute("UPDATE writeback_log SET status='failed', error=%s WHERE line_id=%s;", (err, lid))
            c2.commit(); k2.close(); c2.close()
            problems.append(f"{label}: QuickBooks said {err}"); continue
        c2 = get_conn(); k2 = c2.cursor()
        k2.execute("""UPDATE writeback_log SET status='done', qbo_type=%s, qbo_id=%s, account_fqn=%s, payee=%s
                      WHERE line_id=%s;""", (entity, new_id or None, fqn, payee or None, lid))
        k2.execute("DELETE FROM record_draft WHERE line_id=%s;", (lid,))
        if new_id and is_xfer:
            ent = {**ent, "FromAccountRef": {"value": frm}, "ToAccountRef": {"value": to}, "Amount": abs(amt)}
            touched.update(a for a in store_transfer(k2, new_id, ent, d, desc, names) if a != str(acct_uuid))
        elif new_id and kids:
            # One payment per child; the bank line is matched to all of them.
            tids = []
            for c_id, v, pid in paid:
                k2.execute("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount,
                                  currency, description, counterparty, cleared_status, last_modified, counterparty_ref)
                              VALUES (%s,%s,%s,'Payment',%s,%s,%s,%s,%s,'unknown',now(),%s)
                              ON CONFLICT (account_id, source_txn_type, source_txn_id) DO UPDATE SET amount=EXCLUDED.amount
                              RETURNING txn_id;""",
                           (ORG_ID, acct_uuid, pid, d, v, ccy or "USD", desc, customers[c_id], f"Customer:{c_id}"))
                tids.append(str(k2.fetchone()[0]))
            _confirm_match(k2, s[0], [lid], tids, user)
        elif new_id:
            book_amt = abs(amt) if (atype == "credit_card" or not out) else -abs(amt)
            # The same row the next sync reads back (journal entries as the sync describes them).
            b_desc, b_who, b_cat = ((jnote, "Journal entry", split_ok[parts[0][0]]["fqn"]) if parts else
                                    (desc, payee or None, fqn))
            k2.execute("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount,
                              currency, description, counterparty, reference, category, cleared_status, last_modified,
                              counterparty_ref)
                          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,%s,'unknown',now(),%s)
                          ON CONFLICT (account_id, source_txn_type, source_txn_id) DO NOTHING;""",
                       (ORG_ID, acct_uuid, new_id, entity, d, book_amt, ccy or "USD", b_desc, b_who, b_cat, used_ref))
            # Pair the line with the entry made for it now, so a re-match can't give that entry to an
            # identical line (the bank's charges repeat to the cent on the same day).
            k2.execute("""SELECT txn_id::text FROM book_txn bt WHERE account_id=%s AND source_txn_type=%s AND source_txn_id=%s
                            AND NOT EXISTS (SELECT 1 FROM match_book_txn mbt JOIN match m ON m.match_id=mbt.match_id
                                            WHERE mbt.txn_id=bt.txn_id AND m.status='confirmed');""",
                       (acct_uuid, entity, new_id))
            mine = k2.fetchone()
            if mine:
                _confirm_match(k2, s[0], [lid], [mine[0]], user)
        if not parts:
            # Every recorded line teaches the suggestion engine (strongest tier).
            k2.execute("""INSERT INTO payee_correction (org_id, payee, category, money_out, vendor, vendor_ref, currency)
                          VALUES (%s,%s,%s,%s,%s,%s,%s);""", (ORG_ID, desc, acc["fqn"], out, payee or None, used_ref, ccy))
        c2.commit(); k2.close(); c2.close()
        done += 1
    if done:
        _after_review(s[0])
    if touched:   # the other side of each transfer can clear on its own statement straight away
        c2 = get_conn(); k2 = c2.cursor(); rematch_open(k2, touched); k2.close(); c2.close()
    asked = len(ids)
    msg = (f"Recorded {done} of {asked} selected transaction{'' if asked == 1 else 's'} in QuickBooks."
           if done and asked > 1 else f"Recorded {done} transaction{'' if done == 1 else 's'} in QuickBooks." if done else "")
    if twins:
        msg += (f" {twins} {'was' if twins == 1 else 'were'} already in QuickBooks, so "
                f"{'it was' if twins == 1 else 'they were'} matched to the existing "
                f"entr{'y' if twins == 1 else 'ies'} instead (nothing created or changed).")
    if skipped:
        msg += f" Skipped {skipped} already recorded or in progress."
    if no_acct:
        msg += (f" {no_acct} line{' has' if no_acct == 1 else 's have'} no account, so "
                f"{'it was' if no_acct == 1 else 'they were'} left for later.")
    if problems:
        msg += " Not recorded: " + "; ".join(problems[:5]) + (" …" if len(problems) > 5 else "")
    return msg.strip() or "Nothing to record."


def _confirm_match(cur, sid, lids, tids, user=None):
    """Match bank lines to the entries just recorded for them, as if matched by hand."""
    mid = str(uuid.uuid4())
    cur.execute("""INSERT INTO match (match_id, org_id, statement_id, status, match_type, confidence, amount_delta,
                                      created_by, confirmed_by, confirmed_at)
                   VALUES (%s,%s,%s,'confirmed','manual',1,0,'user',%s,now());""", (mid, ORG_ID, sid, user or "user"))
    execute_values(cur, "INSERT INTO match_statement_line (match_id, line_id) VALUES %s", [(mid, l) for l in lids])
    execute_values(cur, "INSERT INTO match_book_txn (match_id, txn_id) VALUES %s", [(mid, t) for t in tids])


def _record_hedge(lid, d, amt, desc, label, out, acct_uuid, acct_qbo, ccy, home, hacc, hnames, names, dups, touched, token,
                  form=None, user=None):
    """One leg of a forward deal, the way the books have always recorded them:
    USD out  -> Transfer USD bank -> FX in Transit (USD) -> FX in Transit UGX, at the month's rate;
    UGX in   -> Deposit: FX in Transit UGX (USD x the month's rate) + Forex Gain (the difference)."""
    hi = hedge_info(desc)
    if not hi:
        return {"problem": f"{label}: no forward deal number in the bank's text"}
    try:
        rate = Decimal((form.get(f"hedge_rate_{lid}") or "").replace(",", "").strip())
        assert rate > 0
    except Exception:
        return {"problem": f"{label}: type this month's transaction rate for deal {hi['deal']}"}
    need = ("transit", "transit_home") if out else ("transit_home", "gain")
    missing = [k for k in need if not hacc.get(k)]
    if missing:
        what = {"transit": f"FX in Transit ({hi['ccy']})", "transit_home": f"FX in Transit {home}", "gain": "Forex Gain"}
        return {"problem": f"{label}: no {', '.join(what[k] for k in missing)} account in QuickBooks"}
    if dups.get(lid) and not form.get(f"dupok_{lid}"):
        x = dups[lid][0]
        return {"problem": f"{label}: QuickBooks may already have it ({x['date']}, {_money(x['amount'])}) — "
                           f"match it instead, or tick 'Not a duplicate'"}
    usd = None
    if not out:
        try:
            usd = Decimal((form.get(f"hedge_usd_{lid}") or "").replace(",", "").strip())
            assert usd > 0
        except Exception:
            return {"problem": f"{label}: type the {hi['ccy']} amount of deal {hi['deal']}"}
    if not _claim_writeback(lid, user):
        return {}
    conn = get_conn(); cur = conn.cursor()
    cur.execute("INSERT INTO app_config (key, value) VALUES (%s,%s) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value;",
                (f"fx_rate:{hi['ccy']}:{str(d)[:7]}", str(rate)))
    conn.commit(); cur.close(); conn.close()
    ids, fqn = [], None
    try:
        token = token or qbo_token()
        if out:
            names = {**names, **hnames}
            e1, id1, ent1 = qbo_record_transfer(token, acct_qbo, hacc["transit"]["id"], abs(amt), d, desc, ccy, float(rate))
            ids.append(id1)
            e2, id2, ent2 = qbo_record_transfer(token, hacc["transit"]["id"], hacc["transit_home"]["id"], abs(amt), d, desc,
                                                ccy, float(rate))
            ids.append(id2)
            entity, fqn = "Transfer", f"{hacc['transit']['fqn']} -> {hacc['transit_home']['fqn']}"
        else:
            principal = (usd * rate).quantize(Decimal("0.01"))
            entity, id1 = qbo_record_hedge_receipt(token, acct_qbo, hacc["transit_home"]["id"], hacc["gain"]["id"],
                                                   abs(amt), principal, d, desc)
            ids.append(id1)
            fqn = f"{hacc['transit_home']['fqn']} + {hacc['gain']['fqn']}"
    except Exception as e:
        err = f"HTTP {e.code}: {e.read().decode(errors='ignore')[:200]}" if isinstance(e, urllib.error.HTTPError) else str(e)
        c2 = get_conn(); k2 = c2.cursor()
        if ids:   # the first transfer went through: keep it on record so it isn't posted twice
            k2.execute("UPDATE writeback_log SET status='done', qbo_type='Transfer', qbo_id=%s, error=%s WHERE line_id=%s;",
                       (",".join(ids), err, lid))
        else:
            k2.execute("UPDATE writeback_log SET status='failed', error=%s WHERE line_id=%s;", (err, lid))
        c2.commit(); k2.close(); c2.close()
        return {"token": token, "problem": (f"{label}: the transfer into FX in Transit was recorded (#{ids[0]}), but "
                                            f"FX in Transit -> FX in Transit {home} failed: {err}. Post that one in QuickBooks."
                                            if ids else f"{label}: QuickBooks said {err}")}
    c2 = get_conn(); k2 = c2.cursor()
    k2.execute("""UPDATE writeback_log SET status='done', qbo_type=%s, qbo_id=%s, account_fqn=%s WHERE line_id=%s;""",
               (entity, ",".join(ids), fqn, lid))
    k2.execute("DELETE FROM record_draft WHERE line_id=%s;", (lid,))
    if out:
        for tid, ent, frm, to in ((ids[0], ent1, acct_qbo, hacc["transit"]["id"]),
                                  (ids[1], ent2, hacc["transit"]["id"], hacc["transit_home"]["id"])):
            ent = {**ent, "FromAccountRef": {"value": frm}, "ToAccountRef": {"value": to}, "Amount": abs(amt),
                   "CurrencyRef": {"value": ccy}, "ExchangeRate": float(rate)}
            touched.update(a for a in store_transfer(k2, tid, ent, d, desc, names) if a != str(acct_uuid))
        k2.execute("""INSERT INTO hedge_leg (line_id, deal, usd, rate, txn_date, qbo_ids) VALUES (%s,%s,%s,%s,%s,%s)
                      ON CONFLICT (line_id) DO UPDATE SET usd=EXCLUDED.usd, rate=EXCLUDED.rate, qbo_ids=EXCLUDED.qbo_ids;""",
                   (lid, hi["deal"], abs(amt), rate, d, ",".join(ids)))
    else:
        k2.execute("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount,
                          currency, description, counterparty, category, cleared_status, last_modified)
                      VALUES (%s,%s,%s,'Deposit',%s,%s,%s,%s,NULL,%s,'unknown',now())
                      ON CONFLICT (account_id, source_txn_type, source_txn_id) DO NOTHING;""",
                   (ORG_ID, acct_uuid, ids[0], d, abs(amt), ccy or home, desc, hacc["transit_home"]["fqn"]))
    c2.commit(); k2.close(); c2.close()
    return {"token": token, "done": True}


@app.route("/account/<name>/record_save", methods=["POST"])
def record_save(name):
    """Keep what's ticked and chosen in the record table, so a refresh or another visit starts from it."""
    back = redirect(url_for("detail", name=name) + "#sec-record")
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT account_id FROM account WHERE name=%s LIMIT 1;", (name,))
    row = cur.fetchone()
    s = _latest_statement(cur, row[0]) if row else None
    ids = list(dict.fromkeys(request.form.getlist("rowid")))
    if not s or not ids:
        cur.close(); conn.close(); session["detail_msg"] = "Nothing to save."; return back
    cur.execute("SELECT line_id::text FROM statement_line WHERE statement_id=%s AND line_id = ANY(%s::uuid[]);", (s[0], ids))
    valid = [r[0] for r in cur.fetchall()]
    sel = set(request.form.getlist("sel"))
    rows = [(lid, json.dumps({"sel": lid in sel, "acct": request.form.get(f"acct_{lid}") or "",
                              "payee": request.form.get(f"payee_{lid}") or "", "cust": request.form.get(f"cust_{lid}") or "",
                              "rate": request.form.get(f"rate_{lid}") or "", "split": request.form.get(f"split_{lid}") or "",
                              "kids": request.form.get(f"kids_{lid}") or "",
                              "hedge": request.form.get(f"hedge_{lid}") == "1",
                              "hedge_rate": request.form.get(f"hedge_rate_{lid}") or "",
                              "hedge_usd": request.form.get(f"hedge_usd_{lid}") or "",
                              "dupok": bool(request.form.get(f"dupok_{lid}"))}), session.get("name") or "user")
            for lid in valid]
    if rows:
        execute_values(cur, """INSERT INTO record_draft (line_id, data, saved_by) VALUES %s
                               ON CONFLICT (line_id) DO UPDATE SET data=EXCLUDED.data, saved_by=EXCLUDED.saved_by,
                                 saved_at=now()""", rows)
    conn.commit(); cur.close(); conn.close()
    n = sum(1 for lid in valid if lid in sel)
    session["detail_msg"] = (f"Saved your selection: {n} of {len(valid)} line{'' if len(valid) == 1 else 's'} ticked, "
                             f"with the accounts and payees chosen. It stays until you record those lines or discard it.")
    return back


@app.route("/account/<name>/record_discard", methods=["POST"])
def record_discard(name):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""DELETE FROM record_draft WHERE line_id IN (SELECT sl.line_id FROM statement_line sl
                     JOIN statement s ON s.statement_id=sl.statement_id JOIN account a ON a.account_id=s.account_id
                     WHERE a.name=%s);""", (name,))
    conn.commit(); cur.close(); conn.close()
    session["detail_msg"] = "Discarded the saved selection; the suggestions are back."
    return redirect(url_for("detail", name=name) + "#sec-record")


def _record_transfer_one_side(name, lid, other_acct, back):
    """Edit's "Only the account": a Transfer between this bank line's account and another of your
    accounts whose statement isn't uploaded. Only this line is matched."""
    conn = get_conn(); cur = conn.cursor()
    try:
        uuid.UUID(lid)
    except ValueError:
        cur.close(); conn.close(); session["detail_msg"] = "Nothing to record."; return back
    cur.execute("""SELECT sl.line_id, sl.posted_date, sl.amount, coalesce(sl.description,''), s.statement_id, s.signed_off_at,
                          a.account_id, a.source_account_id, a.type, a.currency,
                          EXISTS (SELECT 1 FROM match_statement_line msl JOIN match m ON m.match_id=msl.match_id
                                  WHERE msl.line_id=sl.line_id AND m.status='confirmed')
                   FROM statement_line sl JOIN statement s ON s.statement_id=sl.statement_id
                   JOIN account a ON a.account_id=s.account_id WHERE sl.line_id=%s AND a.name=%s;""", (lid, name))
    me = cur.fetchone()
    targets = {t["id"]: t for t in transfer_targets(cur, me[7], me[8], me[9])} if me else {}
    problem = ("That bank line wasn't found. Reload and try again." if not me else
               "This line is already matched." if me[10] else
               "The statement is signed off. Reopen it to record this transfer." if me[5] else
               "This account must be linked to QuickBooks." if not me[7] else
               "Choose one of your accounts in the same currency." if other_acct not in targets else None)
    if problem:
        cur.close(); conn.close(); session["detail_msg"] = problem; return back
    cur.close(); conn.close()
    # The regular record path does the rest (claim, QuickBooks, books, matching this line).
    msg = _record_run(name, {"only": str(me[0]), f"acct_{me[0]}": other_acct, f"dupok_{me[0]}": "1"}, [str(me[0])],
                      session.get("name"))
    session["detail_msg"] = msg
    return back


@app.route("/account/<name>/transfer", methods=["POST"])
def record_transfer(name):
    """Money visibly left one of your accounts and arrived at another, and neither side is in
    QuickBooks: record ONE Transfer and match both bank lines to it."""
    back = redirect(url_for("detail", name=name) + "#sec-transfers")
    lid, other = request.form.get("line") or "", request.form.get("other") or ""
    other_acct = (request.form.get("other_acct") or "").strip()
    if not other and other_acct:
        return _record_transfer_one_side(name, lid, other_acct, back)
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


def _xfer_line_of(cur, name, lid):
    """The statement line `lid` on account `name`'s latest statement, or None."""
    try:
        uuid.UUID(lid)
    except (ValueError, TypeError):
        return None
    cur.execute("""SELECT sl.line_id FROM statement_line sl JOIN statement s ON s.statement_id=sl.statement_id
                   JOIN account a ON a.account_id=s.account_id WHERE sl.line_id=%s AND a.name=%s;""", (lid, name))
    return cur.fetchone()


@app.route("/account/<name>/transfer_dismiss", methods=["POST"])
def transfer_dismiss(name):
    """'Not a transfer': hide suggested pairings (they can be restored). One pair from its own button
    (line + other), or several ticked at once (pick = "line|other")."""
    pairs = [p.split("|", 1) for p in request.form.getlist("pick") if "|" in p]
    if request.form.get("line"):
        pairs.append([request.form.get("line"), request.form.get("other") or ""])
    conn = get_conn(); cur = conn.cursor()
    n = 0
    for lid, other in pairs[:500]:
        other = other[:80]
        if other and _xfer_line_of(cur, name, lid):
            cur.execute("""INSERT INTO transfer_dismissal (line_id, other, dismissed_by) VALUES (%s,%s,%s)
                           ON CONFLICT DO NOTHING;""", (lid, other, session.get("name") or "user"))
            n += cur.rowcount
    conn.commit()
    if n == 1:
        session["detail_msg"] = "Hidden: it won't be suggested as a transfer again. Restore it below if that was a mistake."
    elif n:
        session["detail_msg"] = f"Hidden {n} suggestions: they won't be suggested as transfers again. Restore any below if that was a mistake."
    else:
        session["detail_msg"] = "Nothing hidden: tick the suggestions that aren't transfers first."
    cur.close(); conn.close()
    return redirect(url_for("detail", name=name) + "#sec-transfers")


@app.route("/account/<name>/transfer_restore", methods=["POST"])
def transfer_restore(name):
    lid, other = request.form.get("line") or "", request.form.get("other") or ""
    conn = get_conn(); cur = conn.cursor()
    if _xfer_line_of(cur, name, lid):
        cur.execute("DELETE FROM transfer_dismissal WHERE line_id=%s AND other=%s;", (lid, other))
        conn.commit()
        session["detail_msg"] = "Restored: the suggestion is back."
    cur.close(); conn.close()
    return redirect(url_for("detail", name=name) + "#sec-transfers")


@app.route("/account/<name>/transfer_undo", methods=["POST"])
def transfer_undo(name):
    """Undo a transfer the app recorded: delete it in QuickBooks, unpair its bank lines (they go back
    to the list), and take it out of the books here. Only while none of its statements is signed off."""
    back = redirect(url_for("detail", name=name) + "#sec-transfers")
    qid = (request.form.get("qbo_id") or "").strip()
    user = session.get("name") or "user"
    conn = get_conn(); cur = conn.cursor()
    # Every bank line the app recorded as this Transfer, and every book row of it.
    cur.execute("""SELECT w.line_id::text, s.statement_id, s.signed_off_at, a.name, a.account_id
                   FROM writeback_log w JOIN statement_line sl ON sl.line_id=w.line_id
                   JOIN statement s ON s.statement_id=sl.statement_id JOIN account a ON a.account_id=s.account_id
                   WHERE w.qbo_type='Transfer' AND w.qbo_id=%s AND w.status='done';""", (qid,))
    lines = cur.fetchall()
    cur.execute("""SELECT bt.txn_id::text, bt.account_id FROM book_txn bt
                   WHERE bt.source_txn_type='Transfer' AND bt.source_txn_id=%s;""", (qid,))
    books = cur.fetchall()
    cur.execute("""SELECT DISTINCT m.match_id::text, s.signed_off_at, s.statement_id FROM match m
                   JOIN statement s ON s.statement_id=m.statement_id
                   WHERE m.match_id IN (SELECT match_id FROM match_book_txn WHERE txn_id = ANY(%s::uuid[]))
                      OR m.match_id IN (SELECT match_id FROM match_statement_line WHERE line_id = ANY(%s::uuid[]));""",
                ([b[0] for b in books], [l[0] for l in lines]))
    matches = cur.fetchall()
    problem = None
    if not qid or not any(l[3] == name for l in lines):
        problem = "That transfer wasn't recorded from this account here, so it can't be undone from this page."
    elif any(l[2] for l in lines) or any(m[1] for m in matches):
        problem = "A statement it's matched on is signed off. Reopen it first, then undo the transfer."
    if problem:
        cur.close(); conn.close(); session["detail_msg"] = problem; return back
    try:
        token = qbo_token()
        ent = qbo_read(token, "Transfer", qid)
        if ent is not None:
            qbo_delete(token, "Transfer", qid, ent.get("SyncToken", "0"))
    except Exception as e:
        err = f"HTTP {e.code}: {str(e.reason)[:200]}" if isinstance(e, urllib.error.HTTPError) else str(e)
        cur.close(); conn.close()
        session["detail_msg"] = f"Not undone: QuickBooks said {err}. Nothing was changed."; return back
    ids = [m[0] for m in matches]
    if ids:
        cur.execute("DELETE FROM match WHERE match_id = ANY(%s::uuid[]);", (ids,))
    cur.execute("""UPDATE book_txn SET is_deleted=true, updated_at=now()
                   WHERE source_txn_type='Transfer' AND source_txn_id=%s;""", (qid,))
    cur.execute("""UPDATE writeback_log SET status='failed', error='undone by ' || %s
                   WHERE qbo_type='Transfer' AND qbo_id=%s AND status='done';""", (user, qid))
    conn.commit(); cur.close(); conn.close()
    for sid in {l[1] for l in lines} | {m[2] for m in matches}:
        _after_review(sid)
    session["detail_msg"] = (f"Undone: transfer #{qid} was deleted in QuickBooks, and its bank line"
                             f"{'s are' if len(lines) > 1 else ' is'} back in the list to record again.")
    return back


@app.route("/account/<name>/transfer_change", methods=["POST"])
def transfer_change(name):
    """Edit a transfer the app recorded: move its other side to a different line (on another
    account's statement) or just a different account. Updated in place in QuickBooks (same number);
    the old counterpart line is unpaired, the new one matched. Not while a statement is signed off."""
    back = redirect(url_for("detail", name=name) + "#sec-transfers")
    qid = (request.form.get("qbo_id") or "").strip()
    other, other_acct = (request.form.get("other") or "").strip(), (request.form.get("other_acct") or "").strip()
    user = session.get("name") or "user"
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""SELECT w.line_id::text, s.statement_id, s.signed_off_at, a.name, a.account_id, a.source_account_id,
                          a.type, a.currency, sl.amount
                   FROM writeback_log w JOIN statement_line sl ON sl.line_id=w.line_id
                   JOIN statement s ON s.statement_id=sl.statement_id JOIN account a ON a.account_id=s.account_id
                   WHERE w.qbo_type='Transfer' AND w.qbo_id=%s AND w.status='done';""", (qid,))
    lines = cur.fetchall()
    me = next((l for l in lines if l[3] == name), None)
    them = None
    if other:
        try:
            uuid.UUID(other)
            cur.execute("""SELECT sl.line_id::text, s.statement_id, s.signed_off_at, a.name, a.account_id, a.source_account_id,
                                  a.type, a.currency, sl.amount,
                                  EXISTS (SELECT 1 FROM match_statement_line msl JOIN match m ON m.match_id=msl.match_id
                                          WHERE msl.line_id=sl.line_id AND m.status='confirmed')
                           FROM statement_line sl JOIN statement s ON s.statement_id=sl.statement_id
                           JOIN account a ON a.account_id=s.account_id WHERE sl.line_id=%s;""", (other,))
            them = cur.fetchone()
        except ValueError:
            them = None
        new_qbo = them[5] if them else None
    else:
        new_qbo = other_acct
    cur.execute("""SELECT bt.txn_id::text, bt.account_id FROM book_txn bt
                   WHERE bt.source_txn_type='Transfer' AND bt.source_txn_id=%s;""", (qid,))
    books = cur.fetchall()
    cur.execute("""SELECT DISTINCT m.match_id::text, s.signed_off_at, s.statement_id FROM match m
                   JOIN statement s ON s.statement_id=m.statement_id
                   WHERE m.match_id IN (SELECT match_id FROM match_book_txn WHERE txn_id = ANY(%s::uuid[]))
                      OR m.match_id IN (SELECT match_id FROM match_statement_line WHERE line_id = ANY(%s::uuid[]));""",
                ([b[0] for b in books], [l[0] for l in lines]))
    matches = cur.fetchall()
    targets = {t["id"]: t for t in transfer_targets(cur, me[5], me[6], me[7])} if me else {}
    problem = None
    if not qid or not me:
        problem = "That transfer wasn't recorded from this account here, so it can't be changed from this page."
    elif any(l[2] for l in lines) or any(m[1] for m in matches) or (them and them[2]):
        problem = "A statement it's matched on is signed off. Reopen it first, then change the transfer."
    elif other and not them:
        problem = "That bank line wasn't found. Reload and try again."
    elif them and them[9]:
        problem = "That bank line is already matched."
    elif them and (me[8] * (-1 if me[6] == "credit_card" else 1)) != -(them[8] * (-1 if them[6] == "credit_card" else 1)):
        problem = "The two lines aren't the same money moving in opposite directions."
    elif not new_qbo or new_qbo not in targets:
        problem = "Choose one of your other accounts in the same currency (linked to QuickBooks)."
    if problem:
        cur.close(); conn.close(); session["detail_msg"] = problem; return back
    try:
        token = qbo_token()
        ent = qbo_read(token, "Transfer", qid)
        if ent is None:
            raise LookupError
        mine = me[5]
        if (ent.get("FromAccountRef") or {}).get("value") == mine:
            old_qbo, side = (ent.get("ToAccountRef") or {}).get("value"), "ToAccountRef"
        else:
            old_qbo, side = (ent.get("FromAccountRef") or {}).get("value"), "FromAccountRef"
        body = {k: v for k, v in ent.items() if k not in ("MetaData", "domain", "sparse")}
        body[side] = {"value": new_qbo}
        if old_qbo != new_qbo:
            qbo_post(token, "Transfer", body)
    except LookupError:
        cur.close(); conn.close()
        session["detail_msg"] = f"Transfer #{qid} isn't in QuickBooks any more. Use Undo to tidy it up here."; return back
    except Exception as e:
        err = f"HTTP {e.code}: {str(e.reason)[:200]}" if isinstance(e, urllib.error.HTTPError) else str(e)
        cur.close(); conn.close()
        session["detail_msg"] = f"Not changed: QuickBooks said {err}. Nothing was changed."; return back
    # Here: unpair everything on the old side (its book row and any counterpart line), keep this line's match.
    my_book = {b[0] for b in books if str(b[1]) == str(me[4])}
    old_rows = [b[0] for b in books if str(b[1]) != str(me[4])]
    old_lines = [l[0] for l in lines if l[0] != me[0]]
    cur.execute("""SELECT DISTINCT m.match_id::text, m.statement_id FROM match m
                   WHERE m.match_id IN (SELECT match_id FROM match_book_txn WHERE txn_id = ANY(%s::uuid[]))
                      OR m.match_id IN (SELECT match_id FROM match_statement_line WHERE line_id = ANY(%s::uuid[]));""",
                (old_rows, old_lines))
    old_matches = cur.fetchall()
    touched = {m[1] for m in old_matches} | {me[1]}
    if old_matches:
        cur.execute("DELETE FROM match WHERE match_id = ANY(%s::uuid[]);", ([m[0] for m in old_matches],))
    if old_rows:
        cur.execute("UPDATE book_txn SET is_deleted=true, updated_at=now() WHERE txn_id = ANY(%s::uuid[]);", (old_rows,))
    if old_lines:
        cur.execute("""UPDATE writeback_log SET status='failed', error='transfer changed by ' || %s
                       WHERE line_id = ANY(%s::uuid[]);""", (user, old_lines))
    cur.execute("SELECT qbo_id, name FROM qbo_coa WHERE qbo_id = ANY(%s);", ([new_qbo, me[5]],))
    names = dict(cur.fetchall())
    names.setdefault(me[5], me[3])
    cur.execute("UPDATE writeback_log SET account_fqn=%s WHERE line_id=%s;", (names.get(new_qbo), me[0]))
    # The new side in the books here (the next sync writes the same row), and its line matched to it.
    ent2 = {**body, "Amount": abs(me[8])}
    d = ent2.get("TxnDate")
    desc = (ent2.get("PrivateNote") or "").replace("Recorded from bank reconciliation: ", "")
    store_transfer(cur, qid, ent2, d, desc, names)
    cur.execute("""UPDATE book_txn SET is_deleted=false, updated_at=now() WHERE source_txn_type='Transfer'
                   AND source_txn_id=%s AND account_id <> %s AND account_id IN
                   (SELECT account_id FROM account WHERE source_account_id=%s) RETURNING txn_id::text, account_id;""",
                (qid, me[4], new_qbo))
    new_row = cur.fetchone()
    matched = False
    if them:
        cur.execute("""INSERT INTO writeback_log (line_id, status, qbo_type, qbo_id, account_fqn, created_by)
                       VALUES (%s,'done','Transfer',%s,%s,%s)
                       ON CONFLICT (line_id) DO UPDATE SET status='done', qbo_type='Transfer', qbo_id=EXCLUDED.qbo_id,
                         account_fqn=EXCLUDED.account_fqn, error=NULL, created_by=EXCLUDED.created_by;""",
                    (them[0], qid, names.get(me[5]), user))
        touched.add(them[1])
        if new_row:
            cur.execute("SELECT period_start, period_end FROM statement WHERE statement_id=%s;", (them[1],))
            ps, pe = cur.fetchone()
            if new_row[0] in {str(t[0]) for t in book_pool(cur, them[4], them[1], ps, pe)}:
                _confirm_match(cur, them[1], [them[0]], [new_row[0]], user)
                matched = True
    conn.commit(); cur.close(); conn.close()
    for sid_ in touched:
        _after_review(sid_)
    session["detail_msg"] = (f"Changed: transfer #{qid} now goes {'to' if side == 'ToAccountRef' else 'from'} "
                             f"{names.get(new_qbo) or 'the new account'} in QuickBooks"
                             + (" and is matched to that bank line." if matched else
                                " (the old counterpart line is back in its list)." if old_lines else "."))
    return back


@app.route("/account/<name>/record_reset", methods=["POST"])
def record_reset(name):
    """After an interrupted write-back, the user checked QuickBooks and it isn't there: allow a retry."""
    lid = request.form.get("reset")
    if lid:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT account_id FROM account WHERE name=%s LIMIT 1;", (name,))
        a = cur.fetchone()
        again = bool(a and taken_elsewhere(cur, a[0], [lid]))   # its entry went to an identical line
        cur.execute("""UPDATE writeback_log SET status='failed', error='cleared by ' || %s
                       WHERE line_id=%s AND (status='pending' OR (status='done' AND %s));""",
                    (session.get("name") or "user", lid, again))
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
            cur.execute("SELECT prepared_by FROM statement WHERE statement_id=%s;", (sid,))
            prep = (cur.fetchone() or [None])[0]
            if rule("two_person") and prep and prep == session.get("name") and not session.get("is_admin"):
                session["detail_msg"] = ("Not signed off: you prepared this reconciliation, so a second person "
                                         "(or an admin) must sign it off.")
                cur.close(); conn.close()
                return redirect(url_for("detail", name=name))
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
            log_activity("signed off the reconciliation" + (" (unbalanced: " + note + ")" if override else ""), name)
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


HISTORY_TEMPLATE = """<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>{{ name }} history · ReconBook</title>""" + CSS + """</head><body>
""" + SHELL_TOP + """<div class=wrap>
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
</div>""" + SHELL_END + """</body></html>"""


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
    return redirect(url_for("settings") if request.form.get("to") == "settings" else url_for("dashboard"))


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
    return redirect(url_for("settings") if request.form.get("to") == "settings" else url_for("dashboard"))


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
    return redirect(url_for("settings") if request.form.get("to") == "settings" else url_for("dashboard"))


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
    return redirect(url_for("settings") if request.form.get("to") == "settings" else url_for("dashboard"))


@app.route("/sync/status")
def sync_status():
    job = sync_job()
    return {"state": job.get("state") or "none", "step": job.get("step") or "", "msg": job.get("msg") or ""}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))