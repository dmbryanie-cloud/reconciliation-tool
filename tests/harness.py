"""Shared test harness.

Runs the app against a throwaway in-memory Postgres (PGlite, started on demand) with every
QuickBooks call mocked by the suites -- nothing here ever talks to a real QuickBooks company.
"""
import atexit
import os
import shutil
import socket
import subprocess
import sys
import time

import psycopg2

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
PORT = int(os.environ.get("TEST_PG_PORT", "54329"))
DB_URL = f"postgresql://postgres@127.0.0.1:{PORT}/postgres?sslmode=disable"
PASSWORD = "testpw"

# The tables the app expects to already exist (mirrors docs/schema.sql). The app's own startup
# code adds its extra columns and auxiliary tables on import, exactly as it does in production.
SCHEMA = """
CREATE TABLE app_config (key text PRIMARY KEY, value text);
CREATE TABLE qbo_auth (id int PRIMARY KEY, refresh_token text, realm_id text, updated_at timestamptz DEFAULT now());
CREATE TABLE account (account_id uuid PRIMARY KEY DEFAULT gen_random_uuid(), org_id uuid NOT NULL,
  connection_id uuid NOT NULL, source_account_id text NOT NULL, name text NOT NULL, type text NOT NULL,
  currency character(3) NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE book_txn (txn_id uuid PRIMARY KEY DEFAULT gen_random_uuid(), org_id uuid NOT NULL, account_id uuid NOT NULL REFERENCES account,
  source_txn_id text NOT NULL, source_txn_type text NOT NULL, posted_date date NOT NULL, amount numeric NOT NULL,
  currency character(3) NOT NULL, description text, counterparty text, reference text,
  cleared_status text NOT NULL DEFAULT 'unknown', is_void boolean NOT NULL DEFAULT false, is_deleted boolean NOT NULL DEFAULT false,
  last_modified timestamptz NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
  category text, UNIQUE (account_id, source_txn_type, source_txn_id));
CREATE TABLE statement (statement_id uuid PRIMARY KEY DEFAULT gen_random_uuid(), org_id uuid NOT NULL, account_id uuid NOT NULL REFERENCES account,
  period_start date NOT NULL, period_end date NOT NULL, opening_balance numeric NOT NULL, closing_balance numeric NOT NULL,
  currency character(3) NOT NULL, source_format text, created_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE TABLE statement_line (line_id uuid PRIMARY KEY DEFAULT gen_random_uuid(), org_id uuid NOT NULL,
  statement_id uuid NOT NULL REFERENCES statement ON DELETE CASCADE, posted_date date NOT NULL, amount numeric NOT NULL,
  currency character(3) NOT NULL, description text, counterparty text, dedupe_key text NOT NULL, UNIQUE (statement_id, dedupe_key));
CREATE TABLE match (match_id uuid PRIMARY KEY DEFAULT gen_random_uuid(), org_id uuid NOT NULL,
  statement_id uuid NOT NULL REFERENCES statement ON DELETE CASCADE, status text NOT NULL DEFAULT 'proposed',
  match_type text NOT NULL, confidence numeric, amount_delta numeric NOT NULL DEFAULT 0,
  created_by text NOT NULL DEFAULT 'engine', confirmed_by text, confirmed_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE match_statement_line (match_id uuid REFERENCES match ON DELETE CASCADE,
  line_id uuid REFERENCES statement_line ON DELETE CASCADE, PRIMARY KEY (match_id, line_id));
CREATE TABLE match_book_txn (match_id uuid REFERENCES match ON DELETE CASCADE,
  txn_id uuid REFERENCES book_txn, PRIMARY KEY (match_id, txn_id));
"""

ORG = "00000000-0000-0000-0000-000000000001"
CONN = "00000000-0000-0000-0000-0000000000c1"


def account_sql(*accts):
    """INSERT for (account_uuid, qbo_id, name, type) tuples, all in UGX."""
    vals = ",\n  ".join(f"('{u}','{ORG}','{CONN}','{q}','{n}','{t}','UGX')" for u, q, n, t in accts)
    return f"INSERT INTO account (account_id, org_id, connection_id, source_account_id, name, type, currency) VALUES\n  {vals};"


# ---------------- the database ----------------
def _port_open():
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", PORT)) == 0


def ensure_db():
    """Start PGlite on PORT unless something is already listening there (e.g. run_all started it)."""
    if _port_open():
        return None
    npx = shutil.which("npx")
    if not npx:
        sys.exit("Node.js (npx) is needed to run the test database. See tests/README.md.")
    if not os.path.isdir(os.path.join(HERE, "node_modules", "@electric-sql", "pglite-socket")):
        sys.exit("Test database not installed. Run:  cd tests && npm install")
    kw = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if os.name == "nt" else {"start_new_session": True}
    proc = subprocess.Popen([npx, "pglite-server", "-p", str(PORT), "-m", "10"], cwd=HERE,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, **kw)
    atexit.register(stop_db, proc)
    deadline = time.time() + 60
    while not _port_open():
        if proc.poll() is not None or time.time() > deadline:
            sys.exit("The PGlite test database didn't start.")
        time.sleep(0.3)
    return proc


def stop_db(proc):
    if proc and proc.poll() is None:
        if os.name == "nt":   # npx spawns node as a child; take the whole tree down
            subprocess.run(["taskkill", "/T", "/F", "/PID", str(proc.pid)], capture_output=True)
        else:
            import signal
            os.killpg(proc.pid, signal.SIGTERM)


class _Shared:
    """PGlite runs one session at a time, and the app opens a second connection while the first
    is mid-transaction (fine on real Postgres, a deadlock here). Route every get_conn() through
    one connection instead; close() is a no-op."""
    def __init__(self, c): self.c = c
    def cursor(self): return self.c.cursor()
    def commit(self): self.c.commit()
    def rollback(self): self.c.rollback()
    def close(self): pass


def share_connection(app_module, c):
    app_module.get_conn = lambda: _Shared(c)
    app_module.SYNC_IN_BACKGROUND = False   # a thread can't share the one connection; run syncs inline


def setup(accounts_sql):
    """Fresh schema + the given accounts, then import the app against it. Returns (app module, connection)."""
    os.environ["SUPABASE_DB_URL"] = DB_URL
    os.environ["APP_PASSWORD"] = PASSWORD
    os.environ.pop("RENDER", None)
    ensure_db()
    c = psycopg2.connect(DB_URL)
    cur = c.cursor()
    cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    cur.execute(SCHEMA)
    cur.execute(accounts_sql)
    c.commit()
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    import app   # runs the app's startup migrations against the test database
    share_connection(app, c)
    # Never ask the real QuickBooks for a rate; suites that record in USD set their own.
    app.qbo_exchange_rate = lambda token, ccy, d: 3700.0
    return app, c


# ---------------- clients + checks ----------------
def browserlike(cl):
    """Make a Flask test client send the session's CSRF token with every POST, like the real forms."""
    raw = cl.post

    def post(url, data=None, **kw):
        with cl.session_transaction() as s:
            tok = s.get("csrf")
        if tok is None:
            cl.get("/login")
            with cl.session_transaction() as s:
                tok = s.get("csrf")
        data = dict(data or {})
        data.setdefault("_csrf", tok)
        return raw(url, data=data, **kw)

    cl.post = post
    cl.raw_post = raw
    return cl


def login(app_module):
    cl = browserlike(app_module.app.test_client())
    r = cl.post("/login", data={"username": "", "password": PASSWORD})
    assert r.status_code == 302, "test login failed"
    return cl


class Checker:
    def __init__(self):
        self.passed, self.failed = 0, []

    def check(self, label, cond):
        print(("PASS " if cond else "FAIL ") + label, flush=True)
        if cond:
            self.passed += 1
        else:
            self.failed.append(label)

    def summary(self):
        print(f"\n{self.passed} passed, {len(self.failed)} failed")
        for f in self.failed:
            print("  FAILED: " + f)
        return 1 if self.failed else 0
