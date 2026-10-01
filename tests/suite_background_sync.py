"""QuickBooks sync in the background: Sync returns at once, pages show progress, one sync at a time.

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_background_sync.py`, or all suites with `python tests/run_all.py`.
"""
import io, json, os, re, subprocess, sys, tempfile, threading, time

import harness as H

A, c = H.setup(H.account_sql(('00000000-0000-0000-0000-0000000000a1', '35', 'Stanbic', 'bank')))
T = H.Checker()
check = T.check

QBO = [{"Id": str(i), "AccountRef": {"value": "35"}, "TotalAmt": 100 + i, "TxnDate": "2026-09-10",
        "EntityRef": {"name": "Vendor"}, "MetaData": {"LastUpdatedTime": "2026-09-20T10:00:00-07:00"}} for i in range(1, 6)]
STATE = {"queries": 0, "gate": None, "boom": False}

def fake_query(entity, token, since=None, changed_since=None, each=None):
    STATE["queries"] += 1
    if STATE["boom"]:
        raise RuntimeError("QuickBooks is down")
    recs = QBO if entity == "Purchase" else []
    out = []
    for i in range(0, len(recs), 2):
        if STATE["gate"] is not None and entity == "Purchase":
            STATE["reached"].set(); STATE["gate"].wait(10)   # hold the download mid-way
        out.extend(each(recs[i:i + 2]) if each else recs[i:i + 2])
    return out

A.qbo_token = lambda: "tok"
A.import_accounts_from_qbo = lambda t: 0
A.qbo_query = fake_query
A.qbo_cdc_deleted = lambda t, e, cs: ({}, False)
A.qbo_realm = lambda: "REAL-1"
A.qbo_is_connected = lambda: True
cl = H.login(A)

def page(url="/"):
    return cl.get(url).get_data(as_text=True)

def job(**kw):
    j = {"state": "running", "full": True, "started": time.time() - 90, "beat": time.time(), "step": "Downloading JournalEntry records (3,000 so far)"}
    j.update(kw); A.set_config("sync_job", json.dumps(j))

def n_books():
    cur = c.cursor(); cur.execute("SELECT count(*) FROM book_txn"); n = cur.fetchone()[0]; c.rollback(); return n

# 1. Sync runs (inline here) and reports once
r = cl.post("/sync", data={})
check("Sync returns to the dashboard", r.status_code == 302 and r.headers["Location"].endswith("/"))
check("books synced", n_books() == 5)
j = A.sync_job()
check("job recorded as done with its summary", j["state"] == "done" and "Synced 5 transactions" in j["msg"])
check("progress was reported", "Re-matching" in j["step"] or "Saving" in j["step"])
p1, p2 = page(), page()
check("result shown once on the next page", "Synced 5 transactions" in p1 and "Synced 5 transactions" not in p2)

# 2. while one is running: no second sync, banner with progress, status endpoint
job()
q0 = STATE["queries"]
r = cl.post("/sync", data={}, follow_redirects=True)
body = r.get_data(as_text=True)
check("second Sync while one runs doesn't start another", STATE["queries"] == q0)
check("…and says one is already running", "already running" in body)
check("dashboard shows progress banner", "in progress" in body and "3,000 so far" in body and "sync/status" in body)
check("account page shows it too", "in progress" in page("/account/Stanbic"))
# the banner's script: polls, updates the step, reloads when done -- unless the user has typed something
js = re.search(r'<div id=syncbar.*?<script>(.*?)</script>', body, re.S).group(1)
NODE = """
let reloaded = 0, calls = 0, timers = [], listeners = {};
global.setTimeout = f => timers.push(f);
const step = {textContent: ''}, bar = {innerHTML: '', querySelector: () => step};
global.document = {addEventListener: (k, f) => listeners[k] = f, getElementById: () => bar};
global.location = {reload() { reloaded++ }};
let state = 'running';
global.fetch = () => { calls++; return Promise.resolve({json: () => ({state, step: 'Saving 9 transactions'})}) };
const tick = () => new Promise(r => setImmediate(r));
function boot() { __BANNER__ }
(async () => {
  boot(); timers.shift()(); await tick(); await tick();
  const stepShown = step.textContent;
  state = 'done'; timers.shift()(); await tick(); await tick();
  const clean = reloaded;
  reloaded = 0; timers = []; boot(); listeners.input(); state = 'done';
  timers.shift()(); await tick(); await tick();
  console.log(JSON.stringify({calls, stepShown, clean, dirtyReloaded: reloaded, bar: bar.innerHTML}));
})();
""".replace("__BANNER__", js)
with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as f:
    f.write(NODE)
try:
    res = subprocess.run(["node", f.name], capture_output=True, text=True, timeout=30)
    if res.returncode:
        print("   node:", res.stderr.strip()[-900:])
    out = json.loads(res.stdout)
finally:
    os.unlink(f.name)
check("banner script shows the latest step", out["stepShown"] == "Saving 9 transactions")
check("banner script reloads when the sync is done", out["clean"] == 1)
check("…but not over half-typed work: offers a reload link instead", out["dirtyReloaded"] == 0 and "Reload to see the results" in out["bar"])
st = cl.get("/sync/status").get_json()
check("status endpoint reports running + step", st["state"] == "running" and "JournalEntry" in st["step"])
r = cl.post("/sync", data={"back": "Stanbic"}, follow_redirects=True)
check("account-page refresh while running stays on the page with the note",
      STATE["queries"] == q0 and "already running" in r.get_data(as_text=True))

# 3. uploads and balance fetches stay out of a running sync
stmt = "Date,Description,Amount\n2026-09-10,Vendor,-101\n"
r = cl.post("/account/Stanbic/upload", data={"statement": (io.BytesIO(stmt.encode()), "s.csv"), "closing_balance": "0",
            "period_start": "2026-09-01", "period_end": "2026-09-30"}, content_type="multipart/form-data", follow_redirects=True)
body = r.get_data(as_text=True)
check("upload during a sync doesn't sync", STATE["queries"] == q0)
check("…loads the statement and explains", "Loaded 1 statement lines" in body and "sync is running" in body)
r = cl.post("/account/Stanbic/balances", data={"action": "fetch_book"}, follow_redirects=True)
check("balance fetch during a sync waits for it: fills in by itself when it finishes",
      STATE["queries"] == q0 and "fills in by itself when it finishes" in r.get_data(as_text=True)
      and "updating from QuickBooks after the sync" in r.get_data(as_text=True))

# 4. a sync the server lost (restart) is shown as stopped, and can be run again
job(beat=time.time() - A.SYNC_STALE_SECS - 5)
check("stale job reads as stalled", A.sync_job()["state"] == "stalled")
check("banner says it stopped", "stopped before finishing" in page())
cl.post("/sync", data={})
check("Sync starts again after a stall", STATE["queries"] > q0 and A.sync_job()["state"] == "done")

# 5. a failing sync is reported, not a server error
STATE["boom"] = True
r = cl.post("/sync", data={})
check("failing sync still returns normally", r.status_code == 302)
check("…recorded as failed with the reason", A.sync_job()["state"] == "failed" and "QuickBooks is down" in A.sync_job()["msg"])
check("…and shown on the page", "Sync failed" in page())
STATE["boom"] = False

# 6. the real thing: a background thread; the request returns while the download is still going
A.SYNC_IN_BACKGROUND = True
STATE["gate"], STATE["reached"] = threading.Event(), threading.Event()
t0 = time.time()
r = cl.post("/sync", data={})
back_fast = time.time() - t0
STATE["reached"].wait(10)
st = cl.get("/sync/status").get_json()
check("Sync returns before the download finishes", r.status_code == 302 and back_fast < 5)
check("status shows it running mid-download", st["state"] == "running" and "Purchase" in st["step"])
STATE["gate"].set()
for th in threading.enumerate():
    if th.name == "qbo-sync":
        th.join(20)
check("background sync finishes and records its result", A.sync_job()["state"] == "done")
check("banner then shows the result", "Synced 5 transactions" in page())
A.SYNC_IN_BACKGROUND = False
STATE["gate"] = None

sys.exit(T.summary())
