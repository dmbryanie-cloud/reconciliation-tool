"""QuickBooks sync: deletions (CDC), moved and voided transactions, full-resync safety guards.

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_sync_deletions.py`, or all suites with `python tests/run_all.py`.
"""
import importlib, io, json, re, sys, urllib.error
from datetime import datetime, timezone, timedelta
from decimal import Decimal as D

import harness as H

A, c = H.setup(H.account_sql(
    ('00000000-0000-0000-0000-0000000000a1', '35', 'Stanbic', 'bank'),
    ('00000000-0000-0000-0000-0000000000a2', '36', 'Centenary', 'bank')))
cur = c.cursor()
T = H.Checker()
check = T.check

# ---------------- fake QuickBooks ----------------
QBO = {k: {} for k in A.QBO_HANDLERS}   # entity -> {id: object}
DELETED = {k: [] for k in A.QBO_HANDLERS}
STATE = {"realm": "REAL-1", "cdc_fail": False, "cdc_calls": 0, "queries": []}

def purchase(i, acct, amt, d="2026-09-10", who="Vendor"):
    QBO["Purchase"][i] = {"Id": i, "AccountRef": {"value": acct}, "TotalAmt": amt, "TxnDate": d,
                          "EntityRef": {"name": who}, "MetaData": {"LastUpdatedTime": "2026-09-20T10:00:00-07:00"}}

def fake_query(entity, token, since=None, changed_since=None, each=None):
    STATE["queries"].append((entity, changed_since))
    recs = list(QBO[entity].values())            # (the fake ignores the watermark: returns everything)
    if not each:
        return recs
    out = []
    for i in range(0, len(recs), 2):             # small pages, like QuickBooks' paging
        out.extend(each(recs[i:i + 2]))
    return out

def fake_cdc(token, entities, changed_since):
    STATE["cdc_calls"] += 1
    if STATE["cdc_fail"]:
        raise RuntimeError("boom")
    return {k: list(v) for k, v in DELETED.items() if v}, False

REAL_CDC = A.qbo_cdc_deleted
A.qbo_token = lambda: "tok"
A.import_accounts_from_qbo = lambda t: 0
A.qbo_query = fake_query
A.qbo_cdc_deleted = fake_cdc
A.qbo_realm = lambda: STATE["realm"]


def row(qid, acct="35"):
    cur.execute("""SELECT bt.amount, bt.is_deleted, bt.is_void FROM book_txn bt JOIN account a USING (account_id)
                   WHERE bt.source_txn_id=%s AND a.source_account_id=%s""", (qid, acct))
    r = cur.fetchone(); c.rollback(); return r

def sync(full=False):
    n, detail, mode, timing = A.sync_from_quickbooks(full=full)
    print("   sync:", mode, "|", detail); return detail, mode

# 1. initial full sync
for i, amt in (("1", 100), ("2", 200), ("3", 300), ("4", 400)):
    purchase(i, "35", amt)
QBO["Deposit"]["9"] = {"Id": "9", "DepositToAccountRef": {"value": "35"}, "TotalAmt": 5000, "TxnDate": "2026-09-05", "MetaData": {"LastUpdatedTime": "2026-09-20T10:00:00-07:00"}}
detail, mode = sync()
check("first sync is full", mode == "full")
check("5 rows synced live", all(row(i) and not row(i)[1] for i in "12349"))

# 2. an open statement matching P2 and P3
cl = H.login(A)
stmt = "Date,Description,Amount\n2026-09-10,Vendor,-200\n2026-09-10,Vendor,-300\n2026-09-10,Vendor,-400\n"
cl.post("/account/Stanbic/upload", data={"statement": (io.BytesIO(stmt.encode()), "s.csv"), "closing_balance": "0",
        "period_start": "2026-09-01", "period_end": "2026-09-30"}, content_type="multipart/form-data")
cur.execute("SELECT count(*) FROM match"); check("statement matched 3 lines", cur.fetchone()[0] == 3); c.rollback()

# 3. incremental: P2 deleted, P3 moved to Centenary, P1 voided
del QBO["Purchase"]["2"]; DELETED["Purchase"].append("2")
purchase("3", "36", 300)
purchase("1", "35", 0)
detail, mode = sync()
check("second sync is incremental", mode.startswith("changes since"))
check("CDC consulted", STATE["cdc_calls"] == 1)
check("P2 flagged deleted", row("2")[1] is True)
check("P3 flagged deleted under Stanbic", row("3")[1] is True)
check("P3 now live under Centenary", row("3", "36") and row("3", "36")[1] is False)
check("P1 flagged void", row("1")[2] is True and row("1")[1] is False)
check("P4 untouched", row("4")[1] is False and row("4")[2] is False)
check("sync message reports removals + re-match", "flagged 2 transactions deleted or moved" in detail and "re-matched 1 open" in detail)
cur.execute("""SELECT count(*) FROM match m JOIN match_book_txn mbt USING (match_id) JOIN book_txn bt USING (txn_id)
               WHERE bt.is_deleted"""); check("open statement re-matched without removed txns", cur.fetchone()[0] == 0); c.rollback()
cur.execute("SELECT count(*) FROM match"); check("only P4 still matched", cur.fetchone()[0] == 1); c.rollback()

# 4. a deleted txn restored in QBO comes back to life
DELETED["Purchase"].clear(); purchase("2", "35", 200)
sync()
check("restored P2 is live again", row("2")[1] is False)

# 5. signed-off statement hit by a deletion
cur.execute("UPDATE statement SET signed_off_at=now()"); c.commit()
A.run_matcher  # (matches already exist for P4)
del QBO["Purchase"]["4"]; DELETED["Purchase"].append("4")
detail, _ = sync()
check("signed-off warning in sync message", "signed-off reconciliations used transactions since removed" in detail and "Stanbic 2026-09-01 to 2026-09-30" in detail)
cur.execute("SELECT count(*) FROM match"); check("signed-off matches left alone", cur.fetchone()[0] >= 1); c.rollback()
page = cl.get("/account/Stanbic").data.decode()
check("account page flags the removed match", "since been deleted, voided or moved in QuickBooks" in page)
DELETED["Purchase"].clear()

# 6. full resync removes rows QBO no longer returns (no CDC needed)
del QBO["Deposit"]["9"]
calls = STATE["cdc_calls"]
detail, mode = sync(full=True)
check("full resync removes vanished deposit", row("9")[1] is True)
check("full resync doesn't call CDC", STATE["cdc_calls"] == calls)

# 7. CSV-imported books are never touched
cur.execute("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency, last_modified)
               VALUES (%s,'00000000-0000-0000-0000-0000000000a1','csvkey','CSV','2026-09-12',-50,'UGX',now())""", (A.ORG_ID,)); c.commit()
sync(full=True)
cur.execute("SELECT is_deleted FROM book_txn WHERE source_txn_type='CSV'"); check("CSV row untouched by full resync", cur.fetchone()[0] is False); c.rollback()

# 8. safety guard: QBO suddenly returns almost nothing
for i in range(100, 160):
    purchase(str(i), "35", i)
sync()
saved = dict(QBO["Purchase"]); QBO["Purchase"].clear(); purchase("100", "35", 100)
detail, _ = sync(full=True)
cur.execute("SELECT count(*) FROM book_txn WHERE source_txn_type='Purchase' AND is_deleted AND source_txn_id::int >= 101")
check("guard blocks mass removal", cur.fetchone()[0] == 0 and "Nothing was removed" in detail); c.rollback()
QBO["Purchase"].update(saved)

# 9. company changed: no removals at all
STATE["realm"] = "OTHER-2"
del QBO["Purchase"]["101"]; DELETED["Purchase"].append("101")
detail, _ = sync()
check("no removals after company change", row("101")[1] is False and "company changed" in detail)
DELETED["Purchase"].clear()
detail, _ = sync(full=True)
check("next sync after company change removes normally", row("101")[1] is True)

# 10. CDC failure holds the watermark
before = A.get_config("last_sync_at")
STATE["cdc_fail"] = True
detail, _ = sync()
check("CDC failure reported", "couldn't read deletions" in detail)
check("watermark not advanced on CDC failure", A.get_config("last_sync_at") == before)
STATE["cdc_fail"] = False

# 11. stale watermark (> 29 days) forces a full sync
A.set_config("last_sync_at", (datetime.now(timezone.utc) - timedelta(days=40)).strftime("%Y-%m-%dT%H:%M:%S-00:00"))
detail, mode = sync()
check("stale watermark -> full sync", mode == "full" and "over 29 days" in detail)

# 12. CDC response parsing, via a fake HTTP response (no network)
class _Resp(io.BytesIO):
    def __enter__(self): return self
    def __exit__(self, *a): pass
payload = {"CDCResponse": [{"QueryResponse": [
    {"Purchase": [{"Id": "7", "status": "Deleted"}, {"Id": "8", "TotalAmt": 1}], "startPosition": 1},
    {"Deposit": [{"Id": "11", "status": "Deleted"}]},
    {"Customer": [{"Id": "99", "status": "Deleted"}]}]}]}
seen_urls = []
def fake_urlopen(req, *a, **k):
    seen_urls.append(req.full_url); return _Resp(json.dumps(payload).encode())
real_urlopen = A.urllib.request.urlopen
A.urllib.request.urlopen = fake_urlopen
try:
    dels, capped = REAL_CDC("tok", list(A.QBO_HANDLERS), "2026-09-01T00:00:00-00:00")
    check("CDC parser: deleted ids by entity, ignores non-deleted and unknown entities", dels == {"Purchase": ["7"], "Deposit": ["11"]})
    check("CDC parser: not capped", capped is False)
    check("CDC request shape", "/v3/company/OTHER-2/cdc?" in seen_urls[0] and "changedSince=2026-09-01" in seen_urls[0])
    payload["CDCResponse"][0]["QueryResponse"][0]["Purchase"] = [{"Id": str(i)} for i in range(1000)]
    check("CDC parser: cap detected", REAL_CDC("tok", ["Purchase"], "x")[1] is True)
finally:
    A.urllib.request.urlopen = real_urlopen

# 13. after a capped CDC, the next sync is full
A.qbo_cdc_deleted = lambda t, e, cs: ({}, True)
sync()
A.qbo_cdc_deleted = fake_cdc
_, mode = sync()
check("capped CDC forces the next sync to be full", mode == "full")
_, mode = sync()
check("…and then back to incremental", mode.startswith("changes since"))

# 14. a full re-pull takes minutes on a real company: never inside an upload or a balance fetch
A.qbo_is_connected = lambda: True
A.set_config("sync_force_full", "1")
check("full sync reported as due", A.sync_full_due() is True)
q0 = len(STATE["queries"])
r = cl.post("/account/Stanbic/upload", data={"statement": (io.BytesIO(stmt.encode()), "s.csv"), "closing_balance": "0",
            "period_start": "2026-09-01", "period_end": "2026-09-30"}, content_type="multipart/form-data", follow_redirects=True)
page = r.get_data(as_text=True)
check("upload while a full sync is due doesn't sync", len(STATE["queries"]) == q0)
check("…still loads and matches the statement", "Loaded 3 statement lines" in page)
check("…and says why books weren't refreshed", "full QuickBooks sync is due" in page)
r = cl.post("/account/Stanbic/balances", data={"action": "fetch_book"}, follow_redirects=True)
check("balance fetch refused while a full sync is due", len(STATE["queries"]) == q0
      and "full QuickBooks sync is due first" in r.get_data(as_text=True))
_, mode = sync()
check("the Sync button runs the full pull", mode == "full" and A.sync_full_due() is False)
q0 = len(STATE["queries"])
cl.post("/account/Stanbic/upload", data={"statement": (io.BytesIO(stmt.encode()), "s.csv"), "closing_balance": "0",
        "period_start": "2026-09-01", "period_end": "2026-09-30"}, content_type="multipart/form-data")
check("after it, uploads refresh books again", len(STATE["queries"]) > q0)

sys.exit(T.summary())
