"""A transfer between two of your accounts confirmed on one account's reconciliation is confirmed on the
other's too, and undoing it there undoes it here.

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_transfer_mirror.py`, or all suites with `python tests/run_all.py`.
"""
import io, sys

import harness as H

STB, CEN = "00000000-0000-0000-0000-0000000000a1", "00000000-0000-0000-0000-0000000000a2"
A, c = H.setup(H.account_sql((STB, "35", "Stanbic UGX", "bank"), (CEN, "36", "Centenary UGX", "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
A.qbo_is_connected = lambda: False


def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
def book(acct, sid, typ, d, amt, desc):
    q("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency,
         description, last_modified) VALUES (%s,%s,%s,%s,%s,%s,'UGX',%s,now()) RETURNING 1""",
      (A.ORG_ID, acct, sid, typ, d, amt, desc))
def line(desc):
    return str(q("SELECT line_id FROM statement_line WHERE description=%s", (desc,))[0][0])
def state(desc):
    """(status, confidence) of the line's match that isn't rejected, or None."""
    r = q("""SELECT m.status, m.confidence::float FROM match m JOIN match_statement_line msl USING (match_id)
             WHERE msl.line_id=%s AND m.status <> 'rejected'""", (line(desc),))
    return r[0] if r else None
def mid(desc):
    return str(q("""SELECT m.match_id FROM match m JOIN match_statement_line msl USING (match_id)
                    WHERE msl.line_id=%s AND m.status <> 'rejected'""", (line(desc),))[0][0])

cl = H.login(A)
def upload(name, rows):
    body = "Date,Description,Amount\n" + "".join(f"{d},{t},{a}\n" for d, t, a in rows)
    r = cl.post(f"/account/{name}/upload", data={"replace": "1", "statement": (io.BytesIO(body.encode()), "s.csv"),
                "closing_balance": "0", "period_start": "2026-09-01", "period_end": "2026-09-30"},
                content_type="multipart/form-data")
    assert r.status_code == 302, r.data[:300]

def manual(name, desc, sid_type, sid):
    t = str(q("SELECT txn_id FROM book_txn WHERE source_txn_type=%s AND source_txn_id=%s AND account_id="
              "(SELECT account_id FROM account WHERE name=%s)", (sid_type, sid, name))[0][0])
    r = cl.post(f"/account/{name}/match", data={"ml": line(desc), "mb": t})
    assert r.status_code == 302


# Transfer #700 in QuickBooks on 05/09; Stanbic shows it on 10/09 and Centenary on 12/09 -- too far from the
# entry for either to be matched by itself.
book(STB, "700", "Transfer", "2026-09-05", -500000, "To Centenary")
book(CEN, "700", "Transfer", "2026-09-05", 500000, "From Stanbic")
book(CEN, "701", "Deposit", "2026-09-05", 80000, "Fees")     # not between your accounts: never mirrored
upload("Stanbic UGX", [("2026-09-10", "TRF TO CENTENARY", -500000)])
upload("Centenary UGX", [("2026-09-12", "TRF FROM STANBIC", 500000), ("2026-09-12", "FEES", 80000),
                         ("2026-09-28", "LATER 500K", 500000)])
check("neither side is matched to start with", state("TRF TO CENTENARY") is None and state("TRF FROM STANBIC") is None)

manual("Stanbic UGX", "TRF TO CENTENARY", "Transfer", "700")
check("matched by hand on Stanbic", state("TRF TO CENTENARY")[0] == "confirmed")
check("…so the other side is confirmed on Centenary too (nearest line to Stanbic's)",
      state("TRF FROM STANBIC") == ("confirmed", A.MIRROR_CONF) and state("LATER 500K") is None)
check("…saying where it came from",
      q("SELECT confirmed_by FROM match WHERE match_id=%s", (mid("TRF FROM STANBIC"),))[0][0] == "Stanbic UGX (the other side)")
check("an entry that isn't between your accounts is left alone", state("FEES") is None)
CEN_SID = q("SELECT statement_id FROM statement WHERE account_id=%s", (CEN,))[0][0]
pg = cl.get("/account/Centenary UGX").data.decode()
check("Centenary's page shows it, labelled", "TRF FROM STANBIC" in pg and "other side confirmed" in pg)
A.run_matcher(CEN_SID)
check("it stays confirmed when Centenary is matched again (a sync)", state("TRF FROM STANBIC") == ("confirmed", A.MIRROR_CONF))

cl.post(f"/account/Stanbic UGX/unmatch/{mid('TRF TO CENTENARY')}")
check("undone on Stanbic", state("TRF TO CENTENARY") is None)
check("…so it's undone on Centenary too", state("TRF FROM STANBIC") is None)

manual("Stanbic UGX", "TRF TO CENTENARY", "Transfer", "700")
cl.post(f"/account/Centenary UGX/review/{mid('TRF FROM STANBIC')}", data={"status": "rejected"})
A.run_matcher(CEN_SID)
check("rejected on Centenary: the rejection stands", state("TRF FROM STANBIC") is None
      and q("SELECT count(*) FROM match WHERE status='rejected' AND statement_id=%s", (CEN_SID,))[0][0] == 1)

# A signed-off reconciliation is left alone.
q("DELETE FROM match WHERE statement_id=%s RETURNING 1", (CEN_SID,))
q("UPDATE statement SET signed_off_at=now(), signed_off_by='x' WHERE statement_id=%s RETURNING 1", (CEN_SID,))
A.run_matcher(CEN_SID)
check("a signed-off reconciliation isn't changed", state("TRF FROM STANBIC") is None)
sys.exit(T.summary())
