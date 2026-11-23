"""Statements shorter than a month (a day, a few days, a week). Within a month, one starting after the open
reconciliation is added to it, so the month builds up and is signed off once; the last day's later lines
go in too. Ticking "separate", or signing each one off, gives each its own reconciliation. A one-line file
keeps its balances, and the dashboard shows a month with an open short reconciliation as still open.

QuickBooks isn't used. Run on its own with `python tests/suite_short_statements.py`, or all suites with
`python tests/run_all.py`.
"""
import html, io, re, sys
from decimal import Decimal as D

import harness as H

ACCT = "00000000-0000-0000-0000-0000000000a1"
NAME = "KCB 09708"
A, c = H.setup(H.account_sql((ACCT, "35", NAME, "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
A.qbo_is_connected = lambda: False


def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
cl = H.login(A)
def page(path=f"/account/{NAME}"):
    return html.unescape(cl.get(path).data.decode())
def upload(body, **form):
    cl.post(f"/account/{NAME}/upload", data={"statement": (io.BytesIO(body.encode()), "s.csv"), **form},
            content_type="multipart/form-data")
    m = re.search(r"<div id=flash[^>]*>(.*?)</div>", page(), re.S)
    return re.sub(r"<[^>]+>", "", m.group(1)) if m else ""
def stmts():
    return [(str(a), str(b), o, cb, n) for a, b, o, cb, n in q(
        """SELECT period_start, period_end, opening_balance, closing_balance,
                  (SELECT count(*) FROM statement_line l WHERE l.statement_id=s.statement_id)
           FROM statement s ORDER BY period_start""")]
def lines(day):
    return sorted(q("""SELECT amount FROM statement_line WHERE posted_date=%s""", (day,)))
HDR = "Date,Description,Amount,Balance\n"

# ---- the panel says so --------------------------------------------------------------------------------------------
p = page()
check("upload panel offers keeping a short statement separate", "Keep as a separate reconciliation" in p
      and "name=separate" in p and "Daily, weekly or few-day statements are fine" in p)

# ---- a week, then the next week: one reconciliation ---------------------------------------------------------------
upload(HDR + "2026-11-02,RENT,-100,900\n2026-11-06,FEES,50,950\n")
check("first week: its own reconciliation, balances from the file",
      stmts() == [("2026-11-02", "2026-11-06", D("1000"), D("950"), 2)])
m = upload(HDR + "2026-11-09,FUEL,-10,940\n2026-11-13,CHARGE,-5,935\n")
check("next week (after the weekend gap) carries on the open reconciliation",
      stmts() == [("2026-11-02", "2026-11-13", D("1000"), D("935"), 4)]
      and "Added to the open reconciliation, which now runs 02/11/2026 to 13/11/2026" in m and "Loaded 2 statement lines" in m)
check("…no missing-statement warning when it continues the balance", "missing statement" not in m)

# ---- the same day again, with more lines since ---------------------------------------------------------------------
m = upload(HDR + "2026-11-13,CHARGE,-5,935\n2026-11-13,CHARGE,-5,930\n2026-11-13,WATER,-30,900\n")
check("a later file for the last day adds only its new lines (a repeated charge counts as new)",
      lines("2026-11-13") == [(D("-30"),), (D("-5"),), (D("-5"),)] and stmts()[0][3] == D("900")
      and "Loaded 2 statement lines" in m and "the lines dated 13/11/2026 that weren't in it yet" in m)
m = upload(HDR + "2026-11-13,WATER,-30,900\n")
check("…the same day once more with nothing new: says so, nothing changed", "Nothing new" in m
      and len(lines("2026-11-13")) == 3)
m = upload(HDR + "2026-11-13,Water bill NWSC,-30,900\n2026-11-16,LEVY,-1,899\n")
check("…the same line worded differently (another file format) isn't added twice",
      len(lines("2026-11-13")) == 3 and lines("2026-11-16") == [(D("-1"),)] and stmts()[0][1] == "2026-11-16")

# ---- a gap: the file doesn't open where the reconciliation closed -------------------------------------------------
m = upload(HDR + "2026-11-20,SALES,500,1300\n")
check("a file opening at a different balance warns of a missing statement",
      "Check for a missing statement: this file opens at 800.00, but the reconciliation closed at 899.00 on 16/11/2026" in m
      and len(stmts()) == 1)

# ---- separate when asked --------------------------------------------------------------------------------------------
m = upload(HDR + "2026-11-23,GAS,-20,1280\n", separate="1")
check("ticked separate: its own reconciliation", len(stmts()) == 2
      and stmts()[1] == ("2026-11-23", "2026-11-23", D("1300"), D("1280"), 1))

# ---- the dashboard: the earliest open one in the month ------------------------------------------------------------
q("UPDATE statement SET signed_off_at=now(), signed_off_by='x' WHERE period_start='2026-11-23' RETURNING 1")
d = page("/?m=2026-11")
check("dashboard: a month with an earlier reconciliation still open shows that one, not signed off",
      "02 Nov – 20 Nov 2026" in d and "23 Nov – 23 Nov 2026" not in d and "0 signed off" in d)

# ---- signing each one off: a new one each time, opening at the last closing ---------------------------------------
q("DELETE FROM statement RETURNING 1")
upload(HDR + "2026-12-01,A,-10,990\n2026-12-03,B,-10,980\n")
q("UPDATE statement SET signed_off_at=now(), signed_off_by='x' RETURNING 1")
m = upload("Date,Description,Amount\n2026-12-07,C,-10\n")
check("after a signed-off week the next one starts the day after it, opening at its closing",
      stmts()[1][:3] == ("2026-12-04", "2026-12-07", D("980")) and len(stmts()) == 2)

# ---- a new month: never added to last month's open one -------------------------------------------------------------
q("UPDATE statement SET signed_off_at=NULL RETURNING 1")
m = upload(HDR + "2027-01-04,D,-10,960\n")
check("the first statement of a new month starts a new reconciliation", len(stmts()) == 3
      and stmts()[2][:2] == ("2027-01-04", "2027-01-04") and "Added to the open" not in m)

# ---- a one-line day keeps its balances ----------------------------------------------------------------------------
q("DELETE FROM statement RETURNING 1")
upload(HDR + "2027-02-02,ONLY,-1,924\n")
check("a one-line file: opening and closing read from its balance", stmts() == [("2027-02-02", "2027-02-02", D("925"), D("924"), 1)])

# ---- no activity: a file with no transactions ----------------------------------------------------------------------
q("DELETE FROM statement RETURNING 1")
upload(HDR + "2027-03-01,A,-10,990\n2027-03-03,B,-10,980\n")
m = upload(HDR)
check("no transactions and no date: asks for the statement date", "set 'Reconcile to' to the statement date" in m
       and stmts()[0][1] == "2027-03-03")
m = upload(HDR, period_end="2027-03-05")
check("no transactions with a date: the open reconciliation runs on to it, balance unchanged",
      stmts() == [("2027-03-01", "2027-03-05", D("1000"), D("980"), 2)] and "now runs 01/03/2027 to 05/03/2027" in m)
m = upload(HDR, period_end="2027-03-04")
check("…a date it already covers: nothing new", "Nothing new" in m and stmts()[0][1] == "2027-03-05")
q("UPDATE statement SET signed_off_at=now(), signed_off_by='x' RETURNING 1")
m = upload(HDR, period_end="2027-03-08")
check("after a signed-off one: a reconciliation with no lines, closing = opening (carried)",
      stmts()[1] == ("2027-03-06", "2027-03-08", D("980"), D("980"), 0)
      and q("SELECT opening_source, closing_source FROM statement WHERE period_start='2027-03-06'") == [("carried", "quiet")])
k = c.cursor(); rec = A.reconcile(k, ACCT, A._latest_statement(k, ACCT)); c.rollback()
check("…and it adds up", rec["foot_diff"] == 0)

# ---- a week across the month end: split there ----------------------------------------------------------------------
q("DELETE FROM statement RETURNING 1")
upload(HDR + "2027-04-20,A,-10,990\n2027-04-26,B,-10,980\n")
m = upload(HDR + "2027-04-28,C,-10,970\n2027-04-30,D,-10,960\n2027-05-03,E,-10,950\n2027-05-04,F,-10,940\n")
check("a week across the month end: April's lines continue April, May's start May",
      stmts() == [("2027-04-20", "2027-04-30", D("1000"), D("960"), 4), ("2027-05-01", "2027-05-04", D("960"), D("940"), 2)]
      and "Split at the month end" in m and "Loaded 4 statement lines" in m and "left out" not in m)
k = c.cursor()
check("…both add up", all(A.reconcile(k, ACCT, s_)["foot_diff"] == 0 for s_ in [
      (lambda sid: (k.execute(f"SELECT {A.STMT_COLS} FROM statement WHERE statement_id=%s", (sid,)), k.fetchone())[1])(sid)
      for (sid,) in q("SELECT statement_id FROM statement ORDER BY period_start")]))
c.rollback()

# ---- choosing which reconciliation to work on ----------------------------------------------------------------------
older, newer = [str(r[0]) for r in q("SELECT statement_id FROM statement ORDER BY period_start")]
p = page()
check("the header lists the reconciliations to choose from", "id=recpick" in p and "20/04/2027 to 30/04/2027 · in progress" in p
      and "olderrec" not in p and "F" in p)
r = cl.get(f"/account/{NAME}?rec={older}")
p = page()
check("picking the earlier one: the page works on it, with a note and a way back",
      r.status_code == 302 and "You're working on an earlier reconciliation" in p and f'value="{older}" selected' in p
      and f"s={older}" in p)
lid = str(q("SELECT line_id FROM statement_line WHERE description='C'")[0][0])
cl.post(f"/account/{NAME}/record_ignore", data={"ignore": lid})
check("…actions apply to it (a line of the earlier one ignored)",
      q("SELECT status FROM writeback_log WHERE line_id=%s", (lid,)) == [("ignored",)])
cl.get(f"/account/{NAME}?rec=latest")
check("back to the newest", "olderrec" not in page())
cl.get(f"/account/{NAME}?rec={older}")
upload(HDR + "2027-05-06,G,-10,930\n")
check("a new upload goes back to the newest", "olderrec" not in page())
rp = page("/reports")
check("Reports: work on any reconciliation", f"rec={older}" in rp and "Work on this reconciliation" in rp)

# ---- sign-off follows the picked reconciliation --------------------------------------------------------------------
q("UPDATE statement SET book_balance=closing_balance, book_balance_source='user' RETURNING 1")
cl.get(f"/account/{NAME}?rec={older}")
cl.post(f"/account/{NAME}/signoff", data={"override": "1", "note": "test"})
check("signing off with the earlier one picked signs off that one, not the newest",
      q("SELECT statement_id::text FROM statement WHERE signed_off_at IS NOT NULL") == [(older,)] and
      q("SELECT signed_off_at IS NULL FROM statement WHERE statement_id=%s", (newer,)) == [(True,)])

# ---- working out a closing balance, only when asked ----------------------------------------------------------------
q("DELETE FROM statement RETURNING 1")
upload(HDR + "2027-06-01,A,-10,990\n2027-06-02,B,-10,980\n")
q("UPDATE statement SET signed_off_at=now(), signed_off_by='x' RETURNING 1")
upload("Date,Description,Amount\n2027-06-03,C,-15\n2027-06-04,D,40\n")
p = page()
check("a file with no balance: closing not entered, 'Work it out' offered",
      q("SELECT closing_source FROM statement WHERE period_start='2027-06-03'") == [(None,)] and "value=work_closing" in p)
cl.post(f"/account/{NAME}/balances", data={"action": "work_closing"})
m = re.sub(r"<[^>]+>", "", re.search(r"<div id=flash[^>]*>(.*?)</div>", page(), re.S).group(1))
check("…worked out as opening + lines, labelled, with a warning to check it",
      q("SELECT closing_balance, closing_source FROM statement WHERE period_start='2027-06-03'") == [(D("1005"), "worked")]
      and "Check it against the bank" in m and "worked out from the lines: check it against the bank" in page()
      and "value=work_closing" not in page())
sys.exit(T.summary())
