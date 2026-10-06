"""Ignore a line that's already in QuickBooks: offered where the app suspects it is (an interrupted
recording, an entry taken by an identical line, "QuickBooks may already have this"); the line leaves
the list to record, can't be recorded by mistake, and Undo puts it back as it was.

QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_ignore.py`, or all suites with `python tests/run_all.py`.
"""
import io, re, sys

import harness as H

STB = "00000000-0000-0000-0000-0000000000a1"
A, c = H.setup(H.account_sql((STB, "35", "Stanbic UGX", "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
POSTS = []
A.qbo_is_connected = lambda: False
A.qbo_token = lambda: "tok"
A.qbo_post = lambda token, entity, body: POSTS.append(entity) or {entity: {"Id": str(700 + len(POSTS))}}
A._store_coa([{"Id": "35", "Name": "Stanbic UGX", "AccountType": "Bank", "CurrencyRef": {"value": "UGX"}},
              {"Id": "83", "Name": "Office Supplies", "AccountType": "Expense", "CurrencyRef": {"value": "UGX"}}])

def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
# Already in QuickBooks, but dated in May: "QuickBooks may already have this" on the June line.
q("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency,
     description, counterparty, last_modified) VALUES (%s,%s,'p9','Purchase','2026-05-29',-77000,'UGX','Pens','Pens Ltd',now()) RETURNING 1""",
  (A.ORG_ID, STB))
cl = H.login(A)
body = ("Date,Description,Amount\n2026-06-02,PENS LTD,-77000\n2026-06-05,PRINTER INK,-45000\n"
        "2026-06-09,PAPER SUPPLIES,-31000\n2026-06-12,CHAIRS,-250000\n")
cl.post("/account/Stanbic UGX/upload", data={"replace": "1", "statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
        "period_start": "2026-06-01", "period_end": "2026-06-30"}, content_type="multipart/form-data")
lid = lambda who: str(q("SELECT line_id FROM statement_line WHERE description=%s", (who,))[0][0])
PENS, INK, PAPER, CHAIRS = lid("PENS LTD"), lid("PRINTER INK"), lid("PAPER SUPPLIES"), lid("CHAIRS")
page = lambda: cl.get("/account/Stanbic UGX").data.decode()
to_record = lambda h: int(re.search(r'To record</span><span class="t-val[^"]*">(\d+)', h).group(1))
status = lambda x: (q("SELECT status FROM writeback_log WHERE line_id=%s", (x,)) or [(None,)])[0][0]

# An interrupted recording, and one whose entry an identical line took (logged done, with its entry's number).
q("INSERT INTO writeback_log (line_id, status) VALUES (%s,'pending') RETURNING 1", (INK,))
h = page()
n0 = to_record(h)
row = lambda h, x: h[h.find(f'value="{x}"'):][:3000] if f'value="{x}"' in h else ""
check("an interrupted recording offers Ignore next to I checked — it's not in QuickBooks",
      re.search(r"I checked — it's not in QuickBooks</button> <button type=submit name=ignore value=\"" + INK
                + r"\"[^>]*record_ignore[^>]*>Ignore — it's in QuickBooks", h) is not None)
check("…and so does a line QuickBooks may already have", re.search(
      r"QuickBooks may already have this.*?name=ignore value=\"" + PENS + r"\"", h, re.S) is not None)
check("lines with nothing suspected don't offer it", f'name=ignore value="{PAPER}"' not in h)

r = cl.post("/account/Stanbic UGX/record_ignore", data={"ignore": INK})
h = page()
check("Ignore: the line is logged as ignored and the page says so",
      r.status_code == 302 and status(INK) == "ignored" and "Ignored the line of 05/06/2026, -45,000.00" in h)
rec = h[h.find("id=rectbl"):h.find("</table>", h.find("id=rectbl"))]
ign = h[h.find("<details class=ignlist"):h.find("</details>", h.find("<details class=ignlist"))]
check("…it's gone from the list to record, and no longer counts as to record",
      INK not in rec and to_record(h) == n0 - 1 and re.search(r"record them \((\d+)\)", h).group(1) == "3")
check("…it's listed under Ignored, by whom, with Undo",
      "Ignored — already in QuickBooks (1)" in ign and INK in ign and "PRINTER INK" in ign and "Undo" in ign)
check("…it's not tickable any more", f'name=sel value="{INK}"' not in h)
check("…and it's in the activity log", q("SELECT count(*) FROM activity_log WHERE action LIKE 'ignored the line of 05/06/2026%%'")[0][0] == 1)
n = len(POSTS)
cl.post("/account/Stanbic UGX/record", data={"only": INK, f"acct_{INK}": "83"})
check("an ignored line can't be recorded by mistake", len(POSTS) == n and status(INK) == "ignored")
check("it stays unmatched: on the statement, not in the books",
      not q("""SELECT 1 FROM match_statement_line msl JOIN match m USING (match_id) WHERE msl.line_id=%s AND m.status='confirmed'""", (INK,)))

cl.post("/account/Stanbic UGX/record_ignore?undo=1", data={"ignore": INK})
h = page()
check("Undo: back in the list, ready to record", status(INK) == "failed" and f'name=sel value="{INK}"' in h
      and to_record(h) == n0 and "Restored the line of 05/06/2026" in h)
cl.post("/account/Stanbic UGX/record", data={"only": INK, f"acct_{INK}": "83"})
check("…and it records as normal", len(POSTS) == n + 1 and status(INK) == "done")

# A line already logged as recorded (its entry number known): Undo brings back that, not a fresh record.
q("INSERT INTO writeback_log (line_id, status, qbo_type, qbo_id) VALUES (%s,'done','Purchase','555') RETURNING 1", (CHAIRS,))
cl.post("/account/Stanbic UGX/record_ignore", data={"ignore": CHAIRS})
cl.post("/account/Stanbic UGX/record_ignore?undo=1", data={"ignore": CHAIRS})
check("Undo of a line with a recorded entry puts it back as recorded",
      q("SELECT status, qbo_id FROM writeback_log WHERE line_id=%s", (CHAIRS,)) == [("done", "555")])

# A line with no log yet (the suspected duplicate): ignoring creates one.
cl.post("/account/Stanbic UGX/record_ignore", data={"ignore": PENS})
check("ignoring a suspected duplicate works with no earlier recording", status(PENS) == "ignored")
cl.post("/account/Stanbic UGX/record_ignore", data={"ignore": PENS})
check("ignoring twice changes nothing more", "Nothing changed" in page()
      and q("SELECT count(*) FROM activity_log WHERE action LIKE 'ignored the line of 02/06/2026%%'")[0][0] == 1)

# Guard rails: another account's line, and a signed-off reconciliation.
cur.execute(H.account_sql(("00000000-0000-0000-0000-0000000000a2", "36", "DFCU UGX", "bank"))); c.commit()
cl.post("/account/DFCU UGX/record_ignore", data={"ignore": PAPER})
check("a line can't be ignored through another account", status(PAPER) is None)
q("UPDATE statement SET signed_off_at=now() RETURNING 1")
cl.post("/account/Stanbic UGX/record_ignore", data={"ignore": PAPER})
check("…nor once the reconciliation is signed off", status(PAPER) is None)
q("UPDATE statement SET signed_off_at=NULL RETURNING 1")
check("Ignore needs the Record permission", A.PERM_BY_ENDPOINT.get("record_ignore") == "record")
sys.exit(T.summary())
