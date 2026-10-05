"""Suggestions only between closely dated transactions. Many amounts repeat (the same fee, the same
transfer), so by default a suggestion needs the dates within 3 days: a payment clearing late, or
several lines adding up to one entry. Both windows can be widened in Settings.

QuickBooks isn't called. Run on its own with `python tests/suite_match_windows.py`, or all suites with
`python tests/run_all.py`.
"""
import html, io, re, sys

import harness as H

ACCT = "00000000-0000-0000-0000-0000000000a1"
A, c = H.setup(H.account_sql((ACCT, "35", "Stanbic", "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
A.qbo_is_connected = lambda: False


def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
def book(i, d, amt, who):
    q("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency, description,
         counterparty, last_modified) VALUES (%s,%s,%s,'Purchase',%s,%s,'UGX',%s,%s,now()) RETURNING 1""", (A.ORG_ID, ACCT, i, d, amt, who, who))
def pairs():
    """{bank description: (status, type, days apart)} for every match."""
    out = {}
    for desc, st, mt, gap in q("""SELECT sl.description, m.status, m.match_type,
                                    (SELECT max(abs(sl2.posted_date - b.posted_date)) FROM match_statement_line x2
                                       JOIN statement_line sl2 USING (line_id) CROSS JOIN match_book_txn y JOIN book_txn b USING (txn_id)
                                     WHERE x2.match_id=m.match_id AND y.match_id=m.match_id)
                                  FROM match m JOIN match_statement_line x USING (match_id) JOIN statement_line sl USING (line_id)"""):
        out[desc] = (st, mt, gap)
    return out

check("defaults: 3 days for a payment clearing late and for a combined match",
      A.rule("clear_days") == 3 and A.rule("group_days") == 3)
book("a", "2026-06-05", -500000, "School supplies")     # 5 days before the bank: too far, though the amount is the same
book("b", "2026-06-10", -750000, "Uniform supplier")    # 2 days before: matches (within the exact-match tolerance)
book("c", "2026-06-20", -900000, "Kitchen")             # two bank lines add up to it, 5 days earlier: too far
book("d", "2026-06-24", -660000, "Transport")           # two bank lines add up to it, 2 days earlier: suggested
body = ("Date,Description,Amount\n2026-06-10,SUPPLIES,-500000\n2026-06-12,UNIFORMS,-750000\n"
        "2026-06-15,KITCHEN PART 1,-400000\n2026-06-15,KITCHEN PART 2,-500000\n"
        "2026-06-22,TRANSPORT PART 1,-300000\n2026-06-22,TRANSPORT PART 2,-360000\n")
cl = H.login(A)
cl.post("/account/Stanbic/upload", data={"statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
        "period_start": "2026-06-01", "period_end": "2026-06-30"}, content_type="multipart/form-data")
p_ = pairs()
check("same amount 5 days apart: not suggested", "SUPPLIES" not in p_)
check("same amount 2 days apart: matched", p_.get("UNIFORMS", (None,))[0] == "confirmed")
check("two lines adding up to an entry 5 days later: not suggested", "KITCHEN PART 1" not in p_ and "KITCHEN PART 2" not in p_)
check("…2 days later: suggested", p_.get("TRANSPORT PART 1") == ("proposed", "many_to_one", 2))
page = html.unescape(cl.get("/account/Stanbic").data.decode())
check("each suggestion says how far apart its dates are", "2 days apart" in page)

# Widened in Settings: the further ones are suggested too.
s = html.unescape(cl.get("/settings").data.decode())
check("Settings shows both windows", "Days a payment may clear late" in s and "Days apart in a combined match" in s
      and 'name=group_days value="3"' in s)
cl.post("/settings", data={"action": "rules", "charges_exact": "1", "date_days": "3", "clear_days": "7", "group_days": "7",
                           "transfer_days": "4"})
check("…saved", A.rule("clear_days") == 7 and A.rule("group_days") == 7)
A.run_matcher(q("SELECT statement_id FROM statement")[0][0])
p_ = pairs()
check("widened: the payment clearing 5 days late is suggested", p_.get("SUPPLIES") == ("proposed", "exact", 5))
check("…and the combined one 5 days apart", p_.get("KITCHEN PART 1") == ("proposed", "many_to_one", 5))
sys.exit(T.summary())
