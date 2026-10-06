"""Bank charges booked as totals on other dates. The bank takes each fee and its excise duty as its
own line; QuickBooks often has them added up per batch, dated a day or two out, so they never pair
one to one. Within a month, the smallest groups whose totals agree are suggested as one match.

QuickBooks isn't called. Run on its own with `python tests/suite_charge_groups.py`, or all suites with
`python tests/run_all.py`.
"""
import io, re, sys
from decimal import Decimal as D

import harness as H

UGX = "00000000-0000-0000-0000-0000000000a1"
A, c = H.setup(H.account_sql((UGX, "244", "Stanbic 7994", "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
A.qbo_is_connected = lambda: False
CH = "Operational Expenses:Finance Expenses:Charges & Fees:Bank charges UGX"


def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
N = [0]
def book(d, amt, text, cat):
    N[0] += 1
    cur.execute("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency,
                   description, category, last_modified) VALUES (%s,%s,%s,'Purchase',%s,%s,'UGX',%s,%s,now())""",
                (A.ORG_ID, UGX, str(N[0]), d, amt, text, cat)); c.commit()

# QuickBooks, as Stanbic 7994's February was booked.
book("2026-02-02", -2000, "FEE ACH INWD CR", CH)
book("2026-02-02", -300, "GOVERNMENT EXCISE DUTY CHARGE", CH)
book("2026-02-02", -2300, "Rent store", "Rent")                       # not a charge: never in a group
book("2026-02-04", -4000, "FEE  ACH INWD CR", CH)
book("2026-02-04", -2300, "", CH)                                     # no text: known by its account
book("2026-02-04", -600, "GOVERNMENT EXCISE DUTY CHARGE", CH)
book("2026-02-05", -4000, "FEE  ACH INWD CR", CH)
book("2026-02-05", -600, "GOVERNMENT EXCISE DUTY CHARGE", CH)
book("2026-03-04", -2500, "FEE ACH INWD CR", CH)                      # March doesn't tie: left alone
book("2026-04-01", -300, "GOVERNMENT EXCISE DUTY CHARGE", CH)         # next month: no group across months
book("2026-03-24", -36000, "MONTHLY MANAGEMENT FEE", CH)               # a day before the bank, after a stray charge
book("2026-02-10", -2300, "EFT BOL FEES INST ID 86116882 Fee Collection", CH)   # same wording as the bank's fee line

lines = [("2026-02-03", "EFT BOL FEES INST ID 85943926 Fee Collection", -2000),
         ("2026-02-03", "Excise Duty EFT BOL FEES 85943926 Fee Collection", -300)]
for n in range(5):
    lines += [("2026-02-04", f"EFT BOL FEES INST ID 8599120{n} Fee Collection", -2000),
              ("2026-02-04", f"Excise Duty EFT BOL FEES 8599120{n} Fee Collection", -300)]
lines += [("2026-02-10", "EFT BOL FEES INST ID 86116882 Fee Collection", -2000),
          ("2026-02-10", "Excise Duty EFT BOL FEES 86116882 Fee Collection", -300),
          ("2026-03-03", "EFT BOL FEES INST ID 86079827 Fee Collection", -2000),
          ("2026-03-25", "MONTHLY MANAGEMENT FEE", -36000),
          ("2026-03-31", "Excise Duty EFT BOL FEES 86116882 Fee Collection", -300),
          ("2026-05-07", "ANTHONY NUWAMANYA 88486905897058", -900000),
          ("2026-05-07", "Reversal Credit Tran Failed 884869 PACH: Cr Sender Acct", 900000),
          ("2026-05-12", "PAYMENT TO OKELLO", -50000),
          ("2026-05-13", "OKELLO PAYMENT BACK", 50000)]
body = "Date,Description,Amount\n" + "".join(f"{d},{t},{a}\n" for d, t, a in lines)
cl = H.login(A)
r = cl.post("/account/Stanbic 7994/upload", data={"replace": "1", "statement": (io.BytesIO(body.encode()), "s.csv"), "closing_balance": "0",
            "period_start": "2026-02-01", "period_end": "2026-05-31"}, content_type="multipart/form-data")
assert r.status_code == 302, r.data[:300]

def groups():
    return q("""SELECT m.status, m.match_type, m.confidence,
                       (SELECT array_agg(sl.amount ORDER BY sl.posted_date, sl.amount) FROM match_statement_line x
                          JOIN statement_line sl USING (line_id) WHERE x.match_id=m.match_id),
                       (SELECT array_agg(b.amount ORDER BY b.posted_date, b.amount) FROM match_book_txn y
                          JOIN book_txn b USING (txn_id) WHERE y.match_id=m.match_id),
                       (SELECT min(b.posted_date) FROM match_book_txn y JOIN book_txn b USING (txn_id) WHERE y.match_id=m.match_id)
                FROM match m WHERE m.confidence = %s ORDER BY 6""", (A.CHARGE_GROUP_CONF,))
g = groups()
g_mar = [x for x in g if x[5].month == 3]
check("March: the stray charge on 03/03 doesn't hold up the fee booked a day early",
      len(g_mar) == 1 and g_mar[0][3] == [D("-36000")] and g_mar[0][4] == [D("-36000")])
g = [x for x in g if x[5].month == 2]
check("two charge groups suggested for February", len(g) == 2 and all(x[0] == "proposed" and x[1] == "many_to_one" for x in g))
check("…the first: 03/02's fee and excise against 02/02's in QuickBooks",
      g and sorted(g[0][3]) == [D("-2000"), D("-300")] and sorted(g[0][4]) == [D("-2000"), D("-300")])
check("…the second: the rest of 04/02's lines against 05/02's entries",
      len(g) == 2 and len(g[1][3]) == 4 and len(g[1][4]) == 2 and sum(g[1][3]) == sum(g[1][4]) == D("-4600"))
same_day = q("""SELECT count(*), sum((SELECT count(*) FROM match_statement_line x WHERE x.match_id=m.match_id)) FROM match m
                WHERE m.match_type='many_to_one' AND m.confidence NOT IN (%s, %s)""", (A.CHARGE_GROUP_CONF, A.REVERSAL_CONF))[0]
check("…04/02's own entries (one with no text) pair with 04/02's lines on the same day first, as does 10/02's",
      same_day == (4, 8))
check("every February charge line is in a suggestion", q("""SELECT count(*) FROM statement_line sl WHERE sl.posted_date < '2026-03-01'
      AND NOT EXISTS (SELECT 1 FROM match_statement_line x WHERE x.line_id=sl.line_id)""")[0][0] == 0)
check("rent (not a charge) stays out of every group", not q("""SELECT 1 FROM match_book_txn y JOIN book_txn b USING (txn_id)
      WHERE b.description='Rent store'"""))
check("a charge with nothing to tie to: nothing suggested", not q("""SELECT 1 FROM match_statement_line x
      JOIN statement_line sl USING (line_id) WHERE sl.posted_date IN ('2026-03-03', '2026-03-31')"""))
fee10 = q("""SELECT m.match_type, m.confidence, (SELECT count(*) FROM match_statement_line x WHERE x.match_id=m.match_id)
             FROM match m JOIN match_book_txn y USING (match_id) JOIN book_txn b USING (txn_id)
             WHERE b.posted_date='2026-02-10' AND b.amount=-2300""")
check("a fee worded like QuickBooks' combined entry isn't paired alone as 'amount differs': fee and excise pair whole",
      fee10 == [("many_to_one", D("0.8"), 2)])
rev = q("""SELECT m.status, m.confidence, (SELECT array_agg(sl.amount ORDER BY sl.amount) FROM match_statement_line x
           JOIN statement_line sl USING (line_id) WHERE x.match_id=m.match_id),
           (SELECT count(*) FROM match_book_txn y WHERE y.match_id=m.match_id) FROM match m WHERE m.confidence=%s""",
        (A.REVERSAL_CONF,))
check("a failed payment and its reversal suggested as a pair, with no QuickBooks entry",
      rev == [("proposed", D(str(A.REVERSAL_CONF)), [D("-900000"), D("900000")], 0)])
check("opposite amounts that don't say reversal aren't paired", not q("""SELECT 1 FROM match_statement_line x
      JOIN statement_line sl USING (line_id) WHERE sl.description LIKE '%%OKELLO%%'"""))
p = cl.get("/account/Stanbic 7994").data.decode()
check("labelled on the page as bank charges with different dates", p.count("bank charges, dates differ") == 3)
check("…and the reversal as a payment and its reversal", "payment and its reversal" in p)

# Confirming keeps it through a re-match; rejecting stops it being suggested again.
mids = [str(r_[0]) for r_ in q("SELECT match_id FROM match m2 WHERE confidence=%s AND EXISTS (SELECT 1 FROM match_book_txn y JOIN book_txn b USING (txn_id) WHERE y.match_id=m2.match_id AND b.posted_date < '2026-03-01') ORDER BY (SELECT count(*) FROM match_statement_line x WHERE x.match_id=m2.match_id)", (A.CHARGE_GROUP_CONF,))]
q("UPDATE match SET status='confirmed' WHERE match_id=%s RETURNING 1", (mids[0],))
q("UPDATE match SET status='rejected' WHERE match_id=%s RETURNING 1", (mids[1],))
sid = q("SELECT statement_id FROM statement")[0][0]
A.run_matcher(sid)
g = groups()
check("after a re-match: the confirmed group stays, the rejected one isn't suggested again",
      sorted(x[0] for x in g if x[5].month == 2) == ["confirmed", "rejected"])

# ---- confirming the reversal; pairing bank lines by hand ----------------------------------------------
rid = str(q("SELECT match_id FROM match WHERE confidence=%s", (A.REVERSAL_CONF,))[0][0])
cl.post(f"/account/Stanbic 7994/review/{rid}", data={"status": "confirmed"})
check("the reversal pair confirms like any suggestion, and stays through the re-match",
      q("SELECT status FROM match WHERE confidence=%s", (A.REVERSAL_CONF,)) == [("confirmed",)])
ok1, ok2 = (str(q("SELECT line_id FROM statement_line WHERE description=%s", (t,))[0][0]) for t in ("PAYMENT TO OKELLO", "OKELLO PAYMENT BACK"))
odd = str(q("SELECT line_id FROM statement_line WHERE posted_date='2026-03-03'")[0][0])
cl.post("/account/Stanbic 7994/match", data={"ml": [ok1, odd]})
check("by hand: bank lines on their own that don't cancel out are refused",
      not q("SELECT 1 FROM match_statement_line WHERE line_id IN (%s::uuid, %s::uuid)", (ok1, odd)))
cl.post("/account/Stanbic 7994/match", data={"ml": [ok1, ok2]})
mm = q("""SELECT m.status, m.match_type, (SELECT count(*) FROM match_book_txn y WHERE y.match_id=m.match_id) FROM match m
          JOIN match_statement_line x USING (match_id) WHERE x.line_id=%s""", (ok1,))
check("by hand: two bank lines that cancel out match on their own", mm == [("confirmed", "manual", 0)])
p = cl.get("/account/Stanbic 7994").data.decode()
check("…the page still renders, the manual hint mentions it", "bank lines that cancel out" in p)

sys.exit(T.summary())
