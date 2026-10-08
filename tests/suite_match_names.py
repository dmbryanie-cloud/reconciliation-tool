"""Equal amounts from different payers in the same week: each bank line pairs with the entry that names the
same payer (or reference), not just the nearest date.

Run on its own with `python tests/suite_match_names.py`, or all suites with `python tests/run_all.py`.
"""
import io, sys

import harness as H

ACCT = "00000000-0000-0000-0000-0000000000a1"
NAME = "DFCU UGX 60570"
A, c = H.setup(H.account_sql((ACCT, "35", NAME, "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
A.qbo_is_connected = lambda: False


def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
def book(tid, d, amt, desc, who):
    q("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency,
         description, counterparty, last_modified) VALUES (%s,%s,%s,'Payment',%s,%s,'UGX',%s,%s,now()) RETURNING 1""",
      (A.ORG_ID, ACCT, tid, d, amt, desc, who))
def paired(desc):
    r = q("""SELECT bt.source_txn_id FROM statement_line sl JOIN match_statement_line msl ON msl.line_id=sl.line_id
             JOIN match m ON m.match_id=msl.match_id AND m.status='confirmed'
             JOIN match_book_txn mbt ON mbt.match_id=m.match_id JOIN book_txn bt ON bt.txn_id=mbt.txn_id
             WHERE sl.description=%s""", (desc,))
    return sorted(x[0] for x in r)

# Two parents pay 574,500 a day apart; each entry carries its own payer's text. By date alone the Marvel line
# (09/02) would take Zoe's entry (09/02, same day) over Marvel's own (07/02).
book("34914", "2026-02-09", 574500, "CSD:ZOE WILLIAM MWANJE", "2021400 Zoe Mwanje (UGX)")
book("34915", "2026-02-07", 574500, "CAMBRIDGE CP EXAMINATION YR 6/MARVEL NIMURUNGI", "2021377 Marvel Nimurungi (UGX)")
# A reference number shared by the line and its entry decides it too.
book("32930", "2026-01-29", 3156000, "MTN-256772455254-38177675437", "20218242 Geoffrey Musoba (UGX)")
book("32931", "2026-01-29", 3156000, "MTN-256772455254-38177622413", "2021740 Crystal Musoba (UGX)")
# Nothing in common: the nearest date, as before.
book("50001", "2026-02-20", 100000, "CASH", "")
book("50002", "2026-02-22", 100000, "CASH", "")

body = ("Date,Description,Amount\n2026-02-09,CAMBRIDGE CP EXAMINATION YR 6/MARVEL NIMURUNGI,574500\n"
        "2026-02-10,CSD:ZOE WILLIAM MWANJE,574500\n2026-01-29,MTN-256772455254-38177622413,3156000\n"
        "2026-01-29,MTN-256772455254-38177675437,3156000\n2026-02-21,DEPOSIT A,100000\n")
cl = H.login(A)
cl.post(f"/account/{NAME}/upload", data={"replace": "1", "statement": (io.BytesIO(body.encode()), "s.csv"),
        "period_start": "2026-01-01", "period_end": "2026-02-28", "closing_balance": "0"},
        content_type="multipart/form-data")

check("Marvel's payment pairs with Marvel's entry, though Zoe's is dated nearer",
      paired("CAMBRIDGE CP EXAMINATION YR 6/MARVEL NIMURUNGI") == ["34915"])
check("…and Zoe's with Zoe's", paired("CSD:ZOE WILLIAM MWANJE") == ["34914"])
check("the same day, same amount: each MTN reference pairs with its own entry",
      paired("MTN-256772455254-38177622413") == ["32931"] and paired("MTN-256772455254-38177675437") == ["32930"])
check("nothing in common: the nearest date still decides", paired("DEPOSIT A") in (["50001"], ["50002"]))

# ---- Unpair: an automatic match that's wrong can be taken apart, and stays apart -----------------------------
page = cl.get(f"/account/{NAME}").data.decode()
mid = q("""SELECT m.match_id::text FROM match m JOIN match_statement_line msl USING (match_id)
           JOIN statement_line sl ON sl.line_id=msl.line_id WHERE sl.description='DEPOSIT A' AND m.status='confirmed'""")[0][0]
check("each automatic match has an Unpair button", f"/review/{mid}" in page and ">Unpair</button>" in page)
was = paired("DEPOSIT A")
r = cl.post(f"/account/{NAME}/review/{mid}", data={"status": "rejected", "back": "matched"})
check("Unpair: that pair is taken apart, and the page opens at Match manually",
      paired("DEPOSIT A") != was and r.headers["Location"].endswith("#sec-manual"))
A.run_matcher(q("SELECT statement_id FROM statement")[0][0])
check("…and the matcher doesn't pair those two again (the other CASH entry may be paired instead)",
      paired("DEPOSIT A") != was and q("SELECT status FROM match WHERE match_id=%s", (mid,)) in ([("rejected",)], []))

# Two identical lines, two equal entries: unpairing one mustn't just swap them round.
q("UPDATE book_txn SET is_deleted=true WHERE source_txn_id IN ('50001','50002') RETURNING 1")
book("60001", "2026-01-27", 420000, "EFT-NAKATUMBA JOYCE", "")
book("60002", "2026-01-30", 420000, "EFT-ALFRED OKECH", "")
body = "Date,Description,Amount" + chr(10) + ("2026-01-27,EFT-NAKATUMBA JOYCE,420000" + chr(10)) * 2
cl.post(f"/account/{NAME}/upload", data={"replace": "1", "statement": (io.BytesIO(body.encode()), "s.csv"),
        "period_start": "2026-01-01", "period_end": "2026-02-28", "closing_balance": "0"}, content_type="multipart/form-data")
def okech_mid():
    r = q("""SELECT m.match_id::text FROM match m JOIN match_book_txn mbt USING (match_id) JOIN book_txn bt ON bt.txn_id=mbt.txn_id
             WHERE bt.source_txn_id='60002' AND m.status='confirmed'""")
    return r[0][0] if r else None
m1 = okech_mid()
check("(one Nakatumba line paired with Okech's entry by date)", m1 is not None)
cl.post(f"/account/{NAME}/review/{m1}", data={"status": "rejected", "back": "matched"})
check("unpaired: Okech's entry isn't given to the other identical line instead", okech_mid() is None)
check("…and the unpaired line is left unmatched, for a match by hand",
      q("""SELECT count(*) FROM statement_line sl WHERE NOT EXISTS (SELECT 1 FROM match_statement_line msl
           JOIN match m ON m.match_id=msl.match_id WHERE msl.line_id=sl.line_id AND m.status='confirmed')""")[0][0] == 1)
sys.exit(T.summary())
