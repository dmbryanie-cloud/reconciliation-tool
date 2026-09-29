"""PDF bank statements: layouts, signs, running-balance checks, password-protected files, refusals.

The PDFs are generated here (tests/pdfgen.py), laid out like real bank statements.
QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_pdf.py`, or all suites with `python tests/run_all.py`.
"""
import io, sys
from decimal import Decimal as D

import harness as H
from pdfgen import make_pdf, encrypt

ACCT = "00000000-0000-0000-0000-0000000000a1"
A, c = H.setup(H.account_sql((ACCT, "35", "Stanbic", "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
A.qbo_is_connected = lambda: False

def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
def lines():
    return q("""SELECT sl.posted_date::text, sl.amount, sl.description FROM statement_line sl JOIN statement s USING (statement_id)
                WHERE s.account_id=%s ORDER BY sl.posted_date, sl.amount, sl.description""", (ACCT,))
def stmt():
    r = q("""SELECT period_start::text, period_end::text, opening_balance, closing_balance, opening_source, closing_source,
                    source_format FROM statement WHERE account_id=%s ORDER BY created_at DESC LIMIT 1""", (ACCT,))
    return r[0] if r else None
def wipe():
    for t in ("match_statement_line", "match_book_txn", "match", "writeback_log", "statement_line", "statement"):
        cur.execute(f"DELETE FROM {t}")
    c.commit()
def n_stmts():
    return q("SELECT count(*) FROM statement")[0][0]

cl = H.login(A)
def upload(pdf, name="statement.pdf", **form):
    r = cl.post("/account/Stanbic/upload", data={"statement": (io.BytesIO(pdf), name), **form},
                content_type="multipart/form-data")
    return r, cl.get("/account/Stanbic").data.decode()


# ---- 1. Debit / Credit / Balance columns, two pages, wrapped descriptions ----------------------
DC, CC, BC = 400, 470, 550           # right edges of the money columns
def hdr(y):
    return [(40, y, "Date"), (95, y, "Value Date"), (160, y, "Description"), (DC, y, "Debit", "r"),
            (CC, y, "Credit", "r"), (BC, y, "Balance", "r")]
def row(y, d, desc, deb, cre, bal, vd=None):
    it = [(40, y, d), (95, y, vd or d), (160, y, desc), (BC, y, bal, "r")]
    if deb: it.append((DC, y, deb, "r"))
    if cre: it.append((CC, y, cre, "r"))
    return it
p1 = [(40, 40, "STANBIC BANK UGANDA LIMITED"), (40, 55, "Statement of Account"),
      (40, 70, "Statement Period: 01/01/2026 to 31/01/2026"), (40, 85, "Account: 9030012345678  Currency: UGX"),
      *hdr(120), (160, 135, "BALANCE B/F"), (BC, 135, "1,000,000.00", "r"),
      *row(150, "03/01/2026", "CASH DEPOSIT KAMPALA ROAD", None, "500,000.00", "1,500,000.00"),
      *row(165, "05/01/2026", "CHQ 000123 PAID TO", "200,000.00", None, "1,300,000.00"),
      (160, 176, "KAMPALA SUPPLIES LTD"),
      *row(191, "05/01/2026", "BANK CHARGES", "2,500.00", None, "1,297,500.00"),
      *row(206, "05/01/2026", "BANK CHARGES", "2,500.00", None, "1,295,000.00"),
      (40, 800, "Page 1 of 2")]
p2 = [*hdr(60),
      *row(75, "12/01/2026", "EFT FROM MOTHER CARE LTD INV 2231", None, "80,000.00", "1,375,000.00"),
      *row(90, "28/01/2026", "URA TAX PAYMENT PRN 2260001234", "175,000.00", None, "1,200,000.00"),
      (160, 101, "DOMESTIC TAX"),
      (160, 125, "Closing Balance"), (BC, 125, "1,200,000.00", "r"),
      (40, 800, "Page 2 of 2")]
STANBIC = make_pdf([p1, p2])

r, page = upload(STANBIC)
got = lines()
check("PDF import accepted", r.status_code == 302 and len(got) == 6)
check("debits are money out, credits money in", (("2026-01-03", D("500000.00"), "CASH DEPOSIT KAMPALA ROAD") in got
      and ("2026-01-28", D("-175000.00"), "URA TAX PAYMENT PRN 2260001234 DOMESTIC TAX") in got))
check("wrapped description joined, page footer not", ("2026-01-05", D("-200000.00"), "CHQ 000123 PAID TO KAMPALA SUPPLIES LTD") in got
      and not any("Page" in l[2] for l in got))
check("two identical bank charges on one day both kept", sum(1 for l in got if l[2] == "BANK CHARGES") == 2)
check("second page read with its own header", any(l[0] == "2026-01-12" for l in got))
check("value-date column not mistaken for part of the description", not any(l[2].startswith("0") for l in got))
s = stmt()
check("statement period read from the PDF", s[:2] == ("2026-01-01", "2026-01-31"))
check("opening and closing balances read from the PDF", s[2] == D("1000000") and s[3] == D("1200000")
      and s[4] == "file" and s[5] == "file" and s[6] == "pdf")
check("message: every running balance checks out", "every running balance checks out" in page)
k = c.cursor(); rec = A.reconcile(k, ACCT, A._latest_statement(k, ACCT)); c.rollback()
check("statement adds up (opening + movements = closing)", rec["foot_diff"] == 0)

again, _ = upload(STANBIC)
check("uploading the same PDF again replaces it, no duplicates", len(lines()) == 6 and n_stmts() == 1)

# ---- 2. Amount + Balance only, newest first, "05 Jan 2026" dates ------------------------------
def rrow(y, d, desc, amt, bal):
    return [(40, y, d), (130, y, desc), (430, y, amt, "r"), (540, y, bal, "r")]
NEWEST = make_pdf([[
    (40, 40, "Centenary Bank"), (40, 55, "Statement period: 01 Feb 2026 to 28 Feb 2026"),
    (40, 90, "Date"), (130, 90, "Narration"), (430, 90, "Amount", "r"), (540, 90, "Balance", "r"),
    *rrow(105, "20 Feb 2026", "MOBILE MONEY WITHDRAWAL", "50,000.00", "1,230,000.00"),
    *rrow(120, "14 Feb 2026", "SALARY FEB", "300,000.00", "1,280,000.00"),
    *rrow(135, "02 Feb 2026", "ATM WITHDRAWAL", "20,000.00", "980,000.00"),
    (130, 150, "Balance brought forward"), (540, 150, "1,000,000.00", "r")]])
wipe()
r, page = upload(NEWEST, "feb.PDF")
got = lines()
check("unsigned amounts: direction taken from the running balance", got == [
    ("2026-02-02", D("-20000.00"), "ATM WITHDRAWAL"), ("2026-02-14", D("300000.00"), "SALARY FEB"),
    ("2026-02-20", D("-50000.00"), "MOBILE MONEY WITHDRAWAL")])
s = stmt()
check("newest-first statement: opening and closing the right way round", s[2] == D("1000000") and s[3] == D("1230000"))
check("'01 Feb 2026 to 28 Feb 2026' period read", s[:2] == ("2026-02-01", "2026-02-28"))

# ---- 3. Money In / Money Out, no running balance -----------------------------------------------
NOBAL = make_pdf([[
    (40, 90, "Date"), (120, 90, "Details"), (400, 90, "Money Out", "r"), (480, 90, "Money In", "r"),
    (40, 105, "2026-03-04"), (120, 105, "POS PURCHASE SHOPRITE"), (400, 105, "45,300.00", "r"),
    (40, 120, "2026-03-09"), (120, 120, "TRANSFER IN"), (480, 120, "1,000,000.00", "r")]])
wipe()
r, page = upload(NOBAL, "mar.pdf", closing_balance="954700")
check("Money In / Money Out columns", lines() == [("2026-03-04", D("-45300.00"), "POS PURCHASE SHOPRITE"),
                                                 ("2026-03-09", D("1000000.00"), "TRANSFER IN")])
check("no running balance: told to compare totals", "no running balance to check against" in page)

# ---- 3b. Withdrawals / Deposits, "05-JAN-26" dates, Cr/Dr balances that go overdrawn ----------
def crow(y, d, desc, w, dep, bal, side):
    it = [(40, y, d), (110, y, desc), (470, y, bal, "r"), (476, y, side)]
    if w: it.append((330, y, w, "r"))
    if dep: it.append((400, y, dep, "r"))
    return it
CRDR = make_pdf([[
    (40, 90, "Txn Date"), (110, 90, "Particulars"), (330, 90, "Withdrawals", "r"), (400, 90, "Deposits", "r"),
    (470, 90, "Balance", "r"),
    (110, 105, "Opening Balance"), (470, 105, "1,000.00", "r"), (476, 105, "Cr"),
    *crow(120, "05-JAN-26", "SCHOOL FEES", "1,200.00", None, "200.00", "Dr"),
    *crow(135, "09-JAN-26", "DEPOSIT", None, "700.00", "500.00", "Cr")]])
wipe()
r, page = upload(CRDR, "usd.pdf")
check("Withdrawals/Deposits, 05-JAN-26 dates, Cr/Dr balances incl. overdrawn", lines() == [
    ("2026-01-05", D("-1200.00"), "SCHOOL FEES"), ("2026-01-09", D("700.00"), "DEPOSIT")]
    and stmt()[2:4] == (D("1000"), D("500")) and "every running balance checks out" in page)

# ---- 4. refusals: nothing half-imported --------------------------------------------------------
wipe()
BROKEN = make_pdf([[*hdr(90),
    *row(105, "03/01/2026", "DEPOSIT", None, "500,000.00", "1,500,000.00"),
    *row(120, "04/01/2026", "WITHDRAWAL", "100,000.00", None, "1,450,000.00"),      # should be 1,400,000
    *row(135, "05/01/2026", "FEE", "1,000.00", None, "1,449,000.00")]])
r, page = upload(BROKEN)
check("running balance that doesn't add up: refused, with the line", n_stmts() == 0
      and "running balance doesn&#39;t add up on 1 line" in page and "WITHDRAWAL" in page)
AMBIG = make_pdf([[(40, 90, "Date"), (120, 90, "Description"), (430, 90, "Amount", "r"),
                   (40, 105, "03/01/2026"), (120, 105, "SOMETHING"), (430, 105, "500,000.00", "r")]])
r, page = upload(AMBIG)
check("amounts with no direction and no balance: refused", n_stmts() == 0 and "can&#39;t be told apart" in page)
SCAN = make_pdf([[]])
r, page = upload(SCAN)
check("scanned PDF (no text): explained", n_stmts() == 0 and "probably a scan" in page)
r, page = upload(b"this is not a pdf", "fake.pdf")
check("not really a PDF: explained", n_stmts() == 0 and "couldn&#39;t be read as a PDF" in page)

# ---- 5. password-protected e-statements --------------------------------------------------------
LOCKED = encrypt(STANBIC, "Kampala#2026")
r, page = upload(LOCKED)
check("locked PDF without a password: asks for it", n_stmts() == 0 and "password-protected" in page
      and r.headers["Location"].endswith("?pdfpw=1"))
check("…and the password box is highlighted", 'autofocus' in cl.get("/account/Stanbic?pdfpw=1").data.decode())
r, page = upload(LOCKED, pdf_password="wrong")
check("wrong password: says so", n_stmts() == 0 and "isn&#39;t right" in page)
r, page = upload(LOCKED, pdf_password="Kampala#2026")
check("right password: imported", len(lines()) == 6 and "every running balance checks out" in page)
check("password never stored", not q("SELECT 1 FROM app_config WHERE value LIKE %s", ("%Kampala#2026%",))
      and "Kampala#2026" not in page)

# ---- 6. the page -------------------------------------------------------------------------------
check("upload box takes PDFs and has a password field", "accept=.pdf,.csv,.ofx" in page and "name=pdf_password" in page)
r = cl.post("/account/Stanbic/upload", data={"statement": (io.BytesIO(b"Date,Description,Amount\n2026-04-02,X,-5\n"), "s.csv"),
            "closing_balance": "0"}, content_type="multipart/form-data")
check("…CSV upload unaffected", r.status_code == 302 and stmt()[6] == "csv")
sys.exit(T.summary())
