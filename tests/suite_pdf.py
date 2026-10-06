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
ACCT2 = "00000000-0000-0000-0000-0000000000a2"
A, c = H.setup(H.account_sql((ACCT, "35", "Stanbic", "bank"), (ACCT2, "36", "DFCU USD 12477", "bank"),
                              ("00000000-0000-0000-0000-0000000000a3", "37", "KCB 09708", "bank")))
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
def upload(pdf, name="statement.pdf", acct="Stanbic", **form):
    r = cl.post(f"/account/{acct}/upload", data={"replace": "1", "statement": (io.BytesIO(pdf), name), **form},
                content_type="multipart/form-data")
    return r, cl.get(f"/account/{acct}").data.decode()


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

# ---- 7. DFCU layout: a day's lines out of posting order, descriptions starting above the date ----
wipe()
def drow(y, d, deb, cre, bal, desc=None):
    it = [(26, y, d), (93, y, d), (BC, y, bal, "r")]
    if desc: it.append((160, y, desc))
    if deb: it.append((DC, y, deb, "r"))
    if cre: it.append((CC, y, cre, "r"))
    return it
DFCU = make_pdf([[(26, 40, "STATEMENT OF ACCOUNT"),
    (26, 55, "Account Name : NORTH GREEN EDUCATIONAL SERVICES LIMITED Account Number : 02183656112477"),
    (26, 70, "Statement Period : 01-06-2026 To 30-06-2026"), (26, 85, "Opening Bal. 27,161.46"),
    (26, 100, "TRAN DATE"), (93, 100, "VALUE DATE"), (160, 100, "DESCRIPTION"), (DC, 100, "DEBIT", "r"),
    (CC, 100, "CREDIT", "r"), (BC, 100, "BALANCE", "r"),
    *drow(115, "02-06-2026", None, "400.00", "27,561.46", "CSD:WILLETTE YR 8"),
    # printed cheque first, but its balance is after the two charges below it
    *drow(130, "05-06-2026", "2,687.00", None, "24,866.73", "CHQW EDITH SUUBI"),
    *drow(145, "05-06-2026", "6.72", None, "27,554.74", "Cash Withdrawl Charges"),
    *drow(160, "05-06-2026", "1.01", None, "27,553.73", "Government excise duty"),
    *drow(175, "15-06-2026", "2.00", None, "24,864.73", "INWARD RTGS CHARGES"),
    (160, 188, "RTGS1:THE NORTH GREEN SCHOOL NORTH"),
    *drow(193, "15-06-2026", None, "11,000.00", "35,864.43"),
    (160, 198, "GREEN:OWN TRAN"),
    *drow(213, "15-06-2026", "0.30", None, "24,864.43", "Govt excise duty"),
    (160, 226, "FXPLSP~1160575~SPOT~SELL~USD/UGX~3,720"),
    *drow(231, "15-06-2026", None, "400.00", "36,264.43"),
    (160, 236, ".00")]])
r, page = upload(DFCU, acct="DFCU USD 12477")
got = q("""SELECT sl.posted_date::text, sl.amount, sl.description FROM statement_line sl JOIN statement s USING (statement_id)
           WHERE s.account_id=%s ORDER BY sl.posted_date, sl.amount""", (ACCT2,))
check("same-day lines out of posting order: accepted, every balance checked", len(got) == 8
      and "every running balance checks out" in page)
check("description above the date belongs to the line below it", ("2026-06-15", D("11000.00"),
      "RTGS1:THE NORTH GREEN SCHOOL NORTH GREEN:OWN TRAN") in got and ("2026-06-15", D("-2.00"), "INWARD RTGS CHARGES") in got)
check("a number split over two lines put back together", ("2026-06-15", D("400.00"),
      "FXPLSP~1160575~SPOT~SELL~USD/UGX~3,720.00") in got)
s2 = q("SELECT opening_balance, closing_balance FROM statement WHERE account_id=%s", (ACCT2,))[0]
check("…opening and closing from the day's real order", s2 == (D("27161.46"), D("36264.43")))
wipe()
SHUFFLED_WRONG = make_pdf([[*hdr(90),
    *row(105, "03/01/2026", "DEPOSIT", None, "500,000.00", "1,500,000.00"),
    *row(120, "04/01/2026", "FEE", "1,000.00", None, "1,399,000.00"),
    *row(135, "04/01/2026", "WITHDRAWAL", "100,000.00", None, "1,450,000.00")]])   # no order makes these chain
r, page = upload(SHUFFLED_WRONG)
check("same-day lines that chain in no order: still refused", n_stmts() == 0 and "doesn&#39;t add up" in page)

# ---- 8. the statement's account number against the account it's uploaded to --------------------
wipe()
r, page = upload(DFCU)
check("another account's statement: refused, naming the right account", n_stmts() == 0
      and "02183656112477" in page and "looks like DFCU USD 12477, not Stanbic" in page)
r, page = upload(DFCU, acct="DFCU USD 12477")
check("…and imported on that account", n_stmts() == 1)
wipe()
r, page = upload(DFCU.replace(b"02183656112477", b"02183656199999"))
check("an account number no account's name matches: imported", n_stmts() == 1)
wipe()
OFX = (b"<OFX><BANKACCTFROM><ACCTID>02183656112477</ACCTID></BANKACCTFROM><BANKTRANLIST>"
       b"<STMTTRN><DTPOSTED>20260602<TRNAMT>400.00<NAME>X</STMTTRN></BANKTRANLIST></OFX>")
r, page = upload(OFX, "s.ofx")
check("OFX for another account: refused too", n_stmts() == 0 and "looks like DFCU USD 12477" in page)

# ---- 6. the page -------------------------------------------------------------------------------
check("upload box takes PDFs and has a password field", "accept=.pdf,.csv,.ofx" in page and "name=pdf_password" in page)
r = cl.post("/account/Stanbic/upload", data={"replace": "1", "statement": (io.BytesIO(b"Date,Description,Amount\n2026-04-02,X,-5\n"), "s.csv"),
            "closing_balance": "0"}, content_type="multipart/form-data")
check("…CSV upload unaffected", r.status_code == 302 and stmt()[6] == "csv")
# ---- an amount too wide for its column: the bank wraps its last digit onto the next line --------
# (Stanbic year statement: "1,488,000,000.0" with the final "0" printed under it, a wrapped piece of
# the description in between). It must be joined again, or the line is lost and balances break.
wipe()
WRAP = make_pdf([[
    (40, 40, "STANBIC BANK UGANDA LIMITED"), (40, 70, "Statement Period: 01/06/2026 to 30/06/2026"),
    (40, 85, "Account: 9030012345678  Currency: UGX"), *hdr(120),
    (160, 135, "BALANCE B/F"), (BC, 135, "1,103,495,040.00", "r"),
    (160, 147, "00000"),
    *row(156, "15/06/2026", "FXPLSP~1160575~SPOT~SELL", "1,488,000,000.0", None, "-384,504,960.00"),
    (160, 165, ".00"), (DC, 172, "0", "r"),
    *row(185, "16/06/2026", "CASH DEPOSIT", None, "4,960.00", "-384,500,000.00"),
    (160, 200, "Closing Balance"), (BC, 200, "-384,500,000.00", "r")]])
r, page = upload(WRAP, period_start="2026-06-01", period_end="2026-06-30")
got = lines()
check("a wrapped billion-shilling amount is joined again and the line kept",
      any(g[0] == "2026-06-15" and g[1] == D("-1488000000.00") for g in got))
check("…and the running balances check out (nothing refused)", len(got) == 2 and "doesn" not in page)
wipe()
r, page = upload(make_pdf([[(40, 70, "Statement Period: 01/06/2026 to 30/06/2026"), *hdr(120), (160, 135, "BALANCE B/F"),
                            (BC, 135, "100.00", "r"), *row(150, "15/06/2026", "X", "5.00", None, "999.00")]]))
check("a refused PDF is never labelled as uploaded", "doesn" in page and "Statement uploaded." not in page)

# ---- part of a PDF: the period chosen, balances from the PDF's own running balance ----------------
r, page = upload(STANBIC, period_start="2026-01-04", period_end="2026-01-31", opening_balance="1,500,000")
s = stmt()
check("PDF, part of its period: only those lines kept", s[:2] == ("2026-01-04", "2026-01-31")
      and not any(l[0] == "2026-01-03" for l in lines()) and "Loaded 5 statement lines" in page)
check("…opening typed for the period kept; closing from the PDF", s[2] == D("1500000") and s[3] == D("1200000")
      and "1 before 04/01/2026" in page)
k = c.cursor(); rec = A.reconcile(k, ACCT, A._latest_statement(k, ACCT)); c.rollback()
check("…and the period adds up", rec["foot_diff"] == 0)

# ---- KCB layout: a Reference column after the balance, whole-shilling amounts, "0" in the unused ---------
# column, and each description centred on its date: half printed above the date's line, half below.
KO, KI, KB, KR = 360, 430, 515, 522       # right edges of Money Out / Money In / Ledger Balance; Reference's left
def krow(y, d, out, inn, bal, ref, above=(), below=()):
    it = [(26, y, d), (90, y, d), (KO, y, out, "r"), (KI, y, inn, "r"), (KB, y, bal, "r"), (KR - 2, y, ref)]
    it += [(160, y - 6 * (len(above) - i), t) for i, t in enumerate(above)]
    it += [(158, y + 5 + 11 * i, t) for i, t in enumerate(below)]
    return it
KCB = make_pdf([[(26, 40, "Account Statement"), (26, 55, "Account: 2321509708"), (26, 70, "Period: Last 12 Months"),
    (26, 100, "Transaction"), (90, 106, "Value Date"), (160, 106, "Transaction Details"), (KO, 106, "Money Out", "r"),
    (KI, 106, "Money In", "r"), (KB, 106, "Ledger Balance", "r"), (KR + 4, 106, "Reference"), (40, 112, "Date"),
    *krow(130, "06.10.2025", "0", "0", "22,320,375", "", above=(), below=()), (160, 130, "BALANCE B/FWD"),
    *krow(160, "07.10.2025", "0", "8,460,000", "30,780,375", "FT25280R2P5C", above=("Transfer Oracle Fusion",),
          below=("NORTH1242782151886 KCB BANK",)),
    *krow(186, "03.11.2025", "-3,450", "0", "30,776,925", "FT253070DDXQ", above=("Transfer Charge TNGS DFCU 4353",),
          below=("AC-UGX1402500040022",)),
    *krow(223, "03.11.2025", "-14,000,000", "0", "16,776,925", "FT253070DDXQ",
          above=("Direct Credits TNGS DFCU 4353", "MRS MATOVU PAYMENT PLANTS"),
          below=("AND HERB OUTWARD CREDIT", "CLEARING ACCOUNT"))]])
r, page = upload(KCB, acct="KCB 09708")
got = q("""SELECT sl.posted_date::text, sl.amount, sl.description FROM statement_line sl JOIN statement s USING (statement_id)
           JOIN account a USING (account_id) WHERE a.name='KCB 09708' ORDER BY sl.posted_date, sl.amount DESC""")
check("KCB: every line read despite the Reference after the balance", [g[:2] for g in got] == [
      ("2025-10-07", D("8460000")), ("2025-11-03", D("-3450")), ("2025-11-03", D("-14000000"))]
      and "every running balance checks out" in page)
check("…each description whole, from above and below its date",
      [g[2] for g in got] == ["Transfer Oracle Fusion NORTH1242782151886 KCB BANK",
                              "Transfer Charge TNGS DFCU 4353 AC-UGX1402500040022",
                              "Direct Credits TNGS DFCU 4353 MRS MATOVU PAYMENT PLANTS AND HERB OUTWARD CREDIT CLEARING ACCOUNT"])
check("…opening from BALANCE B/FWD", q("""SELECT opening_balance FROM statement s JOIN account a USING (account_id)
      WHERE a.name='KCB 09708'""")[0][0] == D("22320375"))
check("…'Account: 2321509708' read as its account number",
      A.parse_pdf(KCB).account_number == "2321509708")

sys.exit(T.summary())
