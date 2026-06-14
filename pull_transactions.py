import os
import json
import urllib.parse
import urllib.request
import urllib.error
import psycopg2
from decimal import Decimal
from qbo_auth import get_access_token

ACCESS_TOKEN = get_access_token()
REALM_ID = os.environ["QBO_REALM_ID"]
DB_URL = os.environ["SUPABASE_DB_URL"]
BASE = "https://sandbox-quickbooks.api.intuit.com"
ORG_ID = "00000000-0000-0000-0000-000000000001"


def qbo_query(entity):
    q = f"SELECT * FROM {entity} MAXRESULTS 1000"
    url = f"{BASE}/v3/company/{REALM_ID}/query?query=" + urllib.parse.quote(q)
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {ACCESS_TOKEN}")
    req.add_header("Accept", "application/json")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read()).get("QueryResponse", {}).get(entity, [])


def D(x):
    return Decimal(str(x or 0))


# Each handler returns (signed_amount, counterparty, description, category) or None.
# category = the QuickBooks account the OTHER side of the transaction was filed under.
def h_purchase(e, acct, atype):
    if e.get("AccountRef", {}).get("value") != acct: return None
    amt = D(e.get("TotalAmt")); is_credit = e.get("Credit", False)
    signed = (-amt if is_credit else amt) if atype == "credit_card" else (amt if is_credit else -amt)
    cat = None
    for ln in e.get("Line", []):
        det = ln.get("AccountBasedExpenseLineDetail")
        if det: cat = det.get("AccountRef", {}).get("name"); break
    return signed, e.get("EntityRef", {}).get("name"), e.get("PrivateNote"), cat

def h_deposit(e, acct, atype):
    if e.get("DepositToAccountRef", {}).get("value") != acct: return None
    cat = None
    for ln in e.get("Line", []):
        det = ln.get("DepositLineDetail")
        if det: cat = det.get("AccountRef", {}).get("name"); break
    return D(e.get("TotalAmt")), None, e.get("PrivateNote"), cat

def h_transfer(e, acct, atype):
    if e.get("ToAccountRef", {}).get("value") == acct:
        return D(e.get("Amount")), "Transfer in", e.get("PrivateNote"), e.get("FromAccountRef", {}).get("name")
    if e.get("FromAccountRef", {}).get("value") == acct:
        return -D(e.get("Amount")), "Transfer out", e.get("PrivateNote"), e.get("ToAccountRef", {}).get("name")
    return None

def h_billpayment(e, acct, atype):
    amt = D(e.get("TotalAmt"))
    if e.get("CheckPayment", {}).get("BankAccountRef", {}).get("value") == acct:
        return -amt, e.get("VendorRef", {}).get("name"), e.get("PrivateNote"), None
    if e.get("CreditCardPayment", {}).get("CCAccountRef", {}).get("value") == acct:
        return amt, e.get("VendorRef", {}).get("name"), e.get("PrivateNote"), None
    return None

def h_payment(e, acct, atype):
    if e.get("DepositToAccountRef", {}).get("value") != acct: return None
    return D(e.get("TotalAmt")), e.get("CustomerRef", {}).get("name"), e.get("PrivateNote"), None

def h_journalentry(e, acct, atype):
    net, hit, cat = Decimal(0), False, None
    for ln in e.get("Line", []):
        det = ln.get("JournalEntryLineDetail")
        if not det: continue
        if det.get("AccountRef", {}).get("value") == acct:
            amt = D(ln.get("Amount"))
            want = "Credit" if atype == "credit_card" else "Debit"
            net += amt if det.get("PostingType") == want else -amt
            hit = True
        elif cat is None:
            cat = det.get("AccountRef", {}).get("name")
    return (net, "Journal entry", e.get("PrivateNote"), cat) if hit else None


HANDLERS = {
    "Purchase": h_purchase, "Deposit": h_deposit, "Transfer": h_transfer,
    "BillPayment": h_billpayment, "Payment": h_payment, "JournalEntry": h_journalentry,
}

cache = {}
for etype in HANDLERS:
    try:
        cache[etype] = qbo_query(etype)
    except urllib.error.HTTPError as ex:
        if ex.code == 401:
            print("Token issue (401). Re-run."); raise SystemExit
        print(f"  (couldn't fetch {etype}: HTTP {ex.code})"); cache[etype] = []

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()
cur.execute("ALTER TABLE book_txn ADD COLUMN IF NOT EXISTS category text;")
conn.commit()

cur.execute("SELECT account_id, source_account_id, name, type FROM account ORDER BY type, name;")
accounts = cur.fetchall()

print("Syncing all types (now capturing the category each was filed under):\n")
for acct_uuid, acct_qbo, name, atype in accounts:
    written = 0
    for etype, handler in HANDLERS.items():
        for e in cache[etype]:
            try:
                res = handler(e, acct_qbo, atype)
            except Exception:
                continue
            if not res: continue
            amount, cp, desc, cat = res
            cur.execute("""
                INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type,
                                      posted_date, amount, currency, description, counterparty,
                                      reference, category, cleared_status, last_modified)
                VALUES (%(org)s,%(acct)s,%(sid)s,%(stype)s,%(date)s,%(amt)s,%(cur)s,%(desc)s,%(cp)s,%(ref)s,%(cat)s,'unknown',%(lm)s)
                ON CONFLICT (account_id, source_txn_type, source_txn_id) DO UPDATE SET
                  posted_date=EXCLUDED.posted_date, amount=EXCLUDED.amount, currency=EXCLUDED.currency,
                  description=EXCLUDED.description, counterparty=EXCLUDED.counterparty,
                  reference=EXCLUDED.reference, category=EXCLUDED.category, last_modified=EXCLUDED.last_modified;
            """, {"org": ORG_ID, "acct": acct_uuid, "sid": e.get("Id"), "stype": etype,
                  "date": e.get("TxnDate"), "amt": amount,
                  "cur": e.get("CurrencyRef", {}).get("value", "USD"),
                  "desc": desc, "cp": cp, "ref": e.get("DocNumber"), "cat": cat,
                  "lm": e.get("MetaData", {}).get("LastUpdatedTime")})
            written += 1
    print(f"  {name} ({atype}): {written}")

conn.commit()
cur.execute("SELECT count(*) FROM book_txn WHERE category IS NOT NULL;")
ncat = cur.fetchone()[0]
print(f"\n{ncat} transactions now carry a category.")
cur.execute("""SELECT counterparty, category, count(*) FROM book_txn
               WHERE category IS NOT NULL AND counterparty IS NOT NULL
               GROUP BY counterparty, category ORDER BY count(*) DESC LIMIT 10;""")
print("\nPayee -> category history (the raw material for the memory):")
for cp, cat, n in cur.fetchall():
    print(f"  {(cp or ''):<28} -> {cat}  ({n}x)")

cur.close()
conn.close()