import os
import sys
import json
import urllib.parse
import urllib.request
import urllib.error
import psycopg2
from qbo_auth import get_access_token
from memory import build_memory, suggest_category, record_correction

ACCESS_TOKEN = get_access_token()
REALM_ID = os.environ["QBO_REALM_ID"]
DB_URL = os.environ["SUPABASE_DB_URL"]
BASE = "https://sandbox-quickbooks.api.intuit.com"
ACCT_NAME = sys.argv[1] if len(sys.argv) > 1 else "Checking"


def qbo_get(query):
    url = f"{BASE}/v3/company/{REALM_ID}/query?query=" + urllib.parse.quote(query)
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {ACCESS_TOKEN}")
    req.add_header("Accept", "application/json")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read()).get("QueryResponse", {})


def create_purchase(paid_from_qbo, expense_id, amount_abs, txn_date, note):
    body = {"AccountRef": {"value": paid_from_qbo}, "PaymentType": "Cash",
            "TxnDate": txn_date, "PrivateNote": note,
            "Line": [{"DetailType": "AccountBasedExpenseLineDetail", "Amount": amount_abs,
                      "AccountBasedExpenseLineDetail": {"AccountRef": {"value": expense_id}}}]}
    url = f"{BASE}/v3/company/{REALM_ID}/purchase"
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST")
    req.add_header("Authorization", f"Bearer {ACCESS_TOKEN}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


memory = build_memory()
accts = qbo_get("SELECT * FROM Account MAXRESULTS 1000").get("Account", [])
expense_accts = [a for a in accts if a.get("AccountType") == "Expense"]
by_name = {a["Name"]: a["Id"] for a in expense_accts}
by_fqn = {a.get("FullyQualifiedName", a["Name"]): a["Id"] for a in expense_accts}
DEFAULT = next((a for a in expense_accts if "office" in a["Name"].lower() or "misc" in a["Name"].lower()),
               expense_accts[0] if expense_accts else None)


def resolve_account(category):
    """Return (account_id, label, matched?) for a category name."""
    if category and category in by_fqn: return by_fqn[category], category, True
    if category and category in by_name: return by_name[category], category, True
    if category:
        leaf = category.split(":")[-1]
        if leaf in by_name: return by_name[leaf], leaf, True
    return (DEFAULT["Id"], DEFAULT["Name"], False) if DEFAULT else (None, None, False)


conn = psycopg2.connect(DB_URL)
cur = conn.cursor()
cur.execute("SELECT account_id, source_account_id FROM account WHERE name=%s LIMIT 1;", (ACCT_NAME,))
row = cur.fetchone()
if not row:
    print(f"No account named '{ACCT_NAME}'."); raise SystemExit
acct_uuid, acct_qbo = row

cur.execute("SELECT statement_id FROM statement WHERE account_id=%s ORDER BY created_at DESC LIMIT 1;", (acct_uuid,))
srow = cur.fetchone()
if not srow:
    print("No statement. Run make_test_statement.py and match.py first."); raise SystemExit
statement_id = srow[0]

cur.execute("""SELECT msl.line_id FROM match m JOIN match_statement_line msl ON msl.match_id=m.match_id
               WHERE m.statement_id=%s AND m.status<>'rejected';""", (statement_id,))
matched = {r[0] for r in cur.fetchall()}
cur.execute("SELECT line_id, posted_date, amount, coalesce(counterparty, description,'') FROM statement_line WHERE statement_id=%s;", (statement_id,))
to_create = [r for r in cur.fetchall() if r[0] not in matched and r[2] < 0]

if not to_create:
    print("No unrecorded money-out lines to write back."); raise SystemExit

print(f"{len(to_create)} unrecorded expense line(s):\n")
for line_id, d, amount, who in to_create:
    cat, conf, mp, score, source = suggest_category(memory, who)
    acct_id, acct_label, _ = resolve_account(cat)
    print(f"{d} | {amount} | {who}")
    if cat:
        if mp and mp != who.strip().lower():
            print(f"   recognized as '{mp}' ({score:.0%} name match)")
        print(f"   suggested: {cat}  (via {source}, {conf:.0%} confidence) -> {acct_label}")
    else:
        print(f"   no confident match -> default: {acct_label}")
    choice = input("   [Enter]=accept · type a category to override · 's'=skip: ").strip()
    if choice.lower() == "s":
        print("   skipped\n"); continue
    corrected = None
    if choice == "":
        use_id, use_label = acct_id, acct_label
    else:
        rid, rlabel, rok = resolve_account(choice)
        if not rok:
            print(f"   '{choice}' isn't a known category. Some options: {', '.join(sorted(by_name)[:8])} ...")
            print("   skipped (re-run and type one of those)\n"); continue
        use_id, use_label, corrected = rid, rlabel, rlabel
    try:
        result = create_purchase(acct_qbo, use_id, float(abs(amount)), str(d), f"Reconciliation write-back: {who}")
        print(f"   created as Purchase #{result.get('Purchase', {}).get('Id')} under {use_label}")
        if corrected:
            record_correction(who, corrected)
            print(f"   learned: '{who}' -> {corrected}  (remembered for next time)")
        print()
    except urllib.error.HTTPError as e:
        print(f"   QuickBooks rejected it (HTTP {e.code}):\n   {e.read().decode()[:400]}\n")

cur.close(); conn.close()
print("Done. Re-run pull_transactions.py then match.py Checking to reconcile.")