import os
import json
import urllib.parse
import urllib.request
import urllib.error
import psycopg2
from qbo_auth import get_access_token

ACCESS_TOKEN = get_access_token()
REALM_ID = os.environ["QBO_REALM_ID"]
DB_URL = os.environ["SUPABASE_DB_URL"]
BASE = "https://sandbox-quickbooks.api.intuit.com"
TAG = "Reconciliation write-back:"   # the note we stamped on every test write-back


def qbo_query(q):
    url = f"{BASE}/v3/company/{REALM_ID}/query?query=" + urllib.parse.quote(q)
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {ACCESS_TOKEN}")
    req.add_header("Accept", "application/json")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read()).get("QueryResponse", {})


def delete_purchase(pid, synctoken):
    url = f"{BASE}/v3/company/{REALM_ID}/purchase?operation=delete"
    body = {"Id": pid, "SyncToken": synctoken}
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST")
    req.add_header("Authorization", f"Bearer {ACCESS_TOKEN}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


# Find the Purchases we created during testing (identified by our note)
purchases = qbo_query("SELECT * FROM Purchase MAXRESULTS 1000").get("Purchase", [])
test_ones = [p for p in purchases if TAG in (p.get("PrivateNote") or "")]

print(f"Found {len(test_ones)} test write-back transaction(s) to remove.\n")
if not test_ones:
    print("Nothing to clean up."); raise SystemExit

for p in test_ones:
    note = (p.get("PrivateNote") or "").replace(TAG, "").strip()
    print(f"  deleting Purchase #{p['Id']}  ({p.get('TxnDate')}, {p.get('TotalAmt')}, {note})")
    try:
        delete_purchase(p["Id"], p["SyncToken"])
    except urllib.error.HTTPError as e:
        print(f"     could not delete: HTTP {e.code} {e.read().decode()[:200]}")

# Remove the same rows from the local database
conn = psycopg2.connect(DB_URL)
cur = conn.cursor()
cur.execute("DELETE FROM book_txn WHERE description LIKE %s;", (TAG + "%",))
removed = cur.rowcount
conn.commit()
cur.close(); conn.close()
print(f"\nRemoved {removed} matching row(s) from your local book_txn.")
print("Now re-run:  python3 pull_transactions.py")