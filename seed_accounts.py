import os
import json
import urllib.parse
import urllib.request
import psycopg2

from qbo_auth import get_access_token
ACCESS_TOKEN = get_access_token()
REALM_ID = os.environ["QBO_REALM_ID"]
DB_URL = os.environ["SUPABASE_DB_URL"]
BASE = "https://sandbox-quickbooks.api.intuit.com"

# 1. Pull accounts from QuickBooks (same call as before)
query = "SELECT * FROM Account MAXRESULTS 100"
url = f"{BASE}/v3/company/{REALM_ID}/query?query=" + urllib.parse.quote(query)
req = urllib.request.Request(url)
req.add_header("Authorization", f"Bearer {ACCESS_TOKEN}")
req.add_header("Accept", "application/json")
with urllib.request.urlopen(req) as resp:
    qbo = json.loads(resp.read())
all_accounts = qbo.get("QueryResponse", {}).get("Account", [])

# 2. Keep only the reconcilable ones: bank and credit-card accounts
TYPE_MAP = {"Bank": "bank", "Credit Card": "credit_card"}
reconcilable = [a for a in all_accounts if a.get("AccountType") in TYPE_MAP]
print(f"Found {len(reconcilable)} reconcilable accounts in QuickBooks.")

# 3. Open the database
conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

# 4. Seed the tenant (organization) and the QuickBooks connection.
#    ON CONFLICT makes re-running safe.
cur.execute("""
    INSERT INTO organization (org_id, name)
    VALUES ('00000000-0000-0000-0000-000000000001', 'My first tenant')
    ON CONFLICT (org_id) DO NOTHING;
""")
cur.execute("""
    INSERT INTO connection (org_id, source_platform, source_company_id, credential_ref, display_name)
    VALUES ('00000000-0000-0000-0000-000000000001', 'qbo', %s, 'replit-secret', 'QBO Sandbox')
    ON CONFLICT (source_platform, source_company_id) DO NOTHING;
""", (REALM_ID,))

# Get the connection_id we just made (or already had)
cur.execute("SELECT connection_id FROM connection WHERE source_platform='qbo' AND source_company_id=%s;", (REALM_ID,))
connection_id = cur.fetchone()[0]

# 5. Insert each reconcilable account
for a in reconcilable:
    cur.execute("""
        INSERT INTO account (org_id, connection_id, source_account_id, name, type, currency)
        VALUES ('00000000-0000-0000-0000-000000000001', %s, %s, %s, %s, %s)
        ON CONFLICT (connection_id, source_account_id) DO NOTHING;
    """, (
        connection_id,
        a.get("Id"),
        a.get("Name"),
        TYPE_MAP[a.get("AccountType")],
        a.get("CurrencyRef", {}).get("value", "USD"),
    ))
    print(f"  wrote: {a.get('Name')} ({TYPE_MAP[a.get('AccountType')]})")

conn.commit()

# 6. Confirm by reading back from the database
cur.execute("SELECT name, type, currency FROM account;")
print("\nNow in your 'account' table:")
for row in cur.fetchall():
    print(f"  - {row[0]} | {row[1]} | {row[2]}")

cur.close()
conn.close()