import os
import base64
import json
import urllib.parse
import urllib.request

TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"


def get_access_token():
    """Use the long-lived refresh token to mint a fresh 1-hour access token."""
    client_id = os.environ["QBO_CLIENT_ID"]
    client_secret = os.environ["QBO_CLIENT_SECRET"]
    refresh_token = os.environ["QBO_REFRESH_TOKEN"]

    creds = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    body = urllib.parse.urlencode({
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }).encode()

    req = urllib.request.Request(TOKEN_URL, data=body, method="POST")
    req.add_header("Authorization", f"Basic {creds}")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    req.add_header("Accept", "application/json")

    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read())

    new_refresh = data.get("refresh_token")
    if new_refresh and new_refresh != refresh_token:
        print("NOTE: your refresh token rotated. Update the QBO_REFRESH_TOKEN secret with this value:")
        print(f"      {new_refresh}")

    return data["access_token"]


if __name__ == "__main__":
    token = get_access_token()
    print("Got a fresh access token automatically (no Playground needed).")
    realm = os.environ["QBO_REALM_ID"]
    base = "https://sandbox-quickbooks.api.intuit.com"
    url = f"{base}/v3/company/{realm}/query?query=" + urllib.parse.quote("SELECT count(*) FROM Account")
    r = urllib.request.Request(url)
    r.add_header("Authorization", f"Bearer {token}")
    r.add_header("Accept", "application/json")
    with urllib.request.urlopen(r) as resp:
        d = json.loads(resp.read())
    total = d.get("QueryResponse", {}).get("totalCount")
    print(f"Test call worked — your sandbox has {total} accounts. Auto-refresh is live!")