"""Security: CSRF tokens on every POST form, refusal of forged/missing tokens, cookie flags.

Run on its own with `python tests/suite_security.py`, or all suites with `python tests/run_all.py`.
"""
import importlib, io, json, re, sys, urllib.error
from datetime import datetime, timezone, timedelta
from decimal import Decimal as D

import harness as H

A, c = H.setup(H.account_sql(
    ('00000000-0000-0000-0000-0000000000a1', '35', 'Stanbic UGX', 'bank')))
cur = c.cursor()
T = H.Checker()
check = T.check

cl = H.login(A)
r = cl.post("/account/Stanbic UGX/upload", data={"statement": (io.BytesIO(b"Date,Description,Amount\n2026-01-05,Fee,-100\n"), "s.csv"),
            "closing_balance": "-100", "period_start": "2026-01-01", "period_end": "2026-01-31"}, content_type="multipart/form-data")
assert r.status_code == 302

fresh = A.app.test_client()
r = fresh.get("/login")
html = r.data.decode()
m = re.search(r'name=_csrf value="([^"]+)"', html)
check("login form carries a CSRF token", m is not None)
r = fresh.post("/login", data={"username": "", "password": H.PASSWORD})
check("sign-in without token refused (400), not signed in", r.status_code == 400 and "expired" in r.data.decode()
      and fresh.get("/").status_code == 302)
r = fresh.post("/login", data={"username": "", "password": H.PASSWORD, "_csrf": m.group(1)})
check("sign-in with token works", r.status_code == 302 and fresh.get("/").status_code == 200)
with fresh.session_transaction() as s_:
    tok = s_["csrf"]
check("token rotated at sign-in", tok != m.group(1))
cookie = r.headers.get("Set-Cookie", "")
check("session cookie is HttpOnly + SameSite=Lax", "HttpOnly" in cookie and "SameSite=Lax" in cookie)

page = fresh.get("/account/Stanbic UGX").data.decode()
n_forms = len(re.findall(r"<form\b[^>]*method=post", page, re.I))
n_tok = len(re.findall(r'<input type=hidden name=_csrf value="' + re.escape(tok) + '">', page))
check(f"every POST form on the account page has the token ({n_tok}/{n_forms})", n_forms > 5 and n_tok == n_forms)
dash = fresh.get("/").data.decode()
check("dashboard forms have it too", len(re.findall(r"<form\b[^>]*method=post", dash, re.I)) ==
      len(re.findall(r'name=_csrf value="' + re.escape(tok), dash)))

cur.execute("SELECT closing_balance FROM statement ORDER BY created_at DESC LIMIT 1"); before = cur.fetchone()[0]; c.rollback()
r1 = fresh.post("/account/Stanbic UGX/balances", data={"closing": "1"})
r2 = fresh.post("/account/Stanbic UGX/balances", data={"closing": "2", "_csrf": "forged"})
cur.execute("SELECT closing_balance FROM statement ORDER BY created_at DESC LIMIT 1"); after = cur.fetchone()[0]; c.rollback()
check("POST with missing or forged token refused, nothing changed", r1.status_code == 400 and r2.status_code == 400 and before == after)
r3 = fresh.post("/account/Stanbic UGX/balances", data={"closing": str(before), "_csrf": tok})
check("same POST with the real token goes through", r3.status_code == 302)
other = A.app.test_client(); other.get("/login")
with other.session_transaction() as s_:
    other_tok = s_["csrf"]
r4 = fresh.post("/account/Stanbic UGX/balances", data={"closing": "3", "_csrf": other_tok})
check("another session's token is refused", r4.status_code == 400)
r5 = fresh.post("/account/Stanbic UGX/upload", data={"statement": (io.BytesIO(b"Date,Description,Amount\n"), "x.csv")},
                content_type="multipart/form-data")
check("multipart upload without token refused", r5.status_code == 400)
check("GETs unaffected (CSV download)", fresh.get("/account/Stanbic UGX/exceptions.csv").status_code == 200)

sys.exit(T.summary())
