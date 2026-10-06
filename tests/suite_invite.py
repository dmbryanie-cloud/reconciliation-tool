"""Adding a user by invitation: the admin sets the name, email, role and (optionally) an access-until date;
the person opens the emailed link and chooses their own username and password.

Email is mocked -- nothing is sent.

Run on its own with `python tests/suite_invite.py`, or all suites with `python tests/run_all.py`.
"""
import html, re, sys
from datetime import date

import harness as H

A, c = H.setup(H.account_sql(("00000000-0000-0000-0000-0000000000a1", "35", "Stanbic", "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
A.qbo_is_connected = lambda: False


def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
admin = H.login(A)
def users_page():
    return html.unescape(admin.get("/users").data.decode())
def flash(p):
    m = re.search(r"<div id=flash[^>]*>(.*?)</div>", p, re.S)
    return re.sub(r"<[^>]+>", "", m.group(1)) if m else ""
def link_on(p):
    m = re.search(r'id=inv-link value="([^"]+)"', p)
    return m.group(1) if m else None
def path(link):
    return "/invite/" + link.rsplit("/invite/", 1)[1]

# ---- no email service: the admin gets the link to send ----------------------------------------------------------
p = users_page()
check("the users page invites instead of asking for a username and password",
      "Invite a user" in p and "name=email" in p and "name=preset" in p and "name=expires" in p and 'id=au-pw' not in p)
admin.post("/users", data={"action": "invite", "name": "Grace Nabirye", "email": "Grace@Example.com", "preset": "assistant",
                           "expires": "2030-12-31"})
p = users_page()
link = link_on(p)
check("email not set up: says so and shows the link to send", "wasn't emailed: email isn't set up" in flash(p) and link
      and link.startswith("https://") and "/invite/" in link)
check("…listed as invited until it's used", "Invited, not set up yet (1)" in p and "grace@example.com".lower() in p.lower()
      and "Accounts assistant" in p and "access until 31 Dec 2030" in p)
check("…only a hash of the link is kept", not q("SELECT 1 FROM user_invite WHERE token_hash=%s", (link.rsplit("/", 1)[1],)))
check("…the link is shown once", link_on(users_page()) is None)

# ---- the person sets themselves up ----------------------------------------------------------------------------------
guest = H.browserlike(A.app.test_client())
g = html.unescape(guest.get(path(link)).data.decode())
check("the link opens without signing in: welcome, with a suggested username", "Welcome, <b>Grace Nabirye</b>" in g
      and 'name=username value="grace"' in g and "as Accounts assistant" in g)
r = guest.post(path(link), data={"username": "grace", "password": "secret1", "confirm": "secret2"})
check("passwords that differ: asked again", "don't match" in html.unescape(r.data.decode()))
r = guest.post(path(link), data={"username": "admin", "password": "secret1", "confirm": "secret1"})
check("a taken username: asked for another", "is taken" in html.unescape(r.data.decode()))
r = guest.post(path(link), data={"username": "Grace.N", "password": "secret1", "confirm": "secret1"})
check("set up: signed straight in", r.status_code == 302 and r.headers["Location"].endswith("/")
      and "Welcome, Grace Nabirye" in html.unescape(guest.get("/").data.decode()))
row = q("SELECT username, name, perms, title, expires, email, is_admin, active FROM app_users WHERE username='grace.n'")
check("…with the name, role and access-until the admin chose", row == [("grace.n", "Grace Nabirye", "upload,review,record",
      "Accounts assistant", date(2030, 12, 31), "Grace@Example.com", False, True)])
check("…and can sign in again with them", H.browserlike(A.app.test_client()).post(
      "/login", data={"username": "grace.n", "password": "secret1"}).status_code == 302)
again = H.browserlike(A.app.test_client())
check("the link works once", "already used" in html.unescape(again.get(path(link)).data.decode()))
check("…and leaves the invited list", "Invited, not set up yet" not in users_page())

# ---- refusals, resend, cancel ---------------------------------------------------------------------------------------
r = admin.post("/users", data={"action": "invite", "name": "G2", "email": "grace@example.com", "preset": "viewer"})
check("an email that already has a sign-in: refused, the form kept open",
      "already has a sign-in (grace.n)" in flash(html.unescape(r.data.decode())) and 'value="G2"' in r.data.decode())
r = admin.post("/users", data={"action": "invite", "name": "Old", "email": "old@example.com", "preset": "viewer", "expires": "2020-01-01"})
check("an access-until date in the past: refused", "already passed" in flash(html.unescape(r.data.decode())))

SENT = []
A.BREVO_API_KEY, A.EMAIL_FROM = "key", "noreply@example.com"
A.send_email = lambda to, name, subject, text, body: SENT.append((to, subject, text)) or None
admin.post("/users", data={"action": "invite", "name": "Auditor Okello", "email": "audit@example.com", "preset": "viewer",
                           "expires": ""})
p = users_page()
first = re.search(r"https?://\S+/invite/\S+", SENT[-1][2]).group(0)
check("email set up: the invitation is emailed, no link on the page", "Invitation sent to Auditor Okello at audit@example.com"
      in flash(p) and link_on(p) is None and SENT[-1][0] == "audit@example.com" and "/invite/" in SENT[-1][2])
check("…no access-until: no end date", "Viewer" in p and "access until" not in p.split("Auditor Okello")[1].split("</div>")[0])
admin.post("/users", data={"action": "reinvite", "email": "audit@example.com"})
second = re.search(r"https?://\S+/invite/\S+", SENT[-1][2]).group(0)
check("Send again: a new link; the old one stops working", len(SENT) == 2 and second != first
      and "isn't valid" in html.unescape(guest.get(path(first)).data.decode())
      and "Welcome" in html.unescape(guest.get(path(second)).data.decode()))
n = len(SENT)
admin.post("/users", data={"action": "invitelink", "email": "audit@example.com"})
p = users_page(); third = link_on(p)
check("Copy link: a new link shown to copy, not emailed; the emailed one stops working", third and len(SENT) == n
      and "isn't valid" in html.unescape(guest.get(path(second)).data.decode())
      and "Welcome" in html.unescape(guest.get(path(third)).data.decode()) and "Copy link" in p)
second = third
admin.post("/users", data={"action": "uninvite", "email": "audit@example.com"})
check("Cancel: the link stops working", "isn't valid" in html.unescape(guest.get(path(second)).data.decode())
      and "Invited, not set up yet" not in users_page())
admin.post("/users", data={"action": "invite", "name": "Late", "email": "late@example.com", "preset": "viewer"})
late = re.search(r"https?://\S+/invite/\S+", SENT[-1][2]).group(0)
q("UPDATE user_invite SET link_until=now() - interval '1 minute' WHERE email='late@example.com' RETURNING 1")
check("an expired link: says so", "expired" in html.unescape(guest.get(path(late)).data.decode()) and "link expired" in users_page())
check("only admins can invite", guest.post("/users", data={"action": "invite", "name": "X", "email": "x@example.com",
      "preset": "admin"}).status_code in (302, 403) and not q("SELECT 1 FROM user_invite WHERE email='x@example.com'"))
sys.exit(T.summary())
