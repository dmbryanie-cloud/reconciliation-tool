"""Users & permissions: each person's email can be set from Edit, shows under their name, and a Role
column names what their ticks amount to (Admin, Accounts assistant, Approver, Viewer, or Custom).

Run on its own with `python tests/suite_users_page.py`, or all suites with `python tests/run_all.py`.
"""
import html, re, sys

import harness as H

A, c = H.setup("SELECT 1;")
cur = c.cursor()
T = H.Checker()
check = T.check
A.qbo_is_connected = lambda: False


def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
admin = H.login(A)
def page(path="/users"):
    return html.unescape(admin.get(path).data.decode())
def row_of(p, name):
    m = re.search(r"<tr[^>]*>(?:(?!</tr>).)*?<b>" + re.escape(name) + r"</b>.*?</tr>", p, re.S)
    return m.group(0) if m else ""

for un, name, preset in (("peter", "Peter", "assistant"), ("ann", "Ann", "approver"), ("vic", "Vic", "viewer")):
    admin.post("/users", data={"action": "save", "new": "1", "name": name, "username": un, "password": "secret1", "preset": preset})

p = page()
check("the table has a Role column", "<th>Role</th>" in p)
check("roles shown in it: Accounts assistant, Approver, Viewer",
      ">Accounts assistant</span>" in row_of(p, "Peter") and ">Approver</span>" in row_of(p, "Ann")
      and ">Viewer</span>" in row_of(p, "Vic"))
check("…not repeated under the name", "peter · no email</span>" in re.sub(r"<span class=faint>no email</span>", "no email", row_of(p, "Peter")))
admin.post("/users", data={"action": "perm", "username": "vic", "perm": "upload", "on": "1"})
check("ticks that match no standard role: Custom", ">Custom</span>" in row_of(page(), "Vic"))

e = page("/users?edit=peter")
check("Edit has an email field", "id=eu-email" in e and 'name=email type=email' in e)
admin.post("/users", data={"action": "save", "username": "peter", "name": "Peter", "email": "peter@example.com", "title": "", "expires": ""})
check("saving it stores the email and shows it under the name",
      q("SELECT email FROM app_users WHERE username='peter'") == [("peter@example.com",)]
      and "peter@example.com" in row_of(page(), "Peter") and 'value="peter@example.com"' in page("/users?edit=peter"))
p = html.unescape(admin.post("/users", data={"action": "save", "username": "ann", "name": "Ann", "email": "PETER@example.com"}).data.decode())
check("the same email for a second user is refused", "already the email of another user" in p
      and q("SELECT email FROM app_users WHERE username='ann'") == [(None,)])
p = html.unescape(admin.post("/users", data={"action": "save", "username": "ann", "name": "Ann", "email": "not-an-email"}).data.decode())
check("a malformed email is refused", "Type a valid email address" in p)
admin.post("/users", data={"action": "save", "username": "peter", "name": "Peter", "email": ""})
check("emptying it removes the email", q("SELECT email FROM app_users WHERE username='peter'") == [(None,)])
check("the contact email on Terms is the ReconBook address", "reconbook@northgreen.ac.ug" in page("/terms"))
sys.exit(T.summary())
