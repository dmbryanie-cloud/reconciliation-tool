"""The weekday-morning reminder email: off until an admin ticks it, sent once a day after REMIND_HOUR (EAT),
Monday to Friday, to active people with an email address who work on reconciliations, listing each open
reconciliation and what's left on it. A test copy can be sent from Settings.

Email is mocked -- nothing is sent. Run on its own with `python tests/suite_reminders.py`, or all suites with
`python tests/run_all.py`.
"""
import html, io, re, sys
from datetime import datetime

import harness as H

ACCT = "00000000-0000-0000-0000-0000000000a1"
NAME = "KCB 09708"
A, c = H.setup(H.account_sql((ACCT, "35", NAME, "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
A.qbo_is_connected = lambda: False
SENT = []
A.BREVO_API_KEY, A.EMAIL_FROM = "key", "noreply@example.com"
A.send_email = lambda to, name, subject, text, body: SENT.append((to, subject, text)) or None


def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.commit(); return r
admin = H.login(A)
def settings():
    return html.unescape(admin.get("/settings").data.decode())
def flash(p):
    m = re.search(r"<div id=flash[^>]*>(.*?)</div>", p, re.S)
    return re.sub(r"<[^>]+>", "", m.group(1)) if m else ""

for un, name, preset, email in (("peter", "Peter", "assistant", "peter@example.com"), ("vic", "Viewer", "viewer", "v@example.com"),
                                ("noemail", "No Email", "assistant", "")):
    admin.post("/users", data={"action": "save", "new": "1", "name": name, "username": un, "password": "secret1", "preset": preset})
    q("UPDATE app_users SET email=%s WHERE username=%s RETURNING 1", (email or None, un))

MON9 = datetime(2026, 11, 23, 9, 0, tzinfo=A.EAT)
check("off by default: nothing sent", A.maybe_send_reminders(MON9) == 0 and not SENT)
p = settings()
check("Settings offers the reminder tick box and a test send", "Email reminders of open reconciliations" in p
      and "<select name=remind" in p and "Quarterly (first weekday)" in p and "Email me today's reminder now" in p)
admin.post("/settings", data={"action": "signoff", "two_person": "1", "close_day": "10", "remind": "1"})
check("daily chosen and saved", A.rule("remind") == 1 and 'value="1" selected' in settings())
check("nothing open: nothing sent", A.maybe_send_reminders(MON9) == 0 and not SENT)

body = "Date,Description,Amount,Balance\n2026-11-02,RENT,-100,900\n2026-11-06,FEES,50,950\n"
admin.post(f"/account/{NAME}/upload", data={"statement": (io.BytesIO(body.encode()), "s.csv")}, content_type="multipart/form-data")
q("DELETE FROM app_config WHERE key='remind_sent' RETURNING 1")
check("before 8 o'clock: not yet", A.maybe_send_reminders(datetime(2026, 11, 23, 7, 30, tzinfo=A.EAT)) == 0 and not SENT)
check("Saturday: not sent", A.maybe_send_reminders(datetime(2026, 11, 28, 9, 0, tzinfo=A.EAT)) == 0 and not SENT)
n = A.maybe_send_reminders(MON9)
to = sorted(t for t, _s, _x in SENT)
check("weekday morning: sent to people who work on reconciliations and have an email (not viewers)",
      n == 1 and to == ["peter@example.com"])
check("…listing each open reconciliation and what's left", SENT and "KCB 09708 (02/11/2026 to 06/11/2026)" in SENT[0][2]
      and "2 bank lines to match or record" in SENT[0][2] and SENT[0][1] == "ReconBook: 1 reconciliation open")
check("…only once a day", A.maybe_send_reminders(datetime(2026, 11, 23, 15, 0, tzinfo=A.EAT)) == 0 and len(SENT) == 1)
check("…and again the next weekday", A.maybe_send_reminders(datetime(2026, 11, 24, 8, 5, tzinfo=A.EAT)) == 1 and len(SENT) == 2)

# ---- how often ----------------------------------------------------------------------------------------------------
def runs(freq, days):
    """Which of these November/December 2026 days (9am) a reminder goes out on, at this frequency."""
    admin.post("/settings", data={"action": "signoff", "two_person": "1", "close_day": "10", "remind": str(freq)})
    q("DELETE FROM app_config WHERE key='remind_sent' RETURNING 1")
    out = []
    for m, d in days:
        if A.maybe_send_reminders(datetime(2026, m, d, 9, 0, tzinfo=A.EAT)):
            out.append((m, d))
    return out
week = [(11, d) for d in range(23, 31)] + [(12, d) for d in range(1, 8)]     # Mon 23 Nov .. Mon 7 Dec
check("weekly: Mondays only (once a week)", runs(2, week) == [(11, 23), (11, 30), (12, 7)])
from datetime import date as _d, timedelta as _td
four = [((_d(2026, 11, 30) + _td(i)).month, (_d(2026, 11, 30) + _td(i)).day) for i in range(28)]
got = runs(3, four)
check("every two weeks: twice in four weeks, 14 days apart, on Mondays", len(got) == 2
      and (_d(2026, *got[1]) - _d(2026, *got[0])).days == 14 and _d(2026, *got[0]).weekday() == 0)
check("monthly: the first weekday of each month", runs(4, [(11, 23), (11, 30), (12, 1), (12, 2), (12, 15)]) == [(11, 23), (12, 1)])
check("quarterly: once a quarter", runs(5, [(11, 23), (12, 1), (12, 31)]) == [(11, 23)]
      and A.remind_period(5, datetime(2027, 1, 4).date()) != A.remind_period(5, datetime(2026, 12, 31).date()))
check("never: nothing sent", runs(0, week) == [])
n_before = len(SENT)

# ---- the test send from Settings -----------------------------------------------------------------------------------
admin.post("/settings", data={"action": "remind_test"})
check("test send without an email on your user: says so", "no email address" in flash(settings()) and len(SENT) == n_before)
peter = H.browserlike(A.app.test_client())
peter.post("/login", data={"username": "peter", "password": "secret1"})
q("UPDATE app_users SET is_admin=true WHERE username='peter' RETURNING 1")
peter.post("/settings", data={"action": "remind_test"})
check("test send: emailed to you only", len(SENT) == n_before + 1 and SENT[-1][0] == "peter@example.com")
sys.exit(T.summary())
