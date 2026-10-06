"""Editing a suggested match: the Edit button opens Match manually with the suggestion's items ticked.

A batched suggestion pairs a 4,600 QuickBooks charge with the wrong 600 excise line; the user swaps
it for the right one. Covers the button (its script run in jsdom), matching the edited group by hand
(the old suggestion is then rejected), and editing one that was already confirmed.
QuickBooks is fully mocked -- no network.

Run on its own with `python tests/suite_edit_match.py`, or all suites with `python tests/run_all.py`.
"""
import io, json, os, shutil, subprocess, sys, tempfile

import harness as H

ACCT = "00000000-0000-0000-0000-0000000000a1"
A, c = H.setup(H.account_sql((ACCT, "35", "Stanbic", "bank")))
cur = c.cursor()
T = H.Checker()
check = T.check
A.qbo_is_connected = lambda: False

cur.execute("""INSERT INTO book_txn (org_id, account_id, source_txn_id, source_txn_type, posted_date, amount, currency,
               description, counterparty, last_modified)
               VALUES (%s,%s,'b1','Purchase','2026-04-30',-4600,'UGX','Bank charges April','Stanbic',now())
               RETURNING txn_id""", (A.ORG_ID, ACCT))
T1 = str(cur.fetchone()[0]); c.commit()

cl = H.login(A)
stmt = ("Date,Description,Amount\n2026-04-30,LEDGER FEE,-4000\n2026-04-30,EXCISE DUTY A,-600\n"
        "2026-05-02,EXCISE DUTY B,-600\n")
r = cl.post("/account/Stanbic/upload", data={"replace": "1", "statement": (io.BytesIO(stmt.encode()), "s.csv"), "closing_balance": "0",
            "period_start": "2026-04-01", "period_end": "2026-05-31"}, content_type="multipart/form-data")
assert r.status_code == 302

def q(sql, args=()):
    cur.execute(sql, args); r = cur.fetchall(); c.rollback(); return r
def lid(desc):
    return str(q("SELECT line_id FROM statement_line WHERE description=%s", (desc,))[0][0])
def matches():
    """{(frozenset(line descs), status, created_by)} for this statement."""
    return {(frozenset(r[0]), r[1], r[2]) for r in q("""
        SELECT array(SELECT sl.description FROM match_statement_line msl JOIN statement_line sl USING (line_id)
                     WHERE msl.match_id=m.match_id), m.status, m.created_by FROM match m""")}
L1, LA, LB = lid("LEDGER FEE"), lid("EXCISE DUTY A"), lid("EXCISE DUTY B")
WRONG = frozenset({"LEDGER FEE", "EXCISE DUTY A"})
RIGHT = frozenset({"LEDGER FEE", "EXCISE DUTY B"})
check("the matcher suggests a batch (4,000 + 600 = 4,600)", (WRONG, "proposed", "engine") in matches())

page = cl.get("/account/Stanbic").data.decode()
check("the batch has an Edit button carrying its items",
      f'data-lines="{L1},{LA}"' in page or f'data-lines="{LA},{L1}"' in page)
check("…and its QuickBooks side", f'data-txns="{T1}"' in page)


def browser(html, script):
    """Run `script` against the page in jsdom; returns the JSON lines it prints."""
    node = shutil.which("node")
    if not node or not os.path.isdir(os.path.join(H.HERE, "node_modules", "jsdom")):
        check("jsdom available (run: cd tests && npm install)", False)
        return []
    with tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8") as f:
        f.write(html)
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, dir=H.HERE, encoding="utf-8") as g:
        g.write("""
const { JSDOM } = require("jsdom");
const html = require("fs").readFileSync(process.argv[2], "utf8");
const dom = new JSDOM(html, { runScripts: "dangerously", url: "http://app.test/account/Stanbic" });
const w = dom.window, doc = w.document;
w.HTMLElement.prototype.scrollIntoView = function () {};
function state() {
  const on = n => Array.from(doc.querySelectorAll("input[name=" + n + "]:checked")).map(c => c.value);
  const first = id => { const r = doc.querySelector("#" + id + " .mmrow input"); return r ? r.value : null; };
  return { ml: on("ml"), mb: on("mb"), orig: doc.getElementById("mmorig").value,
           note: doc.getElementById("mmnote").style.display !== "none", firstLine: first("mml"),
           sum: doc.getElementById("mmsum").textContent };
}
""" + script)
    try:
        out = subprocess.run([node, g.name, f.name], capture_output=True, text=True, encoding="utf-8", cwd=H.HERE)
    finally:
        os.unlink(f.name); os.unlink(g.name)
    if out.stderr.strip():
        print(out.stderr[-1500:])
    check("browser: the page's script ran without errors", "Error" not in out.stderr)
    return [json.loads(l) for l in out.stdout.splitlines() if l.startswith("{")]


got = browser(page, """
console.log(JSON.stringify(state()));
doc.querySelector(".mm-edit").click();
console.log(JSON.stringify(state()));
doc.querySelector(".mm-open") && doc.querySelector(".mm-open").click();
""")
check("browser: nothing ticked before Edit", len(got) == 2 and not got[0]["ml"] and not got[0]["note"])
e = got[1] if len(got) == 2 else {}
check("browser: Edit ticks exactly the suggestion's items", sorted(e.get("ml", [])) == sorted([L1, LA]) and e.get("mb") == [T1])
check("browser: …shows them first in the list", e.get("firstLine") in (L1, LA))
check("browser: …remembers which suggestion is being edited", e.get("orig") in (f"{L1},{LA}|{T1}", f"{LA},{L1}|{T1}"))
check("browser: …says what to do", e.get("note") is True)
check("browser: …totals show it balancing", "Difference 0.00" in e.get("sum", ""))

# The user swaps the wrong excise line for the right one.
orig = e.get("orig") or f"{L1},{LA}|{T1}"
r = cl.post("/account/Stanbic/match", data={"ml": [L1, LB], "mb": [T1], "orig": orig})
msg = cl.get("/account/Stanbic").data.decode()
m = matches()
check("edited group matched by hand", (RIGHT, "confirmed", "user") in m)
check("the old suggestion is marked rejected, not suggested again", (WRONG, "rejected", "engine") in m
      and (WRONG, "proposed", "engine") not in m)
check("…and the message says so", "suggestion you edited is marked rejected" in msg)
check("the line taken out is left to match or record", not any("EXCISE DUTY A" in s and st != "rejected" for s, st, _ in m))

# Undo it: the rejected suggestion can still be restored like any other.
um = q("SELECT match_id FROM match WHERE created_by='user'")[0][0]
cl.post(f"/account/Stanbic/unmatch/{um}")
rj = q("SELECT match_id FROM match WHERE status='rejected'")[0][0]
cl.post(f"/account/Stanbic/review/{rj}", data={"status": "proposed"})
check("undo + restore brings the original suggestion back", (WRONG, "proposed", "engine") in matches())

# Matching the same items as the suggestion isn't an edit: nothing is rejected.
pid = q("SELECT match_id FROM match WHERE status='proposed'")[0][0]
cl.post("/account/Stanbic/match", data={"ml": [L1, LA], "mb": [T1], "orig": f"{L1},{LA}|{T1}"})
check("matching it unchanged rejects nothing", not any(st == "rejected" for _, st, _ in matches()))
um = q("SELECT match_id FROM match WHERE created_by='user'")[0][0]
cl.post(f"/account/Stanbic/unmatch/{um}")

# A confirmed suggestion: Edit takes it back to review and opens the form with its items.
pid = q("SELECT match_id FROM match WHERE status='proposed'")[0][0]
cl.post(f"/account/Stanbic/review/{pid}", data={"status": "confirmed"})
check("(suggestion confirmed)", (WRONG, "confirmed", "engine") in matches())
page = cl.get("/account/Stanbic").data.decode()
check("a confirmed suggestion's Edit goes through the server", 'name=status value=edit' in page)
cid = q("SELECT match_id FROM match WHERE status='confirmed'")[0][0]
r = cl.post(f"/account/Stanbic/review/{cid}", data={"status": "edit"})
check("…opens at Match manually", r.status_code == 302 and r.headers["Location"].endswith("#sec-manual"))
check("…with the suggestion back to review, so its items can be picked", (WRONG, "proposed", "engine") in matches())
page = cl.get("/account/Stanbic").data.decode()
got = browser(page, "console.log(JSON.stringify(state()));")
e = got[0] if got else {}
check("browser: the page opens with its items ticked", sorted(e.get("ml", [])) == sorted([L1, LA]) and e.get("mb") == [T1]
      and e.get("note") is True)
again = cl.get("/account/Stanbic").data.decode()
check("…only once (not on the next visit)", 'var E=null' in again)
sys.exit(T.summary())
