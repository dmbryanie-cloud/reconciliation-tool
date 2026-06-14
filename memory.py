import os
import re
import sys
import psycopg2
from collections import Counter
from difflib import SequenceMatcher

DB_URL = os.environ["SUPABASE_DB_URL"]
ORG_ID = "00000000-0000-0000-0000-000000000001"

STOPWORDS = {"and", "the", "of", "inc", "llc", "ltd", "co", "corp", "company",
             "services", "service", "pos", "debit", "purchase", "payment",
             "card", "visa", "ach", "ppd", "tst"}


def normalize(name):
    if not name: return ""
    s = re.sub(r"[^a-z0-9 ]", " ", name.lower())
    return re.sub(r"\s+", " ", s).strip()

def tokens(name):
    return [t for t in normalize(name).split()
            if len(t) > 1 and not t.isdigit() and t not in STOPWORDS]

def _tok_match(a, b):
    return a == b or (len(a) >= 3 and len(b) >= 3 and (a.startswith(b) or b.startswith(a)))

def _coverage(known, inn):
    if not known: return 0.0
    return sum(1 for k in known if any(_tok_match(k, i) for i in inn)) / len(known)

def _conn():
    return psycopg2.connect(DB_URL)

def ensure_tables():
    conn = _conn(); cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS payee_correction (
        id serial PRIMARY KEY, org_id uuid NOT NULL,
        payee text NOT NULL, category text NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now());""")
    conn.commit(); cur.close(); conn.close()

def record_correction(payee, category, org_id=ORG_ID):
    ensure_tables()
    conn = _conn(); cur = conn.cursor()
    cur.execute("INSERT INTO payee_correction (org_id, payee, category) VALUES (%s,%s,%s);",
                (org_id, payee, category))
    conn.commit(); cur.close(); conn.close()

def build_memory():
    ensure_tables()
    conn = _conn(); cur = conn.cursor()
    cur.execute("""SELECT lower(trim(counterparty)), category FROM book_txn
                   WHERE category IS NOT NULL AND counterparty IS NOT NULL AND trim(counterparty)<>'';""")
    hist_rows = cur.fetchall()
    cur.execute("SELECT lower(trim(payee)), category FROM payee_correction ORDER BY created_at;")
    corr_rows = cur.fetchall()
    cur.close(); conn.close()

    byp = {}
    for p, c in hist_rows: byp.setdefault(p, Counter())[c] += 1
    history = {}
    for p, counts in byp.items():
        total = sum(counts.values()); bc, bn = counts.most_common(1)[0]
        history[p] = (bc, bn / total, total)
    corrections = {}
    for p, c in corr_rows: corrections[p] = c   # ascending order -> latest wins
    return {"history": history, "corrections": corrections}

def _best_match(keys, payee):
    in_toks, in_norm = tokens(payee), normalize(payee)
    best_key, best = None, 0.0
    for key in keys:
        kt = tokens(key)
        if not kt: continue
        score = max(_coverage(kt, in_toks), SequenceMatcher(None, in_norm, normalize(key)).ratio())
        if score > best: best_key, best = key, score
    return best_key, best

def suggest_category(memory, payee, threshold=0.6):
    """Return (category, confidence, matched_payee, name_score, source)."""
    if not payee: return None, 0.0, None, 0.0, None
    key = payee.strip().lower()
    corr, hist = memory["corrections"], memory["history"]
    # Corrections take precedence — they're your explicit instruction
    if key in corr: return corr[key], 1.0, key, 1.0, "your correction"
    bk, score = _best_match(corr.keys(), payee)
    if bk and score >= threshold: return corr[bk], 1.0, bk, score, "your correction"
    # Then fall back to QuickBooks history
    if key in hist:
        c, conf, _ = hist[key]; return c, conf, key, 1.0, "history"
    bk, score = _best_match(hist.keys(), payee)
    if bk and score >= threshold:
        c, conf, _ = hist[bk]; return c, conf, bk, score, "history"
    return None, 0.0, None, 0.0, None


if __name__ == "__main__":
    mem = build_memory()
    print(f"Memory: {len(mem['history'])} payees from history, {len(mem['corrections'])} learned corrections.\n")
    for t in (sys.argv[1:] or ["CHINS GAS #4471 KAMPALA", "Kampala Power Co", "Totally New Vendor"]):
        cat, conf, matched, score, source = suggest_category(mem, t)
        print(f"  '{t}'")
        if cat:
            extra = f", matched '{matched}' {score:.0%}" if matched and matched != t.strip().lower() else ""
            print(f"     -> {cat}  (via {source}{extra}, {conf:.0%} confidence)\n")
        else:
            print("     -> UNKNOWN\n")