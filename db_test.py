import os
import psycopg2

DB_URL = os.environ["SUPABASE_DB_URL"]

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()
cur.execute("SELECT count(*) FROM account;")
count = cur.fetchone()[0]
print(f"Connected to Supabase! The 'account' table currently has {count} rows.")
cur.close()
conn.close()