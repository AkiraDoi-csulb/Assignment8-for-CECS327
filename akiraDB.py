import psycopg2

HOUSE_A_CONN = "postgresql://neondb_owner:npg_BcIL4nvy0CbD@ep-young-voice-anpwrm64-pooler.c-6.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

conn = psycopg2.connect(HOUSE_A_CONN)
cur = conn.cursor()

print("=== table_virtual (5 rows) ===")
cur.execute('SELECT * FROM "table_virtual" LIMIT 5;')
rows = cur.fetchall()
col_names = [desc[0] for desc in cur.description]
print("Columns:", col_names)
for row in rows:
    print(row)

print()

conn.close()