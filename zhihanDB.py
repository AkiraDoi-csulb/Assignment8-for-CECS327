import psycopg2

HOUSE_B_CONN = "postgresql://neondb_owner:npg_WcEeoxSyXQ74@ep-dawn-rice-a4oyqq79-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

conn = psycopg2.connect(HOUSE_B_CONN)
cur = conn.cursor()

# See Table1_virtual columns and sample data
print("=== Table1_virtual (100 rows) ===")
cur.execute('SELECT * FROM "Assignment8_virtual" LIMIT 5;')
rows = cur.fetchall()
col_names = [desc[0] for desc in cur.description]  # get column names
print("Columns:", col_names)
for row in rows:
    print(row)

print()

conn.close()