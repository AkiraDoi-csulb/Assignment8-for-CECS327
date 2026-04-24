import psycopg2

HOUSE_A_CONN = "postgresql://neondb_owner:npg_WcEeoxSyXQ74@ep-dawn-rice-a4oyqq79-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

try:
    conn = psycopg2.connect(HOUSE_A_CONN)
    cur = conn.cursor()

    cur.execute("SELECT NOW();")
    print("✅ Connected! DB time:", cur.fetchone()[0])

    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public';
    """)
    tables = cur.fetchall()
    print("📋 Tables found:", [t[0] for t in tables])

    cur.execute('SELECT COUNT(*) FROM "Table1_virtual";')
    print("📊 Rows in Table1_virtual:", cur.fetchone()[0])

    cur.close()
    conn.close()

except Exception as e:
    print("❌ Connection failed:", e)