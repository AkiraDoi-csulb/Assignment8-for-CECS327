import psycopg2

HOUSE_A_CONN = "postgresql://neondb_owner:npg_BcIL4nvy0CbD@ep-young-voice-anpwrm64-pooler.c-6.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

try:
    conn = psycopg2.connect(HOUSE_A_CONN)
    cur = conn.cursor()

    # Test 1: basic connection
    cur.execute("SELECT NOW();")
    print("✅ Connected! DB time:", cur.fetchone()[0])

    # Test 2: list your tables
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public';
    """)
    tables = cur.fetchall()
    print("📋 Tables found:", [t[0] for t in tables])

    # Test 3: row count in sensor_data
    cur.execute("SELECT COUNT(*) FROM table_virtual;")
    print("📊 Rows in sensor_data:", cur.fetchone()[0])

    conn.close()

except Exception as e:
    print("❌ Connection failed:", e)
