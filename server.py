# Assignment8: Build a Distributed End-to-End IoT System
# Name       : Akira Doi, Zhihan Yao
# Date       : 4/30/2026
# server.py

# socket   : built-in library for TCP network communication
# psycopg2 : PostgreSQL driver to connect to NeonDB
# datetime : for time range calculations and PST conversion
import socket
import os
import json
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta

# CONFIG: Connect with Databases and some setting
# House A = Akira's NeonDB
# HOUSE_A_CONN = "postgresql://neondb_owner:npg_BcIL4nvy0CbD@ep-young-voice-anpwrm64-pooler.c-6.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
# # House B = Zhihan's NeonDB
# HOUSE_B_CONN = "postgresql://neondb_owner:npg_WcEeoxSyXQ74@ep-dawn-rice-a4oyqq79-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

# When DataNiz sharing was enabled (UTC)
HOUSE_A_CONN = os.getenv("House_A_CONN")
HOUSE_B_CONN = os.getenv("House_B_CONN")

HOUSE_A_PRE_TABLE = os.getenv("HOUSE_A_PRE_TABLE", "table_virtual")
HOUSE_B_MAIN_TABLE = os.getenv("HOUSE_B_MAIN_TABLE", "Assignment8_virtual")

SERVER_PORT = int(os.getenv("SERVER_PORT", "5050"))
# When DataNiz sharing was enabled (UTC)
# First shared row appears at 2026-04-24 22:50:03 UTC
SHARING_START = datetime(2026, 4, 24, 22, 50, 0, tzinfo=timezone.utc)

# PST = UTC - 7 hours (daylight saving time in April)
PST = timezone(timedelta(hours=-7))

# Unit conversion: 1 liter = 0.264172 US gallons
LITERS_TO_GALLONS = 0.264172

# DataNiz topic identifiers for each house
TOPIC_A = "akira.doi01@student.csulb.edu"
TOPIC_B = "zhihanyao121@gmail.com"

# DATABASE CONNECTION AND TIME HELPERS
# Return a psycopg2 connection for House A or House B
# connect_timeout=10 prevents indefinite hanging on slow connections
def get_conn(house="A"):
    conn_str = HOUSE_A_CONN if house == "A" else HOUSE_B_CONN
    return psycopg2.connect(conn_str, connect_timeout=10)

# Convert a UTC datetime to a readable PST string for output
def to_pst_str(utc_dt):
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=timezone.utc)
    return utc_dt.astimezone(PST).strftime("%Y-%m-%d %H:%M:%S PST")

# Read the data from the PAYLOAD
#
# All sensor data lives in the payload.
# Each function checks for the correct key for that sensor.
# Returns a float value or None if the row is not relevant.
#
# TABLE is used for actual data:
# House A (Akira):
#   table_virtual        → Akira's pre-sharing data
#   Assignment8_virtual  → post-sharing data for BOTH houses
#                          filter by topic containing TOPIC_A
#
# House B (Zhihan):
#   Table1_virtual       → Zhihan's pre-sharing data
#   Assignment8_virtual  → post-sharing data for BOTH houses
#                          filter by topic containing TOPIC_B
#
# SENSOR KEYS (from actual payload inspection):
#   Moisture  → 'Moisture Meter - Moisture Meter' or starts with same
#   Dishwasher→ 'Float Switch - Float Switch'
#   Electricity→ 'Ammeter' (main board) or 'Ammeter 3 UUID...'

# Extract moisture value from any payload (works for both houses)
# Handles: 'Moisture Meter - Moisture Meter', 'Moisture', 'LM386 - Sensor1'
# and their UUID-suffixed duplicate board variants
def extract_moisture(payload):
    # Primary Akira/Zhihan shared board key
    if 'Moisture Meter - Moisture Meter' in payload:
        val = float(payload['Moisture Meter - Moisture Meter'])
        return val if val > 0 else None
    # Zhihan's original fridge key (newer data)
    if 'Moisture' in payload:
        val = float(payload['Moisture'])
        return val if val > 0 else None
    # Zhihan's early fridge key
    if 'LM386 - Sensor1' in payload:
        val = float(payload['LM386 - Sensor1'])
        return val if val > 0 else None
    # Duplicate board keys with UUID suffix — check all keys
    for key in payload:
        if 'Moisture Meter' in key or (key.startswith('Moisture') and 'Ammeter' not in key):
            try:
                val = float(payload[key])
                return val if val > 0 else None
            except (ValueError, TypeError):
                continue
    return None

# Extract dishwasher water usage from payload (liters per cycle)
# Akira uses: 'Float Switch - Float Switch'
# Zhihan uses: 'water consumption sensor' or 'Float Switch - Float Switch'
def extract_water(payload):
    if 'Float Switch - Float Switch' in payload:
        val = float(payload['Float Switch - Float Switch'])
        return val if val > 0 else None
    if 'water consumption sensor' in payload:
        val = float(payload['water consumption sensor'])
        return val if val > 0 else None
    return None

# Extract electricity reading from payload
# Main board: 'Ammeter'
# Duplicate board: 'Ammeter 3 UUID...' or 'Ammeter 1 UUID...'
def extract_electricity(payload):
    if 'Ammeter' in payload:
        val = float(payload['Ammeter'])
        return val if val > 0 else None
    # Duplicate board ammeter keys with UUID suffix
    for key in payload:
        if key.startswith('Ammeter'):
            try:
                val = float(payload[key])
                if val > 0:
                    return val
            except (ValueError, TypeError):
                continue
    return None


# Use LINKED LIST
# Required by assignment to manage retrieved sensor records

# Single node — holds one sensor reading and its house label
class Node:
    def __init__(self, value, house):
        self.value = value   # float sensor reading
        self.house = house   # "House A" or "House B"
        self.next  = None    # pointer to next node

# Singly linked list to store all readings from both houses
class LinkedList:
    def __init__(self):
        self.head = None
        self.size = 0

    # Add a new reading to the end of the list
    def append(self, value, house):
        new_node = Node(value, house)
        if not self.head:
            self.head = new_node
        else:
            cur = self.head
            while cur.next:
                cur = cur.next
            cur.next = new_node
        self.size += 1

    # Return all (value, house) pairs as a plain Python list
    def get_all(self):
        result = []
        cur = self.head
        while cur:
            result.append((cur.value, cur.house))
            cur = cur.next
        return result

    # Return total values and counts per house (used for electricity)
    def sum_by_house(self):
        totals = {"House A": 0.0, "House B": 0.0}
        counts = {"House A": 0,   "House B": 0}
        cur = self.head
        while cur:
            if cur.house in totals:
                totals[cur.house] += cur.value
                counts[cur.house] += 1
            cur = cur.next
        return totals, counts


# DATA FETCHERS
# Data layout:
#   - House A data: query table_virtual  + Assignment8_virtual (topic=akira) or table_ass8_virtual
#   - House B data: query Table1_virtual + Assignment8_virtual (topic=zhihan)

# Determine house ownership from the topic field in payload
# This uses DataNiz metadata to identify which house owns each reading
# TOPIC_A = akira.doi01@student.csulb.edu → House A
# TOPIC_B = zhihanyao121@gmail.com        → House B
def get_house_from_topic(payload):
    topic = payload.get("topic", "")
    if TOPIC_A in topic:
        return "House A"
    elif TOPIC_B in topic:
        return "House B"
    return None  # unknown topic — skip this record

# Fetch all data from Akira's table_virtual
# House is determined by topic — could contain either house's data
def fetch_table_virtual(start_dt, end_dt, extractor):
    ll = LinkedList()
    try:
        conn = get_conn("A")
        cur  = conn.cursor()
        cur.execute("""
            SELECT payload FROM "table_virtual"
            WHERE time >= %s AND time <= %s
            ORDER BY time ASC
        """, (start_dt, end_dt))
        rows = cur.fetchall()
        conn.close()
        for row in rows:
            # Determine house from topic metadata (DataNiz device ownership)
            house = get_house_from_topic(row[0])
            if house is None:
                continue  # skip unknown topics
            val = extractor(row[0])
            if val is not None:
                ll.append(val, house)
        print(f"[Server] table_virtual: {ll.size} readings")
    except Exception as e:
        print(f"[Server] table_virtual fetch error: {e}")
    return ll

# Fetch post-sharing data from Akira's table_ass8_virtual
# Contains both Akira's and Zhihan's shared devices
# House is determined by topic — NOT by which DB we query
def fetch_table_ass8(start_dt, end_dt, extractor):
    ll = LinkedList()
    try:
        conn = get_conn("A")
        cur  = conn.cursor()
        post_start = max(start_dt, SHARING_START)
        cur.execute("""
            SELECT payload FROM "table_ass8_virtual"
            WHERE time >= %s AND time <= %s
            ORDER BY time ASC
        """, (post_start, end_dt))
        rows = cur.fetchall()
        conn.close()
        for row in rows:
            # Determine house from topic metadata (DataNiz device ownership)
            house = get_house_from_topic(row[0])
            if house is None:
                continue  # skip unknown topics
            val = extractor(row[0])
            if val is not None:
                ll.append(val, house)
        print(f"[Server] table_ass8_virtual: {ll.size} readings")
    except Exception as e:
        print(f"[Server] table_ass8_virtual fetch error: {e}")
    return ll

# Fetch House B pre-sharing data from Zhihan's Table1_virtual
# Only called when query window starts before SHARING_START
# Requires direct connection to House B's NeonDB
def fetch_b_pre(start_dt, extractor):
    ll = LinkedList()
    try:
        conn = get_conn("B")
        cur  = conn.cursor()
        cur.execute("""
            SELECT payload FROM "Table1_virtual"
            WHERE time >= %s AND time < %s
            ORDER BY time ASC
        """, (start_dt, SHARING_START))
        rows = cur.fetchall()
        conn.close()
        for row in rows:
            # Determine house from topic metadata
            house = get_house_from_topic(row[0])
            if house is None:
                house = "House B"  # Table1_virtual is Zhihan's own table
            val = extractor(row[0])
            if val is not None:
                ll.append(val, house)
        print(f"[Server] House B pre-sharing (Table1_virtual): {ll.size} readings")
    except Exception as e:
        print(f"[Server] House B pre-sharing error: {e}")
    return ll

# Main distributed fetch — combines all four data sources
# Implements the query completeness check for the assignment
def fetch_distributed(start_dt, end_dt, extractor):
    # Fetch from Akira's table_virtual (house determined by topic)
    ll_main = fetch_table_virtual(start_dt, end_dt, extractor)
    # Fetch from Akira's table_ass8_virtual (post-sharing, house by topic)
    ll_ass8 = fetch_table_ass8(start_dt, end_dt, extractor)

    # Completeness check: if query window predates sharing,
    # fetch House B pre-sharing data directly from Zhihan's DB
    if start_dt < SHARING_START:
        print(f"[Server] Query includes pre-sharing period — fetching from peer DB...")
        ll_b_pre = fetch_b_pre(start_dt, extractor)
    else:
        print(f"[Server] Query fully covered by local data (after sharing started)")
        ll_b_pre = LinkedList()

    # Merge all three linked lists into one
    merged = LinkedList()
    for val, house in ll_main.get_all():
        merged.append(val, house)
    for val, house in ll_ass8.get_all():
        merged.append(val, house)
    for val, house in ll_b_pre.get_all():
        merged.append(val, house)
    print(f"[Server] Total records merged: {merged.size}")
    return merged


# QUERY HANDLERS:
# Query 1: Average fridge moisture for past hour, week, month
# Sensor: 'Moisture Meter - Moisture Meter' (both houses)
# Output unit: % RH (no imperial conversion needed)
def query_fridge_moisture():
    now = datetime.now(timezone.utc)
    periods = {
        "Past Hour":  now - timedelta(hours=1),
        "Past Week":  now - timedelta(weeks=1),
        "Past Month": now - timedelta(days=30),
    }

    lines = ["=" * 60,
             "  Query 1: Average Kitchen Fridge Moisture",
             "  Both Houses Combined",
             "=" * 60]

    for label, start_dt in periods.items():
        ll   = fetch_distributed(start_dt, now, extract_moisture)
        data = ll.get_all()

        if not data:
            lines.append(f"  {label}: No data available")
            continue

        # Compute combined average and per-house breakdown
        all_vals     = [v for v, h in data]
        house_a_vals = [v for v, h in data if h == "House A"]
        house_b_vals = [v for v, h in data if h == "House B"]

        avg   = sum(all_vals)     / len(all_vals)
        avg_a = sum(house_a_vals) / len(house_a_vals) if house_a_vals else 0
        avg_b = sum(house_b_vals) / len(house_b_vals) if house_b_vals else 0

        lines.append(f"\n  {label}:")
        lines.append(f"    Combined avg : {avg:.2f}% RH  (n={len(all_vals)})")
        lines.append(f"    House A avg  : {avg_a:.2f}% RH  (n={len(house_a_vals)})")
        lines.append(f"    House B avg  : {avg_b:.2f}% RH  (n={len(house_b_vals)})")

    lines.append(f"\n  Queried at: {to_pst_str(now)}")
    return "\n".join(lines)

# Query 2: Average dishwasher water per cycle for past hour, week, month
# Sensor: 'Float Switch - Float Switch' (liters)
# Output unit: gallons (imperial) — converted via x 0.264172
def query_dishwasher_water():
    now = datetime.now(timezone.utc)
    periods = {
        "Past Hour":  now - timedelta(hours=1),
        "Past Week":  now - timedelta(weeks=1),
        "Past Month": now - timedelta(days=30),
    }

    lines = ["=" * 60,
             "  Query 2: Average Dishwasher Water Per Cycle",
             "  Both Houses Combined",
             "=" * 60]

    for label, start_dt in periods.items():
        ll   = fetch_distributed(start_dt, now, extract_water)
        data = ll.get_all()

        if not data:
            lines.append(f"  {label}: No data available")
            continue

        # Compute averages in liters then convert to gallons for output
        all_vals     = [v for v, h in data]
        house_a_vals = [v for v, h in data if h == "House A"]
        house_b_vals = [v for v, h in data if h == "House B"]

        avg_l = sum(all_vals)     / len(all_vals)
        a_l   = sum(house_a_vals) / len(house_a_vals) if house_a_vals else 0
        b_l   = sum(house_b_vals) / len(house_b_vals) if house_b_vals else 0

        lines.append(f"\n  {label}:")
        lines.append(f"    Combined avg : {avg_l * LITERS_TO_GALLONS:.2f} gal/cycle  ({avg_l:.2f} L, n={len(all_vals)})")
        lines.append(f"    House A avg  : {a_l * LITERS_TO_GALLONS:.2f} gal/cycle  (n={len(house_a_vals)})")
        lines.append(f"    House B avg  : {b_l * LITERS_TO_GALLONS:.2f} gal/cycle  (n={len(house_b_vals)})")

    lines.append(f"\n  Queried at: {to_pst_str(now)}")
    return "\n".join(lines)


# Query 3: Which house consumed more electricity in past 24 hours?
# Sensor: 'Ammeter' on main board (both houses)
# Output: kWh totals per house, winner, and difference
def query_electricity():
    now      = datetime.now(timezone.utc)
    start_dt = now - timedelta(hours=24)

    ll             = fetch_distributed(start_dt, now, extract_electricity)
    totals, counts = ll.sum_by_house()

    a_kwh  = totals["House A"]
    b_kwh  = totals["House B"]
    diff   = abs(a_kwh - b_kwh)

    # Determine which house used more electricity
    winner = "House A (Akira)"  if a_kwh >= b_kwh else "House B (Zhihan)"
    loser  = "House B (Zhihan)" if a_kwh >= b_kwh else "House A (Akira)"

    lines = [
        "=" * 60,
        "  Query 3: Electricity Consumption — Past 24 Hours",
        "=" * 60,
        f"  House A (Akira) : {a_kwh:.2f} kWh  ({counts['House A']} readings)",
        f"  House B (Zhihan): {b_kwh:.2f} kWh  ({counts['House B']} readings)",
        f"",
        f"  --> {winner} consumed MORE electricity",
        f"      by {diff:.2f} kWh compared to {loser}",
        f"\n  Queried at: {to_pst_str(now)}",
    ]
    return "\n".join(lines)


# QUERY ROUTER:
# The process is guided by the query provided by the client side.
def route_query(message):
    msg = message.strip().lower()
    print(f"[Server] Routing: '{msg[:60]}...'")
    if "moisture" in msg and "fridge" in msg:
        return query_fridge_moisture()
    elif "water consumption" in msg and "dishwasher" in msg:
        return query_dishwasher_water()
    elif "electricity" in msg and "24 hours" in msg:
        return query_electricity()
    else:
        print(f"[Server] No match found")
        return "ERROR: Unrecognized query."


# TCP server: Configure necessary server-side processing.
def server():
    # Ask user which port to listen on
    port = int(input("Enter port number to listen: "))

    # Create TCP socket — AF_INET = IPv4, SOCK_STREAM = TCP
    TCPSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Bind to all interfaces on this machine
    TCPSocket.bind(("", port))

    # Allow up to 5 queued connections
    TCPSocket.listen(5)

    print(f"[Server] Listening on port {port}...")
    print(f"[Server] Sharing started at: {to_pst_str(SHARING_START)}")

    # Block until a client connects
    incomingSocket, incomingAddress = TCPSocket.accept()
    print(f"[Server] Connected by: {incomingAddress}")

    while True:
        # Receive query — 4096 bytes handles long query strings
        data = incomingSocket.recv(4096)
        if not data:
            break

        message = data.decode("utf-8")
        print(f"\n[Server] Received: {message}")

        # Exit signal from client
        if message.strip().lower() == "exit":
            print("[Server] Client disconnected.")
            break

        # Route query to handler and send result back
        response = route_query(message)
        print(f"[Server] Sending response...")
        incomingSocket.send(response.encode("utf-8"))

    # Close both sockets when done
    incomingSocket.close()
    TCPSocket.close()
    print("[Server] Closed.")


# Run server when executed directly
if __name__ == "__main__":
    server()
