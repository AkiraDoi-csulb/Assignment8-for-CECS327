# Assignment8: Build a Distributed End-to-End IoT System
# Name: Akira Doi, Zhihan Yao
# Date: 4/30/2026
# File: server.py
#
#
# This server connects to two NeonDB databases, reads IoT data from DataNiz tables,
# answers three sensor queries, and sends the results back to the client by TCP.

import socket
import os
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta


# ============================================================
# CONFIGURATION
# ============================================================

load_dotenv()

# Database connection strings are stored in .env for safety.
HOUSE_A_CONN = os.getenv("HOUSE_A_CONN")
HOUSE_B_CONN = os.getenv("HOUSE_B_CONN")
SERVER_PORT = int(os.getenv("SERVER_PORT", "5050"))

if not HOUSE_A_CONN or not HOUSE_B_CONN:
    raise ValueError("Missing HOUSE_A_CONN or HOUSE_B_CONN in .env file.")

# DataNiz sharing start time in UTC.
# First shared row appears around 2026-04-24 22:50:03 UTC.
SHARING_START = datetime(2026, 4, 24, 22, 50, 0, tzinfo=timezone.utc)

# In April, California uses PDT, which is UTC-7.
PACIFIC = timezone(timedelta(hours=-7))

# Unit conversion.
LITERS_TO_GALLONS = 0.264172

# DataNiz topic identifiers.
TOPIC_A = "akira.doi01@student.csulb.edu"
TOPIC_B = "zhihanyao121@gmail.com"


# ============================================================
# DATABASE AND TIME HELPER FUNCTIONS
# ============================================================

# Return a psycopg2 connection for House A or House B.

def get_conn(house):
    
    if house == "A":
        conn_str = HOUSE_A_CONN
    else:
        conn_str = HOUSE_B_CONN

    return psycopg2.connect(conn_str, connect_timeout=10)

# Convert UTC datetime to Pacific time string.

def to_pacific_str(utc_dt):
    
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=timezone.utc)

    return utc_dt.astimezone(PACIFIC).strftime("%Y-%m-%d %H:%M:%S PDT")


# ============================================================
# SENSOR VALUE EXTRACTION FROM PAYLOAD
# Read the data from the PAYLOAD
#
# All sensor data lives in the payload.
# Each function checks for the correct key for that sensor.
# Returns a float value or None if the row is not relevant.
#
# TABLE is used for actual data:
# House A (Akira):
#   table_virtual        → Akira's presharing data
#   table_ass8_virtual   → post sharing data for BOTH houses
#                          filter by topic containing TOPIC_A
#
# House B (Zhihan):
#   Table1_virtual       → Zhihan's presharing data
#   Assignment8_virtual  → post sharing data for BOTH houses
#                          filter by topic containing TOPIC_B
#
# Sensors Data (from actual payload inspection):
#   Moisture  → 'Moisture Meter - Moisture Meter' or starts with same
#   Dishwasher→ 'Float Switch - Float Switch'
#   Electricity→ 'Ammeter' (main board) or 'Ammeter 3 UUID...'

# Extract moisture value from any payload (works for both houses)
# Handles: 'Moisture Meter - Moisture Meter', 'Moisture', 'LM386 - Sensor1'
# and their UUID-suffixed duplicate board variants
# ============================================================

# Extract fridge moisture value from the payload.
# Different DataNiz boards may use different sensor names.
    
def extract_moisture(payload):
    
    possible_keys = [
        "Moisture Meter - Moisture Meter",
        "Moisture",
        "LM386 - Sensor1"
    ]

    for key in possible_keys:
        if key in payload:
            try:
                val = float(payload[key])
                if val > 0:
                    return val
            except (ValueError, TypeError):
                pass

# Check duplicate board keys or UUID-suffixed keys.
    for key in payload:
        if "Moisture Meter" in key or (key.startswith("Moisture") and "Ammeter" not in key):
            try:
                val = float(payload[key])
                if val > 0:
                    return val
            except (ValueError, TypeError):
                continue

    return None

# Extract dishwasher water usage value from the payload.
# The value is treated as liters per cycle.
    
def extract_water(payload):
    

    possible_keys = [
        "Float Switch - Float Switch",
        "water consumption sensor"
    ]

    for key in possible_keys:
        if key in payload:
            try:
                val = float(payload[key])
                if val > 0:
                    return val
            except (ValueError, TypeError):
                pass

    return None

# Extract electricity-related reading from the payload.
# For this assignment, Ammeter values are treated as electricity consumption readings.

def extract_electricity(payload):
    
    if "Ammeter" in payload:
        try:
            val = float(payload["Ammeter"])
            if val > 0:
                return val
        except (ValueError, TypeError):
            pass

    # Check duplicate board keys such as "Ammeter 3 UUID..."
    for key in payload:
        if key.startswith("Ammeter"):
            try:
                val = float(payload[key])
                if val > 0:
                    return val
            except (ValueError, TypeError):
                continue

    return None


# ============================================================
# LINKED LIST CLASSES
# ============================================================

# One linked list node.
# It stores one sensor reading and the house label.
class Node:
    

    def __init__(self, value, house):
        self.value = value
        self.house = house
        self.next = None

# Singly linked list used to store sensor records.
class LinkedList:

    def __init__(self):
        self.head = None
        self.size = 0

    # Add one reading to the end of the linked list.
    def append(self, value, house):
        
        new_node = Node(value, house)

        if self.head is None:
            self.head = new_node
        else:
            current = self.head

            while current.next is not None:
                current = current.next

            current.next = new_node

        self.size += 1
    
    # Convert linked list data to a normal Python list.
    def get_all(self):
        
        result = []
        current = self.head

        while current is not None:
            result.append((current.value, current.house))
            current = current.next

        return result

    # Add all values from another linked list.
    def merge(self, other_list):
        
        for value, house in other_list.get_all():
            self.append(value, house)

    # Return total readings and counts for each house.
    # Used by the electricity query.
    def sum_by_house(self):
        
        totals = {
            "House A": 0.0,
            "House B": 0.0
        }

        counts = {
            "House A": 0,
            "House B": 0
        }

        current = self.head

        while current is not None:
            if current.house in totals:
                totals[current.house] += current.value
                counts[current.house] += 1

            current = current.next

        return totals, counts


# ============================================================
# DATA FETCHING FUNCTIONS
# Data layout:
#   - House A data: query table_virtual  +  table_ass8_virtual
#   - House B data: query Table1_virtual + Assignment8_virtual (topic=zhihan)

# Determine house ownership from the topic field in payload
# This uses DataNiz metadata to identify which house owns each reading
# TOPIC_A = akira.doi01@student.csulb.edu → House A
# TOPIC_B = zhihanyao121@gmail.com        → House B
# ============================================================

# Determine whether a payload belongs to House A or House B.
# DataNiz shared tables can contain data from both houses, so the topic is important.
def get_house_from_topic(payload):

    topic = payload.get("topic", "")

    if TOPIC_A in topic:
        return "House A"

    if TOPIC_B in topic:
        return "House B"

    return None

# Fetch data from Akira's original table_virtual.
def fetch_table_virtual(start_dt, end_dt, extractor):
    
    ll = LinkedList()

    try:
        conn = get_conn("A")
        cur = conn.cursor()

        cur.execute(
            """
            SELECT payload
            FROM "table_virtual"
            WHERE time >= %s AND time <= %s
            ORDER BY time ASC
            """,
            (start_dt, end_dt)
        )

        rows = cur.fetchall()
        cur.close()
        conn.close()

        for row in rows:
            payload = row[0]
            house = get_house_from_topic(payload)

            if house is None:
                continue

            val = extractor(payload)

            if val is not None:
                ll.append(val, house)

        print(f"[Server] table_virtual: {ll.size} readings")

    except Exception as e:
        print(f"[Server] table_virtual fetch error: {e}")

    return ll


# Fetch post-sharing data from Akira's table_ass8_virtual.
# This table can include both houses' shared data.

def fetch_table_ass8(start_dt, end_dt, extractor):
    

    ll = LinkedList()

    try:
        conn = get_conn("A")
        cur = conn.cursor()

        post_start = max(start_dt, SHARING_START)

        cur.execute(
            """
            SELECT payload
            FROM "table_ass8_virtual"
            WHERE time >= %s AND time <= %s
            ORDER BY time ASC
            """,
            (post_start, end_dt)
        )

        rows = cur.fetchall()
        cur.close()
        conn.close()

        for row in rows:
            payload = row[0]
            house = get_house_from_topic(payload)

            if house is None:
                continue

            val = extractor(payload)

            if val is not None:
                ll.append(val, house)

        print(f"[Server] table_ass8_virtual: {ll.size} readings")

    except Exception as e:
        print(f"[Server] table_ass8_virtual fetch error: {e}")

    return ll

# Fetch Zhihan's pre-sharing data from House B's database.
# This is only needed when the query window starts before DataNiz sharing began. 
def fetch_house_b_pre_sharing(start_dt, extractor):

    ll = LinkedList()

    try:
        conn = get_conn("B")
        cur = conn.cursor()

        cur.execute(
            """
            SELECT payload
            FROM "Table1_virtual"
            WHERE time >= %s AND time < %s
            ORDER BY time ASC
            """,
            (start_dt, SHARING_START)
        )

        rows = cur.fetchall()
        cur.close()
        conn.close()

        for row in rows:
            payload = row[0]
            house = get_house_from_topic(payload)

            # If topic is missing, this table still belongs to House B.
            if house is None:
                house = "House B"

            val = extractor(payload)

            if val is not None:
                ll.append(val, house)

        print(f"[Server] House B pre-sharing Table1_virtual: {ll.size} readings")

    except Exception as e:
        print(f"[Server] House B pre-sharing fetch error: {e}")

    return ll

# Main distributed fetch function.
# 1. House A original data
# 2. House A shared table data
# 3. House B pre-sharing data, if needed
    
def fetch_distributed(start_dt, end_dt, extractor):

    print("-" * 60)
    print(f"[Server] Fetch range: {to_pacific_str(start_dt)} to {to_pacific_str(end_dt)}")

    ll_main = fetch_table_virtual(start_dt, end_dt, extractor)
    ll_shared = fetch_table_ass8(start_dt, end_dt, extractor)

    if start_dt < SHARING_START:
        print("[Server] Query includes pre-sharing period. Fetching House B original data...")
        ll_b_pre = fetch_house_b_pre_sharing(start_dt, extractor)
    else:
        print("[Server] Query is fully after sharing start. Local shared data is enough.")
        ll_b_pre = LinkedList()

    merged = LinkedList()
    merged.merge(ll_main)
    merged.merge(ll_shared)
    merged.merge(ll_b_pre)

    print(f"[Server] Total records merged: {merged.size}")

    return merged


# ============================================================
# QUERY HANDLERS
# ============================================================

# Query 1:
# Average fridge moisture for past hour, past week, and past month.
def query_fridge_moisture():
    
    now = datetime.now(timezone.utc)

    periods = {
        "Past Hour": now - timedelta(hours=1),
        "Past Week": now - timedelta(weeks=1),
        "Past Month": now - timedelta(days=30)
    }

    lines = [
        "=" * 60,
        "Query 1: Average Kitchen Fridge Moisture",
        "House A = Akira, House B = Zhihan",
        "=" * 60
    ]

    for label, start_dt in periods.items():
        ll = fetch_distributed(start_dt, now, extract_moisture)
        data = ll.get_all()

        if not data:
            lines.append(f"\n{label}:")
            lines.append("  No moisture data available.")
            continue

        all_vals = [value for value, house in data]
        house_a_vals = [value for value, house in data if house == "House A"]
        house_b_vals = [value for value, house in data if house == "House B"]

        combined_avg = sum(all_vals) / len(all_vals)
        house_a_avg = sum(house_a_vals) / len(house_a_vals) if house_a_vals else 0
        house_b_avg = sum(house_b_vals) / len(house_b_vals) if house_b_vals else 0

        lines.append(f"\n{label}:")
        lines.append(f"  Combined avg : {combined_avg:.2f}% RH  (n={len(all_vals)})")
        lines.append(f"  House A avg  : {house_a_avg:.2f}% RH  (n={len(house_a_vals)})")
        lines.append(f"  House B avg  : {house_b_avg:.2f}% RH  (n={len(house_b_vals)})")

    lines.append(f"\nQueried at: {to_pacific_str(now)}")

    return "\n".join(lines)

# Query 2:
# Average dishwasher water consumption per cycle for past hour, week, and month.
# Values are read as liters and also displayed in gallons.
def query_dishwasher_water():
    
    now = datetime.now(timezone.utc)

    periods = {
        "Past Hour": now - timedelta(hours=1),
        "Past Week": now - timedelta(weeks=1),
        "Past Month": now - timedelta(days=30)
    }

    lines = [
        "=" * 60,
        "Query 2: Average Dishwasher Water Consumption Per Cycle",
        "House A = Akira, House B = Zhihan",
        "=" * 60
    ]

    for label, start_dt in periods.items():
        ll = fetch_distributed(start_dt, now, extract_water)
        data = ll.get_all()

        if not data:
            lines.append(f"\n{label}:")
            lines.append("  No dishwasher water data available.")
            continue

        all_vals = [value for value, house in data]
        house_a_vals = [value for value, house in data if house == "House A"]
        house_b_vals = [value for value, house in data if house == "House B"]

        combined_liters = sum(all_vals) / len(all_vals)
        house_a_liters = sum(house_a_vals) / len(house_a_vals) if house_a_vals else 0
        house_b_liters = sum(house_b_vals) / len(house_b_vals) if house_b_vals else 0

        lines.append(f"\n{label}:")
        lines.append(
            f"  Combined avg : {combined_liters * LITERS_TO_GALLONS:.2f} gal/cycle "
            f"({combined_liters:.2f} L, n={len(all_vals)})"
        )
        lines.append(
            f"  House A avg  : {house_a_liters * LITERS_TO_GALLONS:.2f} gal/cycle "
            f"({house_a_liters:.2f} L, n={len(house_a_vals)})"
        )
        lines.append(
            f"  House B avg  : {house_b_liters * LITERS_TO_GALLONS:.2f} gal/cycle "
            f"({house_b_liters:.2f} L, n={len(house_b_vals)})"
        )

    lines.append(f"\nQueried at: {to_pacific_str(now)}")

    return "\n".join(lines)

# Query 3:
# Compare electricity-related readings between both houses in the past 24 hours.
# For this assignment, Ammeter readings are treated as electricity consumption values.
def query_electricity():

    now = datetime.now(timezone.utc)
    start_dt = now - timedelta(hours=24)

    ll = fetch_distributed(start_dt, now, extract_electricity)
    totals, counts = ll.sum_by_house()

    house_a_total = totals["House A"]
    house_b_total = totals["House B"]
    difference = abs(house_a_total - house_b_total)

    if house_a_total > house_b_total:
        result_line = "House A (Akira) consumed MORE electricity."
        compare_line = f"House A consumed {difference:.2f} kWh more than House B."
    elif house_b_total > house_a_total:
        result_line = "House B (Zhihan) consumed MORE electricity."
        compare_line = f"House B consumed {difference:.2f} kWh more than House A."
    else:
        result_line = "Both houses consumed the same amount of electricity."
        compare_line = "Difference: 0.00 kWh."

    lines = [
        "=" * 60,
        "Query 3: Electricity Consumption in the Past 24 Hours",
        "House A = Akira, House B = Zhihan",
        "=" * 60,
        f"House A total: {house_a_total:.2f} kWh  (n={counts['House A']})",
        f"House B total: {house_b_total:.2f} kWh  (n={counts['House B']})",
        "",
        f"Result: {result_line}",
        compare_line,
        f"\nQueried at: {to_pacific_str(now)}"
    ]

    return "\n".join(lines)


# ============================================================
# QUERY ROUTER
# ============================================================

# Choose which query function to run based on the client's message.
def route_query(message):

    msg = message.strip().lower()

    print(f"[Server] Routing message: {msg}")

    if "moisture" in msg and "fridge" in msg:
        return query_fridge_moisture()

    if "water consumption" in msg and "dishwasher" in msg:
        return query_dishwasher_water()

    if "electricity" in msg and "24 hours" in msg:
        return query_electricity()

    return "ERROR: Unrecognized query."


# ============================================================
# TCP SERVER
# ============================================================

# TCP server: Configure necessary server-side processing.
# Ask user which port to listen on.
# If user just presses Enter, use SERVER_PORT from .env.
def server():
    port_input = input(f"Enter port number to listen [{SERVER_PORT}]: ").strip()

    if port_input == "":
        port = SERVER_PORT
    else:
        port = int(port_input)

    # Create TCP socket — AF_INET = IPv4, SOCK_STREAM = TCP
    TCPSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Allow the port to be reused after restarting the server.
    # This helps avoid: OSError: [Errno 48] Address already in use
    TCPSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        # Bind to all interfaces on this machine
        TCPSocket.bind(("", port))

        # Allow up to 5 queued connections
        TCPSocket.listen(5)

        print("=" * 60)
        print("CECS 327 Assignment 8 TCP Server")
        print("=" * 60)
        print(f"[Server] Listening on port {port}...")
        print(f"[Server] Sharing started at: {to_pacific_str(SHARING_START)}")
        print("=" * 60)

        # Keep server running so it can accept more than one client
        while True:
            # Block until a client connects
            incomingSocket, incomingAddress = TCPSocket.accept()
            print(f"[Server] Connected by: {incomingAddress}")

            with incomingSocket:
                while True:
                    # Receive query — 4096 bytes handles long query strings
                    data = incomingSocket.recv(4096)

                    if not data:
                        print("[Server] Client disconnected.")
                        break

                    message = data.decode("utf-8").strip()
                    print(f"\n[Server] Received: {message}")

                    # Exit signal from client
                    if message.lower() == "exit":
                        print("[Server] Client requested exit.")
                        break

                    # Route query to handler and send result back
                    response = route_query(message)
                    print("[Server] Sending response...")
                    incomingSocket.sendall(response.encode("utf-8"))

    except KeyboardInterrupt:
        print("\n[Server] Stopped by user.")

    except Exception as e:
        print(f"[Server] Error: {e}")

    finally:
        TCPSocket.close()
        print("[Server] Closed.")

if __name__ == "__main__":
    server()