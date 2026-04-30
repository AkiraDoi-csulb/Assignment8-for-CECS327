# Assignment8: Build a Distributed End-to-End IoT System
# Name       : Akira Doi, Zhihan Yao
# Date       : 4/30/2026
# client.py

import socket
import ipaddress

queries = {
    "1": "What is the average moisture inside our kitchen fridges in the past hours, week and month?",
    "2": "What is the average water consumption per cycle across our smart dishwashers in the past hour, week and month?",
    "3": "Which house consumed more electricity in the past 24 hours, and by how much?",
}


def user_menu():
    print("-" * 100)
    print("IoT houses data menu:")
    print()

    for key, query in queries.items():
        print(f"  [{key}] {query}")

    print("  [exit] Disconnect from server")
    print("-" * 100)


def client():
    server_ip = input("Enter the IP address of the server: ").strip()

    try:
        ipaddress.ip_address(server_ip)
    except ValueError:
        print("Error: the IP address is entered incorrectly.")
        return

    server_port = input("Enter the port number of the server: ").strip()

    if not server_port.isdigit() or not (1024 <= int(server_port) <= 65535):
        print("Error: the port number is entered incorrectly.")
        return

    server_port = int(server_port)

    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        tcp_socket.connect((server_ip, server_port))
    except Exception as e:
        print(f"Error: Could not connect to server: {e}")
        return

    while True:
        user_menu()

        message = input("User choice from the menu: ").strip()

        if message.lower() == "exit":
            tcp_socket.sendall("exit".encode("utf-8"))
            print("Client disconnected.")
            break

        query_text = None

        if message in queries:
            query_text = queries[message]
        else:
            for value in queries.values():
                if message.lower() == value.lower():
                    query_text = value
                    break

        if query_text is None:
            print("Sorry, this query cannot be processed. Please try one of the supported queries.")
            continue

        print("\nSending query to server...")

        tcp_socket.sendall(query_text.encode("utf-8"))

        data = tcp_socket.recv(8192)

        print("-" * 100)
        print("Server Reply:")
        print(data.decode("utf-8"))
        print("-" * 100)

    tcp_socket.close()


if __name__ == "__main__":
    client()