# Assignment8: Build a Distributed End-to-End IoT System 
# Name       : Akira Doi, Zhihan Yao 
# Date       : 4/30/2026
# client.py

# import to use socket and ip
import socket
import ipaddress

# Prepare need query
queries = {
    "1": "What is the average moisture inside our kitchen fridges in the past hours, week and month?",
    "2": "What is the average water consumption per cycle across our smart dishwashers in the past hour, week and month?",
    "3": "Which house consumed more electricity in the past 24 hours, and by how much?",
}

# prepare for user input, user select from the query
def userMenu():
    print( "-"*100 )
    print("Iot houses data menu (please select action from the menu):")
    print()
    for key, query in queries.items():
       print(f"  [{key}] {query}")
    print("  [exit] Disconnect from server")
    print( "-"*100 )

def client():
    # 1.Prompt the user to input:
    # - Server IP address
    # - Server port number
    serverIP   = input("Enter the IP address of the server: ").strip()
    # Display an error message if the IP address is entered incorrectly. 
    try:
        ipaddress.ip_address(serverIP)
    except ValueError:
        print("Error: the IP address is entered incorrectly")
        return
    # User sets the port number
    serverPort = input("Enter the port number of the server: ").strip()

    if not serverPort.isdigit() or not (1024 <= int(serverPort) <= 65535):
        print("Error: the port number is entered incorrectly")
        return
    # Make sure if port is integer
    serverPort = int(serverPort)

    # Create TCP socket
    TCPSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        # Connect TCP socket
        TCPSocket.connect((serverIP, serverPort))
    # Error handle for not connection
    except Exception as e:
        print(f"Error: Could not connect to server: {e}")
        return

    # Make infinite loop to allow to do multiple choices
    while True:
        # Show the menu 
        userMenu()

        # Prompt user, send message to the server
        message = input("User choice from the menu(quit by sending 'exit'): ").strip()
        
        # Prepare escape message 'exit' to stop the program 
        if message.lower() == "exit":
            TCPSocket.sendall("exit".encode("utf-8"))
            print("Client disconnected")
            break

        # Accept menu number (1/2/3) OR the full question text
        query_text = None
 
        # Check if user typed a number
        if message in queries:
            query_text = queries[message]
        else:
            # Check if user typed the full question text
            for key, val in queries.items():
                if message.strip().lower() == val.strip().lower():
                    query_text = val
                    break
 
        # Reject anything that does not match a number or full query text
        if query_text is None:
            print("Sorry, this query cannot be processed. Please try one of the supported queries.")
            continue

        print(f"\nSending query to server...")

        # 2.send message to the server
        TCPSocket.send(query_text.encode("utf-8"))

        # Set the maximum message size to recieve 
        data = TCPSocket.recv(4096)
        # print final results
        print( '-'*100 )
        # Display server reply
        print("Server Reply:", data.decode("utf-8"))
        print( '-'*100 )
    TCPSocket.close()

# Run the main program (client())
if __name__ == "__main__":
    client()
