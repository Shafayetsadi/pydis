import socket
from concurrent.futures import ThreadPoolExecutor
from resp import parse_array
from db import Database


def set_key(database: Database, key: str, value: str, expire_at: str = None) -> str:
    database.set(key, value, expire_at)
    return "+OK\r\n"


def respond(connection: socket.socket, database: Database) -> None:
    try:
        while True:  # loop to receive data from client
            try:
                data = connection.recv(1024)
            except socket.error as e:
                print(f"Socket error: {e}")
                break
            except Exception as e:
                print(f"Unexpected error: {e}")
                break

            cmd = parse_array(data.decode())
            if not cmd or len(cmd) == 0:
                response: str = "-ERR\r\n"
            elif len(cmd) == 1 and cmd[0].lower() == "ping":
                response: str = "+PONG\r\n"
            elif cmd[0].lower() == "echo":
                response: str = f"+{cmd[1]}\r\n"
            elif cmd[0].lower() == "set":
                if len(cmd) == 3:
                    response: str = set_key(database, cmd[1], cmd[2])
                elif len(cmd) == 5:
                    response: str = set_key(database, cmd[1], cmd[3], cmd[5])
                else:
                    response: str = "-ERR\r\n"
            elif cmd[0].lower() == "get":
                value = database.get(cmd[1])
                if value:
                    response: str = f"${len(value)}\r\n{value}\r\n"
                else:
                    response: str = "$-1\r\n"
            else:
                response: str = "-ERR\r\n"

            connection.sendall(response.encode())
    finally:
        connection.close()


def main():
    database = Database()
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.listen()
    with ThreadPoolExecutor(max_workers=50) as executor:
        try:
            while True:
                connection, address = server_socket.accept()
                executor.submit(respond, connection, database)
        except KeyboardInterrupt:
            print("Shutting down server")
        finally:
            server_socket.close()


if __name__ == "__main__":
    main()
