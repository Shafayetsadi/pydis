import socket
import logging
from concurrent.futures import ThreadPoolExecutor
from resp import parse_array
from db import Database

ERROR_RESPONSE = "-ERR\r\n"
OK_RESPONSE = "+OK\r\n"

logging.basicConfig(filename="app.log", filemode="w", format="%(name)s - %(levelname)s - %(message)s",
                    level=logging.ERROR)


def handle_set(database: Database, cmd: list) -> str:
    if len(cmd) not in [3, 5]:
        return ERROR_RESPONSE
    key, value = cmd[1], cmd[2]
    expire_at = cmd[3] if len(cmd) == 5 else None
    database.set(key, value, expire_at)
    return OK_RESPONSE


def handle_get(database: Database, cmd: list) -> str:
    value = database.get(cmd[1])
    if value:
        return f"${len(value)}\r\n{value}\r\n"
    else:
        return "$-1\r\n"


def set_key(database: Database, key: str, value: str, expire_at: str = None) -> str:
    database.set(key, value, expire_at)
    return OK_RESPONSE


COMMANDS = {
    "ping": lambda *args: "+PONG\r\n",
    "echo": lambda *args: f"+{args[1][1]}\r\n",
    "set": handle_set,
    "get": handle_get
}


def respond(connection: socket.socket, database: Database) -> None:
    print("Responding to client")
    try:
        while True:  # loop to receive data from client
            try:
                data = connection.recv(1024)
            except socket.error as e:
                logging.error("Socket error: %s", e)
                break
            except Exception as e:
                logging.error(f"Unexpected error: {e}")
                break

            cmd = parse_array(data.decode())
            if not cmd or len(cmd) == 0:
                response: str = ERROR_RESPONSE
            else:
                command = cmd[0].lower()
                if command in COMMANDS:
                    response = COMMANDS[command](database, cmd)
                else:
                    response = ERROR_RESPONSE
            print(response)
            connection.sendall(response.encode())
    finally:
        connection.close()
        print("Closed connection")


def main():
    database = Database()
    server_socket = socket.create_server(("localhost", 6379), reuse_port=False)
    server_socket.listen()
    with ThreadPoolExecutor(max_workers=10) as executor:
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
