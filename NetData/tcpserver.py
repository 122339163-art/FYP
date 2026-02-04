import socket
import struct
import time

HOST = "10.0.0.1"
PORT = 9000

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((HOST, PORT))
s.listen(5)

print("Waiting for video connections...")
while True:
    conn, addr = s.accept()
    print("Connected from", addr)

    # Read 8-byte file size
    size_data = conn.recv(8)
    if len(size_data) < 8:
        conn.close()
        continue
    file_size = struct.unpack("!Q", size_data)[0]

    # Read 2-byte filename length
    name_len_data = conn.recv(2)
    name_len = struct.unpack("!H", name_len_data)[0]

    # Read filename
    filename = conn.recv(name_len).decode()
    if not filename:
        filename = f"received_{int(time.time())}.mp4"

    received = 0
    with open(f"received_{int(time.time())}.mp4", "wb") as f:
        while received < file_size:
            chunk = conn.recv(min(4096, file_size - received))
            if not chunk:
                break
            f.write(chunk)
            received += len(chunk)

    conn.close()
    print(f"Saved {filename}, bytes received: {received}")

