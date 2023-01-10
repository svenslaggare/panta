import socket

def main():
    uds_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)

    for i in range(5):
        uds_socket.sendto("custom.test:{}|g".format(i).encode("utf-8"), "panta.sock")

if __name__ == "__main__":
    main()