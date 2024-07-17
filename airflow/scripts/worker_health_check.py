import socket

def check_health():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect(('localhost', 8793))
        s.close()
        print("Worker is healthy")
    except Exception as e:
        print("Worker is not healthy")
        exit(1)

if __name__ == "__main__":
    check_health()