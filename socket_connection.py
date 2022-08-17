import socket
import pika
import cv2
import numpy as np
import pytz
from datetime import datetime
from select import select
from timeit import default_timer as timer

ImageSeperator = b"newblock"

def await_connection(serv):
    print(f"Awaiting connection on {serv.getsockname()[0]}:{serv.getsockname()[1]}")
    conn, addr = serv.accept()
    connected_device = f"{addr[0]}:{addr[1]}"
    print(f"{connected_device} Connected")
    return conn, addr, connected_device


def fetch_data():
    image_count = 0

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    while True:
        serv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        serv.bind(('0.0.0.0', 8090))
        serv.listen(1)

        conn, addr, connected_device = await_connection(serv)
        conn.setblocking(0)

        buffer = b""

        active_connection = True

        while active_connection == True:
            loop_start = timer()
            try:
                data_is_ready = select([conn], [], [], 10)
                if data_is_ready[0]:
                    buffer += conn.recv(16384)
                    images_in_buffer = buffer.count(ImageSeperator)
                    for i in range(images_in_buffer - 1):
                        nparr = np.frombuffer(buffer[:buffer.find(ImageSeperator)], np.uint8)
                        cv2_image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                        rotated_cv2_image = cv2.rotate(cv2_image, cv2.ROTATE_90_COUNTERCLOCKWISE)
                        rotated_image_bytes = cv2.imencode('.jpg', rotated_cv2_image)[1].tobytes()
                        timestamp_cv2_image = cv2.putText(rotated_cv2_image, str(datetime.now(pytz.timezone('America/Chicago')).strftime("%Y-%m-%d %H:%M:%S")), (10, rotated_cv2_image.shape[0] - 10), 2, 0.4, (255, 255, 2))
                        timestamp_image_bytes = cv2.imencode('.jpg', timestamp_cv2_image)[1].tobytes()

                        channel.basic_publish(exchange='',
                                              routing_key='livestream_images',
                                              body=timestamp_image_bytes)
                        channel.basic_publish(exchange='',
                                              routing_key='motion_processing_images',
                                              body=rotated_image_bytes)
                        buffer = buffer[buffer.find(ImageSeperator) + len(ImageSeperator):]
                        image_count += 1
                elif timer() - loop_start >= 10:
                    print(f"Lost connection to {connected_device}")
                    active_connection = False
                    continue

            except Exception as e:
                print(f"Error in main: {e}")
                raise(e)
                active_connection = False


if __name__ == "__main__":
    fetch_data()
