# ----------------------------- Imports -----------------------------

from flask import Flask, Response, render_template
from flask_bootstrap import Bootstrap
import threading
import pika
from time import sleep

# ----------------------------- Globals -----------------------------

app = Flask(__name__)
Bootstrap(app)

CurrentImage = ""
ImageLock = threading.Lock()

# ----------------------------- Routes -----------------------------

@app.route("/")
def index():
    return "<h1>IOT Hub</h1>"

@app.route('/video_livestream')
def video_livestream():
    return render_template('video_livestream.html')

@app.route('/video_feed')
def video_feed():
    return Response(get_current_image(), mimetype='multipart/x-mixed-replace; boundary=frame')

# ----------------------------- Classes -----------------------------

class ImageUpdateThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.daemon = True

    def run(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.basic_consume(queue='livestream_images',
                              auto_ack=True,
                              on_message_callback=self.update_current_image)
        channel.start_consuming()

    def update_current_image(self, ch, method, properties, body):
        global CurrentImage, ImageLock
        with ImageLock:
            CurrentImage = body

# ----------------------------- Functions -----------------------------

def get_current_image():
    global CurrentImage, ImageLock

    while True:
        with ImageLock:
            yield_image = CurrentImage

        if len(yield_image) == 0:
            with open("images/err.png", "rb") as f:
                yield_image = f.read()

        yield (b'--frame\r\n'
               b'Content-Type: image/jpeg\r\n\r\n' + yield_image + b'\r\n')

        # Share ImageLock time with ImageUpdateThread.update_current_image
        sleep(.01)

if __name__ == "__main__":
    image_update_thread = ImageUpdateThread()
    image_update_thread.start()
    app.run(host='0.0.0.0')

