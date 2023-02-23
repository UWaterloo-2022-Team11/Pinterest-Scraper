import multiprocessing as mp
from multiprocessing import Process, Queue
import os
import psycopg2
import json
import numpy as np
import time
# os.environ["KERAS_BACKEND"] = "plaidml.keras.backend"
import keras
import keras.applications as kapp
from keras.preprocessing import image
from keras.applications.mobilenet import MobileNet, preprocess_input
from io import BytesIO
import urllib
from PIL import Image
from urllib import request
# mp.set_start_method('spawn')

def decode_hex(text):
    return bytes.fromhex(text[2:]).decode('ascii')[1:-1]

def get_image(text, resize=True):
    # url is encoded as bytes in db
    url = decode_hex(text)
    res = request.urlopen(url).read()
    # vgg16 by default uses 224x224, messes up the size of our encoding if we dont do this
    if resize:
        x = Image.open(BytesIO(res)).resize((224,224))
    else:
        x = Image.open(BytesIO(res))
    # x.save('img.png')
    if x.mode == 'CMYK':
        x = x.convert('RGB')

    x = image.img_to_array(x)
    if x.shape[2] == 1: # greyscale image:
        x = np.dstack([x]*3)
    x = np.expand_dims(x, axis=0)
    x = preprocess_input(x)
    if x.shape != (1, 224, 224, 3):
        print(url)
    return x


def image_worker(q, processed_images, event):
    # Fetch raw url and process into input for vgg
    # should just go until event is set, then you call join on it
    while True:
        if event.is_set():
            return
        if processed_images.qsize() < 10:
            work = []
            items = q.get()
            for item in items:
                try:
                    image = get_image(item[1])
                except Exception as e:
                    print('failed to download image')
                    print(e)
                work.append((item[0], image))
            processed_images.put(work)
            print('scraped some images')

def vgg_worker(db_q, processed_images, event):
    # instatiate vgg16
    model = kapp.MobileNet(
        input_shape=(224, 224, 3),
        include_top=False,
        weights="imagenet",
        pooling='avg'
    )
    db_con = {}
    with open('db_con.json', 'r') as f:
        db_con = json.load(f)
    conn = psycopg2.connect(database="postgres",
                        host=db_con['host'],
                        user=db_con['user'],
                        password=db_con['password'],
                        port=db_con['port'])

    cur = conn.cursor()
    while True:
        if event.is_set():
            cur.close()
            conn.close()
            return
        if not processed_images.empty():
            print(f'Image q size: {processed_images.qsize()}')
            try:
                work = processed_images.get()
                # encode image
                preds = []
                for item in work:
                    preds.append((item[0], model.predict(item[1])))
                # db_q.put(preds)
                print('predicted some images')

                sql = "UPDATE public.\"PinData\" SET \"vector\"=%s WHERE \"pinid\"=%s"
                for image in preds:
                    pred = image[1]
                    pinid = image[0]
                    binary_string = pred.tobytes()
                    cur.execute(sql, (psycopg2.Binary(binary_string), pinid))
                conn.commit()
                print('uploaded images')
            except Exception as e:
                print(e)
                print(f'error on image: {item[0]}')

def db_worker(q, event):
    db_con = {}
    with open('db_con.json', 'r') as f:
        db_con = json.load(f)
    conn = psycopg2.connect(database="postgres",
                        host=db_con['host'],
                        user=db_con['user'],
                        password=db_con['password'],
                        port=db_con['port'])

    cur = conn.cursor()
    while True:
        if event.is_set():
            cur.close()
            conn.close()
            return
        # feed pins into the queue
        if q.qsize() < 20:
            cur.execute("SELECT * FROM public.\"PinData\" where \"InUse\"=False LIMIT 300;")
            rows = cur.fetchall()
            work = []
            for row in rows:
                # prevent us from looking at pins we have already finished
                cur.execute(f"UPDATE public.\"PinData\" SET \"InUse\" = true WHERE \"pinid\" = '{row[0]}';")
                # put pin data into queue for image fetching and preprocessing
                work.append((row[0], row[1]))
            q.put(work)
            print('db is adding work to q')
            conn.commit()

def upload_worker(db_q, event):
    db_con = {}
    with open('db_con.json', 'r') as f:
        db_con = json.load(f)
    conn = psycopg2.connect(database="postgres",
                        host=db_con['host'],
                        user=db_con['user'],
                        password=db_con['password'],
                        port=db_con['port'])

    cur = conn.cursor()
    while True:
        if event.is_set():
            cur.close()
            conn.close()
            return
        if not db_q.empty():
            print(db_q.qsize())
            images = db_q.get()
            # insert images
            sql = "UPDATE public.\"PinData\" SET \"vector\"=%s WHERE \"pinid\"=%s"
            for image in images:
                pred = image[1]
                pinid = image[0]
                binary_string = pred.tostring()
                cur.execute(sql, (psycopg2.Binary(binary_string), pinid))
            conn.commit()
            print('uploading images')

        
def main():

    raw_url_q = Queue()
    processed_images = Queue()
    db_q = Queue()

    event = mp.Event()


    start = time.time()

    # start workers
    db_workers = mp.Pool(10, db_worker,(raw_url_q, event,))
    # upload_workers = mp.Pool(8, upload_worker,(db_q, event,))
    image_workers = mp.Pool(10, image_worker,(raw_url_q, processed_images, event,))
    vgg_workers = mp.Pool(10, vgg_worker,(db_q, processed_images, event,))
    # db = Process(target=db_worker, args=(db_q, q, event))
    # db.start()

    # not 100% sure this sleep will not block thread
    # time.sleep(10)
    time.sleep(600000)
    # stop all workers
    event.set()
    time.sleep(60)

    image_workers.close()
    vgg_workers.close()
    db_workers.close()

    # upload_workers.close()

    # upload_workers.join()

    vgg_workers.join()
    db_workers.join()
    image_workers.join()
    end = time.time()
    print(f'delta t: {end-start}')


def test():

    out = Queue()
    q = Queue()
    db_q = Queue()

    event = mp.Event()
    db = Process(target=db_worker, args=(db_q, q, event,))
    db.start()

    # not 100% sure this sleep will not block thread
    time.sleep(100)

    # stop all workers
    event.set()
    db.join()

    # 12733 @ 744
if __name__ == "__main__":
    main()
    # test()
# tests

