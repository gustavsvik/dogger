import numpy
import time
import shutil
import os
#import cv2

import runtime
from metadata import Configure

config = Configure()
env = config.get()

CHANNEL = 160

capture_filename = 'image_' + str(CHANNEL) + '.jpg'

def ReadSamples():
    global env
    try:
        #camera.capture(capture_filename, format='jpeg', quality=10)
        os.system('fswebcam -d /dev/video1 -r 1280x720 --fps 5 -S 1 --jpeg 95 --no-banner --save ' + capture_filename)
        #ret, frame = cam.read()
        #cv2.imwrite(capture_filename, frame)
    except PermissionError as e:
        print(e)


time.sleep(30)

#env = configuration.get()

count = 0
sample_rate = 1.0
divisor = numpy.int64(1/numpy.float64(sample_rate))
current_time = numpy.float64(time.time())
current_secs = numpy.int64(current_time)

#cam = cv2.VideoCapture(-1)

while True :

    sample_secs = current_secs + numpy.int64( divisor - current_secs % divisor )
    current_time = numpy.float64(time.time())
    current_secs = numpy.int64(current_time)
    if sample_secs > current_secs :
        time.sleep(0.1)
    else :
        if env['STORE_PATH'] is not None and os.path.exists(env['STORE_PATH']):
            ReadSamples()
            store_filename = env['STORE_PATH'] + str(CHANNEL) + '_' + str(sample_secs) + '.jpg'
            archive_filename = env['ARCHIVE_PATH'] + str(CHANNEL) + '_' + str(sample_secs) + '.jpg'
            try:
                shutil.copy(capture_filename, store_filename)
                if env['ARCHIVE_PATH'] is not None and os.path.exists(env['ARCHIVE_PATH']):
                    pass
                    #shutil.copy(capture_filename, archive_filename)
            except (FileNotFoundError, PermissionError) as e:
                runtime.logging.exception(e)
            count = count+1
