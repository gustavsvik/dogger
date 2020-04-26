#

import time
import numpy
import time
import shutil
import os
#import cv2

import daqc.device
import runtime
import metadata



class Host:


    def __init__(self):
        pass


        
class NidaqVoltageIn(Host):


    def __init__(self, sample_rate = 1, samplesPerChan = 1, subSamplesPerChan = 1, minValue = 0, maxValue = 10, IPNumber = "", moduleSlotNumber = 1, moduleChanRange = [0], uniqueChanIndexRange = [0]):

        self.nidaq = daqc.device.NidaqVoltageIn(sample_rate, samplesPerChan, subSamplesPerChan, minValue, maxValue, IPNumber, moduleSlotNumber, moduleChanRange, uniqueChanIndexRange)


    def run(self):

        self.nidaq.InitAcquire()
        while True:
            if self.nidaq.globalErrorCode >= 0:
                self.nidaq.LoopAcquire()
                if self.nidaq.globalErrorCode < 0:
                    self.nidaq.StopAndClearTasks()
                    self.nidaq.InitAcquire()
            else:
                self.nidaq.StopAndClearTasks()
                self.nidaq.InitAcquire()
            time.sleep(10)

            
            
class USBCam:
    

    def __init__(self, channels = None, sample_rate = 1.0, start_delay = 0, video_unit = '/dev/video0', video_res = {800, 600}, video_rate = 10):

        self.channels = channels
        self.start_delay = start_delay
        self.sample_rate = sample_rate
        self.video_unit = video_unit
        self.video_res = video_res
        self.video_rate = video_rate
        self.config = metadata.Configure()
        self.env = self.config.get()
        (self.CHANNEL,) = self.channels
        self.capture_filename = 'image_' + str(self.CHANNEL) + '.jpg'

        
    def read_samples(self):
        #global env
        try:
            #camera.capture(capture_filename, format='jpeg', quality=10)
            os.system('fswebcam -d ' + self.video_unit + ' -r ' + str(self.video_res[0]) + 'x' + str(self.video_res[1]) + ' --fps ' + str(self.video_rate) + ' -S 1 --jpeg 95 --no-banner --save ' + self.capture_filename)
            #ret, frame = cam.read()
            #cv2.imwrite(capture_filename, frame)
        except PermissionError as e:
            print(e)

            
    def run(self):

        time.sleep(self.start_delay)

        #env = configuration.get()

        count = 0
        divisor = numpy.int64(1/numpy.float64(self.sample_rate))
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
                if self.env['STORE_PATH'] is not None and os.path.exists(self.env['STORE_PATH']):

                    self.read_samples()

                    store_filename = self.env['STORE_PATH'] + str(self.CHANNEL) + '_' + str(sample_secs) + '.jpg'
                    archive_filename = self.env['ARCHIVE_PATH'] + str(self.CHANNEL) + '_' + str(sample_secs) + '.jpg'
                    try:
                        shutil.copy(self.capture_filename, store_filename)
                        if self.env['ARCHIVE_PATH'] is not None and os.path.exists(self.env['ARCHIVE_PATH']):
                            pass
                            #shutil.copy(capture_filename, archive_filename)
                    except (FileNotFoundError, PermissionError) as e:
                        runtime.logging.exception(e)
                    count = count+1
