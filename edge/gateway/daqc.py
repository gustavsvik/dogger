#

import time
import datetime
import numpy
import shutil
import os
import sys
import io
import ctypes
import base64
import socket
import struct
import pyscreenshot as ImageGrab

import gateway.device as dv
import gateway.runtime as rt
import gateway.metadata as md
import gateway.task as t
import gateway.uplink as ul



class Udp(t.AcquireControlTask):


    def __init__(self):

        t.AcquireControlTask.__init__(self)

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)

        server_address = ('', self.port)
        self.socket.bind(server_address)



class UdpHttp(Udp):


    def __init__(self, port = None, config_filepath = None, config_filename = None):

        self.channels = None
        self.ip_list = None
        self.port = port
        self.start_delay = 0
        self.max_connect_attempts = 50
        self.sample_rate = None

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        self.env = self.get_env()
        if self.ip_list is None: self.ip_list = self.env['IP_LIST']

        Udp.__init__(self)

        self.http = ul.DirectUpload(channels = self.channels, start_delay = self.start_delay, max_connect_attempts = self.max_connect_attempts, config_filepath = self.config_filepath, config_filename = self.config_filename)


    def upload_data(self, channel, sample_secs, data_value):

        self.channels = [channel]

        try :
            for current_ip in self.ip_list :
                res = self.http.send_request(start_time = -9999, end_time = -9999, duration = 10, unit = 1, delete_horizon = 3600, ip = current_ip)
                data_string = str(channel) + ';' + str(sample_secs) + ',' + str(data_value) + ',,,;'
                res = self.http.set_requested(data_string, ip = current_ip)

        except PermissionError as e :
            rt.logging.exception(e)


    def run(self):

        time.sleep(self.start_delay)

        while True :

            time.sleep(0.1)
            data, address = self.socket.recvfrom(4096)
            values = struct.unpack('>HIf', data)
            self.upload_data(int(values[0]), int(values[1]), float(values[2]))



class File(t.AcquireControlTask):


    def __init__(self):

        self.env = self.get_env()
        if self.file_path is None: self.file_path = self.env['FILE_PATH']
        if self.archive_file_path is None: self.archive_file_path = self.env['ARCHIVE_FILE_PATH']

        t.AcquireControlTask.__init__(self)



class NidaqVoltageIn(File):


    def __init__(self, sample_rate = 1, samplesPerChan = 1, subSamplesPerChan = 1, minValue = 0, maxValue = 10, IPNumber = "", moduleSlotNumber = 1, moduleChanRange = [0], uniqueChanIndexRange = [0]):

        self.nidaq = dv.NidaqVoltageIn(sample_rate, samplesPerChan, subSamplesPerChan, minValue, maxValue, IPNumber, moduleSlotNumber, moduleChanRange, uniqueChanIndexRange)

        File.__init__(self)


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



class NidaqCurrentIn(File):


    def __init__(self, sample_rate = 1, samplesPerChan = 1, subSamplesPerChan = 1, minValue = 0, maxValue = 10, IPNumber = "", moduleSlotNumber = 1, moduleChanRange = [0], uniqueChanIndexRange = [0]):

        self.nidaq = daqc.device.NidaqVoltageIn(sample_rate, samplesPerChan, subSamplesPerChan, minValue, maxValue, IPNumber, moduleSlotNumber, moduleChanRange, uniqueChanIndexRange)

        File.__init__(self)


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



class UdpFile(File):


    def __init__(self):

        File.__init__(self)

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)

        server_address = ('', self.port)
        self.socket.bind(server_address)



class PosNmeaUdpFile(UdpFile):


    def __init__(self, channels = None, port = None, start_delay = None, sample_rate = None, file_path = None, archive_file_path = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.port = port
        self.start_delay = start_delay
        self.sample_rate = sample_rate
        self.file_path = file_path
        self.archive_file_path = archive_file_path

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        UdpFile.__init__(self)

        self.field_separator = ','
        self.line_identifier = 'GGA'


    def run(self):

        time.sleep(self.start_delay)

        divisor = numpy.int64(1/numpy.float64(self.sample_rate))
        current_time = numpy.float64(time.time())
        current_secs = numpy.int64(current_time)

        while True :

            time.sleep(0.1)
            sample_secs = current_secs + numpy.int64( divisor - current_secs % divisor )
            current_time = numpy.float64(time.time())
            current_secs = numpy.int64(current_time)

            data = None
            last_line = None

            data, address = self.socket.recvfrom(4096)
            rt.logging.debug(data)
            data_lines = data.decode("utf-8").splitlines()
            selected_lines = [line for line in data_lines if (self.line_identifier in line)]
            if len(selected_lines) > 0 :
                last_line = selected_lines[-1]

            channels = list(self.channels)

            if last_line :

                line_fields = last_line.split(self.field_separator)
                rt.logging.debug(line_fields)
                hour = int(float(line_fields[1])) // 10000
                minute = ( int(float(line_fields[1])) - hour * 10000 ) // 100
                second = int(float(line_fields[1])) - hour * 10000 - minute * 100
                microsecond = int ( ( float(line_fields[1]) - hour * 10000 - minute * 100 - second ) * 1000000 )
                datetime_current = datetime.datetime.fromtimestamp(current_secs)
                current_timestamp = datetime_current.timetuple()
                year = current_timestamp.tm_year
                month = current_timestamp.tm_mon
                monthday = current_timestamp.tm_mday
                datetime_origin = datetime.datetime(year, month, monthday, hour, minute, second, microsecond)
                orig_secs = int(datetime_origin.timestamp())

                channel = channels[0]
                filename = self.file_path + repr(channel) + "_" + repr(current_secs) + '.' + 'txt'
                try:
                    with open(filename, 'w') as text_file:
                        text_file.write(last_line)
                except PermissionError as e:
                    rt.logging.exception(e)

                acquired_microsecs = microsecond

                channel = channels[1]
                filename = self.file_path + repr(channel) + "_" + repr(current_secs) + '.' + 'npy'
                latitude_deg = float(line_fields[2]) // 100
                latitude_min = float(line_fields[2]) - latitude_deg * 100
                latitude = latitude_deg + latitude_min / 60
                if line_fields[3] == 'S' : latitude = -latitude
                latitude_array = numpy.concatenate(([0.0], acquired_microsecs), axis = None)
                latitude_array[0] = latitude
                try:
                    numpy.save(filename, latitude_array)
                except PermissionError as e:
                    rt.logging.exception(e)

                channel = channels[2]
                filename = self.file_path + repr(channel) + "_" + repr(current_secs) + '.' + 'npy'
                longitude_deg = float(line_fields[4]) // 100
                longitude_min = float(line_fields[4]) - longitude_deg * 100
                longitude = longitude_deg + longitude_min / 60
                if line_fields[5] == 'W' : longitude = -longitude
                longitude_array = numpy.concatenate(([0.0], acquired_microsecs), axis = None)
                longitude_array[0] = longitude
                try:
                    numpy.save(filename, longitude_array)
                except PermissionError as e:
                    rt.logging.exception(e)



class Image(File):


    def __init__(self):

        self.env = self.get_env()
        if self.video_res is None: self.video_res = self.env['VIDEO_RES']
        if self.video_quality is None: self.video_quality = self.env['VIDEO_QUALITY']
        (self.channel,) = self.channels
        self.capture_filename = 'image_' + str(self.channel) + '.' + self.file_extension

        File.__init__(self)


    def run(self):

        time.sleep(self.start_delay)

        divisor = numpy.int64(1/numpy.float64(self.sample_rate))
        current_time = numpy.float64(time.time())
        current_secs = numpy.int64(current_time)

        while True :

            sample_secs = current_secs + numpy.int64( divisor - current_secs % divisor )
            current_time = numpy.float64(time.time())
            current_secs = numpy.int64(current_time)
            if sample_secs > current_secs :
                time.sleep(0.1)
            else :

                self.read_samples()

                if self.file_path is not None and os.path.exists(self.file_path):
                    capture_file_timestamp = int(os.path.getmtime(self.capture_filename))
                    store_filename = self.file_path + str(self.channel) + '_' + str(capture_file_timestamp) + '.' + self.file_extension
                    archive_filename = self.archive_file_path + str(self.channel) + '_' + str(capture_file_timestamp) + '.' + self.file_extension
                    try:
                        shutil.copy(self.capture_filename, store_filename)
                        if self.archive_file_path is not None and os.path.exists(self.archive_file_path):
                            pass
                            #shutil.copy(capture_filename, archive_filename)
                    except (FileNotFoundError, PermissionError) as e:
                        rt.logging.exception(e)



class USBCam(Image):


    def __init__(self, channels = None, start_delay = 0, sample_rate = None, file_path = None, archive_file_path = None, file_extension = 'jpg', video_unit = None, video_res = None, video_rate = None, video_quality = None, video_capture_method = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.sample_rate = sample_rate

        self.file_path = file_path
        self.archive_file_path = archive_file_path
        self.file_extension = file_extension
        self.video_unit = video_unit
        self.video_res = video_res
        self.video_rate = video_rate
        self.video_quality = video_quality
        self.video_capture_method = video_capture_method

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        self.env = self.get_env()
        if self.video_unit is None: self.video_unit = self.env['VIDEO_UNIT']
        if self.video_rate is None: self.video_rate = self.env['VIDEO_RATE']
        if self.video_capture_method is None: self.video_capture_method = self.env['VIDEO_CAPTURE_METHOD']

        Image.__init__(self)

        if str.lower(self.video_capture_method) == 'opencv' :

            import cv2

            self.cam = cv2.VideoCapture( int(''.join(filter(str.isdigit, self.video_unit))), cv2.CAP_ANY )  # cv2.CAP_OPENCV_MJPEG

            resolutions = [ (320,200), (320,240), (640,480), (720,480), (854,450), (800,480), (768,576), (800,600), (1024,768), (1152,768), (1280,720), (1280,800), (1280,768), (1280,1024), (1366,768), (1440,960), (1400,1050), (1680,1050), (1600,1200), (1920,1080), (1920,1200) ]			
            for res in resolutions :
                self.cam.set(cv2.CAP_PROP_FRAME_WIDTH , res[0])
                self.cam.set(cv2.CAP_PROP_FRAME_HEIGHT, res[1])
                width = self.cam.get(cv2.CAP_PROP_FRAME_WIDTH)
                height = self.cam.get(cv2.CAP_PROP_FRAME_HEIGHT)
                if width == res[0] and height == res[1] :
                    rt.logging.debug("width, height: ", width, height)

            huge_dim = 10000
            self.cam.set(cv2.CAP_PROP_FRAME_WIDTH , huge_dim)
            self.cam.set(cv2.CAP_PROP_FRAME_HEIGHT, huge_dim)
            max_width = self.cam.get(cv2.CAP_PROP_FRAME_WIDTH)
            max_height = self.cam.get(cv2.CAP_PROP_FRAME_HEIGHT)
            self.cam.set(cv2.CAP_PROP_FRAME_WIDTH , max_width)
            self.cam.set(cv2.CAP_PROP_FRAME_HEIGHT, max_height)

            self.cam.set(cv2.CAP_PROP_FPS , self.video_rate)

        if str.lower(self.video_capture_method) == 'raspicam' :

            import picamera

            self.picam = picamera.PiCamera()
            self.picam.resolution = (1280, 720)
            self.picam.hflip = False
            self.picam.vflip = False
            #self.picam.rotation = 90
            self.picam.brightness = 50
            #self.picam.zoom = (0.6, 0.6, 0.5, 0.5)
            #self.picam.awb_mode = 'off'
            #self.picam.awb_gains = (1.0, 2.5)
            self.picam.shutter_speed = 50000
            self.picam.exposure_compensation = -24
            self.picam.iso = 100
            self.picam.contrast = -15

    def read_samples(self, sample_secs = -9999):

        try :

            time_before = time.time()

            if str.lower(self.video_capture_method) == 'opencv' :
                import cv2
                ret, frame = self.cam.read()
                frame = cv2.resize(frame, tuple(self.video_res), interpolation = cv2.INTER_AREA)
                cv2.imwrite( self.capture_filename, frame, [cv2.IMWRITE_JPEG_QUALITY, self.video_quality] )

            elif str.lower(self.video_capture_method) == 'uvccapture' :
                os.system('uvccapture -m -x' + str(self.video_res[0]) + ' -y' + str(self.video_res[1]) + ' -q' + str(self.video_quality) + ' -d' + self.video_unit + ' -o' + self.capture_filename)

            elif str.lower(self.video_capture_method) == 'ffmpeg' :
                os.system('ffmpeg -y -f v4l2 -hide_banner -loglevel warning -i ' + self.video_unit + ' -s ' + str(self.video_res[0]) + 'x' + str(self.video_res[1]) + ' -vframes 1 ' + self.capture_filename)  # -input_format mjpeg 

            elif str.lower(self.video_capture_method) == 'raspicam' :
                self.picam.capture(self.capture_filename, format='jpeg', quality=10)

            else : # fswebcam
                os.system('fswebcam -q -d ' + self.video_unit + ' -r ' + str(self.video_res[0]) + 'x' + str(self.video_res[1]) + ' --fps ' + str(self.video_rate) + ' -S 2 --jpeg ' + str(self.video_quality) + ' --no-banner --save ' + self.capture_filename) 
                # --set "Brightness"=127 --set "Contrast"=63 --set "Saturation"=127 --set "Hue"=90 --set "Gamma"=250 --set "White Balance Temperature, Auto"=True

            rt.logging.debug("Capture time: ", time.time() - time_before)

        except PermissionError as e :

            rt.logging.exception(e)



class ScreenshotUpload(Image):


    def __init__(self, channels = None, sample_rate = None, host_api_url = None, client_api_url = None, crop = None, video_quality = None, config_filepath = None, config_filename = None):

        self.channels = channels

        self.sample_rate = sample_rate
        self.host_api_url = host_api_url
        self.client_api_url = client_api_url
        self.crop = crop
        self.video_quality = video_quality

        self.start_delay = 0
        self.max_connect_attempts = 50
        self.file_extension = 'jpg'

        self.file_path = None
        self.archive_file_path = None
        self.video_res = None
        self.ip_list = None

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        self.env = self.get_env()
        if self.ip_list is None: self.ip_list = self.env['IP_LIST']

        Image.__init__(self)


    def read_samples(self, sample_secs = -9999):

        try :

            img = ImageGrab.grab( bbox = (self.crop[0], self.crop[1], self.crop[2], self.crop[3]) )
            jpeg_image_buffer = io.BytesIO()
            img.save(jpeg_image_buffer, format="JPEG")
            img_str = base64.b64encode(jpeg_image_buffer.getvalue())
            http = ul.DirectUpload(channels = self.channels, start_delay = self.start_delay, host_api_url = self.host_api_url, client_api_url = self.client_api_url, max_connect_attempts = self.max_connect_attempts)
            (channel,) = self.channels

            for current_ip in self.ip_list :
                res = http.send_request(start_time = sample_secs, end_time = sample_secs, duration = 10, unit = 1, delete_horizon = 3600, ip = current_ip)
                data_string = str(channel) + ';' + str(sample_secs) + ',-9999.0,,' + str(img_str.decode('utf-8')) + ',;'
                rt.logging.debug('data_string', data_string[0:100])
                res = http.set_requested(data_string, ip = current_ip)

        except PermissionError as e :
            rt.logging.exception(e)


    def run(self):

        time.sleep(self.start_delay)

        divisor = numpy.int64(1/numpy.float64(self.sample_rate))
        current_time = numpy.float64(time.time())
        current_secs = numpy.int64(current_time)

        while True :

            sample_secs = current_secs + numpy.int64( divisor - current_secs % divisor )
            current_time = numpy.float64(time.time())
            current_secs = numpy.int64(current_time)
            if sample_secs > current_secs :
                time.sleep(0.1)
            else :

                self.read_samples(sample_secs)



class TempFileUpload(Image):


    def __init__(self, channels = None, sample_rate = None, host_api_url = None, client_api_url = None, config_filepath = None, config_filename = None):

        self.channels = channels

        self.sample_rate = sample_rate
        self.host_api_url = host_api_url
        self.client_api_url = client_api_url

        self.start_delay = 0
        self.max_connect_attempts = 50
        self.file_extension = 'jpg'

        self.file_path = None
        self.archive_file_path = None

        self.video_res = None
        self.video_quality = None
        self.ip_list = None

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        self.env = self.get_env()
        if self.ip_list is None: self.ip_list = self.env['IP_LIST']

        Image.__init__(self)


    def upload_file(self, sample_secs = -9999):

        try :

            with open(self.capture_filename, "rb") as image_file:
                img_str = base64.b64encode(image_file.read())
            http = ul.DirectUpload(channels = self.channels, start_delay = self.start_delay, host_api_url = self.host_api_url, client_api_url = self.client_api_url, max_connect_attempts = self.max_connect_attempts)
            (channel,) = self.channels

            for current_ip in self.ip_list :
                res = http.send_request(start_time = sample_secs, end_time = sample_secs, duration = 10, unit = 1, delete_horizon = 3600, ip = current_ip)
                data_string = str(channel) + ';' + str(sample_secs) + ',-9999.0,,' + str(img_str.decode('utf-8')) + ',;'
                rt.logging.debug('data_string', data_string[0:100])
                res = http.set_requested(data_string, ip = current_ip)

        except PermissionError as e :
            rt.logging.exception(e)


    def run(self):

        time.sleep(self.start_delay)

        divisor = numpy.int64(1/numpy.float64(self.sample_rate))
        current_time = numpy.float64(time.time())
        current_secs = numpy.int64(current_time)

        while True :

            sample_secs = current_secs + numpy.int64( divisor - current_secs % divisor )
            current_time = numpy.float64(time.time())
            current_secs = numpy.int64(current_time)
            if sample_secs > current_secs :
                time.sleep(0.1)
            else :
                self.upload_file(sample_secs)



class AcquireCurrent(File):

    """ Adapted from https://scipy-cookbook.readthedocs.io/items/Data_Acquisition_with_NIDAQmx.html."""


    def __init__(self, channels = None, start_delay = 0, sample_rate = None, file_path = None, archive_file_path = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.sample_rate = sample_rate

        self.file_path = file_path
        self.archive_file_path = archive_file_path

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        File.__init__(self)

        self.nidaq = None
        if sys.platform.startswith('win32') : 
            self.nidaq = ctypes.windll.nicaiu

        self.uInt8 = ctypes.c_ubyte
        self.int32 = ctypes.c_long
        self.uInt32 = ctypes.c_ulong
        self.uInt64 = ctypes.c_ulonglong
        self.float64 = ctypes.c_double
        self.bool32 = ctypes.c_bool
        self.TaskHandle = self.uInt32
        self.pointsWritten = self.uInt32()
        self.pointsRead = self.uInt32()
        self.null = ctypes.POINTER(ctypes.c_int)()
        self.value = self.uInt32()

        self.DAQmx_Val_Cfg_Default = self.int32(-1)
        self.DAQmx_Val_Auto = self.int32(-1)
        self.DAQmx_Val_Internal = self.int32(10200)
        self.DAQmx_Val_Volts = self.int32(10348)
        self.DAQmx_Val_Rising = self.int32(10280)
        self.DAQmx_Val_Falling = self.int32(10171)
        self.DAQmx_Val_CountUp = self.int32(10128)
        self.DAQmx_Val_FiniteSamps = self.int32(10178)
        self.DAQmx_Val_GroupByChannel = self.int32(0)
        self.DAQmx_Val_ChanForAllLines = self.int32(1)
        self.DAQmx_Val_RSE = self.int32(10083)
        self.DAQmx_Val_Diff = self.int32(10106)
        self.DAQmx_Val_Amps = self.int32(10342)
        self.DAQmx_Val_ContSamps = self.int32(10123)
        self.DAQmx_Val_GroupByScanNumber = self.int32(1)
        self.DAQmx_Val_Task_Reserve = self.int32(4)
        self.DAQmx_Val_ChanPerLine = self.int32(0)

        self.SAMPLE_RATE = 1000
        self.SAMPLES_PER_CHAN = 1000

        self.taskCurrent = self.TaskHandle(0)

        self.minCurrent = self.float64(-0.02)
        self.maxCurrent = self.float64(0.02)
        self.bufferSize = self.uInt32(self.SAMPLES_PER_CHAN)
        self.pointsToRead = self.bufferSize
        self.pointsRead = self.uInt32()
        self.sampleRate = self.float64(self.SAMPLE_RATE)
        self.samplesPerChan = self.uInt64(self.SAMPLES_PER_CHAN)
        self.clockSource = ctypes.create_string_buffer(b"OnboardClock")
        self.IPnumber2 = ctypes.create_string_buffer(b"169.254.254.253")
        self.defaultModuleName22 = ctypes.create_string_buffer(b"cDAQ9188-1AD0C2FMod2")
        self.timeout = self.float64(100.0)

        self.device2NameOut = ctypes.create_string_buffer(100)
        self.device2NameOutBufferSize = self.uInt32(100)
        self.device2ModuleNamesOut = ctypes.create_string_buffer(1000)
        self.device2ModuleNamesOutBufferSize = self.uInt32(1000)
        self.module21ChansOut = ctypes.create_string_buffer(2000)
        self.module21ChansOutBufferSize = self.uInt32(2000)
        self.module22ChansOut = ctypes.create_string_buffer(2000)
        self.module22ChansOutBufferSize = self.uInt32(2000)

        self.data = numpy.zeros((1000,),dtype=numpy.float64)

        self.global_error_code = 0


    def CHK(self, err):
        """a simple error checking routine"""
        global global_error_code
        global_error_code = err
        if err < 0:
            buf_size = 1000
            buf = ctypes.create_string_buffer(b"\000" * buf_size)
            self.nidaq.DAQmxGetErrorString(err,ctypes.byref(buf),buf_size)
            #raise RuntimeError('nidaq call failed with error %d: %s'%(err,repr(buf.value)))
            rt.logging.debug('nidaq call failed with error %d: %s'%(err,repr(buf.value)))


    def scale_cond_transmitter(self, I_cond):
        #return I_cond
        return ( I_cond * 1000.0 - 4.0 ) / 16.0 * 10e-6

    def scale_Opto_22_AD3_SATT_ETT45_0101(self, I_temp):
        return ( I_temp * 1000.0 - 4.0 ) / 16.0 * 50.0

    def downsample(self, y, size):
        y_reshape = y.reshape(size, int(len(y)/size))
        y_downsamp = y_reshape.mean(axis=1)
        return y_downsamp

    # Create Task and Voltage Channel and Configure Sample Clock
    def SetupTasks(self):
        self.device2NameOut = b"cDAQ9188-1AD0C2F"

        self.CHK( self.nidaq.DAQmxGetDevChassisModuleDevNames(self.device2NameOut, self.device2ModuleNamesOut, self.device2ModuleNamesOutBufferSize) )
        rt.logging.debug("device2ModuleNamesOut: ", repr(self.device2ModuleNamesOut.value))
        self.CHK( self.nidaq.DAQmxGetDevAIPhysicalChans(self.defaultModuleName22, self.module22ChansOut, 2000) )
        rt.logging.debug("module22ChansOut: ", repr(self.module22ChansOut.value))
        self.CHK(self.nidaq.DAQmxCreateTask("",ctypes.byref(self.taskCurrent)))
        self.CHK(self.nidaq.DAQmxCreateAICurrentChan(self.taskCurrent,self.module22ChansOut,"",self.DAQmx_Val_RSE,self.minCurrent,self.maxCurrent,self.DAQmx_Val_Amps,self.DAQmx_Val_Internal,None,None))
        self.CHK(self.nidaq.DAQmxCfgSampClkTiming(self.taskCurrent, self.clockSource, self.sampleRate, self.DAQmx_Val_Rising, self.DAQmx_Val_ContSamps, self.samplesPerChan))
        #CHK(nidaq.DAQmxCfgInputBuffer(taskCurrent,200000))


    def ReserveTasks(self):
        self.CHK(self.nidaq.DAQmxTaskControl(self.taskCurrent, self.DAQmx_Val_Task_Reserve))

    def StartTasks(self):
        self.CHK(self.nidaq.DAQmxStartTask(self.taskCurrent))

    def ReadCurrent(self):
        self.pointsToRead = self.bufferSize
        self.data = numpy.zeros((16*self.bufferSize.value,),dtype=numpy.float64)
        if global_error_code >= 0:
            self.CHK(self.nidaq.DAQmxReadAnalogF64(self.taskCurrent,self.pointsToRead,self.timeout,self.DAQmx_Val_GroupByChannel,self.data.ctypes.data,self.uInt32(16*self.bufferSize.value),ctypes.byref(self.pointsRead),None))
        return self.data

    def StopAndClearTasks(self):
        if self.taskCurrent.value != 0:
            self.CHK( self.nidaq.DAQmxStopTask(self.taskCurrent) )
            self.CHK( self.nidaq.DAQmxClearTask(self.taskCurrent) )

    def InitAcquire(self):
        self.SetupTasks()
        self.ReserveTasks()
        self.StartTasks()


    def LoopAcquire(self):

        while (global_error_code >= 0):

            current = 0.0
            try:
                current = self.ReadCurrent()
            except OSError as e:
                rt.logging.exception(e)

            if self.file_path is not None and os.path.exists(self.file_path):

                acq_finish_time = numpy.float64(time.time())
                acq_finish_secs = numpy.int64(acq_finish_time)
                acq_finish_microsecs = numpy.int64(acq_finish_time * 1e6)
                acq_microsec_part = acq_finish_microsecs - numpy.int64(acq_finish_secs)*1e6
                if acq_microsec_part > 990000 :
                    time.sleep(0.03)
                if acq_microsec_part < 10000 :
                    time.sleep(0.87)

                for channel_index in range(0, 16):
                    current_array = current[self.SAMPLES_PER_CHAN*(channel_index+0):self.SAMPLES_PER_CHAN*(channel_index+1)]
                    if channel_index == 0 : current_array = self.scale_cond_transmitter(current_array)
                    if channel_index == 1 : current_array = self.scale_Opto_22_AD3_SATT_ETT45_0101(current_array)
                    current_avg = self.downsample(current_array, 1)
                    current_array = numpy.concatenate(([0.0], acq_microsec_part, current_array), axis = None)
                    current_array[0] = current_avg
                    try:
                        filename_current = repr(97+channel_index) + "_" + repr(acq_finish_secs)
                        numpy.save(self.file_path+filename_current, current_array)
                    except PermissionError as e:
                        rt.logging.exception(e)


    def run(self):

        self.InitAcquire()
        while True:
            if global_error_code >= 0:
                self.LoopAcquire()
                if global_error_code < 0:
                    self.StopAndClearTasks()
                    self.InitAcquire()
            else:
                self.StopAndClearTasks()
                self.InitAcquire()
            time.sleep(10)



class AcquireMaster:


    def __init__(self):
        pass


class AcquireVoltage:

    def __init__(self):
        pass
