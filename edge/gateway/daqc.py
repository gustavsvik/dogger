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
import serial
import serial.tools.list_ports
import socket
import struct
import pyscreenshot as ImageGrab

import gateway.transform as tr
import gateway.device as dv
import gateway.runtime as rt
import gateway.task as ta
import gateway.link as li



class Udp(ta.AcquireControlTask):


    def __init__(self):

        ta.AcquireControlTask.__init__(self)

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)

        server_address = ('', self.port)
        self.socket.bind(server_address)



class UdpHttp(Udp) :


    def __init__(self):

        self.channels = None
        self.ip_list = None
        self.start_delay = 0
        self.max_connect_attempts = 50
        self.sample_rate = None

        self.env = self.get_env()
        if self.ip_list is None: self.ip_list = self.env['IP_LIST']

        Udp.__init__(self)


    def upload_data(self, channel, sample_secs, data_value, byte_string) :

        try :
            http = li.DirectUpload(channels = [channel], start_delay = self.start_delay, max_connect_attempts = self.max_connect_attempts, config_filepath = self.config_filepath, config_filename = self.config_filename)
            for current_ip in self.ip_list :
                res = http.send_request(start_time = -9999, end_time = -9999, duration = 10, unit = 1, delete_horizon = 3600, ip = current_ip)
                data_string = str(channel) + ';' + str(sample_secs) + ',' + str(data_value) + ',,' + byte_string.decode() + ',;'
                res = http.set_requested(data_string, ip = current_ip)
        except PermissionError as e :
            rt.logging.exception(e)



class UdpValueHttp(UdpHttp) :


    def __init__(self, port = None, config_filepath = None, config_filename = None):

        self.port = port

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        UdpHttp.__init__(self)


    def run(self):

        time.sleep(self.start_delay)

        while True :

            time.sleep(0.1)
            data, address = self.socket.recvfrom(4096)
            rt.logging.debug("data", data, "len(data)", len(data))
            values = struct.unpack('>HIf', data)
            rt.logging.debug("values", values)
            self.upload_data(int(values[0]), int(values[1]), float(values[2]), b'')



class UdpBytesHttp(UdpHttp) :


    def __init__(self, port = None, config_filepath = None, config_filename = None):

        self.port = port

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        UdpHttp.__init__(self)


    def run(self):

        time.sleep(self.start_delay)

        while True :

            time.sleep(0.1)
            data, address = self.socket.recvfrom(4096)
            print("data", data, "len(data)", len(data))
            values = struct.unpack_from('>HI', data, offset = 0)
            print("values", values)
            byte_string = struct.unpack_from( '{}s'.format(len(data) - 6), data[6:len(data)], offset = 0)
            replaced_byte_string = byte_string[0].replace(b',', b'|').replace(b';', b'~')
            print("replaced_byte_string", replaced_byte_string)
            self.upload_data(int(values[0]), int(values[1]), -9999.0, replaced_byte_string)



class ProcessFile(ta.AcquireControlTask) :


    def __init__(self) :

        self.env = self.get_env()
        if self.file_path is None: self.file_path = self.env['FILE_PATH']



class IngestFile(ProcessFile) :


    def __init__(self) :

        self.env = self.get_env()
        if self.archive_file_path is None: self.archive_file_path = self.env['ARCHIVE_FILE_PATH']

        ProcessFile.__init__(self)


    def write(self, data_array = None, selected_tag = None, timestamp_secs = None, timestamp_microsecs = None) :

        print("data_array", data_array)
        selected_channel_array = [ tag_channels for channel_tag, tag_channels in self.channels.items() if channel_tag == selected_tag ]
        print("selected_channel_array", selected_channel_array)

        for channel, file_type in selected_channel_array[0].items() :
            print ("channel, file_type", channel, file_type) 
            print("selected_channel_array[0][channel]", selected_channel_array[0][channel])
            if self.file_path is not None and os.path.exists(self.file_path) :

                capture_file_timestamp = timestamp_secs 
                store_filename = self.file_path + str(channel) + '_' + str(capture_file_timestamp) + '.' + file_type

                if file_type == 'npy' :
                    float_array = data_array[0][channel]
                    float_avg = tr.downsample(numpy.float64(float_array), 1)
                    float_array = numpy.concatenate(([0.0], timestamp_microsecs, float_array), axis = None)
                    float_array[0] = float_avg
                    try:
                        numpy.save(store_filename, float_array)
                    except PermissionError as e:
                        rt.logging.exception(e)

                if file_type == 'txt' and data_array[0][channel] != '' :
                    try:
                        with open(store_filename, 'w') as text_file :
                            text_file.write(data_array[0][channel])
                    except PermissionError as e:
                        rt.logging.exception(e)

                if self.file_path is not None and os.path.exists(self.file_path):
                    try:
                        if self.archive_file_path is not None and os.path.exists(self.archive_file_path) :
                            archive_filename = self.archive_file_path + str(channel) + '_' + str(capture_file_timestamp) + '.' + file_type
                            #shutil.copy(store_filename, archive_filename)
                    except (FileNotFoundError, PermissionError) as e:
                        rt.logging.exception(e)



class StaticFileNmeaFile(IngestFile) :


    def __init__(self, channels = None, start_delay = None, sample_rate = None, static_file_path_name = None, file_path = None, archive_file_path = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.sample_rate = sample_rate
        self.static_file_path_name = static_file_path_name
        self.file_path = file_path
        self.archive_file_path = archive_file_path

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        IngestFile.__init__(self)

        self.nmea = tr.Nmea(prepend = '', append = '')


    def run(self) :

        text_strings = []

        try :
            text_file = open(self.static_file_path_name, "r")
            text_strings = text_file.read().splitlines()
            print("text_strings", text_strings)
        except OSError as e :
            rt.logging.exception(e)

        while True :

            string_dict = {}

            for text_string in text_strings :

                print("text_string", text_string)
                #acquired_base64 = text_string.encode("utf-8") # base64.b64encode(
                #print("acquired_base64", acquired_base64)

                timestamp_secs, current_timetuple, timestamp_microsecs, next_sample_secs = tr.timestamp_to_date_times(sample_rate = self.sample_rate)

                for selected_tag, channels in self.channels.items() :
                    print("channels", channels)
                    channels_list = list(channels)

                    if selected_tag in text_string : 
                        print("selected_tag", selected_tag, "channels_list", channels_list)
                        current_string_value = ''
                        try :
                            current_string_value = string_dict[selected_tag]
                        except KeyError as e :
                            pass
                        string_dict[selected_tag] = current_string_value + text_string

            print("string_dict", string_dict)

            for selected_tag, channels in self.channels.items() :

                channels_list = list(channels)

                dict_string = ''
                try :
                    dict_string = string_dict[selected_tag] + '\r\n'
                except KeyError as e :
                    pass

                data_array = None

                if selected_tag == 'MMB' :
                    value = self.nmea.to_float(dict_string, self.start_pos)
                    data_array = [ dict( [ (channels_list[0], dict_string) , (channels_list[1], [value]) ] ) ]

                if selected_tag == 'VDM' :
                    data_array = [ dict( [ (channels_list[0], dict_string) ] ) ]

                if selected_tag == 'VDO' :
                    data_array = [ dict( [ (channels_list[0], dict_string) ] ) ]

                if selected_tag == 'ALV' :
                    data_array = [ dict( [ (channels_list[0], dict_string) ] ) ]

                if selected_tag == 'ALR' :
                    data_array = [ dict( [ (channels_list[0], dict_string) ] ) ]

                if selected_tag == 'TTM' :
                    data_array = [ dict( [ (channels_list[0], dict_string) ] ) ]

                print("data_array", data_array)
                print("timestamp_secs", timestamp_secs)
                if data_array is not None :
                    self.write(data_array = data_array, selected_tag = selected_tag, timestamp_secs = timestamp_secs, timestamp_microsecs = timestamp_microsecs) 

            time.sleep(1/self.sample_rate)




class NidaqVoltageIn(IngestFile):


    def __init__(self, sample_rate = 1, samplesPerChan = 1, subSamplesPerChan = 1, minValue = 0, maxValue = 10, IPNumber = "", moduleSlotNumber = 1, moduleChanRange = [0], uniqueChanIndexRange = [0]):

        self.nidaq = dv.NidaqVoltageIn(sample_rate, samplesPerChan, subSamplesPerChan, minValue, maxValue, IPNumber, moduleSlotNumber, moduleChanRange, uniqueChanIndexRange)

        IngestFile.__init__(self)


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



class NidaqCurrentIn(IngestFile):


    def __init__(self, sample_rate = 1, samplesPerChan = 1, subSamplesPerChan = 1, minValue = 0, maxValue = 10, IPNumber = "", moduleSlotNumber = 1, moduleChanRange = [0], uniqueChanIndexRange = [0]):

        self.nidaq = daqc.device.NidaqVoltageIn(sample_rate, samplesPerChan, subSamplesPerChan, minValue, maxValue, IPNumber, moduleSlotNumber, moduleChanRange, uniqueChanIndexRange)

        IngestFile.__init__(self)


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



class SerialFile(IngestFile) :


    def __init__(self):

        IngestFile.__init__(self)


    def init_acquire(self) :

        comport_list = serial.tools.list_ports.comports()
        for comport in comport_list:
            port_string = str(comport.device) + ';' + str(comport.name) + ';' + str(comport.description) + ';' + str(comport.hwid) + ';' + str(comport.vid) + ';' + str(comport.pid) + ';' + str(comport.serial_number) + ';' + str(comport.location) + ';' + str(comport.manufacturer) + ';' + str(comport.product) + ';' + str(comport.interface)
            print("port_string", port_string)

            if self.port is None :
                print("self.location", self.location)
                if (port_string.find(self.location)) >= 0:
                    self.port = comport.device
                    print("self.port", self.port)

        self.serial_conn = None 

        try :
            self.serial_conn = serial.Serial(port = self.port, baudrate = self.baudrate, timeout = self.timeout, parity = serial.PARITY_EVEN, stopbits = serial.STOPBITS_ONE, bytesize = serial.SEVENBITS) #, write_timeout=1, , , , xonxoff=False, rtscts=False, dsrdtr=False)
            time.sleep(0.1)
            if (self.serial_conn.isOpen()):
                print("connected to : " + self.serial_conn.portstr)
        except serial.serialutil.SerialException as e:
            rt.logging.exception(e)



class SerialNmeaFile(SerialFile) :


    def __init__(self, channels = None, port = None, start_delay = None, sample_rate = None, location = None, baudrate = None, timeout = None, parity = None, stopbits = None, bytesize = None, delay_waiting_check = None, file_path = None, archive_file_path = None, start_pos = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.port = port
        self.start_delay = start_delay
        self.sample_rate = sample_rate
        self.location = location
        self.baudrate = baudrate
        self.timeout = timeout
        self.parity = parity
        self.stopbits = stopbits
        self.bytesize = bytesize
        self.delay_waiting_check = delay_waiting_check
        self.file_path = file_path
        self.archive_file_path = archive_file_path
        self.start_pos = start_pos

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        SerialFile.__init__(self)

        self.nmea = tr.Nmea(prepend = '', append = '')


    def run(self) :


        while True:

            self.init_acquire()
        
            data_string = ''

            while self.serial_conn.isOpen():

                response = ''

                #timestamp_secs, current_timetuple, timestamp_microsecs, next_sample_secs = tr.timestamp_to_date_times(sample_rate = self.sample_rate)
                #prev_timestamp_secs = timestamp_secs

                while self.serial_conn.in_waiting:

                    response = self.serial_conn.read()

                    response_string = ''
                    try:
                        response_string = response.decode()
                    except UnicodeDecodeError as e:
                        rt.logging.exception(e)
                    data_string += response_string

                    time.sleep(self.delay_waiting_check)

                if data_string != '' :

                    print("data_string", data_string)

                    data_lines = data_string.splitlines()
                    print("data_lines", data_lines)

                    timestamp_secs, current_timetuple, timestamp_microsecs, next_sample_secs = tr.timestamp_to_date_times(sample_rate = self.sample_rate)

                    string_dict = {}

                    for selected_line in data_lines :

                        print("selected_line", selected_line)
                        for selected_tag, channels in self.channels.items() :
                            print("channels", channels)
                            channels_list = list(channels)

                            if selected_tag in selected_line : 
                                print("selected_tag", selected_tag, "channels_list", channels_list)
                                current_string_value = ''
                                try :
                                    current_string_value = string_dict[selected_tag]
                                except KeyError as e :
                                    pass
                                string_dict[selected_tag] = current_string_value + selected_line
                                #if timestamp_secs == prev_timestamp_secs ;
                                #    string_dict[selected_tag] = string_dict[selected_tag] + selected_line
                                #else :
                                #    string_dict[selected_tag] = ''
                                #    prev_timestamp_secs = timestamp_secs
                    print("string_dict", string_dict)

                    for selected_tag, channels in self.channels.items() :

                        channels_list = list(channels)

                        dict_string = ''
                        try :
                            dict_string = string_dict[selected_tag] + '\r\n'
                        except KeyError as e :
                            pass

                        data_array = None

                        if selected_tag == 'MMB' :
                            value = self.nmea.to_float(dict_string, self.start_pos)
                            data_array = [ dict( [ (channels_list[0], dict_string) , (channels_list[1], [value]) ] ) ]

                        if selected_tag == 'VDM' :
                            data_array = [ dict( [ (channels_list[0], dict_string) ] ) ]

                        if selected_tag == 'VDO' :
                            data_array = [ dict( [ (channels_list[0], dict_string) ] ) ]

                        if selected_tag == 'ALV' :
                            data_array = [ dict( [ (channels_list[0], dict_string) ] ) ]

                        if selected_tag == 'ALR' :
                            data_array = [ dict( [ (channels_list[0], dict_string) ] ) ]

                        if selected_tag == 'TTM' :
                            data_array = [ dict( [ (channels_list[0], dict_string) ] ) ]

                        print("data_array", data_array)
                        print("timestamp_secs", timestamp_secs)
                        if data_array is not None :
                            self.write(data_array = data_array, selected_tag = selected_tag, timestamp_secs = timestamp_secs, timestamp_microsecs = timestamp_microsecs) 

                data_string = ''

                time.sleep(1/self.sample_rate)

            self.serial_conn.close()

            time.sleep(5)



class UdpFile(IngestFile):


    def __init__(self):

        IngestFile.__init__(self)

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)

        server_address = ('', self.port)
        self.socket.bind(server_address)



class NmeaUdpFile(UdpFile):


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
        
        self.nmea = tr.Nmea(prepend = '', append = '')


    def run(self):

        time.sleep(self.start_delay)

        while True :

            time.sleep(0.1)

            data = None
            address = None

            data, address = self.socket.recvfrom(4096)
            print("address", address, "data", data)

            timestamp_secs, current_timetuple, current_microsec_part, next_sample_secs = tr.timestamp_to_date_times(sample_rate = self.sample_rate)
            print("current_timetuple", current_timetuple)

            data_lines = []
            if data is not None :
                data_lines = data.decode("utf-8").splitlines()
                print("data_lines", data_lines)

            for selected_line in data_lines :

                print("selected_line", selected_line)
                for selected_tag, channels in self.channels.items() :
                    channels_list = list(channels)

                    if selected_tag in selected_line : 

                        print("selected_tag", selected_tag, "channels_list", channels_list)
                        data_array = None

                        if selected_tag == 'GGA' :
                            year = current_timetuple[0]
                            month = current_timetuple[1]
                            monthday = current_timetuple[2]
                            orig_secs, timestamp_microsecs, latitude, longitude = self.nmea.gga_to_time_pos(selected_line, year, month, monthday)
                            data_array = [ dict( [ (channels_list[0], selected_line) , (channels_list[1], [latitude]) , (channels_list[2], [longitude]) ] ) ]
                            
                        if selected_tag == 'VTG' :
                            data_array = [ dict( [ (channels_list[0], selected_line) ] ) ]

                        print("data_array", data_array)
                        if data_array is not None :
                            self.write(data_array = data_array, selected_tag = selected_tag, timestamp_secs = timestamp_secs, timestamp_microsecs = timestamp_microsecs) 



class Image(IngestFile):


    def __init__(self):

        self.env = self.get_env()
        if self.video_res is None: self.video_res = self.env['VIDEO_RES']
        if self.video_quality is None: self.video_quality = self.env['VIDEO_QUALITY']
        (self.channel,) = self.channels
        self.capture_filename = 'image_' + str(self.channel) + '.' + self.file_extension

        IngestFile.__init__(self)


    def run(self):

        time.sleep(self.start_delay)

        divisor = numpy.int64(1/numpy.float64(self.sample_rate))
        current_time = numpy.float64(time.time())
        current_secs = numpy.int64(current_time)

        while True :

            next_sample_secs = current_secs + numpy.int64( divisor - current_secs % divisor )
            current_time = numpy.float64(time.time())
            current_secs = numpy.int64(current_time)
            if next_sample_secs > current_secs :
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
            http = li.DirectUpload(channels = self.channels, start_delay = self.start_delay, host_api_url = self.host_api_url, client_api_url = self.client_api_url, max_connect_attempts = self.max_connect_attempts)
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

            next_sample_secs = current_secs + numpy.int64( divisor - current_secs % divisor )
            current_time = numpy.float64(time.time())
            current_secs = numpy.int64(current_time)
            if next_sample_secs > current_secs :
                time.sleep(0.1)
            else :
                self.read_samples(next_sample_secs)



class TempFileUpload(Image):


    def __init__(self, channels = None, sample_rate = None, host_api_url = None, client_api_url = None, file_extension = 'jpg', config_filepath = None, config_filename = None):

        self.channels = channels

        self.sample_rate = sample_rate
        self.host_api_url = host_api_url
        self.client_api_url = client_api_url

        self.start_delay = 0
        self.max_connect_attempts = 50
        self.file_extension = file_extension

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
            rt.logging.debug("self.capture_filename", self.capture_filename)
            with open(self.capture_filename, "rb") as image_file:
                img_str = base64.b64encode(image_file.read())
            http = li.DirectUpload(channels = self.channels, start_delay = self.start_delay, host_api_url = self.host_api_url, client_api_url = self.client_api_url, max_connect_attempts = self.max_connect_attempts)
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

            next_sample_secs = current_secs + numpy.int64( divisor - current_secs % divisor )
            current_time = numpy.float64(time.time())
            current_secs = numpy.int64(current_time)
            if next_sample_secs > current_secs :
                time.sleep(0.1)
            else :
                self.upload_file(next_sample_secs)



class AcquireCurrent(IngestFile):

    """ Adapted from https://scipy-cookbook.readthedocs.io/items/Data_Acquisition_with_NIDAQmx.html."""


    def __init__(self, channels = None, start_delay = 0, sample_rate = None, file_path = None, archive_file_path = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.sample_rate = sample_rate

        self.file_path = file_path
        self.archive_file_path = archive_file_path

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        IngestFile.__init__(self)

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

    #def downsample(self, y, size):
    #    y_reshape = y.reshape(size, int(len(y)/size))
    #    y_downsamp = y_reshape.mean(axis=1)
    #    return y_downsamp

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
                    current_avg = tr.downsample(current_array, 1)
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
