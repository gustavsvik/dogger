#

import time
import numpy
import shutil
import os
import sys
import io
import cv2
import ctypes
import base64
import pyscreenshot as ImageGrab

import gateway.device as dv
import gateway.runtime as rt
import gateway.metadata as md
import gateway.task as t
import gateway.uplink as ul


 
class File(t.AcquireControl):


    def __init__(self):

        self.env = self.get_env()
        if self.file_path is None: self.file_path = self.env['FILE_PATH']
        if self.archive_file_path is None: self.archive_file_path = self.env['ARCHIVE_FILE_PATH']

        t.AcquireControl.__init__(self)

        
        
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


        
class Image(File):
    

    def __init__(self):

        self.env = self.get_env()
        if self.video_res is None: self.video_res = self.env['VIDEO_RES']
        if self.video_quality is None: self.video_res = self.env['VIDEO_QUALITY']

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
            
                self.read_samples(sample_secs)

                if self.file_path is not None and os.path.exists(self.file_path):

                    store_filename = self.file_path + str(self.channel) + '_' + str(sample_secs) + '.' + self.file_extension
                    archive_filename = self.archive_file_path + str(self.channel) + '_' + str(sample_secs) + '.' + self.file_extension
                    try:
                        shutil.copy(self.capture_filename, store_filename)
                        if self.archive_file_path is not None and os.path.exists(self.archive_file_path):
                            pass
                            #shutil.copy(capture_filename, archive_filename)
                    except (FileNotFoundError, PermissionError) as e:
                        rt.logging.exception(e)


                    
class USBCam(Image):


    def __init__(self, channels = None, start_delay = 0, sample_rate = None, file_path = None, archive_file_path = None, file_extension = 'jpg', video_unit = None, video_res = None, video_rate = None, video_quality = None, config_filepath = None, config_filename = None):

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

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        self.env = self.get_env()
        if self.video_unit is None: self.video_unit = self.env['VIDEO_UNIT']
        if self.video_rate is None: self.video_rate = self.env['VIDEO_RATE']
        
        Image.__init__(self)

        self.cam = cv2.VideoCapture( int(''.join(filter(str.isdigit, self.video_unit))) )
        self.cam.set(cv2.CAP_PROP_FRAME_WIDTH , self.video_res[0])
        self.cam.set(cv2.CAP_PROP_FRAME_HEIGHT, self.video_res[1])
        self.cam.set(cv2.CAP_PROP_FPS , self.video_rate)

    def read_samples(self, sample_secs = -9999):

        try :
            ret, frame = self.cam.read()
            cv2.imwrite( self.capture_filename, frame, [cv2.IMWRITE_JPEG_QUALITY, self.video_quality] )
        except PermissionError as e :
            print(e)



class ScreenshotUpload(Image):


    def __init__(self, channels = None, sample_rate = None, crop = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = 0
        self.sample_rate = sample_rate

        self.file_path = None
        self.archive_file_path = None
        self.file_extension = 'jpg'
        self.video_res = None
        self.ip_list = None
        self.crop = crop
        
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
            http = ul.DirectUpload(channels = self.channels, start_delay = self.start_delay)
            (channel,) = self.channels
            res = http.send_request(start_time = sample_secs, end_time = sample_secs, duration = 10, unit = 1, delete_horizon = 3600, ip = self.ip_list[0])
            data_string = str(channel) + ';' + str(sample_secs) + ',-9999.0,,' + str(img_str.decode('utf-8')) + ',;'
            print('data_string', data_string[0:100])
            res = http.set_requested(data_string, ip = self.ip_list[0])
        except PermissionError as e :
            print(e)


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
            print('nidaq call failed with error %d: %s'%(err,repr(buf.value)))


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
        print("device2ModuleNamesOut: ", repr(self.device2ModuleNamesOut.value))
        self.CHK( self.nidaq.DAQmxGetDevAIPhysicalChans(self.defaultModuleName22, self.module22ChansOut, 2000) )
        print("module22ChansOut: ", repr(self.module22ChansOut.value))
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
                print(e)

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
                        print(e)


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
