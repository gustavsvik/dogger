""" Adapted from https://scipy-cookbook.readthedocs.io/items/Data_Acquisition_with_NIDAQmx.html."""

import ctypes
import numpy
import time
import scipy.interpolate
import os

from metadata import Configure

nidaq = ctypes.windll.nicaiu # load the DLL

#FILE_PATH = "../../data/files/"
#FILE_PATH = "C:/Z/THISBUSINESS/Energilab/PROJECTS/logging/data/files/"

config = Configure()
env = config.get()
FILE_PATH = ''

if env['STORE_PATH'] is not None and os.path.exists(env['STORE_PATH']):
    FILE_PATH = env['STORE_PATH']


##############################
# Setup some typedefs and constants
# to correspond with values in
# C:\Program Files\National Instruments\NI-DAQ\DAQmx ANSI C Dev\include\NIDAQmx.h

# the typedefs
uInt8 = ctypes.c_ubyte
int32 = ctypes.c_long
uInt32 = ctypes.c_ulong
uInt64 = ctypes.c_ulonglong
float64 = ctypes.c_double
bool32 = ctypes.c_bool
TaskHandle = uInt32
pointsWritten = uInt32()
pointsRead = uInt32()
null=ctypes.POINTER(ctypes.c_int)()
value=uInt32()


# the constants
DAQmx_Val_Cfg_Default = int32(-1)
DAQmx_Val_Auto = int32(-1)
DAQmx_Val_Internal = int32(10200)
DAQmx_Val_Volts = int32(10348)
DAQmx_Val_Rising = int32(10280)
DAQmx_Val_Falling = int32(10171)
DAQmx_Val_CountUp = int32(10128)
DAQmx_Val_FiniteSamps = int32(10178)
DAQmx_Val_GroupByChannel = int32(0)
DAQmx_Val_ChanForAllLines = int32(1)
DAQmx_Val_RSE = int32(10083)
DAQmx_Val_Diff = int32(10106)
DAQmx_Val_Amps = int32(10342)
DAQmx_Val_ContSamps = int32(10123)
DAQmx_Val_GroupByScanNumber = int32(1)
DAQmx_Val_Task_Reserve = int32(4)
DAQmx_Val_ChanPerLine = int32(0)

##############################

SAMPLE_RATE = 1000
SAMPLES_PER_CHAN = 1000

# initialize variables

taskCurrent = TaskHandle(0)

minCurrent = float64(-0.02)
maxCurrent = float64(0.02)
bufferSize = uInt32(SAMPLES_PER_CHAN)
pointsToRead = bufferSize
pointsRead = uInt32()
sampleRate = float64(SAMPLE_RATE)
samplesPerChan = uInt64(SAMPLES_PER_CHAN)
clockSource = ctypes.create_string_buffer(b"OnboardClock")
IPnumber2 = ctypes.create_string_buffer(b"169.254.254.253")
defaultModuleName22 = ctypes.create_string_buffer(b"cDAQ9188-1AD0C2FMod2")
timeout = float64(100.0)

device2NameOut = ctypes.create_string_buffer(100)
device2NameOutBufferSize = uInt32(100)
device2ModuleNamesOut = ctypes.create_string_buffer(1000)
device2ModuleNamesOutBufferSize = uInt32(1000)
module21ChansOut = ctypes.create_string_buffer(2000)
module21ChansOutBufferSize = uInt32(2000)
module22ChansOut = ctypes.create_string_buffer(2000)
module22ChansOutBufferSize = uInt32(2000)

data = numpy.zeros((1000,),dtype=numpy.float64)

global_error_code = 0


def CHK(err):
    """a simple error checking routine"""
    global global_error_code
    global_error_code = err
    if err < 0:
        buf_size = 1000
        buf = ctypes.create_string_buffer(b"\000" * buf_size)
        nidaq.DAQmxGetErrorString(err,ctypes.byref(buf),buf_size)
        #raise RuntimeError('nidaq call failed with error %d: %s'%(err,repr(buf.value)))
        print('nidaq call failed with error %d: %s'%(err,repr(buf.value)))


def scale_cond_transmitter(I_cond):
    #return I_cond
    return ( I_cond * 1000.0 - 4.0 ) / 16.0 * 10e-6

def downsample(y, size):
    y_reshape = y.reshape(size, int(len(y)/size))
    y_downsamp = y_reshape.mean(axis=1)
    return y_downsamp

# Create Task and Voltage Channel and Configure Sample Clock
def SetupTasks():
    device2NameOut = b"cDAQ9188-1AD0C2F"

    CHK( nidaq.DAQmxGetDevChassisModuleDevNames(device2NameOut, device2ModuleNamesOut, device2ModuleNamesOutBufferSize) )
    print("device2ModuleNamesOut: ", repr(device2ModuleNamesOut.value))
    CHK( nidaq.DAQmxGetDevAIPhysicalChans(defaultModuleName22, module22ChansOut, 2000) )
    print("module22ChansOut: ", repr(module22ChansOut.value))
    CHK(nidaq.DAQmxCreateTask("",ctypes.byref(taskCurrent)))
    CHK(nidaq.DAQmxCreateAICurrentChan(taskCurrent,module22ChansOut,"",DAQmx_Val_RSE,minCurrent,maxCurrent,DAQmx_Val_Amps,DAQmx_Val_Internal,None,None))
    CHK(nidaq.DAQmxCfgSampClkTiming(taskCurrent, clockSource, sampleRate, DAQmx_Val_Rising, DAQmx_Val_ContSamps, samplesPerChan))
    #CHK(nidaq.DAQmxCfgInputBuffer(taskCurrent,200000))


def ReserveTasks():
    CHK(nidaq.DAQmxTaskControl(taskCurrent, DAQmx_Val_Task_Reserve))

def StartTasks():
    CHK(nidaq.DAQmxStartTask(taskCurrent))

def ReadCurrent():
    pointsToRead = bufferSize
    data = numpy.zeros((16*bufferSize.value,),dtype=numpy.float64)
    if global_error_code >= 0:
        CHK(nidaq.DAQmxReadAnalogF64(taskCurrent,pointsToRead,timeout,DAQmx_Val_GroupByChannel,data.ctypes.data,uInt32(16*bufferSize.value),ctypes.byref(pointsRead),None))
    return data

def StopAndClearTasks():
    if taskCurrent.value != 0:
        CHK( nidaq.DAQmxStopTask(taskCurrent) )
        CHK( nidaq.DAQmxClearTask(taskCurrent) )

def InitAcquire():
    SetupTasks()
    ReserveTasks()
    StartTasks()


def LoopAcquire():

    while (global_error_code >= 0):

        current = 0.0
        try:
            current = ReadCurrent()
        except OSError as e:
            print(e)

        acq_finish_time = numpy.float64(time.time())
        acq_finish_secs = numpy.int64(acq_finish_time)
        acq_finish_microsecs = numpy.int64(acq_finish_time * 1e6)
        acq_microsec_part = acq_finish_microsecs - numpy.int64(acq_finish_secs)*1e6
        if acq_microsec_part > 990000 :
            time.sleep(0.03)
        if acq_microsec_part < 10000 :
            time.sleep(0.87)

        #print("Acquired ", pointsRead.value, " starting at time ", acq_finish_microsecs)

        for channel_index in range(0, 16):
            current_array = scale_cond_transmitter(current[SAMPLES_PER_CHAN*(channel_index+0):SAMPLES_PER_CHAN*(channel_index+1)])
            cond_sec = downsample(current_array, 1)
            current_array = numpy.concatenate(([0.0], acq_microsec_part, current_array), axis=None)
            current_array[0] = cond_sec
            #print(cond_sec)
            try:
                filename_cond = repr(97+channel_index) + "_" + repr(acq_finish_secs)
                numpy.save(FILE_PATH+filename_cond, current_array)
            except PermissionError as e:
                print(e)



InitAcquire()
while True:
    if global_error_code >= 0:
        LoopAcquire()
        if global_error_code < 0:
            StopAndClearTasks()
            InitAcquire()
    else:
        StopAndClearTasks()
        InitAcquire()
    time.sleep(10)
