import ctypes
import numpy
import time
import scipy.interpolate
from dogger.metadata import Config

nidaq = ctypes.windll.nicaiu # load the DLL

#FILE_PATH = "../../data/files/"
#FILE_PATH = "C:/Z/THISBUSINESS/Energilab/PROJECTS/logging/data/files/"

config = Config()
FILE_PATH = config.getDataFilePath()


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
DAQmx_Val_Internal = int32(10200)
DAQmx_Val_Volts = int32(10348)
DAQmx_Val_Rising = int32(10280)
DAQmx_Val_Falling = int32(10171)
DAQmx_Val_CountUp = int32(10128)
DAQmx_Val_FiniteSamps = int32(10178)
DAQmx_Val_GroupByChannel = int32(0)
DAQmx_Val_ChanForAllLines = int32(1)
DAQmx_Val_RSE = int32(10083)
DAQmx_Val_ContSamps = int32(10123)
DAQmx_Val_GroupByScanNumber = int32(1)
DAQmx_Val_Task_Reserve = int32(4)
DAQmx_Val_ChanPerLine = int32(0)
DAQmx_Val_Pt3851 = int32(10071)
DAQmx_Val_DegC = int32(10143)
DAQmx_Val_4Wire = int32(4)
##############################

SAMPLE_RATE = 10
SAMPLES_PER_CHAN = 10


# initialize variables
taskRTD = TaskHandle(0)

minRTD = float64(-50.0)
maxRTD = float64(500.0)
RTDexcitationCurrent = float64(0.001)
bufferSize = uInt32(SAMPLES_PER_CHAN)
pointsToRead = bufferSize
pointsRead = uInt32()
sampleRate = float64(SAMPLE_RATE)
samplesPerChan = uInt64(SAMPLES_PER_CHAN)
RTDzeroResistance = float64(100.0)
#chan = ctypes.create_string_buffer(b"Dev1/ai1")
clockSource = ctypes.create_string_buffer(b"OnboardClock")
IPnumber1 = ctypes.create_string_buffer(b"169.254.254.254")
defaultModuleName13 = ctypes.create_string_buffer(b"cDAQ9188-1AD0C62Mod3")
timeout = float64(100.0)
autoStart = bool32(1)

device1NameOut = ctypes.create_string_buffer(100)
device1NameOutBufferSize = uInt32(100)
device1ModuleNamesOut = ctypes.create_string_buffer(1000)
device1ModuleNamesOutBufferSize = uInt32(1000)
module13ChansOut = ctypes.create_string_buffer(2000)
module13ChansOutBufferSize = uInt32(2000)

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


def scale_RTD(T):
    return T

def downsample(y, size):
    y_reshape = y.reshape(size, int(len(y)/size))
    y_downsamp = y_reshape.mean(axis=1)
    return y_downsamp

# Create Task and Voltage Channel and Configure Sample Clock
def SetupTasks():
    device1NameOut = b"cDAQ9188-1AD0C62"
    CHK( nidaq.DAQmxGetDevChassisModuleDevNames(device1NameOut, device1ModuleNamesOut, device1ModuleNamesOutBufferSize) )
    print("device1ModuleNamesOut: ", repr(device1ModuleNamesOut.value))
    CHK( nidaq.DAQmxGetDevAIPhysicalChans(defaultModuleName13, module13ChansOut, 2000) )
    print("module13ChansOut: ", repr(module13ChansOut.value))
    CHK(nidaq.DAQmxCreateTask("",ctypes.byref(taskRTD)))
    CHK(nidaq.DAQmxCreateAIRTDChan(taskRTD, b"cDAQ9188-1AD0C62Mod3/ai0", "", minRTD, maxRTD, DAQmx_Val_DegC, DAQmx_Val_Pt3851, DAQmx_Val_4Wire, DAQmx_Val_Internal, RTDexcitationCurrent, RTDzeroResistance))
    CHK(nidaq.DAQmxCfgSampClkTiming(taskRTD, clockSource, sampleRate, DAQmx_Val_Rising, DAQmx_Val_ContSamps, samplesPerChan))
    #CHK(nidaq.DAQmxCfgInputBuffer(taskRTD,200000))

    #DAQmxSetAIADCTimingMode(taskRTD, "", DAQmx_Val_HighSpeed)

def ReserveTasks():
    CHK(nidaq.DAQmxTaskControl(taskRTD, DAQmx_Val_Task_Reserve))
    
#Start Task
def StartTasks():
    CHK(nidaq.DAQmxStartTask(taskRTD))

#Read Samples

def ReadRTD():
    pointsToRead = bufferSize
    data = numpy.zeros((8*bufferSize.value,),dtype=numpy.float64)
    if global_error_code >= 0: 
        CHK(nidaq.DAQmxReadAnalogF64(taskRTD,pointsToRead,timeout,DAQmx_Val_GroupByChannel,data.ctypes.data,uInt32(8*bufferSize.value),ctypes.byref(pointsRead),None))
    return data


def StopAndClearTasks():
    if taskRTD.value != 0:
        CHK( nidaq.DAQmxStopTask(taskRTD) )
        CHK( nidaq.DAQmxClearTask(taskRTD) )


def InitAcquire():
    SetupTasks()
    ReserveTasks()
    StartTasks()


def LoopAcquire():
  
    while (global_error_code >= 0):

        rtd = 0.0
        try:
            rtd = ReadRTD()
        except OSError as e:
            print(e)

        acq_finish_time = numpy.float64(time.time())
        prev_acq_finish_time = acq_finish_time - numpy.float64(SAMPLES_PER_CHAN) / numpy.float64(SAMPLE_RATE)
        prev_acq_finish_secs = numpy.int64(prev_acq_finish_time)
        prev_acq_finish_microsecs = numpy.int64(prev_acq_finish_time * 1e6)
        acq_finish_microsecs = numpy.int64(acq_finish_time * 1e6)
        print("Acquired ", pointsRead.value, " at time ", prev_acq_finish_microsecs)

        rtd_sec = downsample(scale_RTD(rtd[SAMPLES_PER_CHAN*(0+0):SAMPLES_PER_CHAN*(0+1)]), 1)
        print(rtd_sec)
        try:
            filename_rtd = repr(40) + "_" + repr(prev_acq_finish_secs)
            numpy.save(FILE_PATH+filename_rtd, rtd_sec)
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

