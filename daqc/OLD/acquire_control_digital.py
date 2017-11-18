import ctypes
import numpy
import time
import scipy.interpolate
import sys
import os
import math
from dogger.metadata import Configure

nidaq = ctypes.windll.nicaiu # load the DLL

#FILE_PATH = "../../data/files/"
#FILE_PATH = "C:/Z/THISBUSINESS/Energilab/PROJECTS/logging/data/files/"

config = Configure()
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
##############################

#SAMPLE_RATE = 100
#SAMPLES_PER_CHAN = 100

# initialize variables
taskCount = TaskHandle(0)
taskDO = TaskHandle(0)

#sampleRate = float64(SAMPLE_RATE)
#samplesPerChan = uInt64(SAMPLES_PER_CHAN)
clockSource = ctypes.create_string_buffer(b"OnboardClock")
IPnumber1 = ctypes.create_string_buffer(b"169.254.254.254")
defaultModuleName12 = ctypes.create_string_buffer(b"cDAQ9188-1AD0C62Mod2")

timeout = float64(100.0)
autoStart = bool32(1)
switchOn = uInt8(1)
switchOff = uInt8(0)
singleSampPerChan = uInt32(1)

device1NameOut = ctypes.create_string_buffer(100)
device1NameOutBufferSize = uInt32(100)
device1ModuleNamesOut = ctypes.create_string_buffer(1000)
device1ModuleNamesOutBufferSize = uInt32(1000)
module12ChansOut = ctypes.create_string_buffer(2000)
module12ChansOutBufferSize = uInt32(2000)
module12DOLinesOut = ctypes.create_string_buffer(2000)
module12DOLinesOutBufferSize = uInt32(2000)
module12DOPortsOut = ctypes.create_string_buffer(2000)
module12DOPortsOutBufferSize = uInt32(2000)

InitialCount=uInt32(0)
min_pulse_width = float64(0.000006425)
min_pulse_width_enable = bool32(1);

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


def SetupTasks():
    device1NameOut = ctypes.create_string_buffer(b"cDAQ9188-1AD0C62")
    CHK( nidaq.DAQmxGetDevChassisModuleDevNames(device1NameOut, device1ModuleNamesOut, device1ModuleNamesOutBufferSize) )
    print("device1ModuleNamesOut: ", repr(device1ModuleNamesOut.value))
    CHK( nidaq.DAQmxGetDevDOLines(defaultModuleName12, module12DOLinesOut, 2000) )
    print("module12DOLinesOut: ", repr(module12DOLinesOut.value))
    CHK( nidaq.DAQmxGetDevDOPorts(defaultModuleName12, module12DOPortsOut, 2000) )
    print("module12DOPortsOut: ", repr(module12DOPortsOut.value))
    CHK( nidaq.DAQmxGetDevCIPhysicalChans(defaultModuleName12, module12ChansOut, 2000) )
    print("module12ChansOut: ", repr(module12ChansOut.value))
    CHK(nidaq.DAQmxCreateTask("",ctypes.byref(taskCount)))
    CHK(nidaq.DAQmxCreateCICountEdgesChan(taskCount,b"cDAQ9188-1AD0C62Mod2/ctr0","",DAQmx_Val_Rising,InitialCount,DAQmx_Val_CountUp))
    CHK(nidaq.DAQmxCreateTask("",ctypes.byref(taskDO)))
    CHK(nidaq.DAQmxCreateDOChan(taskDO,b"cDAQ9188-1AD0C62Mod2/port0/line7","",DAQmx_Val_ChanPerLine))

def ReserveTasks():
    CHK(nidaq.DAQmxTaskControl(taskCount, DAQmx_Val_Task_Reserve))
    CHK(nidaq.DAQmxTaskControl(taskDO, DAQmx_Val_Task_Reserve))
    
def StartTasks():
    CHK(nidaq.DAQmxStartTask(taskCount))
    CHK(nidaq.DAQmxStartTask(taskDO))

def ReadCount():
    if global_error_code >= 0:
        CHK(nidaq.DAQmxReadCounterScalarU32(taskCount,timeout,ctypes.POINTER(ctypes.c_uint32)(value),null))
    count = value.value
    return count

def SwitchOn():
    if global_error_code >= 0:
        CHK(nidaq.DAQmxWriteDigitalLines(taskDO, singleSampPerChan, autoStart, timeout, DAQmx_Val_GroupByChannel, ctypes.byref(switchOn), ctypes.byref(pointsWritten), null))
    return pointsWritten

def SwitchOff():
    if global_error_code >= 0: 
        CHK(nidaq.DAQmxWriteDigitalLines(taskDO, singleSampPerChan, autoStart, timeout, DAQmx_Val_GroupByChannel, ctypes.byref(switchOff), ctypes.byref(pointsWritten), null))
    return pointsWritten


def StopAndClearTasks():
    if taskCount.value != 0:
        CHK( nidaq.DAQmxStopTask(taskCount) )
        CHK( nidaq.DAQmxClearTask(taskCount) )
    if taskDO.value != 0:
        CHK( nidaq.DAQmxStopTask(taskDO) )
        CHK( nidaq.DAQmxClearTask(taskDO) )

def InitAcquire():
    SetupTasks()
    ReserveTasks()
    StartTasks()


def LoopAcquire():

    count = 0
    prev_counts = 0
    wind = 0

    while (global_error_code >= 0):

        counts = 0
        try:
            counts = ReadCount()
        except OSError as e:
            print(e)

        diff_counts = counts - prev_counts
        prev_counts = counts
        
        acq_finish_time = numpy.float64(time.time())
        acq_finish_microsecs = numpy.int64(acq_finish_time * 1e6)
        print('acq_finish_time', acq_finish_time)
        print('acq_finish_microsecs', acq_finish_microsecs)

        current_wind = diff_counts/1.2517+0.28
        if current_wind < 0.402:
            current_wind = 0
        wind *= 0.7
        wind += 0.3*current_wind
        print('wind', wind)
            
        count = count+1    

        for channel_index in {39}:
            
            files = []
            
            with os.scandir(FILE_PATH) as it:
                for entry in it:
                    if entry.name.startswith(repr(channel_index) + '_') and entry.is_file():
                        files.append(entry.name)
            print("len(files)", len(files))
            if len(files) > 0 :
                        
                for current_file in files:
                    
                    position = current_file.find("_")
                    acquired_time_string = current_file[position+1:position+1+10]
                    acquired_time = int(acquired_time_string)

                    if acquired_time < int(time.time()) :

                        acquired_value = -9999.0
                        try:
                            acquired_values = numpy.load(FILE_PATH + current_file)
                            print("acquired_values",acquired_values)
                            acquired_value = acquired_values
                        except OSError as e:
                            print(e)
                        print("len(files)",len(files))
                        if current_file == files[len(files)-1] and not math.isnan(acquired_value):
                            if acquired_value>0.9: SwitchOn() 
                            if acquired_value<0.1: SwitchOff()

                        try:
                            os.remove(FILE_PATH + current_file)
                        except (PermissionError, FileNotFoundError) as e:
                            print(e)


        time.sleep(1.0)



InitAcquire()
while True:
    if global_error_code >= 0:
        LoopAcquire()
        if global_error_code < 0:
            StopAndClearTasks()
            InitAcquire()
    else:
        StopAndClearTasks
        InitAcquire()
    time.sleep(10)

