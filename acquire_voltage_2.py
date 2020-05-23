""" Adapted from https://scipy-cookbook.readthedocs.io/items/Data_Acquisition_with_NIDAQmx.html."""

import ctypes
import numpy
import time
import scipy.interpolate
import os

import gateway.metadata as md


nidaq = ctypes.windll.nicaiu # load the DLL

#FILE_PATH = "../../data/files/"
#FILE_PATH = "C:/Z/THISBUSINESS/Energilab/PROJECTS/logging/data/files/"

config = md.Configure(filepath = 'Z:\\app\\python\\dogger\\', filename = 'conf_voltage_2.ini')
env = config.get()
         
FILE_PATH = ''

if env['STORE_PATH'] is not None and os.path.exists(env['STORE_PATH']):
    FILE_PATH = env['STORE_PATH']

FILE_PATH = 'Z:/data/files/voltage/'


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

SAMPLE_RATE = 10
SAMPLES_PER_CHAN = 10

# initialize variables

taskVoltage = TaskHandle(0)

minVoltage = float64(-10.0)
maxVoltage = float64(10.0)
bufferSize = uInt32(SAMPLES_PER_CHAN)
pointsToRead = bufferSize
pointsRead = uInt32()
sampleRate = float64(SAMPLE_RATE)
samplesPerChan = uInt64(SAMPLES_PER_CHAN)
clockSource = ctypes.create_string_buffer(b"OnboardClock")
IPnumber2 = ctypes.create_string_buffer(b"169.254.254.253")
defaultModuleName21 = ctypes.create_string_buffer(b"cDAQ9188-1AD0C2FMod1")
timeout = float64(100.0)

device2NameOut = ctypes.create_string_buffer(100)
device2NameOutBufferSize = uInt32(100)
device2ModuleNamesOut = ctypes.create_string_buffer(1000)
device2ModuleNamesOutBufferSize = uInt32(1000)
module21ChansOut = ctypes.create_string_buffer(2000)
module21ChansOutBufferSize = uInt32(2000)
module21ChansIn = ctypes.create_string_buffer(200)
module21ChansInBufferSize = uInt32(200)
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

def scale_valmet_current_input(U):
    return ( U/250.0 * 1000.0 - 4.0 ) / 16.0 * (100-(-15)) + (-15)

def scale_Opto_22_AD3_SATT_ETT45_0101(U_par):
    return ( U_par/249.0 * 1000.0 - 4.0 ) / 16.0 * 50.0
    
def downsample(y, size):
    y_reshape = y.reshape(size, int(len(y)/size))
    y_downsamp = y_reshape.mean(axis=1)
    return y_downsamp

# Create Task and Voltage Channel and Configure Sample Clock
def SetupTasks():
    device2NameOut = b"cDAQ9188-1AD0C2F"

    CHK( nidaq.DAQmxGetDevChassisModuleDevNames(device2NameOut, device2ModuleNamesOut, device2ModuleNamesOutBufferSize) )
    print("device2ModuleNamesOut: ", repr(device2ModuleNamesOut.value))
    CHK( nidaq.DAQmxGetDevAIPhysicalChans(defaultModuleName21, module21ChansOut, 2000) )
    print("module21ChansOut: ", repr(module21ChansOut.value))
    module21ChansIn = b'cDAQ9188-1AD0C2FMod1/ai0'
    CHK(nidaq.DAQmxCreateTask("",ctypes.byref(taskVoltage)))
    #CHK(nidaq.DAQmxCreateAIVoltageChan(taskVoltage,module21ChansOut,"",DAQmx_Val_Diff,minVoltage,maxVoltage,DAQmx_Val_Volts,None))
    CHK(nidaq.DAQmxCreateAIVoltageChan(taskVoltage,module21ChansOut,"",DAQmx_Val_RSE,minVoltage,maxVoltage,DAQmx_Val_Volts,None))
    CHK(nidaq.DAQmxCfgSampClkTiming(taskVoltage, clockSource, sampleRate, DAQmx_Val_Rising, DAQmx_Val_ContSamps, samplesPerChan))


def ReserveTasks():
    CHK(nidaq.DAQmxTaskControl(taskVoltage, DAQmx_Val_Task_Reserve))

def StartTasks():
    CHK(nidaq.DAQmxStartTask(taskVoltage))

def ReadVoltage():
    pointsToRead = bufferSize
    #pointsToRead = DAQmx_Val_Auto
    data = numpy.zeros((32*bufferSize.value,),dtype=numpy.float64)
    if global_error_code >= 0: 
        CHK(nidaq.DAQmxReadAnalogF64(taskVoltage,pointsToRead,timeout,DAQmx_Val_GroupByChannel,data.ctypes.data,uInt32(32*bufferSize.value),ctypes.byref(pointsRead),None))
    return data

def StopAndClearTasks():
    if taskVoltage.value != 0:
        CHK( nidaq.DAQmxStopTask(taskVoltage) )
        CHK( nidaq.DAQmxClearTask(taskVoltage) )

def InitAcquire():
    SetupTasks()
    ReserveTasks()
    StartTasks()


def LoopAcquire():

    while (global_error_code >= 0):

        voltage = 0.0
        try:
            voltage = ReadVoltage()
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

        #for channel_index in range(0, 1):
        #    valmet_sec = downsample(scale_valmet_current_input(voltage[SAMPLES_PER_CHAN*(channel_index+0):SAMPLES_PER_CHAN*(channel_index+1)]), 1)
        #    print(valmet_sec)
        #    try:
        #        filename_valmet = repr(65+channel_index) + "_" + repr(prev_acq_finish_secs)
        #        numpy.save(FILE_PATH+filename_valmet, valmet_sec)
        #    except PermissionError as e:
        #        print(e)
        
        for channel_index in range(16, 32):

            voltage_array = voltage[SAMPLES_PER_CHAN*(channel_index+0):SAMPLES_PER_CHAN*(channel_index+1)]
            if channel_index == 20 : voltage_array = scale_Opto_22_AD3_SATT_ETT45_0101(voltage_array)
            if channel_index == 21 : voltage_array = scale_Opto_22_AD3_SATT_ETT45_0101(voltage_array)
            if channel_index == 22 : voltage_array = scale_Opto_22_AD3_SATT_ETT45_0101(voltage_array)
            if channel_index == 23 : voltage_array = scale_Opto_22_AD3_SATT_ETT45_0101(voltage_array)
            voltage_avg = downsample(voltage_array, 1)
            voltage_array = numpy.concatenate(([0.0], acq_microsec_part, voltage_array), axis=None)
            voltage_array[0] = voltage_avg
            try:
                filename_voltage = repr(1+channel_index) + "_" + repr(acq_finish_secs)
                numpy.save(FILE_PATH+filename_voltage, voltage_array)
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
