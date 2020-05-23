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

config = md.Configure(filepath = 'Z:\\app\\python\\dogger\\', filename = 'conf.ini')
env = config.get()

FILE_PATH = ''

if env['STORE_PATH'] is not None and os.path.exists(env['STORE_PATH']):
    FILE_PATH = env['STORE_PATH']

FILE_PATH = 'Z:/data/files/'


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
DAQmx_Val_ContSamps = int32(10123)
DAQmx_Val_GroupByScanNumber = int32(1)
DAQmx_Val_Task_Reserve = int32(4)
DAQmx_Val_ChanPerLine = int32(0)
##############################

SAMPLE_RATE = 1000
SAMPLES_PER_CHAN = 1000

temp_tot = numpy.array([-1.0, 1.0])
rad_tot = numpy.array([5.0, 10.0])

# initialize variables
taskHandle = TaskHandle(0)
#taskCount = TaskHandle(0)
#taskDO = TaskHandle(0)

minVoltage = float64(-10.0)
maxVoltage = float64(10.0)
bufferSize = uInt32(SAMPLES_PER_CHAN)
pointsToRead = bufferSize
pointsRead = uInt32()
sampleRate = float64(SAMPLE_RATE)
samplesPerChan = uInt64(SAMPLES_PER_CHAN)
#chan = ctypes.create_string_buffer(b"Dev1/ai1")
clockSource = ctypes.create_string_buffer(b"OnboardClock")
IPnumber1 = ctypes.create_string_buffer(b"169.254.254.254")
IPnumber2 = ctypes.create_string_buffer(b"169.254.254.253")
defaultModuleName11 = ctypes.create_string_buffer(b"cDAQ9188-1AD0C62Mod1")
#defaultModuleName12 = ctypes.create_string_buffer(b"cDAQ9188-1AD0C62Mod2")
#defaultModuleName13 = ctypes.create_string_buffer(b"cDAQ9188-1AD0C62Mod3")
defaultModuleName21 = ctypes.create_string_buffer(b"cDAQ9188-1AD0C2FMod1")
attemptReservation = bool32(1)
overrideReservation = bool32(1)
timeout = float64(100.0)
autoStart = bool32(1)
#switchOn = uInt8(1)
#switchOff = uInt8(0)
#singleSampPerChan = uInt32(1)

device1NameOut = ctypes.create_string_buffer(100)
device1NameOutBufferSize = uInt32(100)
device1ModuleNamesOut = ctypes.create_string_buffer(1000)
device1ModuleNamesOutBufferSize = uInt32(1000)
module11ChansOut = ctypes.create_string_buffer(2000)
module11ChansOutBufferSize = uInt32(2000)
#module12ChansOut = ctypes.create_string_buffer(2000)
#module12ChansOutBufferSize = uInt32(2000)
#module12DOLinesOut = ctypes.create_string_buffer(2000)
#module12DOLinesOutBufferSize = uInt32(2000)
#module12DOPortsOut = ctypes.create_string_buffer(2000)
#module12DOPortsOutBufferSize = uInt32(2000)


device2NameOut = ctypes.create_string_buffer(100)
device2NameOutBufferSize = uInt32(100)
device2ModuleNamesOut = ctypes.create_string_buffer(1000)
device2ModuleNamesOutBufferSize = uInt32(1000)
module21ChansOut = ctypes.create_string_buffer(2000)
module21ChansOutBufferSize = uInt32(2000)

#InitialCount=uInt32(0)
#min_pulse_width = float64(0.000006425)
#min_pulse_width_enable = bool32(1);

data = numpy.zeros((1000,),dtype=numpy.float64)

global_error_code = 0


def CHK(err):
    """a simple error checking routine"""
    global global_error_code
    global_error_code = err
    #print('global_error_code', global_error_code)
    if err < 0:
        buf_size = 1000
        buf = ctypes.create_string_buffer(b"\000" * buf_size)
        nidaq.DAQmxGetErrorString(err,ctypes.byref(buf),buf_size)
        #raise RuntimeError('nidaq call failed with error %d: %s'%(err,repr(buf.value)))
        print('nidaq call failed with error %d: %s'%(err,repr(buf.value)))


def scale_10K3A2B(U_therm, U_exc):
    R_thermistor = 1e3 * numpy.array([336.098,314.553,294.524,275.897,258.563,242.427,227.398,213.394,200.339,188.163,176.803,166.198,156.294,147.042,138.393,130.306,122.741,115.661,109.032,102.824,97.006,91.553,86.439,81.641,77.138,72.911,68.940,65.209,61.703,58.405,55.304,52.385,49.638,47.050,44.613,42.317,40.151,38.110,36.184,34.366,32.651,31.031,29.500,28.054,26.687,25.395,24.172,23.016,21.921,20.885,19.903,18.973,18.092,17.257,16.465,15.714,15.001,14.324,13.682,13.073,12.493,11.943,11.420,10.923,10.450,10.000,9.572,9.165,8.777,8.408,8.056,7.721,7.402,7.097,6.807,6.530,6.266,6.014,5.774,5.544,5.325,5.116,4.916,4.724,4.542,4.367,4.200,4.040,3.887,3.741,3.601])
    t_thermistor = numpy.arange(-40.0, 51.0)
    f_R_thermistor = scipy.interpolate.interp1d(R_thermistor, t_thermistor, kind='cubic', bounds_error=False, fill_value=-9999.0)
    R_ext = 9.93e3
    R = R_ext * 1 / (U_exc/U_therm - 1)
    return f_R_thermistor(R)

#def scale_Opto_22_AD3_SATT_ETT45_0101(U_par):
#    return ( U_par/249.0 * 1000.0 - 4.0 ) / 16.0 * 100.0

def scale_D_PYRPA_CA(U_pyran):
    return U_pyran / 2.5 * 1000.0

def scale_3_phase_analyzer(U_pow):
    return ( U_pow / 220 * 1000.0 - 4.0 ) / 16.0 * 1e-6

def downsample(y, size):
    y_reshape = y.reshape(size, int(len(y)/size))
    y_downsamp = y_reshape.mean(axis=1)
    return y_downsamp

# Create Task and Voltage Channel and Configure Sample Clock
def SetupTasks():
    print('SetupTasks()')
    device1NameOut = ctypes.create_string_buffer(100)
    CHK( nidaq.DAQmxAddNetworkDevice(IPnumber1, "", attemptReservation, timeout, device1NameOut, device1NameOutBufferSize) )
    print("device1NameOut: ", repr(device1NameOut.value))
    CHK( nidaq.DAQmxReserveNetworkDevice(device1NameOut, overrideReservation) )

    device1NameOut = b"cDAQ9188-1AD0C2F"
    CHK( nidaq.DAQmxGetDevChassisModuleDevNames(device1NameOut, device1ModuleNamesOut, device1ModuleNamesOutBufferSize) )
    print("device1ModuleNamesOut: ", repr(device1ModuleNamesOut.value))
    CHK( nidaq.DAQmxGetDevAIPhysicalChans(defaultModuleName11, module11ChansOut, 2000) )
    print("module11ChansOut: ", repr(module11ChansOut.value))

    CHK(nidaq.DAQmxCreateTask("",ctypes.byref(taskHandle)))
    CHK(nidaq.DAQmxCreateAIVoltageChan(taskHandle,module11ChansOut,"",DAQmx_Val_RSE,minVoltage,maxVoltage,DAQmx_Val_Volts,None))
    CHK(nidaq.DAQmxCfgSampClkTiming(taskHandle, clockSource, sampleRate, DAQmx_Val_Rising, DAQmx_Val_ContSamps, samplesPerChan))

    #if global_error_code < 0:
    #    if taskHandle.value != 0:
    #        StopAndClearTasks1()
    #        #print('global_error_code after StopAndClearTasks1 in SetupTasks', global_error_code)
    #    if global_error_code < 0:
    #        DisconnectNetworkDevice(device1NameOut)

    CHK( nidaq.DAQmxAddNetworkDevice(IPnumber2, "", attemptReservation, timeout, device2NameOut, device2NameOutBufferSize) )
    print("device2NameOut: ", repr(device2NameOut.value))
    CHK( nidaq.DAQmxReserveNetworkDevice(device2NameOut, overrideReservation) )

    #device2NameOut = b"cDAQ9188-1AD0C2F"
    #CHK( nidaq.DAQmxGetDevChassisModuleDevNames(device2NameOut, device2ModuleNamesOut, device2ModuleNamesOutBufferSize) )
    #print("device2ModuleNamesOut: ", repr(device2ModuleNamesOut.value))
    #CHK( nidaq.DAQmxGetDevAIPhysicalChans(defaultModuleName21, module21ChansOut, 2000) )
    #print("module21ChansOut: ", repr(module21ChansOut.value))

    #if global_error_code < 0:
    #    if global_error_code < 0:
    #        DisconnectNetworkDevice(device2NameOut)


def ReserveTasks():
    print('ReserveTasks()')
    CHK(nidaq.DAQmxTaskControl(taskHandle, DAQmx_Val_Task_Reserve))
    #CHK(nidaq.DAQmxTaskControl(taskCount, DAQmx_Val_Task_Reserve))
    #CHK(nidaq.DAQmxTaskControl(taskDO, DAQmx_Val_Task_Reserve))
    
#Start Task
def StartTasks():
    print('StartTasks()')
    CHK(nidaq.DAQmxStartTask(taskHandle))
    #CHK(nidaq.DAQmxStartTask(taskCount))
    #CHK(nidaq.DAQmxStartTask(taskDO))

def ReadSamples():
    #print('ReadSamples()')
    pointsToRead = bufferSize
    #pointsToRead = DAQmx_Val_Auto
    data = numpy.zeros((32*bufferSize.value,),dtype=numpy.float64)
    if global_error_code >= 0: 
        CHK(nidaq.DAQmxReadAnalogF64(taskHandle,pointsToRead,timeout,DAQmx_Val_GroupByChannel,data.ctypes.data,uInt32(32*bufferSize.value),ctypes.byref(pointsRead),None))
    return data

#def ReadCount():
#    CHK(nidaq.DAQmxReadCounterScalarU32(taskCount,timeout,ctypes.POINTER(ctypes.c_uint32)(value),null))
#    count = value.value
#    return count

#def SwitchOn():
#    CHK(nidaq.DAQmxWriteDigitalLines(taskDO, singleSampPerChan, autoStart, timeout, DAQmx_Val_GroupByChannel, ctypes.byref(switchOn), ctypes.byref(pointsWritten), null))
#    return pointsWritten

#def SwitchOff():
#    CHK(nidaq.DAQmxWriteDigitalLines(taskDO, singleSampPerChan, autoStart, timeout, DAQmx_Val_GroupByChannel, ctypes.byref(switchOff), ctypes.byref(pointsWritten), null))
#    return pointsWritten


def StopAndClearTasks1():
    print('StopAndClearTasks1()')
    if taskHandle.value != 0:
        CHK( nidaq.DAQmxStopTask(taskHandle) )
        CHK( nidaq.DAQmxClearTask(taskHandle) )
    #if taskCount.value != 0:
    #    nidaq.DAQmxStopTask(taskCount)
    #    nidaq.DAQmxClearTask(taskCount)
    #if taskDO.value != 0:
    #    nidaq.DAQmxStopTask(taskDO)
    #    nidaq.DAQmxClearTask(taskDO)

def DisconnectNetworkDevice(deviceNameOut):
    print('DisconnectNetworkDevice()')
    CHK( nidaq.DAQmxDeleteNetworkDevice(deviceNameOut) )


def InitAcquire():
    print('InitAcquire()')
    SetupTasks()
    ReserveTasks()
    StartTasks()


def LoopAcquire():
    print('LoopAcquire()')

    #count = 0
    #prev_counts = 0
    #wind = 0

    #toggle = True
    #switchedOn = 0
    #witchedOff = 0
        
    while (global_error_code >= 0):
        
        #if toggle == True:
        #    if switchedOn - switchedOff > 5:
        #        toggle = False
        #if toggle == False:
        #    if switchedOn - switchedOff < 0:
        #        toggle = True
        #if toggle:
        #    SwitchOn()
        #    switchedOn += 1
        #    print("switchedOn",switchedOn)
        #else:
        #    SwitchOff()
        #    switchedOff += 1
        #    print("switchedOff",switchedOff)

        readData = None
        try:
            readData = ReadSamples()
        except OSError as e:
            print(e)

        #counts = ReadCount()
        #diff_counts = counts - prev_counts
        #prev_counts = counts
        
        acq_finish_time = numpy.float64(time.time())
        #prev_acq_finish_time = acq_finish_time - numpy.float64(SAMPLES_PER_CHAN) / numpy.float64(SAMPLE_RATE)
        acq_finish_secs = numpy.int64(acq_finish_time)
        #prev_acq_finish_microsecs = numpy.int64(prev_acq_finish_time * 1e6)
        acq_finish_microsecs = numpy.int64(acq_finish_time * 1e6)
        #print("Acquired ", pointsRead.value, " at time ", prev_acq_finish_microsecs)
        acq_microsec_part = acq_finish_microsecs - numpy.int64(acq_finish_secs)*1e6
        if acq_microsec_part > 990000 :
            time.sleep(0.03)
        if acq_microsec_part < 10000 :
            time.sleep(0.87)

        #temp_sec = downsample(scale_10K3A2B(readData[SAMPLES_PER_CHAN*(23+0):SAMPLES_PER_CHAN*(23+1)], readData[SAMPLES_PER_CHAN*(22+0):SAMPLES_PER_CHAN*(22+1)]), 1)
        temp_array = scale_10K3A2B(readData[SAMPLES_PER_CHAN*(23+0):SAMPLES_PER_CHAN*(23+1)], readData[SAMPLES_PER_CHAN*(22+0):SAMPLES_PER_CHAN*(22+1)])
        temp_sec = downsample(temp_array, 1)
        temp_array = numpy.concatenate(([0.0], acq_microsec_part, temp_array), axis=None)
        temp_array[0] = temp_sec
        #print(temp_sec)
        #rad_sec = downsample(scale_D_PYRPA_CA(readData[SAMPLES_PER_CHAN*(21+0):SAMPLES_PER_CHAN*(21+1)]), 1)
        rad_array = scale_D_PYRPA_CA(readData[SAMPLES_PER_CHAN*(21+0):SAMPLES_PER_CHAN*(21+1)])
        rad_sec = downsample(rad_array, 1)
        rad_array = numpy.concatenate(([0.0], acq_microsec_part, rad_array), axis=None)
        rad_array[0] = rad_sec
        #print(rad_sec)
        #turb_cool_temp_sec = downsample(scale_Opto_22_AD3_SATT_ETT45_0101(readData[SAMPLES_PER_CHAN*(20+0):SAMPLES_PER_CHAN*(20+1)]), 1)
        #print(turb_cool_temp_sec)
        #elpower_load_sec = downsample(scale_3_phase_analyzer(readData[SAMPLES_PER_CHAN*(24+0):SAMPLES_PER_CHAN*(24+1)]), 1)
        elpower_load_array = scale_3_phase_analyzer(readData[SAMPLES_PER_CHAN*(24+0):SAMPLES_PER_CHAN*(24+1)])
        elpower_load_sec = downsample(elpower_load_array, 1)
        elpower_load_array = numpy.concatenate(([0.0], acq_microsec_part, elpower_load_array), axis=None)
        elpower_load_array[0] = elpower_load_sec
        #print(elpower_load_sec)
        #current_wind = diff_counts/1.2517+0.28
        #if current_wind < 0.402:
        #    current_wind = 0
        #wind *= 0.7
        #wind += 0.3*current_wind
        #print(wind)

        try:
            filename_temp = repr(23) + "_" + repr(acq_finish_secs)
            numpy.save(FILE_PATH+filename_temp, temp_array)
            filename_rad = repr(21) + "_" + repr(acq_finish_secs)
            numpy.save(FILE_PATH+filename_rad, rad_array)
#            filename_turb_cool = repr(20) + "_" + repr(acq_finish_secs)
#            numpy.save(FILE_PATH+filename_turb_cool, turb_cool_temp_sec)
            filename_elpower = repr(24) + "_" + repr(acq_finish_secs)
            numpy.save(FILE_PATH+filename_elpower, elpower_load_array)
        except PermissionError as e:
            print(e)

        #count = count+1



InitAcquire()
while True:
    if global_error_code >= 0:
        LoopAcquire()
        if global_error_code < 0:
            StopAndClearTasks1()
            InitAcquire()
    else:
        StopAndClearTasks1()
        InitAcquire()
    time.sleep(10)
