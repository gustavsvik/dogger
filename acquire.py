#

import ctypes
import numpy
import time
import scipy.interpolate
from dogger.metadata import Configure



class Nidaq:

    uInt8 = ctypes.c_ubyte
    int32 = ctypes.c_long
    uInt32 = ctypes.c_ulong
    uInt64 = ctypes.c_ulonglong
    float64 = ctypes.c_double
    bool32 = ctypes.c_bool
    TaskHandle = uInt32
    pointsWritten = uInt32()
    pointsRead = uInt32()
    null = ctypes.POINTER(ctypes.c_int)()
    value = uInt32()

    DAQmx_Val_Cfg_Default = int32(-1)
    DAQmx_Val_Internal = int32(10200)
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

    attemptReservation = bool32(1)
    overrideReservation = bool32(1)
    autoStart = bool32(1)

    deviceNameBufferSize = uInt32(100)
    deviceModuleNamesBufferSize = uInt32(1000)
    moduleChanNamesBufferSize = uInt32(2000)

    def downsample(y, size):
        yReshape = y.reshape(size, int(len(y)/size))
        yDownsamp = yReshape.mean(axis=1)
        return yDownsamp


    def __init__(self, sampleRate = 1, samplesPerChan = 1, minValue = 0, maxValue = 10, IPNumber = "", moduleSlotNumber = 1, moduleChanRange = [0]):

        self.nidaq = ctypes.windll.nicaiu

        self.sampleRate = sampleRate
        self.sampleRateC = Nidaq.float64(sampleRate)
        self.samplesPerChan = samplesPerChan
        self.samplesPerChanC = Nidaq.uInt64(samplesPerChan)
        self.minValue = minValue
        self.minValueC = Nidaq.float64(minValue)
        self.maxValue = maxValue
        self.maxValueC = Nidaq.float64(maxValue)
        self.IPNumber = IPNumber
        self.IPNumberC = ctypes.create_string_buffer(str.encode(IPNumber))
        self.moduleSlotNumber = moduleSlotNumber
        self.moduleChanRange = numpy.array(moduleChanRange)

        self.clockSourceC = ctypes.create_string_buffer(b"OnboardClock")
        self.timeoutC = Nidaq.float64(100.0)
        self.global_error_code = 0

        self.taskHandleC = Nidaq.TaskHandle(0)
        self.bufferSizeC = Nidaq.uInt32(samplesPerChan)
        self.pointsToReadC = self.bufferSizeC
        self.pointsReadC = Nidaq.uInt32(0)

        self.deviceNameC = ctypes.create_string_buffer(100)
        self.deviceModuleNamesC = ctypes.create_string_buffer(1000)
        self.moduleChanNamesC = ctypes.create_string_buffer(2000)


        config = Configure()
        self.dataFilepath = config.getDataFilePath()

        self.deviceModuleNamesArray = numpy.array
        self.moduleChanNamesArray = numpy.array



    def CHK(self, err):
        self.global_error_code = err
        if err < 0:
            buf_size = 1000
            buf = ctypes.create_string_buffer(b"\000" * buf_size)
            nidaq.DAQmxGetErrorString(err, ctypes.byref(buf), buf_size)
            print('nidaq call failed with error %d: %s'%(err, repr(buf.value)))


    def SetupDevice(self):
        self.CHK( self.nidaq.DAQmxAddNetworkDevice(self.IPNumberC, "", Nidaq.attemptReservation, self.timeoutC, self.deviceNameC, Nidaq.deviceNameBufferSize) )
        print( "deviceName: ", (self.deviceNameC.value).decode() )
        self.CHK( self.nidaq.DAQmxReserveNetworkDevice(self.deviceNameC, Nidaq.overrideReservation) )


    def SetupChassis(self):
        self.CHK( self.nidaq.DAQmxGetDevChassisModuleDevNames(self.deviceNameC, self.deviceModuleNamesC, Nidaq.deviceModuleNamesBufferSize) )
        deviceModuleNamesString = (self.deviceModuleNamesC.value).decode()
        deviceModuleNamesList = (deviceModuleNamesString.replace(" ", "")).split(",")
        self.deviceModuleNamesArray = numpy.array(deviceModuleNamesList)
        print( "deviceModuleNamesArray:", repr(self.deviceModuleNamesArray) )


    def SetupTask(self):
        self.CHK( self.nidaq.DAQmxCreateTask("", ctypes.byref(self.taskHandleC)) )


    def ReserveTask(self):
        self.CHK( self.nidaq.DAQmxTaskControl(self.taskHandleC, Nidaq.DAQmx_Val_Task_Reserve))

    def StartTask(self):
        self.CHK( self.nidaq.DAQmxStartTask(self.taskHandleC) )

    def StopAndClearTasks():
        if self.taskHandleC.value != 0:
            CHK( self.nidaq.DAQmxStopTask(self.taskHandleC) )
            CHK( self.nidaq.DAQmxClearTask(self.taskHandleC) )
            
    def DisconnectNetworkDevice():
        CHK( nidaq.DAQmxDeleteNetworkDevice(self.deviceNameC) )



class NidaqVoltageIn(Nidaq):

    DAQmx_Val_Volts = Nidaq.uInt32(10348)

    def scale_10K3A2B(U_therm, U_exc):
        R_thermistor = 1e3 * numpy.array([336.098,314.553,294.524,275.897,258.563,242.427,227.398,213.394,200.339,188.163,176.803,166.198,156.294,147.042,138.393,130.306,122.741,115.661,109.032,102.824,97.006,91.553,86.439,81.641,77.138,72.911,68.940,65.209,61.703,58.405,55.304,52.385,49.638,47.050,44.613,42.317,40.151,38.110,36.184,34.366,32.651,31.031,29.500,28.054,26.687,25.395,24.172,23.016,21.921,20.885,19.903,18.973,18.092,17.257,16.465,15.714,15.001,14.324,13.682,13.073,12.493,11.943,11.420,10.923,10.450,10.000,9.572,9.165,8.777,8.408,8.056,7.721,7.402,7.097,6.807,6.530,6.266,6.014,5.774,5.544,5.325,5.116,4.916,4.724,4.542,4.367,4.200,4.040,3.887,3.741,3.601])
        t_thermistor = numpy.arange(-40.0, 51.0)
        f_R_thermistor = scipy.interpolate.interp1d(R_thermistor, t_thermistor, kind='cubic', bounds_error=False, fill_value=-9999.0)
        R_ext = 9.93e3
        R = R_ext * 1 / (U_exc/U_therm - 1)
        return f_R_thermistor(R)

    def scale_Opto_22_AD3_SATT_ETT45_0101(U_par):
        return ( U_par/249.0 * 1000.0 - 4.0 ) / 16.0 * 100.0

    def scale_D_PYRPA_CA(U_pyran):
        return U_pyran / 2.5 * 1000.0

    def scale_3_phase_analyzer(U_pow):
        return ( U_pow / 220 * 1000.0 - 4.0 ) / 16.0 * 1e-6


    def SetupModule(self):
        moduleName = ctypes.create_string_buffer(str.encode(self.deviceModuleNamesArray[self.moduleSlotNumber - 1]))
        print("moduleName", repr(moduleName.value))
        self.CHK( self.nidaq.DAQmxGetDevAIPhysicalChans(moduleName, self.moduleChanNamesC, Nidaq.moduleChanNamesBufferSize) )
        moduleChanNamesString = (self.moduleChanNamesC.value).decode()
        moduleChanNamesList = (moduleChanNamesString.replace(" ", "")).split(",")
        self.moduleChanNamesArray = numpy.array(moduleChanNamesList)
        print( "moduleChanNamesArray[moduleChanRange]: ", repr(self.moduleChanNamesArray[self.moduleChanRange]) )
        moduleChanNamesSelected = ctypes.create_string_buffer(str.encode((",".join((self.moduleChanNamesArray[self.moduleChanRange]).tolist()))))
        print(repr(moduleChanNamesSelected.value))
        self.CHK( self.nidaq.DAQmxCreateAIVoltageChan(self.taskHandleC, moduleChanNamesSelected, "", Nidaq.DAQmx_Val_RSE, self.minValueC, self.maxValueC, NidaqVoltageIn.DAQmx_Val_Volts, None) )
        self.CHK( self.nidaq.DAQmxCfgSampClkTiming(self.taskHandleC, self.clockSourceC, self.sampleRateC, Nidaq.DAQmx_Val_Rising, Nidaq.DAQmx_Val_ContSamps, self.samplesPerChanC) )

    def InitAcquire(self):
        self.SetupDevice()
        self.SetupChassis()
        self.SetupTask()
        self.SetupModule()
        self.ReserveTask()
        self.StartTask()

    def ReadSamples(self):
        self.pointsToReadC = self.bufferSizeC
        data = numpy.zeros((32*(self.bufferSizeC).value,),dtype=numpy.float64)
        if self.global_error_code >= 0: 
            self.CHK( self.nidaq.DAQmxReadAnalogF64(self.taskHandleC, self.pointsToReadC, self.timeoutC, Nidaq.DAQmx_Val_GroupByChannel, data.ctypes.data, Nidaq.uInt32(32*(self.bufferSizeC).value), ctypes.byref(self.pointsReadC), None) )
        return data


    def LoopAcquire(self):

        while (self.global_error_code >= 0):
        
            readData = None
            try:
                readData = self.ReadSamples()
            except OSError as e:
                print(e)

            acq_finish_time = numpy.float64(time.time())
            prev_acq_finish_time = acq_finish_time - numpy.float64(self.samplesPerChan) / numpy.float64(self.sampleRate)
            prev_acq_finish_secs = numpy.int64(prev_acq_finish_time)
            prev_acq_finish_microsecs = numpy.int64(prev_acq_finish_time * 1e6)
            acq_finish_microsecs = numpy.int64(acq_finish_time * 1e6)
            print("Acquired ", (self.pointsReadC).value, " at time ", prev_acq_finish_microsecs)

            temp_sec = Nidaq.downsample(NidaqVoltageIn.scale_10K3A2B(readData[self.samplesPerChan*(1+0):self.samplesPerChan*(1+1)], readData[self.samplesPerChan*(4+0):self.samplesPerChan*(4+1)]), 1)
            print(temp_sec)
            rad_sec = Nidaq.downsample(NidaqVoltageIn.scale_D_PYRPA_CA(readData[self.samplesPerChan*(0+0):self.samplesPerChan*(0+1)]), 1)
            print(rad_sec)
            turb_cool_temp_sec = Nidaq.downsample(NidaqVoltageIn.scale_Opto_22_AD3_SATT_ETT45_0101(readData[self.samplesPerChan*(2+0):self.samplesPerChan*(2+1)]), 1)
            print(turb_cool_temp_sec)
            elpower_load_sec = Nidaq.downsample(NidaqVoltageIn.scale_3_phase_analyzer(readData[self.samplesPerChan*(3+0):self.samplesPerChan*(3+1)]), 1)
            print(elpower_load_sec)

            try:
                filename_temp = repr(23) + "_" + repr(prev_acq_finish_secs)
                numpy.save(self.dataFilepath+filename_temp, temp_sec)
                filename_rad = repr(21) + "_" + repr(prev_acq_finish_secs)
                numpy.save(self.dataFilepath+filename_rad, rad_sec)
                filename_turb_cool = repr(20) + "_" + repr(prev_acq_finish_secs)
                numpy.save(self.dataFilepath+filename_turb_cool, turb_cool_temp_sec)
                filename_elpower = repr(24) + "_" + repr(prev_acq_finish_secs)
                numpy.save(self.dataFilepath+filename_elpower, elpower_load_sec)
            except PermissionError as e:
                print(e)


class NidaqCurrentIn(Nidaq):

    DAQmx_Val_Amps = Nidaq.int32(10342)


class NidaqTemperatureIn(Nidaq):

    DAQmx_Val_Pt3851 = Nidaq.int32(10071)
    DAQmx_Val_DegC = Nidaq.int32(10143)
    DAQmx_Val_4Wire = Nidaq.int32(4)

