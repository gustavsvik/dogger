#

import ctypes
import numpy
import time

import gateway.metadata as md


class Device:

    def downsample(y, size):
        yReshape = y.reshape(size, int(len(y)/size))
        yDownsamp = yReshape.mean(axis=1)
        return yDownsamp

    def __init__(self, sample_rate = 1, samplesPerChan = 1, subSamplesPerChan = 1, minValue = 0, maxValue = 10, IPNumber = "", moduleSlotNumber = 1, moduleChanRange = [0], uniqueChanIndexRange = [0]):

        self.sample_rate = sample_rate
        self.samplesPerChan = samplesPerChan
        self.subSamplesPerChan = subSamplesPerChan
        self.minValue = minValue
        self.maxValue = maxValue
        self.IPNumber = IPNumber
        self.moduleSlotNumber = moduleSlotNumber
        self.moduleChanRange = numpy.array(moduleChanRange)
        self.uniqueChanIndexRange = numpy.array(uniqueChanIndexRange)

        config = md.Configure(filepath = '/home/heta/Z/app/python/dogger/', filename = 'conf_voltage_2.ini')
        env = config.get()
        self.dataFilepath = env['STORE_PATH']



class Nidaq(Device):

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


    def __init__(self, sample_rate = 1, samplesPerChan = 1, subSamplesPerChan = 1, minValue = 0, maxValue = 10, IPNumber = "", moduleSlotNumber = 1, moduleChanRange = [0], uniqueChanIndexRange = [0]):

        Device.__init__(self, sample_rate, samplesPerChan, subSamplesPerChan, minValue, maxValue, IPNumber, moduleSlotNumber, moduleChanRange, uniqueChanIndexRange)

        self.nidaq = ctypes.windll.nicaiu

        self.sample_rateC = Nidaq.float64(self.sample_rate)
        self.samplesPerChanC = Nidaq.uInt64(self.samplesPerChan)
        self.minValueC = Nidaq.float64(self.minValue)
        self.maxValueC = Nidaq.float64(self.maxValue)
        self.IPNumberC = ctypes.create_string_buffer(str.encode(self.IPNumber))

        self.clockSourceC = ctypes.create_string_buffer(b"OnboardClock")
        self.timeoutC = Nidaq.float64(100.0)
        self.globalErrorCode = 0

        self.taskHandleC = Nidaq.TaskHandle(0)
        self.bufferSizeC = Nidaq.uInt32(self.samplesPerChan)
        self.pointsToReadC = self.bufferSizeC
        self.pointsReadC = Nidaq.uInt32(0)

        self.deviceNameC = ctypes.create_string_buffer(100)
        self.deviceModuleNamesC = ctypes.create_string_buffer(1000)
        self.moduleChanNamesC = ctypes.create_string_buffer(2000)

        self.deviceModuleNamesArray = numpy.array
        self.moduleChanNamesArray = numpy.array


    def CHK(self, err):
        self.globalErrorCode = err
        if err < 0:
            buf_size = 1000
            buf = ctypes.create_string_buffer(b"\000" * buf_size)
            self.nidaq.DAQmxGetErrorString(err, ctypes.byref(buf), buf_size)
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

    def StopAndClearTasks(self):
        if self.taskHandleC.value != 0:
            CHK( self.nidaq.DAQmxStopTask(self.taskHandleC) )
            CHK( self.nidaq.DAQmxClearTask(self.taskHandleC) )
            
    def DisconnectNetworkDevice(self):
        CHK( nidaq.DAQmxDeleteNetworkDevice(self.deviceNameC) )



class NidaqVoltageIn(Nidaq):

    DAQmx_Val_Volts = Nidaq.uInt32(10348)

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
        self.CHK( self.nidaq.DAQmxCfgSampClkTiming(self.taskHandleC, self.clockSourceC, self.sample_rateC, Nidaq.DAQmx_Val_Rising, Nidaq.DAQmx_Val_ContSamps, self.samplesPerChanC) )

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
        if self.globalErrorCode >= 0: 
            self.CHK( self.nidaq.DAQmxReadAnalogF64(self.taskHandleC, self.pointsToReadC, self.timeoutC, Nidaq.DAQmx_Val_GroupByChannel, data.ctypes.data, Nidaq.uInt32(32*(self.bufferSizeC).value), ctypes.byref(self.pointsReadC), None) )
        return data


    def LoopAcquire(self):

        while (self.globalErrorCode >= 0):
        
            readData = None
            try:
                readData = self.ReadSamples()
            except OSError as e:
                print(e)

            acqFinishTime = numpy.float64(time.time())
            prevAcqFinishTime = acqFinishTime - numpy.float64(self.samplesPerChan) / numpy.float64(self.sample_rate)
            prevAcqFinishSecs = numpy.int64(prevAcqFinishTime)
            prevAcqFinishMicrosecs = numpy.int64(prevAcqFinishTime * 1e6)
            acqFinishMicrosecs = numpy.int64(acqFinishTime * 1e6)
            print("Acquired ", (self.pointsReadC).value, " at time ", prevAcqFinishMicrosecs)

            noOfChans = len(self.moduleChanRange)
            for chanIndex in range(0, noOfChans):
                filename = repr(self.uniqueChanIndexRange[chanIndex]) + "_" + repr(prevAcqFinishSecs)
                secValue = Device.downsample(readData[self.samplesPerChan*(chanIndex+0):self.samplesPerChan*(chanIndex+1)], 1)
                fileValues = secValue
                if self.subSamplesPerChan > 1:
                    subSampleValues = Device.downsample(readData[self.samplesPerChan*(chanIndex+0):self.samplesPerChan*(chanIndex+1)], self.subSamplesPerChan)
                    fileValues = numpy.concatenate([secValue, subSampleValues])
                try:
                    numpy.save(self.dataFilepath+filename, fileValues)
                except PermissionError as e:
                    print(e)


class NidaqCurrentIn(Nidaq):

    DAQmx_Val_Amps = Nidaq.int32(10342)


class NidaqTemperatureIn(Nidaq):

    DAQmx_Val_Pt3851 = Nidaq.int32(10071)
    DAQmx_Val_DegC = Nidaq.int32(10143)
    DAQmx_Val_4Wire = Nidaq.int32(4)

