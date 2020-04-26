#

import ctypes
import numpy
import time
import metadata
import task



class Channel:


    def __init__(self, channel_unique_index = 0, channel_text_id = '', channel_description = '', offset = 0, module_index = 0, device_index = 0, host_index = 0):

        self.unique_index = channel_unique_index # unique key (member, required)
        self.text_id = channel_text_id           # text label (member, optional)
        self.description = channel_description   # description (member, optional)
        self.offset = offset                     # offset (parent, optional) Kan denna inte bara vara positionen i arrayen?
        self.module_index = module_index         # module (parent, optional) Behövs den? (array av kanaler istället)
        self.device_index = device_index         # device (parent, optional) Behövs den? (array av kanaler istället)
        self.host_index = host_index             # host (parent, optional) Behövs den? (array av kanaler istället)



class AnalogChannel(Channel):


    def __init__(self, channel_unique_index = 0, channel_min_value = 0.0, channel_max_value = 1.0, channel_text_id = '', channel_description = '', channel_function = '', channel_lookup = '', offset = 0, module_index = 0, device_index = 0, host_index = 0):

        Channel.__init__(self, channel_unique_index, channel_text_id, channel_description, offset, module_index, device_index, host_index):

        self.min_value = channel_min_value       # minimum value (member, required)
        self.max_value = channel_max_value       # maximum value (member, required)
        self.function = channel_function         # data transform function (member, optional)
        self.lookup = channel_lookup             # data transform lookup table (member, optional)



class Module:


    def __init__(self, module_unique_index = 0, module_text_id = '', module_description = '', device_index = 0, device_slot = 0, module_channels = None):

        self.unique_index = module_unique_index # unique key (member, required)
        self.text_id = module_text_id           # text label (member, optional)
        self.description = module_description   # description (member, optional)
        self.device_index = device_index        # device (parent, required)
        self.device_slot = device_slot          # slot (parent, required)
        self.channels = module_channels         # channel list (child, optional)



class Device:


    def __init__(self, device_unique_index = 0, device_port = None, device_modules = None, device_channels = None):

        self.unique_index = device_unique_index
        self.port = device_port
        self.modules = device_modules
        self.channels = device_channels



class Host:


    def __init__(
        self,
        host_unique_index,
        host_persistent_data_path = '',
        host_channels = None):

        self.unique_index = host_unique_index
        self.persistent_data_path = host_persistent_data_path
        self.channels = host_channels



class Task:


    def __init__(
        self,
        task_host = None,
        task_device = None,
        task_sample_rate = 0.0,
        task_samples_per_chan = 1,
        task_subsamples_per_chan = 1):

        self.host = task_host
        self.device = task_device
        self.sample_rate = task_sample_rate
        self.samples_per_chan = samples_per_chan
        self.subsamples_per_chan = subsamples_per_chan


    def downsample(y, size):

        y_reshape = y.reshape(size, int(len(y)/size))
        y_downsamp = y_reshape.mean(axis=1)
        return y_downsamp













class CdaqDevice(Device)


    def setup_tasks()
        pass

    def __init__(self, device_ip_number = '', device_modules = None):

        Device.__init__(self,  device_ip_number = '', device_modules = None)

        device_modules[0].setup_tasks()



class NidaqChannel(Channel):

    uint8 = ctypes.c_ubyte
    int32 = ctypes.c_long
    uint32 = ctypes.c_ulong
    uint64 = ctypes.c_ulonglong
    float64 = ctypes.c_double
    bool32 = ctypes.c_bool
    task_handle = uint32
    points_read = uint32()
    null = ctypes.POINTER(ctypes.c_int)()
    value = uint32()


    def __init__(self, sample_rate = 1, samples_per_chan = 1, subsamples_per_chan = 1, min_value = 0, max_value = 10, module_chan_index = 0, unique_chan_index = 0):

        Channel.__init__(self, sample_rate, samples_per_chan, subsamples_per_chan, min_value, max_value, module_chan_index, unique_chan_index)

        self.nidaq = ctypes.windll.nicaiu

        self.sample_rate_c = NidaqChannel.float64(self.sample_rate)
        self.samples_per_chan_c = NidaqChannel.uint64(self.samples_per_chan)
        self.min_value_c = NidaqChannel.float64(self.min_value)
        self.max_value_c = NidaqChannel.float64(self.max_value)





class RpiChannel(Channel):

    pass












class NidaqDevice(Device):


    def CHK(self, err):
        self.global_error_code = err
        if err < 0:
            buf_size = 1000
            buf = ctypes.create_string_buffer(b"\000" * buf_size)
            self.nidaq.DAQmxGetErrorString(err, ctypes.byref(buf), buf_size)
            print('nidaq call failed with error %d: %s'%(err, repr(buf.value)))


    def SetupDevice(self):
        self.CHK( self.nidaq.DAQmxAddNetworkDevice(self.ip_number_c, "", Nidaq.attempt_reservation, self.timeout_c, self.deviceName_c, Nidaq.device_name_buffer_size) )
        print( "deviceName: ", (self.deviceName_c.value).decode() )
        self.CHK( self.nidaq.DAQmxReserveNetworkDevice(self.deviceName_c, Nidaq.override_reservation) )

    def SetupChassis(self):

        deviceModuleNamesString = (self.deviceModuleNames_c.value).decode()
        deviceModuleNamesList = (deviceModuleNamesString.replace(" ", "")).split(",")
        self.deviceModuleNamesArray = numpy.array(deviceModuleNamesList)
        print( "deviceModuleNamesArray:", repr(self.deviceModuleNamesArray) )

    def DisconnectNetworkDevice(self):
        CHK( nidaq.DAQmxDeleteNetworkDevice(self.deviceName_c) )

def GetDevChassisModuleDevNames:
        self.CHK( self.nidaq.DAQmxGetDevChassisModuleDevNames(self.deviceName_c, self.deviceModuleNames_c, Nidaq.device_module_names_buffer_size) )














class Nidaq(Device):

    uint8 = ctypes.c_ubyte
    int32 = ctypes.c_long
    uint32 = ctypes.c_ulong
    uint64 = ctypes.c_ulonglong
    float64 = ctypes.c_double
    bool32 = ctypes.c_bool
    task_handle = uint32
    #points_written = uint32()
    points_read = uint32()
    null = ctypes.POINTER(ctypes.c_int)()
    value = uint32()

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

    attempt_reservation = bool32(1)
    override_reservation = bool32(1)
    #auto_start = bool32(1)

    device_name_buffer_size = uint32(100)
    device_module_names_buffer_size = uint32(1000)
    module_chan_names_buffer_size = uint32(2000)


    def __init__(self, sample_rate = 1, samples_per_chan = 1, subsamples_per_chan = 1, min_value = 0, max_value = 10, ip_number = "", module_slot_number = 1, module_chans = [0], unique_chans = [0]):

        Device.__init__(self, sample_rate, samples_per_chan, subsamples_per_chan, min_value, max_value, ip_number, module_slot_number, module_chans, unique_chans)

        self.nidaq = ctypes.windll.nicaiu

        self.sample_rate_c = Nidaq.float64(self.sample_rate)
        self.samples_per_chan_c = Nidaq.uint64(self.samples_per_chan)
        self.min_value_c = Nidaq.float64(self.min_value)
        self.max_value_c = Nidaq.float64(self.max_value)
        self.ip_number_c = ctypes.create_string_buffer(str.encode(self.ip_number))

        self.clock_source_c = ctypes.create_string_buffer(b"OnboardClock")
        self.timeout_c = Nidaq.float64(100.0)
        self.global_error_code = 0

        self.task_handle_c = Nidaq.task_handle(0)
        self.buffer_size_c = Nidaq.uint32(self.samples_per_chan)
        self.points_to_read_c = self.buffer_size_c
        self.points_read_c = Nidaq.uint32(0)

        self.deviceName_c = ctypes.create_string_buffer(100)
        self.deviceModuleNames_c = ctypes.create_string_buffer(1000)
        self.moduleChanNames_c = ctypes.create_string_buffer(2000)

        self.deviceModuleNamesArray = numpy.array
        self.moduleChanNamesArray = numpy.array


    def CHK(self, err):
        self.global_error_code = err
        if err < 0:
            buf_size = 1000
            buf = ctypes.create_string_buffer(b"\000" * buf_size)
            self.nidaq.DAQmxGetErrorString(err, ctypes.byref(buf), buf_size)
            print('nidaq call failed with error %d: %s'%(err, repr(buf.value)))


    def SetupDevice(self):
        self.CHK( self.nidaq.DAQmxAddNetworkDevice(self.ip_number_c, "", Nidaq.attempt_reservation, self.timeout_c, self.deviceName_c, Nidaq.device_name_buffer_size) )
        print( "deviceName: ", (self.deviceName_c.value).decode() )
        self.CHK( self.nidaq.DAQmxReserveNetworkDevice(self.deviceName_c, Nidaq.override_reservation) )


    def SetupChassis(self):
        self.CHK( self.nidaq.DAQmxGetDevChassisModuleDevNames(self.deviceName_c, self.deviceModuleNames_c, Nidaq.device_module_names_buffer_size) )
        deviceModuleNamesString = (self.deviceModuleNames_c.value).decode()
        deviceModuleNamesList = (deviceModuleNamesString.replace(" ", "")).split(",")
        self.deviceModuleNamesArray = numpy.array(deviceModuleNamesList)
        print( "deviceModuleNamesArray:", repr(self.deviceModuleNamesArray) )


    def SetupTask(self):
        self.CHK( self.nidaq.DAQmxCreateTask("", ctypes.byref(self.task_handle_c)) )


    def ReserveTask(self):
        self.CHK( self.nidaq.DAQmxTaskControl(self.task_handle_c, Nidaq.DAQmx_Val_Task_Reserve))

    def StartTask(self):
        self.CHK( self.nidaq.DAQmxStartTask(self.task_handle_c) )

    def StopAndClearTasks(self):
        if self.task_handle_c.value != 0:
            CHK( self.nidaq.DAQmxStopTask(self.task_handle_c) )
            CHK( self.nidaq.DAQmxClearTask(self.task_handle_c) )

    def DisconnectNetworkDevice(self):
        CHK( nidaq.DAQmxDeleteNetworkDevice(self.deviceName_c) )



class NidaqVoltageIn(Nidaq):

    DAQmx_Val_Volts = Nidaq.uint32(10348)

    def SetupModule(self):
        moduleName = ctypes.create_string_buffer(str.encode(self.deviceModuleNamesArray[self.module_slot_number - 1]))
        print("moduleName", repr(moduleName.value))
        self.CHK( self.nidaq.DAQmxGetDevAIPhysicalChans(moduleName, self.moduleChanNames_c, Nidaq.module_chan_names_buffer_size) )
        moduleChanNamesString = (self.moduleChanNames_c.value).decode()
        moduleChanNamesList = (moduleChanNamesString.replace(" ", "")).split(",")
        self.moduleChanNamesArray = numpy.array(moduleChanNamesList)
        print( "moduleChanNamesArray[module_chans]: ", repr(self.moduleChanNamesArray[self.module_chans]) )
        moduleChanNamesSelected = ctypes.create_string_buffer(str.encode((",".join((self.moduleChanNamesArray[self.module_chans]).tolist()))))
        print(repr(moduleChanNamesSelected.value))

        self.CHK( self.nidaq.DAQmxCreateAIVoltageChan(self.task_handle_c, moduleChanNamesSelected, "", Nidaq.DAQmx_Val_RSE, self.min_value_c, self.max_value_c, NidaqVoltageIn.DAQmx_Val_Volts, None) )
        self.CHK( self.nidaq.DAQmxCfgSampClkTiming(self.task_handle_c, self.clock_source_c, self.sample_rate_c, Nidaq.DAQmx_Val_Rising, Nidaq.DAQmx_Val_ContSamps, self.samples_per_chan_c) )

    def InitAcquire(self):
        self.SetupDevice()
        self.SetupChassis()
        self.SetupTask()
        self.SetupModule()
        self.ReserveTask()
        self.StartTask()

    def ReadSamples(self):
        self.points_to_read_c = self.buffer_size_c
        data = numpy.zeros((32*(self.buffer_size_c).value,),dtype=numpy.float64)
        if self.global_error_code >= 0:
            self.CHK( self.nidaq.DAQmxReadAnalogF64(self.task_handle_c, self.points_to_read_c, self.timeout_c, Nidaq.DAQmx_Val_GroupByChannel, data.ctypes.data, Nidaq.uint32(32*(self.buffer_size_c).value), ctypes.byref(self.points_read_c), None) )
        return data


    def LoopAcquire(self):

        while (self.global_error_code >= 0):

            readData = None
            try:
                readData = self.ReadSamples()
            except OSError as e:
                print(e)

            acqFinishTime = numpy.float64(time.time())
            prevAcqFinishTime = acqFinishTime - numpy.float64(self.samples_per_chan) / numpy.float64(self.sample_rate)
            prevAcqFinishSecs = numpy.int64(prevAcqFinishTime)
            prevAcqFinishMicrosecs = numpy.int64(prevAcqFinishTime * 1e6)
            acqFinishMicrosecs = numpy.int64(acqFinishTime * 1e6)
            print("Acquired ", (self.points_read_c).value, " at time ", prevAcqFinishMicrosecs)

            noOfChans = len(self.module_chans)
            for chanIndex in range(0, noOfChans):
                filename = repr(self.unique_chans[chanIndex]) + "_" + repr(prevAcqFinishSecs)
                secValue = Device.downsample(readData[self.samples_per_chan*(chanIndex+0):self.samples_per_chan*(chanIndex+1)], 1)
                fileValues = secValue
                if self.subsamples_per_chan > 1:
                    subSampleValues = Device.downsample(readData[self.samples_per_chan*(chanIndex+0):self.samples_per_chan*(chanIndex+1)], self.subsamples_per_chan)
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






class NidaqTask(Task):


    def SetupTask(self):
        self.CHK( self.nidaq.DAQmxCreateTask("", ctypes.byref(self.task_handle_c)) )


    def ReserveTask(self):
        self.CHK( self.nidaq.DAQmxTaskControl(self.task_handle_c, Nidaq.DAQmx_Val_Task_Reserve))

    def StartTask(self):
        self.CHK( self.nidaq.DAQmxStartTask(self.task_handle_c) )

    def StopAndClearTasks(self):
        if self.task_handle_c.value != 0:
            CHK( self.nidaq.DAQmxStopTask(self.task_handle_c) )
            CHK( self.nidaq.DAQmxClearTask(self.task_handle_c) )
