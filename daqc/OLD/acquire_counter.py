import ctypes
import numpy
import time
import logging

nidaq = ctypes.windll.nicaiu # load the DLL

int32 = ctypes.c_long
uInt32 = ctypes.c_ulong
uInt64 = ctypes.c_ulonglong
float64 = ctypes.c_double
TaskHandle = uInt32

DAQmx_Val_Rising = 10280
DAQmx_Val_CountUp = 10128
DAQmx_Val_ContSamps = 10123

     
def CHK(err):
    """a simple error checking routine"""
    if err < 0:
        buf_size = 1000
        buf = ctypes.create_string_buffer(b"\000" * buf_size)
        nidaq.DAQmxGetErrorString(err,ctypes.byref(buf),buf_size)
        raise RuntimeError('nidaq call failed with error %d: %s'%(err,repr(buf.value)))


devchan = ctypes.create_string_buffer(b"cDAQ9188-1AD0C62Mod2/ctr0")
src = (b"/cDAQ9188-1AD0C62/PFI0")

taskHandle = TaskHandle(0)

print(repr(taskHandle))
CHK(nidaq.DAQmxCreateTask("", ctypes.byref(taskHandle)))
print(repr(taskHandle))

CHK(nidaq.DAQmxCreateCICountEdgesChan(taskHandle, devchan, "", DAQmx_Val_Rising, int32(0), DAQmx_Val_CountUp))
if src is not None and src != "":
    CHK(nidaq.DAQmxSetCICountEdgesTerm(taskHandle, devchan, src))
#CHK(nidaq.DAQmxCfgSampClkTiming(taskHandle, src, float64(100.0), DAQmx_Val_Rising, DAQmx_Val_ContSamps, uInt64(1000)))
CHK(nidaq.DAQmxStartTask(taskHandle))

    
while (True):

    try:

        pointsRead = uInt32()
        #count = uInt32()
        bufferSize = uInt32(1000)
        data = numpy.zeros((1*bufferSize.value,),dtype=numpy.uint32)
        #CHK(nidaq.DAQmxReadCounterScalarU32(taskHandle, ctypes.byref(count), float64(-1.0), None))
        #print(count)
        CHK(nidaq.DAQmxReadCounterU32(taskHandle, uInt32(-1), float64(100.0), data.ctypes.data, uInt32(1*bufferSize.value), ctypes.byref(pointsRead), None));
        #CHK(nidaq.DAQmxReadCounterF64(taskHandle, int32(samples), float64(timeout), data.ctypes.data, int32(samples), ctypes.byref(nread), None))
        print("pointsRead: ", repr(pointsRead))
        print(repr(data))
        
    except Exception as e:
        logging.error('NI DAQ call failed: %s', repr(e))

    finally:
        time.sleep(1.0)

if taskHandle.value != 0:
    nidaq.DAQmxStopTask(taskHandle)
    nidaq.DAQmxClearTask(taskHandle)

