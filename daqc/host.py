#

import time
import daqc.device


class Host:


    def __init__(self):
        pass


class NidaqVoltageIn(Host):
    
    
    def __init__(self):

        self.nidaq = daqc.device.NidaqVoltageIn(100, 100, 1, 0, 10, "169.254.254.254", 1, [21,23,20,24,22], [21,23,20,24,22])


    def run(self):

        self.nidaq.InitAcquire()
        while True:
            if self.nidaq.globalErrorCode >= 0:
                self.nidaq.LoopAcquire()
                if self.nidaq.globalErrorCode < 0:
                    self.nidaq.StopAndClearTasks1()
                    self.nidaq.InitAcquire()
            else:
                self.nidaq.StopAndClearTasks1()
                self.nidaq.InitAcquire()
            time.sleep(10)
