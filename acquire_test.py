import time
from dogger.acquire import NidaqVoltageIn

nidaq = NidaqVoltageIn(100, 100, 0, 10, "169.254.254.254", 1, [21,23,20,24,22])

nidaq.InitAcquire()
while True:
    if nidaq.global_error_code >= 0:
        nidaq.LoopAcquire()
        if nidaq.global_error_code < 0:
            nidaq.StopAndClearTasks1()
            nidaq.InitAcquire()
    else:
        nidaq.StopAndClearTasks1()
        nidaq.InitAcquire()
    time.sleep(10)
