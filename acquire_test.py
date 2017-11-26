import daqc.host

nidaq = daqc.host.NidaqVoltageIn(100, 100, 1, 0, 10, "169.254.254.254", 1, [21,23,20,24,22], [21,23,20,24,22])
nidaq.run()
