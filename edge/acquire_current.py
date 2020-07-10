import gateway.daqc


acquire_current = gateway.daqc.AcquireCurrent(
    file_path = 'Z:\\data\\files\\current\\', 
    archive_file_path = None)

acquire_current.run()
