import gateway.daqc


acquire_current = gateway.daqc.AcquireCurrent(
    file_path = 'Z:\\data\\files\\current\\', 
    archive_file_path = None, 
    config_filepath = 'Z:\\app\\python\\dogger\\', 
    config_filename = 'conf_current.ini')

acquire_current.run()
