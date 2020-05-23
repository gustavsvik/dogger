import gateway.daqc

acquire_current = gateway.daqc.AcquireCurrent(config_filepath = 'Z:\\app\\python\\dogger\\', config_filename = 'conf_current.ini')
acquire_current.run()
