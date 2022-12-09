import gateway.daqc


acquire_static_file = gateway.daqc.StaticFileNmeaFile(
    channels = { 'XDR':{171:'txt'} },
    start_delay = 0,
    sample_rate = 0.5,
    static_filename = 'nmea_engine.txt')

acquire_static_file.run()
