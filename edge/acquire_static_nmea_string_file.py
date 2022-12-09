import gateway.daqc


acquire_static_file = gateway.daqc.StaticFileNmeaFile(
    channels = { 'TTM':{162:'txt'} },
    start_delay = 0,
    sample_rate = 0.5,
    concatenate = True,
    static_filename = 'nmea_arpa.txt')

acquire_static_file.run()
