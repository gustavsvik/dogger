import gateway.daqc


acquire_static_file = gateway.daqc.StaticFileNmeaFile(
    channels = { 'RSA':{163:'txt'} },
    start_delay = 0,
    sample_rate = 1.0,
    static_filename = 'nmea_rudder.txt')

acquire_static_file.run()
