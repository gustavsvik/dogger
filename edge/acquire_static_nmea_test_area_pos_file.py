import gateway.daqc


acquire_static_file = gateway.daqc.StaticFileNmeaFile(
    channels = { 'GGA':{167:'txt', 168:'npy', 169:'npy'} },
    start_delay = 0,
    sample_rate = 0.5,
    static_filename = 'nmea_test_area_pos.txt',
    nmea_prepend = 'GPGGA,',
    nmea_append = ',1,9,0.91,44.7,M,24.4,M,,')

acquire_static_file.run()
