import gateway.daqc


acquire_static_file = gateway.daqc.StaticFileNmeaFile(
    channels = { 'GGA':{164:'txt'} },
    start_delay = 0,
    sample_rate = 0.5,
    static_file_path_name = '/home/heta/Z/app/python/dogger/edge/nmea_pos.txt',
    file_path = '/home/heta/Z/data/files/',
    nmea_prepend = 'GPGGA,',
    nmea_append = ',1,9,0.91,44.7,M,24.4,M,,',
    config_filepath = '/home/heta/Z/app/python/dogger/',
    config_filename = 'conf.ini')

acquire_static_file.run()
