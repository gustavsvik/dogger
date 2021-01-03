import gateway.daqc


mob_acquire_nmea_udp_file = gateway.daqc.NmeaUdpFile(
    channels = {'GGA':{61010:'txt',61011:'npy',61012:'npy'}, 'VTG':{61013:'txt'}}, 
    port = 50000,
    start_delay = 0,
    sample_rate = 1.0,
    file_path = '/srv/dogger/files/',
    archive_file_path = '/srv/dogger/files/',
    config_filepath = '/srv/dogger/', 
    config_filename = 'conf.ini')

mob_acquire_nmea_udp_file.run()
