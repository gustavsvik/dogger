import gateway.daqc


acquire_nmea = gateway.daqc.PosNmeaUdpFile(
    channels = {61010,61011,61012}, 
    port = 50000,
    start_delay = 0,
    sample_rate = 1.0,
    file_path = '/srv/dogger/files/',
    archive_file_path = '/srv/dogger/files/',
    config_filepath = '/srv/dogger/', 
    config_filename = 'conf.ini')

acquire_nmea.run()
