import gateway.daqc


sie_acquire_udp_http = gateway.daqc.RawUdpFile(
    channels = {'992659996':{168:'npy',169:'npy'}}, 
    start_delay = 0, 
    port = 61012,
    sample_rate = 1.0,
    file_path = '/srv/dogger/files/',
    archive_file_path = '/srv/dogger/files/',
    config_filepath = '/srv/dogger/', 
    config_filename = 'conf.ini')

sie_acquire_udp_http.run()
