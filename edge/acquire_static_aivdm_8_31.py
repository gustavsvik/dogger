import gateway.daqc


acquire_static_file = gateway.daqc.StaticFileNmeaFile(
    channels = { 'VDM':{144:'txt'} },
    start_delay = 0,
    sample_rate = 0.5,
    static_file_path_name = '/home/heta/Z/app/python/dogger/edge/aivdm_8_31.txt',
    file_path = '/home/heta/Z/data/files/',
    config_filepath = '/home/heta/Z/app/python/dogger/',
    config_filename = 'conf.ini')

acquire_static_file.run()
