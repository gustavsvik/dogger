import gateway.daqc


acquire_static_file = gateway.daqc.StaticFileNmeaFile(
    channels = { 'VDM':{158:'txt'} },
    start_delay = 0,
    sample_rate = 0.1,
    static_file_path = '/home/scc01/Z/dogger/edge/',
    static_filename = 'aivdm_8_31.txt')

acquire_static_file.run()
