import gateway.daqc


sie_acquire_udp_http = gateway.daqc.HttpAishubAivdmFile(
    channels = {'AISHUB':{0:'txt',152:'txt'}}, 
    start_delay = 0, 
    sample_rate = 1/(60.0*2),
    transmit_rate = 1/(60.0*2),
    file_path = '/srv/dogger/files/',
    archive_file_path = '/srv/dogger/files/',
    config_filepath = '/srv/dogger/', 
    config_filename = 'conf_aishub_api.ini')

sie_acquire_udp_http.run()
