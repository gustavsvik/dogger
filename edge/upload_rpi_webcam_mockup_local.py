import gateway.daqc


screenshot = gateway.daqc.TempFileUpload(
    channels = {180},
    sample_rate = 1.0,
    host_api_url = '/host/',
    client_api_url = '/client/',
    file_extension = 'jpg',
    config_filepath = '/home/heta/Z/app/python/dogger/',
    config_filename = 'conf_local.ini')

screenshot.run()
