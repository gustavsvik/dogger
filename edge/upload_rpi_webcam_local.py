import gateway.daqc


screenshot = gateway.daqc.TempFileDirectUpload(
    channels = {320},
    sample_rate = 1.0,
    host_api_url = '/host/',
    client_api_url = '/client/',
    config_filename = 'conf_local.ini')

screenshot.run()
