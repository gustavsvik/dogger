import gateway.daqc


screenshot = gateway.daqc.TempFileUpload(
    channels = {2000},
    sample_rate = 1.0,
    host_api_url = '/host/',
    client_api_url = '/client/',
    file_extension = 'jpg')

screenshot.run()
