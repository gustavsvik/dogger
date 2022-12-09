import gateway.daqc


screenshot = gateway.daqc.TempFileUpload(
    channels = {140},
    sample_rate = 1.0,
    host_api_url = '/host/',
    client_api_url = '/client/')

screenshot.run()
