import gateway.daqc


screenshot = gateway.daqc.ScreenshotUpload(
    channels = {603},
    sample_rate = 1.0,
    host_api_url = '\\host\\',
    client_api_url = '\\client\\',
    crop = [0,184,1243,1040],
    video_quality = 80,
    config_filepath = 'Z:\\app\\python\\dogger\\',
    config_filename = 'conf.ini')

screenshot.run()
