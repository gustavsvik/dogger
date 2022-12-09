import gateway.daqc


screenshot = gateway.daqc.ScreenshotUpload(
    channels = {603},
    sample_rate = 1.0,
    host_api_url = '\\host\\',
    client_api_url = '\\client\\',
    crop = [0,1243,184,1040],
    video_quality = 80)

screenshot.run()
