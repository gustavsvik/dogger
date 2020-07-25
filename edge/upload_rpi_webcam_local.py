import gateway.daqc


screenshot = gateway.daqc.TempFileUpload(
    channels = {140}, 
    sample_rate = 1.0, 
    config_filepath = '/home/pi/webcam_ethernet/dogger/',
    config_filename = 'conf_local.ini')

screenshot.run()
