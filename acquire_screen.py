import gateway.daqc


screenshot = gateway.daqc.Screenshot(
    channels = {600}, 
    start_delay = 0, 
    sample_rate = 1.0, 
    file_path = None, 
    archive_file_path = None, 
    video_res = None, 
    ip_list = ['109.74.8.89'],
    crop = [0,184,1243,1040],
    config_filepath = None, 
    config_filename = None)

screenshot.run()
