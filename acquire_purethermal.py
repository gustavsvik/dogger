import gateway.daqc

usb_cam = gateway.daqc.USBCam(
    channels = {180}, 
    start_delay = 10, 
    sample_rate = 1.0, 
    file_path = '/home/heta/Z/data/files/images/', 
    archive_file_path = None, 
    video_unit = '/dev/video2', 
    video_res = [80, 60], 
    video_rate = 5,
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf_binary.ini')

usb_cam.run()
