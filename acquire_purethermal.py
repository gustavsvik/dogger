import gateway.daqc

usb_cam = gateway.daqc.USBCam(
    channels = {180}, 
    start_delay = 10, 
    sample_rate = 1.0, 
    file_path = '/home/heta/Z/data/files/images/', 
    archive_file_path = None, 
    video_unit = '/dev/video2', 
    video_res = [1280, 720], 
    video_rate = 5)

usb_cam.run()
