import gateway.daqc


usb_cam = gateway.daqc.USBCam(
    channels = {160}, 
    start_delay = 10, 
    sample_rate = 1.0, 
    file_path = '/home/heta/Z/data/files/images/', 
    video_unit = '/dev/video0', 
    video_res = [1280, 720], 
    video_quality = 80, 
    video_capture_method = 'FSWEBCAM', 
    video_rate = 5)

usb_cam.run()
