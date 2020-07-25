import gateway.daqc


usb_cam = gateway.daqc.USBCam(
    channels = {140}, 
    start_delay = 10, 
    sample_rate = 1.0, 
    video_unit = '/dev/video0', 
    video_res = [1280, 720], 
    video_quality = 80, 
    video_capture_method = 'RASPICAM', 
    video_rate = 5)

usb_cam.run()
