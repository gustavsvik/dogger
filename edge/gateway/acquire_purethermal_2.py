import gateway.daqc


usb_cam = gateway.daqc.USBCam(
    channels = {220},
    start_delay = 10,
    sample_rate = 1.0,
    file_path = '/home/heta/Z/data/files/images/',
    video_unit = '/dev/video5',
    video_res = [160, 120],
    video_quality = 80,
    video_capture_method = 'OPENCV',
    video_rate = 9)

usb_cam.run()
