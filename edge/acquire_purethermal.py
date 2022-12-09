import gateway.daqc


usb_cam = gateway.daqc.USBCam(
    channels = {200},
    start_delay = 10,
    sample_rate = 1.0,
    video_unit = '/dev/video0',
    video_res = [160, 120],
    video_quality = 80,
    video_capture_method = 'OPENCV',
    video_rate = 9,
    config_filename = 'conf_video.ini')

usb_cam.run()
