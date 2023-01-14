import gateway.daqc


usb_cam = gateway.daqc.USBCam(
    channels = {320},
    start_delay = 0,
    sample_rate = 1.0,
    video_unit = '/dev/video0',
    video_res = [1440, 1080],
    video_crop_origin = [70, 140],
    video_crop = [1350, 860],
    video_quality = 80,
    video_capture_method = 'PICAMERA2',
    video_rate = 5)

usb_cam.run()
