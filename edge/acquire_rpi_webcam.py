import gateway.daqc


usb_cam = gateway.daqc.USBCam(
    channels = {320},
    sample_rate = 1.0,
    video_unit = '/dev/video0',
    video_res = [1080, 720],
    video_crop_origin = [0, 0],
    video_crop = [1080, 720],
    video_quality = 80,
    video_capture_method = 'PICAMERA2',
    video_rate = 5)

usb_cam.run()
