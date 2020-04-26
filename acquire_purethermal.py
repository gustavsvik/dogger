import daqc.host

usb_cam = daqc.host.USBCam(channels = {180}, sample_rate = 1.0, start_delay = 0, video_unit = '/dev/video0', video_res = [80, 60], video_rate = 5)
usb_cam.run()
