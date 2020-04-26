import daqc.host

usb_cam = daqc.host.USBCam(channels = {160}, sample_rate = 1.0, start_delay = 10, video_unit = '/dev/video1', video_res = [1280, 720], video_rate = 5)
usb_cam.run()
