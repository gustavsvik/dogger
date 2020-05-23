import gateway.daqc

usb_cam = gateway.daqc.USBCam(config_filepath = '/home/heta/Z/app/python/dogger/', config_filename = 'conf_binary.ini', channels = {160}, sample_rate = 1.0, start_delay = 10, video_unit = '/dev/video0', video_res = [1280, 720], video_rate = 5)
usb_cam.run()
