import gateway.daqc

usb_cam = gateway.daqc.USBCam(config_filepath = '/home/heta/Z/app/python/dogger/', config_filename = 'conf_binary.ini', channels = {180}, sample_rate = 1.0, start_delay = 0, video_unit = '/dev/video2', video_res = [80, 60], video_rate = 5)
usb_cam.run()
