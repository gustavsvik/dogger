import gateway.uplink

http_upload_binary = gateway.uplink.HttpTask(config_filepath = '/home/heta/Z/app/python/dogger/', config_filename = 'conf.ini', channels = {140, 160, 180, 600, 10002}, start_delay = 0, ip_list = ['109.74.8.89'], max_connect_attempts = 50)
http_upload_binary.run()
