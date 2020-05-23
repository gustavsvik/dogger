import gateway.uplink

http_upload = gateway.uplink.HttpTask(config_filepath = '/home/heta/Z/app/python/dogger/', config_filename = 'conf.ini', channels = {20,21,23,24,40,97,98,99,100,161,500}, start_delay = 0, ip_list = ['109.74.8.89'], max_connect_attempts = 50)
http_upload.run()
