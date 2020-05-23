import gateway.uplink

http_upload_local = gateway.uplink.HttpTask(config_filepath = '/home/heta/Z/app/python/dogger/', config_filename = 'conf.ini', channels = {20,21,23,24,40,97,161,500}, start_delay = 0, ip_list = ['192.168.1.103'], max_connect_attempts = 50)
http_upload_local.run()
