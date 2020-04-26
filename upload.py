import upload.host

http_upload = upload.host.Http(channels = {20, 21, 23, 24, 40, 97, 98, 99, 100, 161, 500}, start_delay = 0, ip_list = ['109.74.8.89'], max_connect_attempts = 50)
http_upload.clear_channels()
http_upload.run()
