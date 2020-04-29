import upload.host

http_upload_local = upload.host.Http(channels = {20, 21, 23, 24, 40, 97, 161, 500}, start_delay = 0, ip_list = ['192.168.1.103'], max_connect_attempts = 50)
http_upload_local.run()
