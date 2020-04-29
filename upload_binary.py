import upload.host

http_upload_binary = upload.host.Http(channels = {140, 160, 180, 600, 10002}, start_delay = 0, ip_list = ['109.74.8.89'], max_connect_attempts = 50)
http_upload_binary.run()
