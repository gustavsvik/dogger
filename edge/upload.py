import gateway.link


http_upload = gateway.link.Replicate(
    channels = {20,21,23,24,40,99,100,161,500},
    start_delay = 0,
    transmit_rate = 1,
    host_api_url = '/host/',
    max_connect_attempts = 50)

http_upload.run()
