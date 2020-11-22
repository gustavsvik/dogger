import gateway.uplink


http_upload_binary = gateway.uplink.Replicate(
    channels = {140, 160, 180, 600, 10002},
    start_delay = 0, 
    host_api_url = '/host/',
    max_connect_attempts = 50, 
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf.ini')

http_upload_binary.run()
