import gateway.uplink


http_download = gateway.uplink.Download(
    channels = {61011,61012}, 
    start_delay = 0, 
    client_api_url = '/client/', 
    max_connect_attempts = 50, 
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf.ini')

http_download.run()
