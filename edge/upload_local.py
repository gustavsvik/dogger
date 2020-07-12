import gateway.uplink


http_upload_local = gateway.uplink.Replicate(
    channels = {20,21,23,24,40,97,98,161,500}, 
    start_delay = 0, 
    host_api_url = '/host/', 
    client_api_url = '/client/', 
    max_connect_attempts = 50,
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf_local.ini')

http_upload_local.run()
