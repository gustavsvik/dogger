import gateway.uplink


http_upload_binary_local = gateway.uplink.Replicate(
    channels = {140,160,180,600,10002}, 
    start_delay = 0, 
    host_api_url = '/host/', 
    client_api_url = '/client/', 
    max_connect_attempts = 50,
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf_local.ini')

http_upload_binary_local.run()
