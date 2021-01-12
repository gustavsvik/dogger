import gateway.link


http_download = gateway.link.HttpSql(
    channels = {61011,61012,143,97,98,144,145,162}, 
    start_delay = 0, 
    transmit_rate = 1, 
    client_api_url = '/client/', 
    max_connect_attempts = 50, 
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf_opener.ini')

http_download.run()
