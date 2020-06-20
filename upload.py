import gateway.uplink


http_upload = gateway.uplink.Replicate(
    channels = {20,21,23,24,40,97,98,99,100,161,500}, 
    start_delay = 0, 
    gateway_database_connection = {"host": "host", "user": "user", "passwd": "passwd", "db": "db"}, 
    host_api_url = '/host/', 
    client_api_url = '/client/', 
    ip_list = ['109.74.8.89'], 
    max_connect_attempts = 50, 
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf_upload.ini')

http_upload.run()
