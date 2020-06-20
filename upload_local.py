import gateway.uplink


http_upload_local = gateway.uplink.Replicate(
    channels = {20,21,23,24,40,97,161,500}, 
    start_delay = 0, 
    gateway_database_connection = {"host": "host", "user": "user", "passwd": "passwd", "db": "db"}, 
    host_api_url = '/host/', 
    client_api_url = '/client/', 
    ip_list = ['127.0.0.1'], 
    max_connect_attempts = 50,
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf_upload.ini')

http_upload_local.run()
