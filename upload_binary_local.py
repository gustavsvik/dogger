import gateway.uplink


http_upload_binary_local = gateway.uplink.Replicate(
    channels = {140,160,180,600,10002}, 
    start_delay = 0, 
    gateway_database_connection = {"host": "host", "user": "user", "passwd": "passwd", "db": "db"}, 
    host_api_url = '/host/', 
    client_api_url = '/client/', 
    ip_list = ['192.168.1.103'], 
    max_connect_attempts = 50,
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf_upload.ini')

http_upload_binary_local.run()
