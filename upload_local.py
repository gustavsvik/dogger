import gateway.uplink


http_upload_local = gateway.uplink.Http(
    channels = {20,21,23,24,40,97,161,500}, 
    start_delay = 0, 
    gateway_database_connection = {"host": "localhost", "user": "root", "passwd": "admin", "db": "test"}, 
    cloud_api_url = '/host/', 
    ip_list = ['192.168.1.103'], 
    max_connect_attempts = 50,
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf_upload.ini')

http_upload_local.run()
