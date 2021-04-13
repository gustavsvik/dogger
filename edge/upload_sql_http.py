import gateway.link


sie_upload_sql_http = gateway.link.SqlHttp(
    channels = {168,169,170,148}, 
    start_delay = 0, 
    transmit_rate = 0.5,
    max_age = 10,
    max_connect_attempts = 50, 
    config_filepath = '/srv/dogger/', 
    config_filename = 'conf.ini')

sie_upload_sql_http.run()
