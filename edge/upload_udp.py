import gateway.link


mob_upload_sql_udp = gateway.link.SqlUdpRaw(
    channels = {142,98}, 
    start_delay = 0,
    port = 61010,
    max_age = 10,
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf.ini')

mob_upload_sql_udp.run()
