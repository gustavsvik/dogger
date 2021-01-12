import gateway.link


mob_upload_sql_udp = gateway.link.SqlUdpRawValue(
    channels = {143,97,98}, 
    start_delay = 0,
    transmit_rate = 1, 
    port = 61010,
    max_age = 10,
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf.ini')

mob_upload_sql_udp.run()
