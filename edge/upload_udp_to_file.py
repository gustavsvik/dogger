import gateway.link


mob_upload_sql_udp = gateway.link.SqlUdpRawValue(
    channels = {168,169},
    start_delay = 0,
    transmit_rate = 0.5, 
    port = 61012,
    max_age = 10,
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf.ini')

mob_upload_sql_udp.run()
