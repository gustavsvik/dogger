import gateway.link


mob_upload_sql_udp = gateway.link.SqlUdpNmeaLines(
    channels = {144,145}, 
    start_delay = 0,
    transmit_rate = 0.5, 
    port = 3265,
    max_age = 20,
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf_udp_aishub.ini')

mob_upload_sql_udp.run()
