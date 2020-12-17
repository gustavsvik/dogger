import gateway.link


mob_upload_sql_udp = gateway.link.SqlUdpRaw(
    channels = {61011,61012}, 
    start_delay = 0,
    port = 61010,
    max_age = 10,
    config_filepath = '/srv/dogger/', 
    config_filename = 'conf.ini')

mob_upload_sql_udp.run()
