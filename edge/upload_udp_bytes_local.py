import gateway.link


mob_upload_sql_udp = gateway.link.SqlUdpRawBytes(
    #channels = {144},
    channels = {144,145,150,151,155},
    start_delay = 0,
    #transmit_rate = 1,
    transmit_rate = 0.5,
    port = 61011,
    max_age = 10,
    config_filename = 'conf_local.ini')

mob_upload_sql_udp.run()
