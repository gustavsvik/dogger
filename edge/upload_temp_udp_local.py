import gateway.uplink


udp_upload_local = gateway.uplink.UdpNmea(
    channels = {98}, 
    start_delay = 0,
    port = 4444,
    nmea_prepend = 'IIXDR,C,',
    nmea_append = ',C,ENV_WATER_T',
    max_age = 10,
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf_udp_local.ini')

udp_upload_local.run()