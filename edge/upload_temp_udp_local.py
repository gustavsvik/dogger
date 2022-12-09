import gateway.link


udp_upload_local = gateway.link.SqlUdpNmeaValue(
    channels = {98},
    start_delay = 0,
    transmit_rate = 1,
    port = 4444,
    multiplier = 1.0,
    decimals = 2,
    nmea_prepend = 'IIXDR,C,',
    nmea_append = ',C,ENV_WATER_T',
    max_age = 10,
    config_filename = 'conf_udp_local.ini')

udp_upload_local.run()
