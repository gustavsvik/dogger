import gateway.link


udp_upload_pos_local = gateway.link.SqlUdpNmeaPos(
    channels = {61011,61012},
    start_delay = 0,
    transmit_rate = 1,
    port = 4444,
    nmea_prepend = 'GPGLL,',
    nmea_append = ',A',
    max_age = 10,
    config_filename = 'conf_udp_local.ini')

udp_upload_pos_local.run()
