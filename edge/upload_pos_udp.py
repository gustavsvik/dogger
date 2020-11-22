import gateway.uplink


udp_upload_pos = gateway.uplink.UdpNmeaPos(
    channels = {61011,61012}, 
    start_delay = 0,
    port = 62010,
    nmea_prepend = 'GPGLL,',
    nmea_append = ',A,A',
    max_age = 10,
    config_filepath = '/srv/dogger/', 
    config_filename = 'conf.ini')

udp_upload_pos.run()
