import gateway.link


udp_upload_local = gateway.link.SqlUdpNmeaValue(
    channels = {143}, 
    start_delay = 0,
    transmit_rate = 1, 
    port = 4444,
    multiplier = 0.001,
    decimals = 5,
    nmea_prepend = 'IIMDA,,I,',
    nmea_append = ',B,,C,,C,,,,C,,T,,M,,N,,M',
    max_age = 10,
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf_udp_local.ini')

udp_upload_local.run()
