import gateway.link


udp_upload_ais = gateway.link.SqlUdpBinaryBroadcastMessageAreaNoticeCircle(
    channels = {61011,61012}, 
    start_delay = 0,
    transmit_rate = 1, 
    port = 4444,
    max_age = 10,
    mmsi = 123456789,
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf_udp_local.ini')

udp_upload_ais.run()
