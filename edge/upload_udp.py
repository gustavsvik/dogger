import gateway.uplink


udp_upload = gateway.uplink.UdpRaw(
    channels = {61011,61012}, 
    start_delay = 0,
    port = 61010,
    max_age = 10,
    config_filepath = '/srv/dogger/', 
    config_filename = 'conf.ini')

udp_upload.run()
