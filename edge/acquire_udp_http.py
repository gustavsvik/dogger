import gateway.daqc


sie_acquire_udp_http = gateway.daqc.UdpValueHttp(
    channels = set(), 
    start_delay = 0, 
    port = 61010,
    max_connect_attempts = 50, 
    config_filepath = '/srv/dogger/', 
    config_filename = 'conf.ini')

sie_acquire_udp_http.run()
