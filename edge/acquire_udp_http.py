import gateway.daqc


sie_acquire_udp_http = gateway.daqc.UdpValueHttp(
    port = 61010,
    config_filepath = '/srv/dogger/', 
    config_filename = 'conf.ini')

sie_acquire_udp_http.run()
