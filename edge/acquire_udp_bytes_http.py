import gateway.daqc


sie_acquire_udp_http = gateway.daqc.UdpBytesHttp(
    port = 61011,
    config_filepath = '/srv/dogger/', 
    config_filename = 'conf.ini')

sie_acquire_udp_http.run()
