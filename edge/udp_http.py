import gateway.daqc


udp_http = gateway.daqc.UdpHttp(
    port = 61010,
    start_delay = 0,
    config_filepath = '/srv/dogger/', 
    config_filename = 'conf.ini')

udp_http.run()
