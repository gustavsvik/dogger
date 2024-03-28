import gateway.daqc


sie_acquire_udp_http = gateway.daqc.HttpNetmoreJsonFile(
    channels = {'NETMORE':{0:'txt',152:'txt'}},
    start_delay = 0,
    sample_rate = 1/(1.0*2),
    transmit_rate = 1/(1.0*2),
    config_filename = 'conf_netmore_api.ini')

sie_acquire_udp_http.run()
