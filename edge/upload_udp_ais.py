import gateway.uplink


udp_upload_ais = gateway.uplink.UdpAis(
    channels = {60000,60001}, 
    start_delay = 0,
    port = 60000,
    max_age = 10,
    config_filepath = '/srv/dogger/', 
    config_filename = 'conf.ini')

udp_upload_ais.run()
