import gateway.link


udp_upload_ais = gateway.link.SqlUdpAivdmPosition(
    channels = {61011,61012}, 
    start_delay = 0,
    transmit_rate = 1, 
    port = 4444,
    max_age = 10,
    mmsi = 123456789,
    vessel_name = "SERVICE/SAFETY UNIT", 
    call_sign = "FJJ976", 
    ship_type = 33,
    nav_status = 3,
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf_udp_local.ini')

udp_upload_ais.run()
