import gateway.link


udp_upload_ais = gateway.link.SqlHttpUpdateStatic(
    channels = {XXX}, 
    start_delay = 0,
    transmit_rate = 0.5, 
    max_age = 10,
    max_connect_attempts = 50, 
    target_channels = {'VDM':{'mmsi|t_device/DEVICE_HARDWARE_ID':'json'}},
    config_filepath = '/srv/dogger/', 
    config_filename = 'conf_cloud_db.ini')

udp_upload_ais.run()
