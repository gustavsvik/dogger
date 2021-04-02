import gateway.link


udp_upload_ais = gateway.link.SqlFileAisData(
    channels = {144}, 
    start_delay = 0,
    transmit_rate = 0.5, 
    max_age = 10,
    file_path = '/srv/dogger/files/',
    config_filepath = '/srv/dogger/', 
    config_filename = 'conf_cloud_db.ini')

udp_upload_ais.run()
