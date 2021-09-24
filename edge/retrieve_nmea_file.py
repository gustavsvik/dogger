import gateway.link


mob_upload_sql_udp = gateway.link.SqlFileRawBytes(
    channels = {164},
    start_delay = 0,
    transmit_rate = 0.5,
    nmea_prepend = 'GPGGA,',
    nmea_append = ',1,9,0.91,44.7,M,24.4,M,,',
    max_age = 10,
    max_number = 1,
    target_channels = { 'GGA':{164:'txt'} },
    file_path = '/home/heta/Z/data/files/others/',
    archive_file_path = '/home/heta/Z/data/files/others/',
    config_filepath = '/home/heta/Z/app/python/dogger/',
    config_filename = 'conf.ini')

mob_upload_sql_udp.run()
