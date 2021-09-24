import gateway.maint


network_time = gateway.maint.NetworkTime(
    start_delay = 0,
    ntp_url = 'ntp.se',
    ntp_port = 123,
    adjust_interval = 600,
    max_connect_attempts = 50,
    config_filepath = '/home/heta/Z/app/python/dogger/',
    config_filename = 'conf.ini')

network_time.run()
