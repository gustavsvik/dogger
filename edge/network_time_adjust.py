import gateway.maint


network_time = gateway.maint.NetworkTime(
    start_delay = 0,
    ntp_url = 'ntp.se',
    ntp_port = 123,
    adjust_interval = 600,
    max_connect_attempts = 50)

network_time.run()
