import gateway.link


http_download = gateway.link.HttpSql(
    channels = {143,97,98},
    start_delay = 0,
    transmit_rate = 1,
    client_api_url = '/client/',
    max_connect_attempts = 50,
    config_filename = 'conf_opener.ini')

http_download.run()
