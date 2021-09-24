import gateway.link


http_upload_local = gateway.link.Replicate(
    channels = {20,21,23,24,40,97,98,143,161,500},
    start_delay = 0,
    transmit_rate = 1,
    host_api_url = '/host/',
    max_connect_attempts = 50,
    config_filepath = '/home/heta/Z/app/python/dogger/',
    config_filename = 'conf_local.ini')

http_upload_local.run()
