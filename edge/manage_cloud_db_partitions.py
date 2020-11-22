import gateway.maint


partition = gateway.maint.PartitionCloudDatabase(
    start_delay = 0,
    maint_api_url = '/maint/',
    keep_partitions_horizon = 10,
    config_filepath = '/home/heta/Z/app/python/dogger/',
    config_filename = 'conf.ini')

partition.run()
