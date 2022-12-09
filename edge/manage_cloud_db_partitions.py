import gateway.maint


partition = gateway.maint.PartitionCloudDatabase(
    start_delay = 0,
    maint_api_url = '/maint/',
    keep_partitions_horizon = 10)

partition.run()
