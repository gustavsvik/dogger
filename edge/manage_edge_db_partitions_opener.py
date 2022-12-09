import gateway.maint


partition = gateway.maint.PartitionEdgeDatabase(
    start_delay = 0,
    keep_partitions_horizon = 10,
    config_filename = 'conf_opener.ini')

partition.run()
