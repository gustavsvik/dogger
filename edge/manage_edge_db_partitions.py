import gateway.maint


partition = gateway.maint.PartitionEdgeDatabase(
    start_delay = 0,
    keep_partitions_horizon = 10,
    config_filepath = '/home/scc01/Z/dogger/',
    config_filename = 'conf.ini')

partition.run()
