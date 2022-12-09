import gateway.maint


partition = gateway.maint.PartitionEdgeDatabase(
    start_delay = 0,
    keep_partitions_horizon = 10)

partition.run()
