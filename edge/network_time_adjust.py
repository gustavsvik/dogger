from gateway.maintenance import NetworkTime
network_time = NetworkTime()
network_time.run_continuous_adjustment(600)
