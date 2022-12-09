import gateway.persist


accumulate_acquired = gateway.persist.Accumulate(
    channels = {20,21,22,23,24,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112},
    start_delay = 20)

accumulate_acquired.run()
