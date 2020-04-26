cdaq_device = CdaqDevice(...)
cdaq_task = CdaqTask(...)

cdaq_device.connect()

cdaq_task.add_channels(cdaq_device.module_in_slot(1).channels_in_module(20:23))
