import gateway.daqc


cmems_acquire_task = gateway.daqc.NativeCmemsNumpyFile(
    channels = {'NETCDF':{154:'txt'}},
    start_delay = 0,
    sample_rate = 1/(60*20),
    service_id = 'BALTICSEA_ANALYSISFORECAST_WAV_003_010-TDS',
    product_id = 'cmems_mod_bal_wav_anfc_PT1h-i',
    out_file_name = 'BALTICSEA_ANALYSISFORECAST_WAV_003_010-TDS.nc',
    config_filename = 'conf_cmems_api.ini')

cmems_acquire_task.run()
