import gateway.daqc


cmems_acquire_task = gateway.daqc.NativeCmemsNumpyFile(
    channels = {'NETCDF':{153:'npy'}},
    start_delay = 0,
    sample_rate = 1/(60*15),
    service_id = 'BALTICSEA_ANALYSISFORECAST_WAV_003_010-TDS',
    product_id = 'dataset-bal-analysis-forecast-wav-hourly',
    out_file_name = 'BALTICSEA_ANALYSISFORECAST_WAV_003_010-TDS.nc',
    file_path = '/srv/dogger/files/',
    archive_file_path = '/srv/dogger/files/',
    config_filepath = '/srv/dogger/',
    config_filename = 'conf_cmems_api.ini')

cmems_acquire_task.run()
