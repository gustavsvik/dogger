import dogger.store.host

numpy_sql = dogger.store.host.Numpy(channels = {21,23,20,24,22})
numpy_sql.run()


#while True:
#    time.sleep(0.5)
#    for channel_index in CHANNELS:
#        insert_failure = False
#        files = numpy_sql.get_filenames(channel = channel_index)        
#        print('channel_index', channel_index, 'len(files)', len(files))
#        if len(files) > 2 :
#            numpy_sql.connect_db()
#            for current_file in files:
#                if len(files) > 2:
#                    numpy_sql.retrieve_file_data(current_file)
#                    store_result = numpy_sql.store_file_data()
#                    if store_result <= -1: insert_failure = True
#                if not insert_failure:
#                    numpy_sql.commit_transaction()
#            numpy_sql.close_db_connection()
