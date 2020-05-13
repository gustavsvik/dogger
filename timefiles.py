def get_all_files(path, extensions, channel) :

    import glob

    files = []
    for extension in extensions :
        file_pattern = str(channel) + '_*.' + extension
        pattern = path + file_pattern
        files += sorted(glob.glob(pattern))

    return files

    
def delete_multiple(files) :

    import os

    for current_file in files:
        try :
            os.remove(current_file)
        except (PermissionError, FileNotFoundError) as e:
            print(e)


def get_timestamped_range(files, lower_timestamp, higher_timestamp) :

    selected_files = []
    for current_file in files :
        start = current_file.rfind('_')
        end = current_file.rfind('.')
        current_timestamp_string = current_file[start+1:end]
        current_timestamp = int(current_timestamp_string)
        if current_timestamp <= higher_timestamp and current_timestamp >= lower_timestamp :
            selected_files.append(current_file)

    return selected_files


def get_file_timestamp(current_file = None):

    before_start_position = current_file.find("_")
    after_end_position = current_file.find(".")
    acquired_time_string = current_file[before_start_position+1:after_end_position]
    acquired_time = int(acquired_time_string)

    return acquired_time


def get_file_local_datetime(current_file = None, datetime_pattern = '%Y%m%d%H%M%S'):

    import datetime
    import time

    before_start_position = current_file.find("_")
    after_end_position = current_file.find(".")
    acquired_local_datetime_string = current_file[before_start_position+1:after_end_position]
    local_datetime = datetime.datetime.strptime(acquired_local_datetime_string, datetime_pattern)
    acquired_time = int(time.mktime(local_datetime.timetuple()))

    return acquired_time


def get_file_channel(current_file = None):

    position_after = current_file.find("_")
    string_before_timestamp = current_file[0:position_after]
    position_before = current_file.rfind("/")
    channel_string = current_file[position_before+1:position_after]
    channel = int(channel_string)

    return channel
