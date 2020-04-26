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

