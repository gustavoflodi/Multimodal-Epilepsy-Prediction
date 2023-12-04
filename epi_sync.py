import pandas as pd
import seerpy

# Connect to the Seer platform
def connect_seer():
    return seerpy.SeerConnect()

# Get study metadata by name
def get_study_metadata(client, study_name):
    return pd.DataFrame(client.get_all_study_metadata_dataframe_by_names(study_name))

# Filter metadata for a specific sensor
def filter_sensor_metadata(metadata, sensor_name):
    return metadata[metadata['channels.name'] == sensor_name].sort_values(by='segments.startTime', ascending=True)

# Merge two metadata DataFrames based on specified columns
def merge_metadata(meta_data_old, meta_data, columns_to_merge):
    return meta_data_old.merge(meta_data, on=columns_to_merge)

# Get synchronized metadata for a list of sensors in a study
def get_sync_metadata(client, study_name, sensors_list):
    study_metadata = get_study_metadata(client, study_name)
    
    # Initialize with metadata for the first sensor
    first_sensor = sensors_list[0]
    meta_data_old = filter_sensor_metadata(study_metadata, first_sensor)
    meta_data_dict = {sensors_list[0]:meta_data_old}
    
    # Iterate through the remaining sensors
    for sensor in sensors_list[1:]:
        sensor_metadata = filter_sensor_metadata(study_metadata, sensor)
        meta_data_dict[sensor] = sensor_metadata
        meta_data_old = merge_metadata(meta_data_old, sensor_metadata, 
                                        ['segments.duration', 'segments.startTime', 'channelGroups.sampleRate'])
    
    return meta_data_old.sort_values(by='segments.startTime', ascending=True), meta_data_dict
    

    