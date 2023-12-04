import pandas as pd
import seerpy

def get_raw_data(client, meta_data, sensors_list, meta_data_dict):
    substring = 'segments.id'
    segments_ids = meta_data.filter(regex=substring, axis=1)
    
    segments_ids.columns = sensors_list
    data = dict()
    for sensor, df in zip(sensors_list, meta_data_dict.values()):

        filter_ids = pd.DataFrame(segments_ids[sensor])
        filter_ids.columns = ['segments.id']
        # Merge df1 and df2 to identify matching rows
        merged_df = df.merge(filter_ids, on='segments.id', how='inner', indicator=True)
        
        # Filter the rows in df1 that were identified as both in df1 and df2
        filtered_df = merged_df[merged_df['_merge'] == 'both']
        
        # Drop the '_merge' column, which was used for identification
        filtered_df = filtered_df.drop(columns='_merge')
        data[sensor] = client.get_channel_data(filtered_df).sort_values(by='time', ascending=True).set_index('time')

    concatenated_data = pd.concat(data.values(), axis=1)

    return concatenated_data
