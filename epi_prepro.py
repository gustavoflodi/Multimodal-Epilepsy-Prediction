"""Module/Script Description.

This module/script provides the functions to pre-process the data.

Author: Gustavo Lodi
Date: 01/12/2023

Functions:
    get_info: get list of outliers and null rows.
    remove_outliers_and_nulls: remove the outliers and null rows based on non-ictal or pre-ictal condition.
    normalize_dataframe: normalize the values with min-max strategy.
    windowing: get time windows to extract features.

"""

import pandas as pd
import seerpy
import numpy as np


def get_info(concatenated_data, meta_data):
    concatenated_data = concatenated_data.compute()
    substring = 'channels.name'
    sensors = np.array(meta_data.filter(regex=substring, axis=1).loc[0])
    nulls = dict()
    outliers = dict()
    for sensor in sensors:
        print(sensor)
        IQR = concatenated_data[sensor].describe().loc['75%'] - concatenated_data[sensor].describe().loc['25%']
        # print(IQR)
        selection_max = concatenated_data[sensor] >= concatenated_data[sensor].describe().loc['75%'] + 1.5*IQR
        selection_min = concatenated_data[sensor] < concatenated_data[sensor].describe().loc['25%'] - 1.5*IQR
        
        print(f"For sensor {sensor}:\n",
            f"The absolute number/total of null values is: {sum(concatenated_data[sensor].isna())}"+"/"+f"{concatenated_data.shape[0]}\n\n",
            f"And the absolute number of outliers is {(concatenated_data[selection_max | selection_min][sensor].shape[0])}\n\n",
            f"Now the count of the labels for the outlier's rows is {(concatenated_data[selection_max | selection_min].label.value_counts())}\n\n",
            f"Finally, the basic stats are {concatenated_data[sensor].describe()}\n\n\n\n")
        
        nulls[sensor] = list(concatenated_data[concatenated_data[sensor].isna()].time)
        outliers[sensor] = list(concatenated_data[selection_max | selection_min][['time', sensor]].time)
    
    return nulls, outliers


def remove_outliers_and_nulls(nulls, outliers, concatenated_data, meta_data):
    
    concatenated_data = concatenated_data.compute()
    
    substring = 'channels.name'
    sensors = np.array(meta_data.filter(regex=substring, axis=1).loc[0])
    
    # Getting the intersection of the null timestamps
    common_null_timestamps = set(nulls[sensors[0]])
    
    new_nulls = {k:v for k, v in nulls.items() if k != sensors[0]} 
    
    for sensor, timestamps in new_nulls.items():
        common_null_timestamps = common_null_timestamps.intersection(timestamps)
    
    common_null_timestamps = np.array(common_null_timestamps)
    
    # Getting the intersection of the outliers timestamps
    common_outlier_timestamps = set(outliers[sensors[0]])
    
    new_outliers = {k:v for k, v in outliers.items() if k != sensors[0]} 
    
    for sensor, timestamps in new_outliers.items():
        common_outlier_timestamps = common_outlier_timestamps.intersection(timestamps)
    
    common_outlier_timestamps = np.array(common_outlier_timestamps)
    
    # nulls_timestamps = set(null for sublist in list(nulls.values()) for null in sublist)
    # outliers_timestamps = set(outlier for sublist in list(outliers.values()) for outlier in sublist)
    
    criteria_nulls = (concatenated_data[sensors].loc[common_null_timestamps]) & (concatenated_data['label'] == 0)
    criteria_outliers = (concatenated_data[sensors].loc[common_outlier_timestamps]) & (concatenated_data['label'] == 0)
    concatenated_data = concatenated_data[~criteria_nulls]
    concatenated_data = concatenated_data[~criteria_outliers]

    return None


def normalize_dataframe(data, sensors_list):
    """
    Normalize each column in a DataFrame using Min-Max scaling.
    
    Parameters:
    df (pd.DataFrame): The input DataFrame with columns to be normalized.
    
    Returns:
    pd.DataFrame: A new DataFrame with normalized values.
    """
    # Copy the input DataFrame to avoid modifying the original data
    normalized_df = data.copy()
    
    # Iterate through columns and normalize each one
    for column in sensors_list:
        min_val = data[column].min()
        max_val = data[column].max()
        normalized_df[column] = (data[column] - min_val) / (max_val - min_val)
    
    return normalized_df


def windowing(concatenated_data, labels):

    window_size = min([label['duration'] for label in labels])
    # window_size = 3

    window_features = []
    label_window = []
    
    # Iterate over time windows
    for count_window in range(int(len(concatenated_data)/window_size) + 1):
        
        low_end = (min(concatenated_data.index) + count_window*window_size)
        high_end = (min(concatenated_data.index) + (count_window+1)*window_size)
        
        window_selection = (
                            (concatenated_data.index >= low_end)
                            &
                           (concatenated_data.index < high_end)
                            )
        
        # Extract time window
        timestamps_window = np.mean(concatenated_data[window_selection].time)
        window_df = concatenated_data[window_selection].drop(columns=['time', 'label'])
        
        label_list = concatenated_data[window_selection]['label'].unique()
        
        if 1 in label_list:
            label_window.append(1)
        elif 2 in label_list:
            label_window.append(2)
        else:
            label_window.append(0)

        window_df['id'] = count_window
        
        # Perform feature extraction for the window
        stat_features = ["mean", "std", "min", "max"]
        
        features_df = window_df.groupby("id").agg({
            'Acc Mag': stat_features,
            'TEMP': stat_features,
            'EDA': stat_features
        })
        
        # Store the features for this window
        window_features.append(features_df)

    # Concatenate features from all windows
    # print(window_features)
    label_window = pd.Series(label_window)
    
    result_df = pd.concat(window_features, axis=0)
    result_df = pd.concat([label_window, result_df], axis=1)
    result_df.rename(columns={0: "label"}, inplace=True)
    return result_df
