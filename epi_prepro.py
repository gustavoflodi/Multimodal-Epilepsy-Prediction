from tsfresh import extract_features
from tsfresh.feature_extraction.settings import MinimalFCParameters
import pandas as pd
import seerpy

def get_info(concatenated_data, meta_data):
    substring = 'channels.name'
    sensors = meta_data.filter(regex=substring, axis=1)
    nulls = dict()
    outliers = dict()
    for sensor in sensors:
        IQR = concatenated_data[sensor].describe().loc['75%'] - concatenated_data[sensor].describe().loc['25%']
        selection_max = concatenated_data[sensor] >= concatenated_data[sensor].describe().loc['75%'] + 1.5*IQR
        selection_min = concatenated_data[sensor] < concatenated_data[sensor].describe().loc['25%'] - 1.5*IQR
        print(f"For sensor {sensor}:\n",
            f"The absolute number/total of null values is: {sum(concatenated_data[sensor].isna())}"+"/{concatenated_data.shape[0]}",
            f"And the outliers are  {(concatenated_data[sensor][selection_max | selection_min])}"
            f"{(concatenated_data[selection_max | selection_min].label.value_counts())}",
            f"Finally, the basic stats are {concatenated_data[sensor].describe()}",)
        nulls[sensor] = list(concatenated_data[concatenated_data[sensor].isna()].time)
        outliers[sensor] = list(concatenated_data[sensor][selection_max | selection_min].time)
    return nulls, outliers

def normalize_dataframe(data):
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
    for column in data.columns:
        min_val = data[column].min()
        max_val = data[column].max()
        normalized_df[column] = (data[column] - min_val) / (max_val - min_val)
    
    return normalized_df


def remove_outliers_and_nulls(nulls, outliers, concatenated_data, meta_data):
    
    substring = 'channels.name'
    sensors = meta_data.filter(regex=substring, axis=1)
    
    nulls_timestamps = set(null for sublist in list(nulls.values()) for null in sublist)
    outliers_timestamps = set(outlier for sublist in list(outliers.values()) for outlier in sublist)
    criteria_nulls = (concatenated_data[sensors].loc[nulls_timestamps]) & (concatenated_data['label'] == 0)
    criteria_outliers = (concatenated_data[sensors].loc[outliers_timestamps]) & (concatenated_data['label'] == 0)
    concatenated_data = concatenated_data[~criteria_nulls]
    concatenated_data = concatenated_data[~criteria_outliers]

    return None


def windowing(concatenated_data, labels):

    window_size = min([label['duration'] for label in labels])

    window_features = []
    label_window = []

    # Iterate over time windows
    for count_window in range(len(concatenated_data) - window_size + 1):
        window_selection = ((concatenated_data.time >= min(concatenated_data.time) + count_window*window_size) &
                            (concatenated_data.time < concatenated_data.time + count_window*window_size))
        # Extract time window
        window_df = concatenated_data[window_selection].drop(columns=['label'])

        label_list = concatenated_data['label'].unique()

        if 1 in label_list:
            label_window.append(1)
        elif 2 in label_list:
            label_window.append(2)
        else:
            label_window.append(0)

        window_df['id'] = count_window
        # Perform feature extraction for the window
        settings = MinimalFCParameters()
        window_extracted_features = extract_features(window_df, column_id='id', column_sort='time', default_fc_parameters=settings)
        
        # Store the features for this window
        window_features.append(window_extracted_features)

    # Concatenate features from all windows
    result_df = pd.concat(window_features)
    result_df = result_df.concat(label_window, axis=1)

    return result_df