"""Module/Script Description.

This module/script provides the functions to label the data.

Author: Gustavo Lodi
Date: 01/12/2023

Functions:
    get_labels: based on duration and start times, classify the rows with the three labels.

"""

import pandas as pd
import dask.dataframe as dd
import seerpy

def get_labels(client, meta_data, concatenated_data):
    concatenated_data['label'] = 0  # no seizure
    
    study_id = meta_data.iloc[0]['id']
    label_group_id = client.get_label_groups_for_studies_dataframe(study_id)['labelGroup.id'][0]
    labels = client.get_labels(study_id, label_group_id)['labelGroup']['labels']

    for label in labels:
        selection_ictal = (label['startTime'] <= concatenated_data.index) &  (label['startTime'] + label['duration'] >= concatenated_data.index)
        selection_preictal = (label['startTime'] - 30*60*1000 <= concatenated_data.index) &  (label['startTime'] > concatenated_data.index)

        meta = pd.DataFrame({'label': [0]}, index=concatenated_data.index)

        # Use .map with meta to specify the metadata explicitly
        concatenated_data['label'] = concatenated_data['label'].map(
            lambda x: x if not selection_ictal.any() else 1, 
            meta=meta['label']
        )  # seizure
        
        concatenated_data['label'] = concatenated_data['label'].map(
            lambda x: x if not selection_preictal.any() else 2, 
            meta=meta['label']
        )  # pre-seizure

    return labels, concatenated_data
