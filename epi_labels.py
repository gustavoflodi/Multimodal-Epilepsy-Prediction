import pandas as pd
import seerpy

def get_labels(client, meta_data, concatenated_data):
    concatenated_data['label'] = 0 # no seizure
    
    study_id = meta_data.iloc[0]['id']  # 9ef709ff-51f6-493f-aabb-3fa8eb3eca12
    label_group_id = client.get_label_groups_for_studies_dataframe(study_id)['labelGroup.id'][0] # 2dd69519-2c34-436a-bcc1-6a45a8001203
    labels = client.get_labels(study_id, label_group_id)['labelGroup']['labels']


    for label in labels:
        selection_ictal = (label['startTime'] <= concatenated_data.index) &  (label['startTime'] + label['duration'] >= concatenated_data.index)
        selection_preictal = (label['startTime'] - 30*60*1000 <= concatenated_data.index) &  (label['startTime'] > concatenated_data.index)
        concatenated_data['label'] = concatenated_data.label.where(~selection_ictal, 1) # seizure
        concatenated_data['label'] = concatenated_data.label.where(~selection_preictal, 2) # pre-seizure

    return labels
