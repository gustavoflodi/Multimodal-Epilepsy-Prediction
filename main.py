import epi_sync as es
import epi_raw_data as er
import epi_labels as el

client = es.connect_seer()

study_name = input('Please write the desired study')

sensors_list = ['Acc Mag', 'TEMP', 'EDA']

meta_data, meta_data_dict = es.get_sync_metadata(client, study_name, sensors_list)
print(meta_data)
# print(meta_data_dict)

raw_data = er.get_raw_data(client, meta_data, sensors_list, meta_data_dict)

# labels = el.get_labels(client, meta_data, raw_data)