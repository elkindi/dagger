import pandas as pd
import numpy as np
import holoviews as hv
import seaborn as sns
import string


rooms = string.ascii_lowercase
n_patients = 100000
raw_data = pd.DataFrame()


for i in range(n_patients):

    num_ts = np.random.randint(1, 10)
    timestamp = np.random.randint(1, 10, num_ts)
    id_rooms = np.random.randint(1, 20, num_ts)

    current_rooms = id_rooms
    id = np.repeat(i, num_ts)

    current_data = pd.DataFrame([id, timestamp, current_rooms]).transpose()
    raw_data = pd.concat([raw_data, current_data], ignore_index=True)

print('Step 1')
raw_data.columns = np.asarray(['ID', 'c', 'room'])
data_list = [list(row) for row in raw_data.loc[ :,  ['ID', 'c']].itertuples(index=False)]

MGH_ID = raw_data.ID
data = raw_data.copy()
data = data.drop_duplicates(subset=['ID','c'], keep="last")


# data_no_dupl.to_csv('data.csv', index = False)
# data = pd.read_csv('test_scripts/data/data.csv')

print('Step 2')
ID_time_data = data.iloc[ : , 0:2]

print('Step 3')
data_ord = data.sort_values(by =['ID', 'c'])
data_ord = data_ord.reset_index()
n_rows = data_ord.shape[0]

data = data_ord.copy()
indexes = list(range(n_rows))
data['row'] = indexes
data2 = data.copy()
data2['row'] = [i+1 for i in indexes]


data_merge = data.merge( data2, how = 'left',
                         on = ['ID', 'row'],
                         suffixes=['_out', '_in'])

