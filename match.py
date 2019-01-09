# Name: Lavanya Surikapuram
# date: 4/12/2018
# descriptions: match data Program.
# File Name: match.py

import pandas as pd
import numpy as np
Percentage = []

# Loading data political_data_vendor
A = pd.read_csv('political_data_vendor.csv', names=['political_id', 'first_name', 'last_name', \
                                                    'city', 'birth_year', 'gender'], \
                dtype={'political_id': 'str', 'first_name': 'str', 'last_name': 'str', 'city': \
                       'str', 'birth_year': 'str',  'gender': 'str' }, header=1, skiprows=[0])


# Filtering political_data_vendor(To find the common last name)
df1 = A.replace(np.nan, '', regex=True)
df1_not_null_fs= df1[(df1['first_name']!="null")]
df1_not_null_ls= df1_not_null_fs[(df1_not_null_fs['last_name']!="null")]
del df1_not_null_ls['first_name']

# Filtering political_data_vendor(To find the common first name)
ds1 = A.replace(np.nan, '', regex=True)
ds1_not_null_fs= ds1[(ds1['first_name']!="null")]
ds1_not_null_ls= ds1_not_null_fs[(ds1_not_null_fs['last_name']!="null")]
del ds1_not_null_ls['last_name']


# Loading data resume_data_vendor
B = pd.read_csv('resume_data_vendor.csv', names=['resume_id', 'first_name', 'last_name', 'degree',\
                                                 'degree_start', 'local_region'], \
                dtype={'resume_id': 'str', 'first_name': 'str', 'last_name': 'str', 'degree': 'str',\
                       'degree_start': 'str',  'local_region': 'str' }, header=1, \
                error_bad_lines=False, skiprows=[0])

# Filtering resume_data_vendor(To find the common last name)
df2 = B.replace(np.nan, '', regex=True)
df2_not_null_fs= df2[(df2['first_name']!="null")]
df2_not_null_ls= df2_not_null_fs[(df2_not_null_fs['last_name']!="null")]
del df2_not_null_ls['first_name']

# Filtering resume_data_vendor(To find the common first name)
ds2 = B.replace(np.nan, '', regex=True)
ds2_not_null_fs= ds2[(ds2['first_name']!="null")]
ds2_not_null_ls= ds2_not_null_fs[(ds2_not_null_fs['last_name']!="null")]
del ds2_not_null_ls['last_name']


# commaon Function that merges two data frames
def outer_parts(dfM1, dfM2):
    dfM3 = dfM1.merge(dfM2, indicator=True, how='outer')
    return {n: g.drop('_merge', 1) for n, g in dfM3.groupby('_merge')}

# Merge of dataframes with common Last Name
dfs = outer_parts(df1_not_null_ls, df2_not_null_ls)
dfs_1 = dfs['both']

# Appending a extra column which denotes the same_last_name(If the value is 1)
dfs_1['same_last_name']='1'

# Merge of dataframes with common First Name
ds_m = outer_parts(ds1_not_null_ls, ds2_not_null_ls)
dfs_2 = ds_m['both']

# Appending a extra column which denotes the same_first_name(If the value is 1)
dfs_2['same_first_name']='1'

# Merging the same_first_name and same_last_name dataframes based on 'political_id', 'resume_id'
dfall = pd.merge(dfs_1, dfs_2, on=['political_id', 'resume_id'], how='outer')
dfall_no_nan = dfall.replace(np.nan, '', regex=True)

# For each row in the column,
for index, row in dfall_no_nan.iterrows():  
    if row['same_last_name'] == '1' or row['same_first_name'] == '1':
        if row['same_last_name'] == '1' and row['same_first_name'] == '1':
            if row['local_region_y'].split(':')[0] == row['city_y']:
                Percentage.append('90')
            else:
                Percentage.append('80')
        elif row['same_last_name'] == '1':
            if row['local_region_y'].split(':')[0] == row['city_y']:
                Percentage.append('60')
            else:
                Percentage.append('50')
        elif row['same_first_name'] == '1':
            if row['local_region_y'].split(':')[0] == row['city_y']:
                Percentage.append('60')
            else:
                Percentage.append('50')    
    else:
        Percentage.append('0')
        
dfall_no_nan['Percentage'] = Percentage


dfall_no_nan_sub = dfall_no_nan[['political_id', 'resume_id', 'same_first_name', 'same_last_name', 'Percentage']].copy()

print(dfall_no_nan_sub)

dfall_no_nan_sub.to_csv('exact_matches.csv')
