from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
import collections

def kill_underscore(row,feature,labels,flag):
    lst = row[feature].split('_')
    for i, word in enumerate(lst):
        for j, label in enumerate(labels):
            if word == label:
                lst[i] = str(j+1)
    if flag == 'o' and lst[len(labels)]!='':
        lst[len(labels)] = str(len(labels)+1)
    return '_'.join(lst)

def get_age(row):
    if row['Birthdate']!= '__':
        birthday = datetime.strptime(row['Birthdate'], '%Y_%m_%d').date()
        age = relativedelta(datetime.today(), birthday).years
        return age
    return np.nan

def get_age__(row,csd):
    if row[csd]!= '__' and row['Birthdate']!= '__':
        #print(row.name)
        birthday = datetime.strptime(row['Birthdate'], '%Y_%m_%d').date()
        csday = datetime.strptime(row[csd], '%Y_%m_%d').date()
        age = relativedelta(csday, birthday).years
        return age
    return np.nan

def get_stats_infertility_period(df,group):
    print('Sth on avg grouped by sth: ',df.groupby(group).mean())

def get_missing_percentage_of_cols(df):
    return list(df.isnull().sum() * 100 / len(df))

df = pd.read_csv("D:/data.csv", skiprows = [0, 2, 3], usecols=[5,*range(14,19),*range(21,24),*range(28,36),*range(38,41),*range(45,56),
                                                                               *range(68,78),*range(86,88),*range(89,97),*range(98,112),*range(164,170),
                                                                               *range(172,186),*range(187,199),*range(253,704)])#,104])#3,,254,404,554,704

df['feature_A'] = df.apply(kill_underscore, args=('feature_A',['value1','value2','value3','value4','value5'],'o',), axis=1)
df['feature_B'] = df['feature_B'].replace(['_','value1','value2','value3'],['repl1','repl2','repl3',''])
df = df.rename(columns={'feature_A': 'feature_A_'})
for col_name in ['col1','col2','col3','col4','col5']:
    df[col_name] = df[col_name].replace(r"_", "", regex=True)
    df[col_name] = df.apply(get_age__, args=(col_name,), axis=1)
df['derived_feature'] = df.apply(get_age__, args=('Examination date\n* If you are not sure, please describe as much as you know, such as Day only',), axis=1)
df.loc[df['feature_C'].str.contains('val0'), 'feature_C'] = '20'

###################################################################################################################################################

def create_combination_key(long_list, lat_list):
    long_list = ['{:7f}'.format(x) for x in long_list]
    lat_list = ['{:7f}'.format(x) for x in lat_list]
    long_list = [long.replace('.', '_') for long in long_list]
    lat_list = [lat.replace('.', '_') for lat in lat_list]
    combination_key = []
    for (long_item, lat_item) in zip(long_list, lat_list):
        combination_key.append(long_item + '_' + lat_item)
    return combination_key

def mergeDict(dict1, dict2):
   ''' Merge dictionaries and keep values of common keys in list'''
   dict3 = {**dict1, **dict2}
   for key, value in dict3.items():
       if key in dict1 and key in dict2:
               dict3[key] = [value , dict1[key]]
   return dict3

def get_combination_data(file_list):
    result = []
    for filename in file_list:
        df = pd.read_csv('D:/data/'+filename+'.csv')
        #if filename == 'MedicalInstitute_Coordinate1':
        df = df[df['Longitude']!=-1]
        combination_key = create_combination_key(df['Longitude'], df['Latitude'])
        df['Coordinate'] = combination_key
        #df = df[df['Medical institution name'].isna()==False]
        #address_list = list(df['住所'])
        dict = df.groupby('Coordinate')['Address'].apply(set).map(list).to_dict()
        result.append(dict)
    key_list = []
    for dict in result:
        for key, value in list(dict.items()):
            if len(value)<2:
               del dict[key]
    dict1 = mergeDict(result[0], result[1])
    dict2 = mergeDict(result[2], result[3])
    final_dict = mergeDict(dict1,dict2)
    for k, v in final_dict.items():
        for sublist in v:
            if isinstance(sublist, list):
                final_dict[k] = [item for sublist in v for item in sublist]
                break
    final_dict = collections.OrderedDict(sorted(final_dict.items(), key=lambda x: len(x[1]), reverse=True))
    address_list = []
    coor_list = []
    for key, value in final_dict.items():
        if len(value) > 1:
            address_list.append(value)
            key = key.split('_')
            key = '(' + key[0] + '.' + key[1] + ', ' + key[2]+ '.' + key[3] + ')'
            coor_list.append([key] * len(value))

    coor_list = [item for sublist in coor_list for item in sublist]
    address_list = [item for sublist in address_list for item in sublist]
    result = list(zip(coor_list,address_list))
    final_result = pd.DataFrame(result, columns =['Coordinate', 'Address'])
    final_result.to_csv('Address_Coordinate_Duplicate.csv',index=False)

############################################################################################################################
import os

cwd = os.path.abspath('D:/data/folder')
files = os.listdir(cwd)
cols = ['ID', 'Name', 'Address', 'Gender', 'Type']
df_total = pd.DataFrame(columns=cols)
for file in files:  # loop through Excel files
    if file.endswith('.xlsx'):
        file_name = cwd + '\\' + file
        xl_file = pd.ExcelFile(file_name)
        dfs = {sheet_name: xl_file.parse(sheet_name) for sheet_name in xl_file.sheet_names}
        id_list = []
        name_list = []
        address_list = []
        prefecture_list = []
        type_list = []
        for key,value in dfs.items():
            key = key.split(' ')
            pft = key[0]
            type = key[2]
            key_location = list(zip(*np.where(value.values == 'specific sign')))
            val = key_location[0][0]
            value = value.drop(value.index[0:val+1])
            id_list.append(value.iloc[:, 1].astype(str))
            name_list.append(value.iloc[:, 2])
            address_list.append(value.iloc[:, 3])
            prefecture_list.append([pft] * value.shape[0])
            type_list.append([type] * value.shape[0])
        # df_total['ID'] = get_flat_list(id_list)
        # df_total['Name'] = get_flat_list(name_list)
        # df_total['Address'] = get_flat_list(address_list)
        # df_total['Prefecture'] = get_flat_list(prefecture_list)
        # df_total['Type'] = get_flat_list(type_list)
        df_total.to_csv('total.csv')
        print()