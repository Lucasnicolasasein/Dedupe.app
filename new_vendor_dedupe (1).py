# Library imports
import pandas as pd
import pandas_dedupe
import numpy as np
import re
import dedupe
import random
import os


def dupe_train(csv_file):
    df_sample = pd.read_csv(csv_file)
    value_file = input('Would you like to retrain the model? Press (y) for Yes and (n) for No ')
    if value_file == 'y':
        # Try to delete the file ##
        try:
            os.remove('dedupe_dataframe_learned_settings')
            print('Setting file removed')
        except OSError as e:  ## if failed, report it back to the user ##
            print("Error: %s - %s." % (e.filename, e.strerror))
        try:
            os.remove('dedupe_dataframe_training.json')
            print('json file removed')
        except OSError as e:  ## if failed, report it back to the user ##
            print("Error: %s - %s." % (e.filename, e.strerror))
    else:
        pass

    print('Sample Shape \n')
    print(df_sample.shape)
    print('\n Sample Head \n')
    print(df_sample.head())

    # Columns names for data training
    columns_name = pd.DataFrame(df_sample.columns)
    columns_name_list = df_sample.columns
    print('Columns names \n')
    print(columns_name)
    value = input(
        'Enter which column number (integer) you use to train the model. For example, if you would like to dedupe Vendor Name Column you have to enter the number linked to that column name. You can refer to the above print out to find the right number. (Note: The first column is column 0)\n')
    column = columns_name_list[int(value)]
    columns_list = []
    columns_list.append(column)
    value = input(
        'Select which columns you want to use to train the model, the first columns is columns number 0, you should input an integer, f if you are already done \n')
    while value != 'f':
        print(f'The model will be done with the column {columns_list}')
        column = columns_name_list[int(value)]
        columns_list.append(column)
        value = input(
            'Enter additional columns tot be used to train the column. For example, if you would like to also dedupe based on Address Column you have to enter the number linked to that column name. You can refer to the above print out to find the right number. (Note: The first column is column 0). If there are no additional columns required enter (F) for finish.\n')

    print(f'Columns for clustering will be {columns_list}')

    for c in range(0, len(columns_list)):
        df_sample[columns_list[c]] = df_sample[columns_list[c]].astype('str')
        df_sample[columns_list[c]] = df_sample[columns_list[c]].str.strip()
        df_sample[columns_list[c]] = df_sample[columns_list[c]].str.replace('\n', ' ')

    print(df_sample.head())

    # initiate deduplication
    df_train = pandas_dedupe.dedupe_dataframe(df_sample, [column])

    # Sort values by cluster_id
    df_train.sort_values('cluster id', inplace=True)
    df_train.reset_index(inplace=True)
    df_train.drop(columns=['index'], inplace=True)

    print(df_train.head())
    print(df_train.dtypes)

    return df_train


if __name__ == '__main__':

    # excel files closed warning
    print('All spreadsheets should be closed')
    new_data_file = input(
        'Please enter the name of the spreadsheet you would like to dedupe. Make sure to add the “.csv” extension. ')
    df_final = dupe_train(new_data_file)

    # should we delete the trainning file so we can re train the model
    # ACA ESTABA EL CODIGO DE LA FUNCION

    # Showing some cluster
    # print('value counts cluster id\n')
    value_count_df = pd.DataFrame(df_final['cluster id'].value_counts())
    # print(value_count_df)
    value_count_df = value_count_df[(value_count_df['cluster id'] > 1) & (value_count_df['cluster id'] < 21)][
        'cluster id']
    print(value_count_df)
    print('CLUSTER EXAMPLES \n')
    for i in range(0, 10):
        rand = random.randint(0, len(value_count_df))
        cluster_id = value_count_df[rand]
        print('Example Cluster n°: ' + str(i) + ':\n')
        df_example = df_final[df_final['cluster id'] == cluster_id]
        print(df_example)

    re_train_model = 'y'
    while re_train_model == 'y':
        re_train_model = input('Are the examples OK, or should we retrain? y(es) for retrainning, n(o) for moving on')
        if re_train_model == 'y':
            del df_final
            df_final = dupe_train(new_data_file)
        else:
            pass

    # Importing already normalized names
    df_norm_names = pd.read_csv('normalized_names.csv')
    df_norm_names.head()

    # select which columns has the vendors name so we can compare
    columns_name_normalized = pd.DataFrame(df_norm_names.columns)
    columns_name_normalized_list = df_norm_names.columns
    print('Columns names for the Base Data \n')
    print(columns_name_normalized)
    value = input(
        'Select which column from the Base Data set we will compare to your dedupe file.\n')
    column_norm = columns_name_normalized_list[int(value)]
    df_norm_names[column_norm] = df_norm_names[column_norm].str.upper()

    print('Columns names for the new dedupe file\n')
    print(columns_name)
    value = input(
        'Select which column from the new dedupe Data set we will compare to your Base Data file\n')
    column = columns_name_list[int(value)]
    df_final[column] = df_final[column].str.upper()
    df_join_vendors = pd.merge(df_norm_names, df_final[[column, 'cluster id']]
                               , how='outer', left_on=[column_norm], right_on=[column])
    df_join_vendors[column_norm] = df_join_vendors[column_norm].fillna('99999999')

    print('Columns names for the Base Data\n')
    print(columns_name_normalized)
    value = input(
        'Select the column to index the clusters \n')
    column_norm_id = columns_name_normalized_list[int(value)]
    print(column_norm_id)
    max_unique_id = max(df_join_vendors[column_norm_id])

    print('Merging starting \n')
    for i in range(0, len(df_join_vendors) - 1):
        if (df_join_vendors.loc[i, 'cluster id'] == df_join_vendors.loc[i + 1, 'cluster id']) & (
                df_join_vendors.loc[i, column_norm] == '99999999') & (
                df_join_vendors.loc[i + 1, column_norm] != '99999999'):
            df_join_vendors.loc[i, column_norm] = df_join_vendors.loc[i, column]
            df_join_vendors.loc[i, column_norm_id] = df_join_vendors.loc[
                i + 1, column_norm_id]
        elif (df_join_vendors.loc[i, 'cluster id'] == df_join_vendors.loc[i + 1, 'cluster id']) & (
                df_join_vendors.loc[i, column_norm] == '99999999') & (
                df_join_vendors.loc[i + 1, column_norm] == '99999999'):
            df_join_vendors.loc[i, column_norm] = df_join_vendors.loc[i, column]
            max_unique_id += 1
            df_join_vendors.loc[i, column_norm_id] = max_unique_id
        elif (df_join_vendors.loc[i, 'cluster id'] >= 0) & (
                df_join_vendors.loc[i, column_norm_id] != '99999999'):
            # New vendors that only have 1 vendor name
            df_join_vendors.loc[i, column_norm] = df_join_vendors.loc[i, column]
            max_unique_id += 1
            df_join_vendors.loc[i, column_norm_id] = max_unique_id

    df_join_vendors = df_join_vendors[['Original Supplier Name', 'Normalized Name Unique ID']]
    print(df_join_vendors.head())
    df_join_vendors.to_csv('normalized_names_new.csv')
