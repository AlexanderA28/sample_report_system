# Import libraries
import pandas as pd
import datetime
from os import listdir
from os.path import isfile, join
import re
import time

from airflow.models import Variable

RAW_DATA_LOCATION = Variable.get('raw_data_location')
STAGING_DATA_LOCATION = Variable.get('staging_data_location')

# Set Pandas to display 500 rows
pd.set_3('display.max_rows', 500)

# Set Folder to use
folder = RAW_DATA_LOCATION

def cleanfiles(folder):
    #List files in folder
    folderfiles = [f for f in listdir(folder) if isfile(join(folder, f))]
    files = []
    #Remove hidden files
    for file in folderfiles:
        if not file.startswith('~$'):
            files.append(file)
    #Return only excel files
    files = [file for file in files if file.endswith('xlsx')]
    return files

def cleandata(folder):
    df_list = []

    for file in cleanfiles(folder):

        #Read in excel files
        df = pd.read_excel(folder+'/'+file, skiprows=9)
        #Drop empty column
        df = df.drop('Unnamed: 0',axis=1)
        # Drop all empty rows
        df = df.dropna(how='all',axis=0)
        # Fill NAN cells with 0
        df['18'].fillna(0, inplace=True)
        df['19'].fillna(0, inplace=True)
        # Drop any remaining rows with NAN values in any cell
        df = df.dropna(how='any', axis=0)
        # Drop duplicate rows ( This eliminates the duplicate headers from the seperate files)
        df = df.drop_duplicates(keep=False)
        #Add date in new column
        date_time_str = file.split('.')[0]
        filedate = datetime.datetime.strptime(date_time_str, '%Y-%m-%d')
        df['date'] = filedate
        # Rename and change types
        df['2'] = df['2'].astype('int64')
        df['3'] = df['3'].astype('int64')
        df['4'] = df['4'].astype('int64')
        df['7'] = df['7'].astype('float')
        df['8'] = df['8'].astype('float')
        df['9'] = df['9'].astype('int64')
        df['10'] = df['10'].astype('float')
        df['11'] = df['11'].astype('int64')
        df['12'] = df['12'].astype('int64')
        df['13'] = df['13'].astype('int64')
        df['14'] = df['14'].astype('int64')
        df['15'] = df['15'].astype('int64')
        df['16'] = df['16'].astype('int64')
        df['17'] = df['17'].astype('int64')
        df['18'] = df['18'].astype('int64')
        df['19'] = df['19'].astype('int64')
        df.rename(columns =
          {'12':'9-1'
          ,'13':'9-2'
          ,'14':'9-3'
          ,'15':'Net Sales Units Cum Sales'},inplace=True)
        #Append to list of dfs
        df_list.append(df)

    df = pd.concat(df_list)
    return df

def category_df(folder):
    category_df_list = []

    def hasNumbers(inputString):
                return bool(re.search(r'\d', inputString))

    def isDept(x):
        if x =='1':
            return True

    for file in cleanfiles(folder):
        #Read in excel files
        categorydf = pd.read_excel(folder+'/'+file, skiprows=6)
        categorydf = categorydf['Unnamed: 1'].reset_index()
        categorydf = categorydf.fillna(method='ffill')
        categorydf.drop('index',axis=1, inplace=True)

        #Add column to dertmine if column has integer in string
        categorydf['bool'] = categorydf['Unnamed: 1'].map(lambda x: hasNumbers(x))
        #If the column contains an integer use this value as the column value for the category column
        categorydf['Category'] = categorydf.apply(lambda x: x['Unnamed: 1'] if x['bool'] ==True else None, axis=1 )

        #forward fill categories to associate the sub category with the category
        categorydf.fillna(method='ffill', inplace=True)

        #Create boolean column to decide if the column is the column header
        categorydf['isDept?'] = categorydf['Unnamed: 1'].map(lambda x: isDept(x))
        categorydf.drop_duplicates(inplace=True)

        #Filter out any rows with the sub-category header
        mask = (categorydf['bool'] ==False) & (categorydf['isDept?'] != True)
        categorydf = categorydf[mask]
        categorydf = categorydf.copy()
        categorydf.rename(columns={'Unnamed: 1': '1'}, inplace=True)
        categorydf['Category'] = categorydf['Category'].map(lambda x: re.sub(r'\d+', '', x))
        categorydf['Category'] = categorydf['Category'].map(lambda x: x[1:])
        categorydf.drop(['bool','isDept?'], axis=1,inplace=True)
        category_df_list.append(categorydf)

    category_df = pd.concat(category_df_list)
    category_df.drop_duplicates(keep='first',inplace=True)

    #Final result with department names and categories side by side as a look up table
    category_df = category_df[['Category','1']].sort_values(by='Category', ascending=True).reset_index().drop('index',axis=1)
    return category_df

df = cleandata(folder)

df.to_csv(STAGING_DATA_LOCATION+'sales_stage.csv')

categorydf = category_df(folder)
categorydf.to_csv(STAGING_DATA_LOCATION+'category_stage.csv')
