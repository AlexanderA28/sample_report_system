# Import libraries
import pandas as pd
import datetime
from os import listdir
from os.path import isfile, join
from airflow.models import Variable


def dim_status(df):
    current_status = df['6'].drop_duplicates().reset_index().drop('index',axis=1)
    current_status['Status_SK'] =  range(1, len(current_status.index)+1)
    current_status = current_status[['Status_SK','6']]
    return current_status

def to_integer(dt_time):
    return 10000*dt_time.year + 100*dt_time.month + dt_time.day

def create_date_table(df):
    start = df['date'].min()
    end = df['date'].max()
    df = pd.DataFrame({"Date": pd.date_range(start, end)})
    df['Date_SK'] = df['Date'].map(lambda x: to_integer(x))
    df["Day"] = df.Date.dt.day_name
    df["Week"] = df.Date.dt.weekofyear
    df["Quarter"] = df.Date.dt.quarter
    df["Year"] = df.Date.dt.year
    df["Year_half"] = (df.Quarter + 1) // 2
    date_table = df
    cols = ['Date_SK','Date','Day','Week','Quarter','Year','Year_half']
    date_table = date_table[cols]
    return date_table


def dim_item(df):
    mask = ['5','1','4','2','3']
    df2 = df[mask].drop_duplicates().sort_values(by='5')
    df2['Item_SK'] = range(1, len(df2.index)+1)
    Item = df2[['Item_SK','5','1','4','2','3']]
    Item = Item.merge(category_df,left_on='1', right_on='1', how='left')
    Item_codes = Item['3']

    Item_urls = []
    for code in Item_codes:
        URL = 'https://www.company.com/uk//tops/p/{}'.format(code)
        Item_urls.append(URL)

    Image_urls = []
    for code in Item_codes:
        URL = 'https://media2.companyassets.com/i/company/{}/'.format(code)
        Image_urls.append(URL)

    Item['ItemURL'] = Item_urls
    Item['ImageURL'] = Image_urls

    return Item


def sales(df):
    df = df.merge(Status, left_on='6',right_on='6', how='left').drop('6', axis=1)
    cols_to_use = Item.columns.difference(df.columns)
    Item[cols_to_use]
    columns = ['2','3','5','4','1']
    sales = df.merge(Item, left_on=columns, right_on=columns, how='left').drop(columns,axis=1)
    sales.rename(columns={'date':'Date'}, inplace=True)
    sales['Date']= pd.to_datetime(sales['Date'])
    Date_table = Date[['Date_SK','Date']]
    sales = sales.merge(Date_table,left_on='Date',right_on='Date')
    cols = ['Item_SK','Status_SK','Date_SK','7', '8', '9',
           '10', '11', '9-1', '9-2',
           '9-3', '',
           '16', '17', '18',
           '19' ]
    sales = sales[cols]

    return sales


STAGING_DATA_LOCATION = Variable.get('staging_data_location')
STAR_SCHEMA_LOCATION = Variable.get('star_schema_location')

staging_df = pd.read_csv(STAGING_DATA_LOCATION+'sales_stage.csv')
category_df = pd.read_csv(STAGING_DATA_LOCATION+'category_stage.csv')

Status = dim_status(staging_df)
Item = dim_item(staging_df)
Date = create_date_table(staging_df)
Sales = sales(staging_df)

table_dict = {'Status':Status,'Item':Item,'Date':Date,'Sales':Sales}

for name,table in table_dict.items():
    table.to_csv(STAR_SCHEMA_LOCATION+name+'.csv')
