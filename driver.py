import os
import weasyprint
from jinja2 import Environment, FileSystemLoader

from google.cloud import bigquery
import google.cloud.bigquery as bq
import pandas as pd
import urllib.request
import plotly.express as px
import regex as re
import plotly.graph_objects as go
from datetime import datetime
from datetime import timedelta
import numpy as np

# Get data
client = bq.Client.from_service_account_json(".../secrets/key.json")
sql = """
SELECT * FROM 17_data_mart_dev.17_sales_all_dev
"""
df = client.query(sql).to_dataframe()
dates = list(df['Date'].unique())
dates.sort()
date_str = df['Date'].max().strftime('%d %m %Y')
date_str = re.sub('[^A-Za-z0-9]+', '_', date_str)

top_5_df_path = '/home/alexander/Documents/Apps/airflow/Data/csv_reports/08_11_2020/top_5_df_Net_Sales_Units_LW.csv'

top_5_df = pd.read_csv(top_5_df_path)
top_5_df.rename(columns={'Unnamed: 0':'Category'},inplace=True)
top_5_df.drop('Unnamed: 1', axis=1, inplace=True)
top_5_df = top_5_df.loc[:,~top_5_df.columns.duplicated()]

client = bq.Client.from_service_account_json("..../secrets/key.json")
sql = """
SELECT

Distinct(Category)

FROM

17_data_mart_dev.Item

ORDER BY Category ASC
"""
category_df = client.query(sql).to_dataframe()
category_list = category_df['Category'].unique()

# jinja2
ROOT = '/home/alexander/Documents/Apps/airflow/reporting_env'
ASSETS_DIR = os.path.join(ROOT, 'assets')
TEMPLAT_SRC = os.path.join(ROOT, 'templates')
CSS_SRC = os.path.join(ROOT, 'static/')
DEST_DIR = os.path.join(ROOT, 'output')

# TEMPLATES = ['layout_driver_sales.html','layout_driver_cover.html']

# TEMPLATE = 'layout_driver_sales.html'
CSS = 'print-landscape.css'

REPORT_TEMPLATE_DICT = {'Net_Sales_Units_LW':
 {'report_name':'_Top_5__by_Last_Week_Sales',
  'html_template':'layout_driver_sales.html',
  'df':'top_5_df_Net_Sales_Units_LW.csv'},
 'Cover':
  {'report_name':'_Top_5_by_least_cover',
  'html_template':'layout_driver_cover.html',
  'df':'top_5_df_Cover.csv'}
}

def start():

    for key, value in REPORT_TEMPLATE_DICT.items():

        print('Generating Reports',REPORT_TEMPLATE_DICT[key]['report_name'])

        OUTPUT_FILENAME = date_str+REPORT_TEMPLATE_DICT[key]['report_name']+'.pdf'

        env = Environment(loader=FileSystemLoader(TEMPLAT_SRC))

        template = env.get_template(REPORT_TEMPLATE_DICT[key]['html_template'])

        css = os.path.join(CSS_SRC, CSS)
        df_dir = '/Data/csv_reports'
        df_date = date_str
        df_name = REPORT_TEMPLATE_DICT[key]['df']

        df_path = os.path.join(df_dir,df_date,df_name)

        top_5_df = pd.read_csv(df_path)

        # variables
        template_vars = { 'assets_dir': 'file://' + ASSETS_DIR , 'top_5_df':top_5_df , 'date_str':date_str, 'field': str(key), 'category_list':category_list}

        # rendering to html string
        rendered_string = template.render(template_vars)

        html = weasyprint.HTML(string=rendered_string)
        report = os.path.join(DEST_DIR, OUTPUT_FILENAME)
        html.write_pdf(report, stylesheets=[css])
        print('file is generated successfully and under {}', DEST_DIR)


if __name__ == '__main__':
    start()
