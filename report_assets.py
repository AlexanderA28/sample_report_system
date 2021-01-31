#!/usr/bin/env python
# coding: utf-8

# # Import Libraries

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
import os
import shutil


# # GET DATA
client = bq.Client.from_service_account_json("/home/alexander/Documents/Apps/airflow/secrets/key.json")
sql = """ SELECT * FROM company_data_mart_dev.company_sales_all_dev """
df = client.query(sql).to_dataframe()

sort_fields = ['Cover', '9']

dirs = ['three_week_sales_plots','stock_plots','sales_trends', 'sales_kpi', 'cover_kpi','stock_kpi']
date_str = df['Date'].max().strftime('%d %m %Y')
date_str = re.sub('[^A-Za-z0-9]+', '_', date_str)

for sub_dir in dirs:
    parent_dir = '/home/alexander/Documents/Apps/airflow/reporting_env/assets'
    path = os.path.join(parent_dir, sub_dir, date_str )
    try:
        shutil.rmtree(path)
        os.mkdir(path)
    except:
        os.mkdir(path)


for sub_dir in dirs:
    parent_dir = '/home/alexander/Documents/Apps/airflow/reporting_env/assets'
    path = os.path.join(parent_dir, sub_dir, date_str)
    try:
        shutil.rmtree(path)
        os.mkdir(path)
    except:
        os.mkdir(path)
    for sort_field in sort_fields:
        path = os.path.join(parent_dir, sub_dir, date_str,sort_field)
        try:
            shutil.rmtree(path)
            os.mkdir(path)
        except:
            os.mkdir(path)


def generate_report_assets(sort_field,asc):

    client = bq.Client.from_service_account_json("/home/alexander/Documents/Apps/airflow/secrets/key.json")

    sql = """ SELECT * FROM company_data_mart_dev.company_sales_all_dev """
    df = client.query(sql).to_dataframe()
    dates = list(df['Date'].unique())
    dates.sort()
    mask = df['Date'] == df['Date'].max()
    latest_data = df[mask]


    all_time = client.query(sql).to_dataframe()
    date_sql = """  SELECT * FROM company_data_mart_dev.Date """
    date_df = client.query(date_sql).to_dataframe()
    all_time = all_time.merge(date_df,left_on='Date', right_on='Date')

    date_str = df['Date'].max().strftime('%d %m %Y')
    date_str = re.sub('[^A-Za-z0-9]+', '_', date_str)

    category_list = latest_data[['Category',sort_field]].groupby(['Category'])[sort_field].sum().reset_index().sort_values(sort_field, ascending=asc)['Category']
    category_dfs = {}

    for category in category_list:
        mask = latest_data['Category'] == category
        category_dfs[category] = latest_data[mask]


    top_5_per_category_dfs = {}

    for category in category_list:
        mask = latest_data['Category'] == category
        top_5_per_category_dfs[category] = latest_data[mask].sort_values(by=sort_field, ascending=asc).head(5).reset_index().drop('index',axis=1)
        top_5_per_category_dfs[category].index += 1
        top_5_per_category_dfs[category].insert (0, 'Rank',top_5_per_category_dfs[category].index)


    top_5_df = pd.concat(top_5_per_category_dfs)


    top_5_per_category_dfs_time = {}

    for category in category_list:
        mask = all_time['Category'] == category
        top_5_per_category_dfs_time[category] = all_time[mask].sort_values(by=sort_field, ascending=asc).head(5).reset_index().drop('index',axis=1)
        top_5_per_category_dfs_time[category].index += 1
        top_5_per_category_dfs_time[category].insert (0, 'Rank',top_5_per_category_dfs_time[category].index)


    top_5_df['plot_id'] = top_5_df.apply(lambda x: str(x['Category'])+'-'+str(x['Rank'])+'-'+str(x['5']),axis=1)
    top_5_df['plot_id'] = top_5_df['plot_id'].map(lambda x: re.sub('[^A-Za-z0-9]+', '_', x))
    top_5_df['plot_id'] = top_5_df['plot_id'].map(lambda x: x+'.png')
    top_5_df['Cover'] = top_5_df['Cover'].map( lambda x: np.round(x,decimals=2))
    top_5_df['date_str'] = top_5_df['Date'].map(lambda x: x.strftime('%d %m %Y'))
    top_5_df['date_str'] = top_5_df['date_str'].map(lambda x: re.sub('[^A-Za-z0-9]+', '_', x))
    date_str = df['Date'].max().strftime('%d %m %Y')
    date_str = re.sub('[^A-Za-z0-9]+', '_', date_str)

    parent_dir = '/home/alexander/Documents/Apps/airflow/Data/csv_reports/'
    path = os.path.join(parent_dir, date_str )

    try:
        os.mkdir(path)
    except:
        None

    try:
        path = '/home/alexander/Documents/Apps/airflow/Data/csv_reports/'+date_str+'/top_5_df_'+sort_field+'.csv'
        os.remove(path)
        top_5_df.to_csv(path)
    except:
        top_5_df.to_csv(path)


    # # PLOTS

    # ## Weekly Sales Plots
    def sales_plot():
        plot_dict = {}
        for category in category_list:
            for i in list(range(1,6,1)):
                mask = top_5_per_category_dfs[category]['Rank'] == i
                if not mask.empty:
                    df = top_5_per_category_dfs[category][mask]
                    5 = df['5']
                    if not 5.empty:
                        5 = df['5'].values[0]
                        df2 = df[['9','9_1','9_2','9_3']]
                        df2 = df2.copy()
                        df2.rename(columns={'9':'Units_LW','9_1':'Units_LW_1','9_2':'Units_LW_2','9_3':'Units_LW_3','9_4':'Units_LW_4'}, inplace=True)
                        if not df2.empty:
                            df2 = df2.T
                            df2.rename(columns={i:'Units'}, inplace=True)
                            df2.reset_index(inplace=True)
                            df2.rename(columns={'index':'Week'}, inplace=True)
                            fig = px.bar(df2, x = 'Week', y='Units', text='Units',color_discrete_sequence =['#C9B5AF']*len(df2))
        #                     fig.update_layout(title={'text':'4 Week Unit Sales: ' + 5,'y':0.9,'x':0.5,'xanchor': 'center','yanchor': 'top'})
                            fig.update_layout(autosize=False,width=600,height=350,margin={'l': 0, 'r': 0, 't': 20, 'b': 0}, paper_bgcolor='rgba(0,0,0,0)',plot_bgcolor='rgba(0,0,0,0)')

                            fig['layout']['xaxis']['autorange'] = "reversed"
                            plot_dict[str(category)+'-'+str(i)+'-'+str(5)] = fig
        return plot_dict


    plot_dict = sales_plot()
    parent_dir = '/home/alexander/Documents/Apps/airflow/reporting_env/assets'
    sub_dir = 'three_week_sales_plots'
    path = os.path.join(parent_dir, sub_dir, date_str , sort_field )

    for key, value in plot_dict.items():
        filename = str(re.sub('[^A-Za-z0-9]+', '_', key))+'.png'
        value.write_image(os.path.join(path,filename))

    def sales_kpi():
        sales_kpi_dict = {}
        for category in category_list:
            for i in list(range(1,6,1)):
                mask = top_5_per_category_dfs[category]['Rank'] == i
                if not mask.empty:
                    df = top_5_per_category_dfs[category][mask]
                    5 = df['5']
                    if not 5.empty:
                        5 = df['5'].values[0]
                        df2 = df[['9','9_1']]
                        fig = go.Figure()
                        fig.add_trace(go.Indicator(
                            mode = "number+delta",
                            value = df2['9'].values[0],
                            delta = {'reference': df2['9_1'].values[0]},
                            domain = {'row': 0, 'column': 1}))

                        fig.update_layout(autosize=False,width=600,height=350,margin={'l': 0, 'r': 0, 't': 0, 'b': 0}, paper_bgcolor='rgba(0,0,0,0)',plot_bgcolor='rgba(0,0,0,0)')

                        sales_kpi_dict[str(category)+'-'+str(i)+'-'+str(5)] = fig

        return sales_kpi_dict

    sales_kpi_dict = sales_kpi()

    parent_dir = '/home/alexander/Documents/Apps/airflow/reporting_env/assets'
    sub_dir = 'sales_kpi'
    path = os.path.join(parent_dir, sub_dir, date_str , sort_field)

    for key, value in sales_kpi_dict.items():
        filename = str(re.sub('[^A-Za-z0-9]+', '_', key))+'.png'
        value.write_image(os.path.join(path,filename))

##########################################################################################################
    def cover_kpi():
        cover_kpi_dict = {}
        for category in category_list:
            for i in list(range(1,6,1)):
                mask = top_5_per_category_dfs[category]['Rank'] == i
                if not mask.empty:
                    df = top_5_per_category_dfs[category][mask]
                    5 = df['5']
                    if not 5.empty:
                        5 = df['5'].values[0]
                        df2 = df[['Cover']]
                        fig = go.Figure()
                        fig.add_trace(go.Indicator(
                            mode = "number+delta",
                            value = df2['10'].values[0],
                            # delta = {'reference': df2['9_1'].values[0]},
                            domain = {'row': 0, 'column': 1}))

                        fig.update_layout(autosize=False,width=600,height=350,margin={'l': 0, 'r': 0, 't': 0, 'b': 0}, paper_bgcolor='rgba(0,0,0,0)',plot_bgcolor='rgba(0,0,0,0)')

                        cover_kpi_dict[str(category)+'-'+str(i)+'-'+str(5)] = fig

        return cover_kpi_dict

    cover_kpi_dict = cover_kpi()

    parent_dir = '/home/alexander/Documents/Apps/airflow/reporting_env/assets'
    sub_dir = 'cover_kpi'
    path = os.path.join(parent_dir, sub_dir, date_str , sort_field)

    for key, value in cover_kpi_dict.items():
        filename = str(re.sub('[^A-Za-z0-9]+', '_', key))+'.png'
        value.write_image(os.path.join(path,filename))
##########################################################################################################

    def stock_kpi():
        stock_kpi_dict = {}
        for category in category_list:
            for i in list(range(1,6,1)):
                mask = top_5_per_category_dfs[category]['Rank'] == i
                if not mask.empty:
                    df = top_5_per_category_dfs[category][mask]
                    5 = df['5']
                    if not 5.empty:
                        5 = df['5'].values[0]
                        df2 = df[['17']]
                        fig = go.Figure()
                        fig.add_trace(go.Indicator(
                            mode = "number+delta",
                            value = df2['17'].values[0],
                            # delta = {'reference': df2['9_1'].values[0]},
                            domain = {'row': 0, 'column': 1}))

                        fig.update_layout(autosize=False,width=600,height=350,margin={'l': 0, 'r': 0, 't': 0, 'b': 0}, paper_bgcolor='rgba(0,0,0,0)',plot_bgcolor='rgba(0,0,0,0)')

                        stock_kpi_dict[str(category)+'-'+str(i)+'-'+str(5)] = fig

        return stock_kpi_dict

    stock_kpi_dict = stock_kpi()

    parent_dir = '/home/alexander/Documents/Apps/airflow/reporting_env/assets'
    sub_dir = 'stock_kpi'
    path = os.path.join(parent_dir, sub_dir, date_str , sort_field)

    for key, value in stock_kpi_dict.items():
        filename = str(re.sub('[^A-Za-z0-9]+', '_', key))+'.png'
        value.write_image(os.path.join(path,filename))
##########################################################################################################
    # ## Sales Trend Plots


    def sales_trends():
        sales_trend_plots = {}
        for category in category_list:
            for code in top_5_per_category_dfs[category]['3']:
                rank_mask = top_5_per_category_dfs[category]['3'] == code
                rank = top_5_per_category_dfs[category][rank_mask]['Rank']
                rank = rank.values[0]
                mask = all_time['3'] == code
                df5 = all_time[mask]
                5 = df5['5'].values[0]
                df5 = df5[['Date','9']].sort_values('Date')
                df5.rename(columns={'9':'Sales Units'}, inplace=True)
                df5['lag']= df5['Date'].diff()


                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x = df5['Date'],
                    y = df5['Sales Units'],
                    mode='lines' ,
                    marker_color ='#C9B5AF'))
                # Add shape regions
                quarters = {}
                for Q in list(all_time['Quarter'].unique()):
                    mask = all_time[['Date','Quarter']].sort_values('Date')['Quarter'] == Q
                    quarter_df = all_time[mask]
                    quarters[str(Q)] = {'min':quarter_df['Date'].min()- timedelta(days = 7) , 'max': quarter_df['Date'].max()}

                for quarter in quarters:
                    if '1' in quarter:
                        quarters[quarter]['colour'] = 'rgb(178, 249, 255)'
                    elif '2' in quarter:
                        quarters[quarter]['colour'] = 'rgb(118, 245, 255)'
                    elif '3' in quarter:
                        quarters[quarter]['colour'] = 'rgb(200, 206, 255)'
                    elif '4' in quarter:
                        quarters[quarter]['colour'] = 'rgb(155, 166, 252)'

                for quarter in quarters:
                    fig.add_vrect(
                        x0=quarters[quarter]['min'], x1=quarters[quarter]['max'],
                        fillcolor=quarters[quarter]['colour'], opacity=0.2,
                        layer="below", line_width=0,annotation_text='Q'+str(quarter)
                    )

                fig.add_vrect(
                        x0= '2020-10-18', x1='2020-10-23',
                        fillcolor='yellow', opacity=0.2,
                        layer="below", line_width=0,annotation_text="Missing: "+str('2020-10-18'),
                        annotation_position="bottom left",
                        annotation_font_size=6,
                    )



        #         fig = px.line(df5, x="Date", y="Sales Units", title='Sales Trend over time', color_discrete_sequence =['#C9B5AF']*len(df5))
                fig.update_layout(autosize=False,width=900,height=300,margin={'l': 0, 'r': 0, 't': 20, 'b': 0}, paper_bgcolor='rgba(0,0,0,0)',plot_bgcolor='rgba(0,0,0,0)')
                sales_trend_plots[str(category)+'-'+str(rank)+'-'+str(5)] = fig

        return sales_trend_plots




    sales_trend_plots = sales_trends()



    parent_dir = '/home/alexander/Documents/Apps/airflow/reporting_env/assets'
    sub_dir = 'sales_trends'
    path = os.path.join(parent_dir, sub_dir, date_str, sort_field )

    for key, value in sales_trend_plots.items():
        filename = str(re.sub('[^A-Za-z0-9]+', '_', key))+'.png'
        value.write_image(os.path.join(path,filename))

    stock_dict = {}
    for category in category_list:
        for i in list(range(1,6,1)):
            mask = top_5_per_category_dfs[category]['Rank'] == i
            if not mask.empty:
                df = top_5_per_category_dfs[category][mask]
                5 = df['5']
                if not 5.empty:
                    5 = df['5'].values[0]
                    df2 = df[['16','17','17']]
                    df2 = df2.copy()
                    df2.rename(columns={'16':'Branch','17':'Total','17':'Commitment'}, inplace=True)
                    if not df2.empty:
                        df2 = df2.T
                        df2.rename(columns={i:'Units'}, inplace=True)
                        df2.reset_index(inplace=True)
                        df2.rename(columns={'index':'Stock'}, inplace=True)
                        fig = px.bar(df2, x = 'Stock', y='Units', text='Units',color_discrete_sequence =['#C9B5AF']*len(df2))
    #                     fig.update_layout(title={'text':'Stock Information: ' + 5,'y':0.9,'x':0.5,'xanchor': 'center','yanchor': 'top'})
                        fig.update_layout(autosize=False,width=600,height=350,margin={'l': 0, 'r': 0, 't': 20, 'b': 0}, paper_bgcolor='rgba(0,0,0,0)',plot_bgcolor='rgba(0,0,0,0)')
                        fig['layout']['xaxis']['autorange'] = "reversed"
                        stock_dict[str(category)+'-'+str(i)+'-'+str(5)] = fig

    parent_dir = '/home/alexander/Documents/Apps/airflow/reporting_env/assets'
    sub_dir = 'stock_plots'
    path = os.path.join(parent_dir, sub_dir, date_str , sort_field)

    for key, value in stock_dict.items():
        filename = str(re.sub('[^A-Za-z0-9]+', '_', key))+'.png'
        value.write_image(os.path.join(path,filename))


for sort_field in sort_fields:
    if sort_field == 'Cover':
        generate_report_assets(sort_field,True)
    else:
        generate_report_assets(sort_field,False)
