#!/usr/bin/env python
# coding: utf-8

# In[18]:


import pandas as pd
import cx_Oracle
import dash
import oracledb
import dash_html_components as html
import dash_core_components as dcc
import plotly.graph_objects as go
import plotly.express as px
from dash import Dash, dcc, html, Input, Output, callback
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import dash_table
from datetime import datetime
from dash.exceptions import PreventUpdate
import io
import base64

mes_tib_conn = cx_Oracle.connect("MES_TIB_NEW/MES_TIB_NEW@10.10.2.21:1521/TIBCO")
mes_conn = cx_Oracle.connect("MES/MES@crmrac.jsw.in:1521/CRM2MESP")
mes_tib_query = """ SELECT message_no, MAX(MODIFIED) AS MODIFIED FROM receive_sap GROUP BY message_no union
                 SELECT message_no, MAX(MODIFIED) AS MODIFIED FROM send_sap GROUP BY message_no union
                 SELECT IDOC_STATUS as message_no, MAX(ACK_DATE) AS MODIFIED FROM IDOC_ACK_TBL GROUP BY IDOC_STATUS"""

mes_tib_data = pd.read_sql(mes_tib_query, mes_tib_conn)

mes_query = """ SELECT message_no, MAX(MODIFIED) AS MES_MODIFIED FROM RECEIVE_piece where message_no like
('10%') GROUP BY message_no union SELECT message_no, MAX(MODIFIED) AS MES_MODIFIED FROM send_piece where message_no like ('10%')
    GROUP BY message_no union SELECT message_no, MAX(MODIFIED) AS MES_MODIFIED FROM receive_mfctorder where message_no like ('10%')
    GROUP BY message_no union SELECT message_no, MAX(MODIFIED) AS MES_MODIFIED FROM receive_inventory where message_no like ('10%') GROUP BY message_no"""

mes_data = pd.read_sql(mes_query, mes_conn)
combined_data_mes2 = mes_tib_data.merge(mes_data, on='MESSAGE_NO', how='left')
combined_data_mes2.to_excel('combined_data.xlsx', index=False)

ack_query = """ SELECT message_no, MAX(MODIFIED) AS MES_MODIFIED FROM RECEIVE_piece where message_no like ('21%')
        GROUP BY message_no """
ack_data = pd.read_sql(ack_query, mes_conn)

combined_data_mes2 = pd.concat([combined_data_mes2, ack_data])
combined_data_mes2["INTERFACE"] = combined_data_mes2["MESSAGE_NO"]
combined_data_mes2['INTERFACE'] = combined_data_mes2['INTERFACE'].map({1000: 'Sales Order', 1001: 'BatchUpdate_Bp', 1002: "SO_ammend", 1: "IDOC_Acknowledgement"
                                                      ,1013: "QualityClearance_Bp", 1009: "StorageLocation_Bp", 1030: "DispatchRoadRake" ,
                                                      1024: "DispatchRoadRake", 21003: "ProductionConfirmationAck", 21013: "QualityConfirmationAck"
                                                       , 1032: "APO MCD", 1026: "PurchaseCoil_Attribute", 10080: "MR_Release", 10060: "PieceAttribute",
                                                       10120: "FG_orderSwap" , 1034: "FG_Recall",1010:"CRM1 to CRM2",1007: "CRM2 to CRM1"})
combined_data_mes2 = combined_data_mes2.reindex(columns=["INTERFACE","MESSAGE_NO","MODIFIED","MES_MODIFIED"])
combined_data_mes2.to_csv("output.csv", index=False)

rec_count_query = """SELECT message_no,count(*) as record_count FROM send_sap WHERE TRUNC(MODIFIED) = TRUNC(SYSDATE)
                  group by message_no union SELECT message_no,count(*) as record_count FROM receive_sap WHERE
                  TRUNC(MODIFIED) = TRUNC(SYSDATE) group by message_no"""
rec_count_mes_query = """select message_no, count(*) as record_count from receive_piece where message_no like ('21%') and
                        TRUNC(MODIFIED) = TRUNC(SYSDATE) group by message_no"""

rec_count_data = pd.read_sql(rec_count_query, mes_tib_conn)
rec_count_mes_query = pd.read_sql(rec_count_mes_query, mes_conn)
rec_count_data = pd.concat([rec_count_data, rec_count_mes_query])
rec_count_data.to_csv('rec_count_data.csv')

total_rec_query = """ SELECT message_no, modified FROM send_sap WHERE TRUNC(MODIFIED) = TRUNC(SYSDATE) union SELECT
              message_no,modified FROM receive_sap WHERE TRUNC(MODIFIED) = TRUNC(SYSDATE)"""
total_rec_data = pd.read_sql(total_rec_query, mes_tib_conn)
total_rec_data["MESSAGE_NO"] = total_rec_data["MESSAGE_NO"].astype(str)
total_rec_data.to_csv('total_rec_data.csv')

last_mod_query = """SELECT message_no,max(modified) AS MODIFIED FROM send_sap group by message_no union SELECT message_no,
            max(modified) FROM receive_sap Group by message_no"""
last_mod_data = pd.read_sql(last_mod_query, mes_tib_conn)

last_mod_data = pd.DataFrame(last_mod_data, columns=["MESSAGE_NO", "MODIFIED"])
last_mod_data["INTERFACE"] = last_mod_data["MESSAGE_NO"]
last_mod_data['INTERFACE'] = last_mod_data['INTERFACE'].map({1000: 'Sales Order', 1001: 'BatchUpdate_Bp', 1002: "SO_ammend", 1: "IDOC_Acknowledgement"
                                                      ,1013: "QualityClearance_Bp", 1009: "StorageLocation_Bp", 1030: "DispatchRoadRake" ,
                                                      1024: "DispatchRoadRake", 21003: "ProductionConfirmationAck", 21013: "QualityConfirmationAck"
                                                       , 1032: "APO MCD", 1026: "PurchaseCoil_Attribute", 10080: "MR_Release", 10060: "PieceAttribute",
                                                       10120: "FG_orderSwap" , 1034: "FG_Recall", 1003:"Production Confirmation",1005: "Batch Swap",1010:"CRM1 to CRM2",1007: "CRM2 to CRM1"})

last_mod_data = last_mod_data.reindex(columns=["INTERFACE", "MESSAGE_NO", "MODIFIED"])
last_mod_data["MODIFIED"] = last_mod_data["MODIFIED"].astype(str)


def parse_date(date_str, date_format):
    return datetime.strptime(date_str, date_format)


def calculate_total_minutes(given_date_str):
    try:
        given_date_format = "%Y-%m-%d %H:%M:%S.%f"
        current_date = datetime.now()
        given_date = parse_date(given_date_str, date_format=given_date_format)
        time_difference = current_date - given_date
        total_minutes = time_difference.total_seconds() / 60
        return total_minutes
    except Exception as e:
        return None


last_mod_data['MODIFIED_1'] = last_mod_data['MODIFIED'].apply(calculate_total_minutes)

app = dash.Dash(__name__)
custom_colors = ['cyan', 'lightblue', 'royalblue', 'darkblue', 'lightcyan']

fig1 = px.pie(rec_count_data, values="RECORD_COUNT", names="MESSAGE_NO", color_discrete_sequence=custom_colors)
fig2 = px.scatter(total_rec_data, x="MESSAGE_NO", y="MODIFIED", color="MESSAGE_NO")

app.layout = html.Div([html.Div(html.H1('Tibco Monitoring Dashboard'), style={'text-align': 'center'}),
    html.Div([
        dcc.Graph(id='firstgraph1', figure=fig1,style={'display': 'inline-block', 'width': '50%'}),
        dash_table.DataTable(
            id='data-table',
    data=last_mod_data.to_dict('records'),
    columns=[{'name': 'INTERFACE', 'id': 'INTERFACE'}, {'name': 'MESSAGE_NO', 'id': 'MESSAGE_NO'}, {'name': 'MODIFIED', 'id': 'MODIFIED'}],
            style_table={'display': 'inline-block', 'width': '80%'},
            style_cell={'minWidth': '200px', 'width': '200px', 'maxWidth': '200px', 'textAlign': 'left'},
    tooltip_conditional=[
        {
            'if': {'column_id': 'MODIFIED', 'filter_query': '{MODIFIED_1} > 30'},
            'type': 'markdown',
            'value': 'This process hasn\'t received any record from 30 mins'
        },
        {
            'if': {'column_id': 'MODIFIED', 'filter_query': '{MODIFIED_1} > 60'},
            'type': 'markdown',
            'value': 'This process hasn\'t received any record from 60 mins'
        },
        {
            'if': {'column_id': 'MODIFIED', 'filter_query': '{MODIFIED_1} > 90'},
            'type': 'markdown',
            'value': 'This process hasn\'t received any record from 90 mins'
        }
    ],
    style_data={
        'text-align': 'left'  # Align cell text to the right
    },
    style_header={
        'text-align': 'left'  # Align header text to the left
    },
    style_data_conditional=[
        {
            'if': {'column_id': 'MODIFIED', 'filter_query': '{MODIFIED_1} > 30' },
            'backgroundColor': 'yellow',
            'color': 'black'
        },
        {
            'if': {'column_id': 'MODIFIED', 'filter_query': '{MODIFIED_1} > 60'},
            'backgroundColor': 'purple',  # Color for greater than 90 minutes
            'color': 'white'  # Text color for greater than 90 minutes
        },
        {
            'if': {'column_id': 'MODIFIED', 'filter_query': '{MODIFIED_1} > 90'},
            'backgroundColor': 'red',  # Color for greater than 90 minutes
            'color': 'white'  # Text color for greater than 90 minutes
        }
    ],
    tooltip_delay=0,
    tooltip_duration=None
)
    ], style={'display': 'flex','text-align': 'center'}),

    # Add the scatter plot below the pie chart and table
    dcc.Graph(id='firstgraph2', figure=fig2),

    html.Div([
        html.A("Download Excel", id="btn_xlsx", download="output.xlsx", href="", target="_blank"),
    ]),
])

#Define a callback to generate and provide the Excel file for download
@app.callback(
    Output("btn_xlsx", "href"),
    Input("btn_xlsx", "n_clicks"),
    prevent_initial_call=True
)

def download_xlsx(n_clicks):
    if n_clicks is None:
        raise PreventUpdate

    # Create the Excel file content
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df4.to_excel(writer, sheet_name='Sheet1', index=False)
    writer.save()
    output.seek(0)
    xlsx_data = base64.b64encode(output.read()).decode()

    return f'data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{xlsx_data}'

if __name__ == '__main__':
    app.run_server(port=8080, host='127.0.0.1', debug=True)


# In[16]:


import pandas as pd
import cx_Oracle
import dash
import dash_html_components as html
import dash_core_components as dcc
import plotly.graph_objects as go
import plotly.express as px
from dash import dcc, html, Input, Output
import dash_table
from datetime import datetime
from dash.exceptions import PreventUpdate
import io
import base64

mes_tib_conn = cx_Oracle.connect("MES_TIB_NEW/MES_TIB_NEW@10.10.2.21:1521/TIBCO")
mes_conn = cx_Oracle.connect("MES/MES@crmrac.jsw.in:1521/CRM2MESP")
mes_tib_query = """SELECT message_no, MAX(MODIFIED) AS MODIFIED FROM receive_sap GROUP BY message_no union
                 SELECT message_no, MAX(MODIFIED) AS MODIFIED FROM send_sap GROUP BY message_no union
                 SELECT IDOC_STATUS as message_no, MAX(ACK_DATE) AS MODIFIED FROM IDOC_ACK_TBL GROUP BY IDOC_STATUS"""

mes_tib_data = pd.read_sql(mes_tib_query, mes_tib_conn)

mes_query = """SELECT message_no, MAX(MODIFIED) AS MES_MODIFIED FROM RECEIVE_piece where message_no like
('10%') GROUP BY message_no union SELECT message_no, MAX(MODIFIED) AS MES_MODIFIED FROM send_piece where message_no like ('10%')
    GROUP BY message_no union SELECT message_no, MAX(MODIFIED) AS MES_MODIFIED FROM receive_mfctorder where message_no like ('10%')
    GROUP BY message_no union SELECT message_no, MAX(MODIFIED) AS MES_MODIFIED FROM receive_inventory where message_no like ('10%') GROUP BY message_no"""

mes_data = pd.read_sql(mes_query, mes_conn)
combined_data_mes2 = mes_tib_data.merge(mes_data, on='MESSAGE_NO', how='left')
combined_data_mes2.to_excel('combined_data.xlsx', index=False)

ack_query = """SELECT message_no, MAX(MODIFIED) AS MES_MODIFIED FROM RECEIVE_piece where message_no like ('21%')
        GROUP BY message_no """
ack_data = pd.read_sql(ack_query, mes_conn)

combined_data_mes2 = pd.concat([combined_data_mes2, ack_data])
combined_data_mes2["INTERFACE"] = combined_data_mes2["MESSAGE_NO"]
combined_data_mes2['INTERFACE'] = combined_data_mes2['INTERFACE'].map({1000: 'Sales Order', 1001: 'BatchUpdate_Bp', 1002: "SO_ammend", 1: "IDOC_Acknowledgement"
                                                      ,1013: "QualityClearance_Bp", 1009: "StorageLocation_Bp", 1030: "DispatchRoadRake" ,
                                                      1024: "DispatchRoadRake", 21003: "ProductionConfirmationAck", 21013: "QualityConfirmationAck"
                                                       , 1032: "APO MCD", 1026: "PurchaseCoil_Attribute", 10080: "MR_Release", 10060: "PieceAttribute",
                                                       10120: "FG_orderSwap" , 1034: "FG_Recall",1010:"CRM1 to CRM2",1007: "CRM2 to CRM1"})
combined_data_mes2 = combined_data_mes2.reindex(columns=["INTERFACE","MESSAGE_NO","MODIFIED","MES_MODIFIED"])
combined_data_mes2.to_csv("output.csv", index=False)

rec_count_query = """SELECT message_no,count(*) as record_count FROM send_sap WHERE TRUNC(MODIFIED) = TRUNC(SYSDATE)
                  group by message_no union SELECT message_no,count(*) as record_count FROM receive_sap WHERE
                  TRUNC(MODIFIED) = TRUNC(SYSDATE) group by message_no"""
rec_count_mes_query = """SELECT message_no, count(*) as record_count from receive_piece where message_no like ('21%') and
                        TRUNC(MODIFIED) = TRUNC(SYSDATE) group by message_no"""

rec_count_data = pd.read_sql(rec_count_query, mes_tib_conn)
rec_count_mes_query = pd.read_sql(rec_count_mes_query, mes_conn)
rec_count_data = pd.concat([rec_count_data, rec_count_mes_query])
rec_count_data.to_csv('rec_count_data.csv')

total_rec_query = """SELECT message_no, modified FROM send_sap WHERE TRUNC(MODIFIED) = TRUNC(SYSDATE) union SELECT
              message_no,modified FROM receive_sap WHERE TRUNC(MODIFIED) = TRUNC(SYSDATE)"""
total_rec_data = pd.read_sql(total_rec_query, mes_tib_conn)
total_rec_data["MESSAGE_NO"] = total_rec_data["MESSAGE_NO"].astype(str)
total_rec_data.to_csv('total_rec_data.csv')

last_mod_query = """SELECT message_no,max(modified) AS MODIFIED FROM send_sap group by message_no union SELECT message_no,
            max(modified) FROM receive_sap GROUP by message_no"""
last_mod_data = pd.read_sql(last_mod_query, mes_tib_conn)

last_mod_data = pd.DataFrame(last_mod_data, columns=["MESSAGE_NO", "MODIFIED"])
last_mod_data["INTERFACE"] = last_mod_data["MESSAGE_NO"]
last_mod_data['INTERFACE'] = last_mod_data['INTERFACE'].map({1000: 'Sales Order', 1001: 'BatchUpdate_Bp', 1002: "SO_ammend", 1: "IDOC_Acknowledgement"
                                                      ,1013: "QualityClearance_Bp", 1009: "StorageLocation_Bp", 1030: "DispatchRoadRake" ,
                                                      1024: "DispatchRoadRake", 21003: "ProductionConfirmationAck", 21013: "QualityConfirmationAck"
                                                       , 1032: "APO MCD", 1026: "PurchaseCoil_Attribute", 10080: "MR_Release", 10060: "PieceAttribute",
                                                       10120: "FG_orderSwap" , 1034: "FG_Recall", 1003:"Production Confirmation",1005: "Batch Swap",1010:"CRM1 to CRM2",1007: "CRM2 to CRM1"})

last_mod_data = last_mod_data.reindex(columns=["INTERFACE", "MESSAGE_NO", "MODIFIED"])
last_mod_data["MODIFIED"] = last_mod_data["MODIFIED"].astype(str)

def parse_date(date_str, date_format):
    return datetime.strptime(date_str, date_format)

def calculate_total_minutes(given_date_str):
    try:
        given_date_format = "%Y-%m-%d %H:%M:%S.%f"
        current_date = datetime.now()
        given_date = parse_date(given_date_str, date_format=given_date_format)
        time_difference = current_date - given_date
        total_minutes = time_difference.total_seconds() / 60
        return total_minutes
    except Exception as e:
        return None

last_mod_data['MODIFIED_1'] = last_mod_data['MODIFIED'].apply(calculate_total_minutes)

app = dash.Dash(__name__)
custom_colors = ['cyan', 'lightblue', 'royalblue', 'darkblue', 'lightcyan']

fig1 = px.pie(rec_count_data, values="RECORD_COUNT", names="MESSAGE_NO", color_discrete_sequence=custom_colors)
fig2 = px.scatter(total_rec_data, x="MESSAGE_NO", y="MODIFIED", color="MESSAGE_NO")

app.layout = html.Div(style={'backgroundColor': 'black'}, children=[
    html.H1('Tibco Monitoring Dashboard', style={'textAlign': 'center', 'color': 'white'}),
    html.Div([
        dcc.Graph(id='firstgraph1', figure=fig1, style={'display': 'inline-block', 'width': '50%'}),
        dash_table.DataTable(
            id='data-table',
            data=last_mod_data.to_dict('records'),
            columns=[{'name': 'INTERFACE', 'id': 'INTERFACE'}, {'name': 'MESSAGE_NO', 'id': 'MESSAGE_NO'}, {'name': 'MODIFIED', 'id': 'MODIFIED'}],
            style_table={'display': 'inline-block', 'width': '80%'},
            style_cell={'minWidth': '200px', 'width': '200px', 'maxWidth': '200px', 'textAlign': 'left'},
            tooltip_conditional=[
                {
                    'if': {'column_id': 'MODIFIED', 'filter_query': '{MODIFIED_1} > 30'},
                    'type': 'markdown',
                    'value': 'This process hasn\'t received any record from 30 mins'
                },
                {
                    'if': {'column_id': 'MODIFIED', 'filter_query': '{MODIFIED_1} > 60'},
                    'type': 'markdown',
                    'value': 'This process hasn\'t received any record from 60 mins'
                },
                {
                    'if': {'column_id': 'MODIFIED', 'filter_query': '{MODIFIED_1} > 90'},
                    'type': 'markdown',
                    'value': 'This process hasn\'t received any record from 90 mins'
                }
            ],
            style_data={
                'text-align': 'left'  # Align cell text to the right
            },
            style_header={
                'text-align': 'left'  # Align header text to the left
            },
            style_data_conditional=[
                {
                    'if': {'column_id': 'MODIFIED', 'filter_query': '{MODIFIED_1} > 30' },
                    'backgroundColor': 'yellow',
                    'color': 'black'
                },
                {
                    'if': {'column_id': 'MODIFIED', 'filter_query': '{MODIFIED_1} > 60'},
                    'backgroundColor': 'purple',  # Color for greater than 90 minutes
                    'color': 'white'  # Text color for greater than 90 minutes
                },
                {
                    'if': {'column_id': 'MODIFIED', 'filter_query': '{MODIFIED_1} > 90'},
                    'backgroundColor': 'red',  # Color for greater than 90 minutes
                    'color': 'white'  # Text color for greater than 90 minutes
                }
            ],
            tooltip_delay=0,
            tooltip_duration=None
        )
    ], style={'display': 'flex','text-align': 'center'}),

    dcc.Graph(id='firstgraph2', figure=fig2, style={'backgroundColor': 'black'}),

    html.Div([
        html.A("Download Excel", id="btn_xlsx", download="output.xlsx", href="", target="_blank"),
    ]),
])

@app.callback(
    Output("btn_xlsx", "href"),
    Input("btn_xlsx", "n_clicks"),
    prevent_initial_call=True
)

def download_xlsx(n_clicks):
    if n_clicks is None:
        raise PreventUpdate

    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    last_mod_data.to_excel(writer, sheet_name='Sheet1', index=False)
    writer.save()
    output.seek(0)
    xlsx_data = base64.b64encode(output.read()).decode()

    return f'data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{xlsx_data}'

if __name__ == '__main__':
    app.run_server(port=8080, host='127.0.0.1', debug=True)


# In[ ]:





# In[ ]:




