import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import plotly.io as pio

pio.templates.default = "plotly_white"
color_pal1 = ['#59733F','#D99982','#F2B33D','#BF7B6B','#4A868C','#2C3740',
              '#BF544B','#A68877','#8C8B77','#F2B33D','#8C8C8C','#A3A651']
color_pal2 = ['#735439','#A7BF8F','#F2D5C4','#468AD8']

def historical_position(df:object):
    fig = make_subplots(rows=2, cols=1, subplot_titles=("per market", "per position"),
                        row_heights=[0.6,0.4], shared_xaxes=True, vertical_spacing = 0.10)

    col1 = 0
    for ser1 in df['Market_and_Exchange_Names'].unique():
        temp = pd.pivot_table(data=df[df['Market_and_Exchange_Names'] == ser1].copy(),
                              index=['As_of_Date_In_Form_YYMMDD'],
                              values=['Value_signed']).reset_index()
        
        fig.add_trace(go.Scatter(x=temp['As_of_Date_In_Form_YYMMDD'], y=temp['Value_signed'],
                                 name=ser1, line=dict(color=color_pal1[col1])), row=1, col=1)
        col1 +=1

    col2 = 0
    for ser2 in df['Position_type'].unique():
        temp = pd.pivot_table(data=df[df['Position_type'] == ser2].copy(),
                            index=['As_of_Date_In_Form_YYMMDD'],
                            values=['Value_signed']).reset_index()
        
        fig.add_trace(go.Scatter(x=temp['As_of_Date_In_Form_YYMMDD'], y=temp['Value_signed'],
                                 name=ser2, line=dict(color=color_pal2[col2])), row=2, col=1)
        col2 +=1
    
    fig.update_layout(height=740, width=1300, title_text="CFTC position")
    
    return fig