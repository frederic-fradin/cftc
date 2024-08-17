import streamlit as st
from src import read_com_disagg, read_args_com_disagg, historical_position

st.set_page_config(
    page_title="CFTC",
    page_icon="👋",
    layout = 'wide'
    )

st.Page("./pages/disagg.py", title="Disaggregated Reports", icon=":material/add_circle:")

st.title("CFTC Dashbord")
st.markdown("""---""")

pvt_args = read_args_com_disagg()

col1, col2, col3 = st.columns([7.5,0.25,2.25])

select_1 = col3.multiselect('Exchange', pvt_args['CFTC_Market_Code'].unique())
select_2 = col3.multiselect('Market', pvt_args[pvt_args['CFTC_Market_Code'].isin(select_1)]['Market_and_Exchange_Names'].unique())
select_3 = col3.multiselect('Classifications', pvt_args['Classifications'].unique())
select_4 = col3.multiselect('Position', pvt_args[pvt_args['Classifications'].isin(select_3)]['Position_type'].unique())
validate = col3.button('UPDATE', key='val1', use_container_width=True)

if validate:
    # try:
        df = read_com_disagg(exchange=select_1, market=select_2, classif=select_3, position=select_4)
        col1.plotly_chart(historical_position(df), use_container_width=True)
        col1.dataframe(df, use_container_width=True, hide_index=True)
    # except:
    #     col3.error('Invalid requests')