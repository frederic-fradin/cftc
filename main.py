import streamlit as st
from src import read_com_disagg

st.set_page_config(
    page_title="CFTC",
    page_icon="👋",
    layout = 'wide'
    )

create_page = st.Page("./pages/disagg.py", title="Disaggregated Reports", icon=":material/add_circle:")

st.title("Disaggregated Reports")
st.markdown("""---""")

df, market_code = read_com_disagg()

col1, col2, col3 = st.columns([7.5,0.25,2.25])
market = col3.selectbox('Market code', options=market_code)

if market:
    contrat_code = df[(df['CFTC_Market_Code'] == market)]['Market_and_Exchange_Names'].unique().tolist()

contract = col3.multiselect('Contract code', options=contrat_code)
validate = col3.button('Load data', 'load', use_container_width=True)
st.write('')

if validate:
    data = df[(df['Market_and_Exchange_Names'].isin(contract))].copy()
    col1.dataframe(data)

