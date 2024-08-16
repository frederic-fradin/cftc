import pandas as pd
import os
import requests
from zipfile import ZipFile
from datetime import date
import streamlit as st

today = date.today()
current_year = today.year

'''
Text file per year from 
https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalCompressed/index.htm

Disaggregated Futures-and-Options Combined Reports
https://www.cftc.gov/files/dea/history/com_disagg_txt_2024.zip

Traders in Financial Futures ; Futures Only Reports
https://www.cftc.gov/files/dea/history/fut_fin_txt_2024.zip

'''

def load_com_disagg(year:int=2024) -> None:
    #  Download the data behind the URL
    url = f'https://www.cftc.gov/files/dea/history/com_disagg_txt_{year}.zip'
    response = requests.get(url)

    #  Open the response generated into a new file in your local called image.jpg
    open("../data/raw/cftc.zip", "wb").write(response.content)

    # loading the temp.zip and creating a zip object 
    with ZipFile('../data/raw/cftc.zip', 'r') as zObject: 
        zObject.extractall(path='../data/raw/')
        zObject.extract("c_year.txt", path='../data/raw/') 
        zObject.close()

    # rename file with year to loop through a period
    try:
        os.rename("../data/raw/c_year.txt", f"../data/raw/cftc_{year}.txt")
    except FileExistsError:
        print("File already Exists")
        print("Removing existing file")
        # skip the below code if you don't' want to forcefully rename
        os.remove(f"../data/raw/cftc_{year}.txt")
        # rename it
        os.rename("../data/raw/c_year.txt", f"../data/raw/cftc_{year}.txt")
        print('Done renaming a file')

    print(f'CFTC {year} download with success!')

def consolidate_com_disagg() -> None:
    # assign directory
    directory = '../data/raw/'
    init = pd.DataFrame()

    # iterate over files in that directory
    for filename in os.listdir(directory):
        file = os.path.join(directory, filename)
        split_tup = os.path.splitext(filename)
        # checking if it is a file
        if os.path.isfile(file) and split_tup[1] == '.txt':
            raw = pd.read_table(file, delimiter=",", low_memory=False)
            first_23_columns = raw.iloc[:, :23]
            last_6_columns = raw.iloc[:, -6:]
            select = pd.concat([first_23_columns, last_6_columns], axis=1)
            init = pd.concat([init, select], axis=0)
            init.replace('.', 0, inplace=True)

    init.to_parquet('../data/processed/cftc.parquet', engine='fastparquet')
    print('Consolidate with success!')

    keep_columns = ['Market_and_Exchange_Names', 'As_of_Date_In_Form_YYMMDD',
       'Report_Date_as_MM_DD_YYYY', 'CFTC_Contract_Market_Code',
       'CFTC_Market_Code', 'CFTC_Region_Code', 'CFTC_Commodity_Code',
       'Contract_Units', 'CFTC_Contract_Market_Code_Quotes',
       'CFTC_Market_Code_Quotes', 'CFTC_Commodity_Code_Quotes',
       'CFTC_SubGroup_Code', 'FutOnly_or_Combined',
       'Report_Date_as_YYYY-MM-DD']

    # Melting the DataFrame to transform other columns into rows
    df_melted = init.melt(id_vars=keep_columns, 
                        var_name='argument', 
                        value_name='value')

    df_melted['As_of_Date_In_Form_YYMMDD'] = pd.to_datetime(df_melted['As_of_Date_In_Form_YYMMDD'])
    df_melted['Report_Date_as_MM_DD_YYYY'] = pd.to_datetime(df_melted['Report_Date_as_MM_DD_YYYY'])
    df_melted['Report_Date_as_YYYY-MM-DD'] = pd.to_datetime(df_melted['Report_Date_as_YYYY-MM-DD'])

    print(f'Melt dataframe with success!', end='\n\n')
    df_melted.to_parquet('../data/cleaned/cftc.parquet', engine='fastparquet')
    print(df_melted.info())

@st.cache_data
def read_com_disagg() -> object:
    df = pd.read_parquet('./data/cleaned/cftc.parquet', engine='fastparquet')

    market_code = df['CFTC_Market_Code'].unique().tolist()

    return df, market_code

if __name__ == "__main__":
    for i in range(2024, 2024, 1):
        load_com_disagg(i)
    # load_com_disagg(current_year)
    consolidate_com_disagg()