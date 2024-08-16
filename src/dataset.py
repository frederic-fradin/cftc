import pandas as pd
import os
import requests
from zipfile import ZipFile

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
            select = raw.iloc[:,:23]
            print(select.shape)
            init = pd.concat([init, select], axis=0)
            init.replace('.', 0, inplace=True)

    print(f'CFTC consolidé : {init.shape}')
    init.to_parquet('../data/processed/cftc.parquet', engine='fastparquet')


if __name__ == "__main__":
    consolidate_com_disagg()