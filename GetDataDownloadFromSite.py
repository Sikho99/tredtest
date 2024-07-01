import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from importnse_data_sqlite import StoreDataintoDBSqlite
from environment_data import *
import io
import zipfile

def download_NSE_csv(date, folder_path):
    print(f"folder path : {folder_path}")
    url = f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{date.strftime('%d')}{date.strftime('%m')}{date.strftime('%Y')}.csv"
    response = requests.get(url)
    if response.status_code == 200:
        filename = f"sec_bhavdata_full_{date.strftime('%d')}{date.strftime('%m')}{date.strftime('%Y')}.csv"
        file_path = os.path.join(folder_path, filename)
        with open(file_path, 'wb') as file:
            file.write(response.content)
        return file_path
    else:
        print(f"Failed to download file for date: {date.strftime('%Y-%m-%d')}")
        return None
def download_BSE_csv(date, folder_path):
    print(f"Folder path: {folder_path}")
    url = f"https://www.bseindia.com/download/BhavCopy/Equity/EQ{date.strftime('%d')}{date.strftime('%m')}{date.strftime('%Y')}_CSV.ZIP"
    response = requests.get(url)
    if response.status_code == 200:
        zip_file = zipfile.ZipFile(io.BytesIO(response.content))
        filename = f"EQ{date.strftime('%d')}{date.strftime('%m')}{date.strftime('%Y')}.csv"
        for file_info in zip_file.infolist():
            if file_info.filename.endswith('.CSV'):
                extracted_file = zip_file.extract(file_info, folder_path)
                os.rename(extracted_file, os.path.join(folder_path, filename))
                return os.path.join(folder_path, filename)
        print("CSV file not found in the zip archive.")
        return None
    else:
        print(f"Failed to download file for date: {date.strftime('%Y-%m-%d')}")
        return None

def ImportDataFromweb(folder_path,from_date,to_date,db_config):
    #from_date = input("Enter the from date (YYYY-MM-DD): ")
    #to_date = input("Enter the to date (YYYY-MM-DD): ")
    #folder_path = input("Enter the folder path to save CSV files: ")
    #folder_path=f"E:\TCL\Projects\ANIL\SM\NSE2"

    os.makedirs(folder_path, exist_ok=True)
    start_date = datetime.strptime(from_date, '%Y-%m-%d')
    end_date = datetime.strptime(to_date, '%Y-%m-%d')
    delta = timedelta(days=1)

    while start_date <= end_date:
        filename = f"sec_bhavdata_full_{start_date.strftime('%d')}{start_date.strftime('%m')}{start_date.strftime('%Y')}.csv"
        file_path_check= Path(folder_path) / filename
        file_path = os.path.join(folder_path, filename)
        if file_path_check.exists():
            print(f"File exist : {file_path}")
        else:
            file_full_path = download_NSE_csv(start_date, folder_path)
            if not file_full_path == None:
                StoreDataintoDBSqlite(from_date,to_date,start_date,file_full_path)

            """
            filename = f"EQ{start_date.strftime('%d')}{start_date.strftime('%m')}{start_date.strftime('%Y')}_CSV.ZIP"
            file_path_check= Path(folder_path) / filename
            file_path = os.path.join(folder_path, filename)
            if file_path_check.exists():
                print(f"File exist : {file_path}")
            else:
                file_full_path = download_BSE_csv(start_date, folder_path)
                if not file_full_path == None:
                    StoreDataintoDBSqlite(from_date,to_date,start_date,file_full_path)
            """



    #    store_in_db(file_path, db_connection)
        start_date += delta
