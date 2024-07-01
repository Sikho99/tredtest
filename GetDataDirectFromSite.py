import os
import requests
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import io
import zipfile
from importnse_data_sqlite import StoreDataintoDBSqlite 
import time
from environment_data import *
def download_NSE_csv(date):
    max_retries=int(app_config['max_retries'])
    retry_delay=int(app_config['retry_delay'])
    url = f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{date.strftime('%d%m%Y')}.csv"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    for attempt in range(int(max_retries)):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.content.decode('utf-8')
        except requests.exceptions.ChunkedEncodingError as e:
            print(f"ChunkedEncodingError on attempt {attempt + 1}: {e}")
            if attempt + 1 < int(max_retries):
                time.sleep(retry_delay)
            else:
                raise
        except requests.exceptions.RequestException as e:
            print(f"RequestException on attempt {attempt + 1}: {e}")
            if attempt + 1 < int(max_retries):
                time.sleep(retry_delay)
            else:
                raise
    return None

def download_BSE_csv(date):
    url = f"https://www.bseindia.com/download/BhavCopy/Equity/EQ{date.strftime('%d')}{date.strftime('%m')}{date.strftime('%Y')}_CSV.ZIP"
    response = requests.get(url)
    if response.status_code == 200:
        zip_file = zipfile.ZipFile(io.BytesIO(response.content))
        for file_info in zip_file.infolist():
            if file_info.filename.endswith('.CSV'):
                with zip_file.open(file_info) as file:
                    return file.read().decode('utf-8')
        print("CSV file not found in the zip archive.")
        return None
    else:
        print(f"Failed to download file for date: {date.strftime('%Y-%m-%d')}")
        return None

def store_data_in_db(csv_content, table_name, conn):
    try:
        df = pd.read_csv(io.StringIO(csv_content))
        df.to_sql(table_name, conn, if_exists='append', index=False)
        print(f"Data stored in table {table_name} successfully.")
    except Exception as e:
        print(f"Failed to store data in table {table_name}. Error: {e}")

def import_data_from_web_WO_Dwnlod(from_date, to_date, db_config):
    db_path = db_config['db_path']
    conn = sqlite3.connect(db_path)
    
    start_date = datetime.strptime(from_date, '%Y-%m-%d')
    end_date = datetime.strptime(to_date, '%Y-%m-%d')
    delta = timedelta(days=1)
    
    while start_date <= end_date:
        try:
            csv_content = download_NSE_csv(start_date)
            if csv_content:
                StoreDataintoDBSqlite(db_path, from_date, to_date, start_date, CSV_file_path="",csv_content=csv_content, onlineMode=True)
            #store_data_in_db(csv_content, 'NSE_LIVEDATA', conn)
                print(f"Loading data for date : {start_date}")
        except Exception as e:
            print(f"Failed to import data: {e}")
        # Uncomment below lines if you want to include BSE data as well
        """
        csv_content = download_BSE_csv(start_date)
        if csv_content:
            store_data_in_db(csv_content, 'BSE_LIVEDATA', conn)
        """
        start_date += delta

    conn.close()
