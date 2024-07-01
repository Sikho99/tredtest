import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import io
import os

def StoreDataintoDBSqlite(db_path, Start_date, End_date, Cur_date, CSV_file_path,csv_content=None, onlineMode=False):

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    file_date_present=False
    Series_EQ=False
    CSV_folder_path=os.path.dirname(CSV_file_path)
    def convert_date_format(date_str):
        try:
            return datetime.strptime(date_str, '%d-%b-%Y').strftime('%Y-%m-%d')
        except ValueError as e:
            print(f"Error converting date: {e}")
            return None 
    def date_exists(date):
        cursor.execute("SELECT 1 FROM NSEData_date WHERE Date1 = ?", (date,))
        return cursor.fetchone() is not None

    start_date_dt = datetime.strptime(Start_date, '%Y-%m-%d')
    end_date_dt = datetime.strptime(End_date, '%Y-%m-%d')

    if onlineMode==False:
        if CSV_file_path == "":
            for filename in os.listdir(CSV_folder_path):
                if filename.startswith('sec_bhavdata_full_') and filename.endswith('.csv'):
                    date_str = filename.replace('sec_bhavdata_full_', '').replace('.csv', '')
                    file_date_dt = datetime.strptime(date_str, '%d%m%Y')

                    if start_date_dt <= file_date_dt <= end_date_dt:
                        CSV_file_path = os.path.join(CSV_folder_path, filename)
                        data_df = pd.read_csv(CSV_file_path)
                        data_df.columns = data_df.columns.str.strip()
                        data_df['DATE1'] = data_df['DATE1'].apply(lambda x: convert_date_format(x.strip()))
                        data_df['Ratio'] = data_df['TTL_TRD_QNTY'] / data_df['NO_OF_TRADES']
                        file_date = data_df['DATE1'].iloc[0]
                        if not date_exists(file_date):
                            file_date_present=False

                            columns = ', '.join(data_df.columns)
                            placeholders = ', '.join(['?'] * len(data_df.columns))
                            
                            for _, row in data_df.iterrows():
                                if row['SERIES'].strip() == 'EQ':
                                    insert_sql = f"INSERT INTO NSE_LIVEDATA ({columns}) VALUES ({placeholders})"
                                    Series_EQ=True

                                else:
                                    insert_sql = f"INSERT INTO NSE_LIVEDATA_Oth ({columns}) VALUES ({placeholders})"
                                    Series_EQ=False
                                
                                cursor.execute(insert_sql, tuple(row))
                            conn.commit()
                            cursor.execute("INSERT INTO NSEData_date (Date1) VALUES (?)", (file_date,))
                            conn.commit()
                            print(f"Data inserted successfully from : {CSV_file_path}.")
                        else:
                            print(f"Data present in database of Date  {file_date}.")
                            file_date_present=True
        else:
            try:
                data_df = pd.read_csv(CSV_file_path)
                data_df.columns = data_df.columns.str.strip()
                data_df['DATE1'] = data_df['DATE1'].apply(lambda x: convert_date_format(x.strip()))
                data_df['Ratio'] = data_df['TTL_TRD_QNTY'] / data_df['NO_OF_TRADES']
                file_date = data_df['DATE1'].iloc[0]
                if not date_exists(file_date):
                    file_date_present=False
                    columns = ', '.join(data_df.columns)
                    placeholders = ', '.join(['?'] * len(data_df.columns))

                    for _, row in data_df.iterrows():
                        try:
                            if row['SERIES'].strip() == 'EQ':
                                insert_sql = f"INSERT INTO NSE_LIVEDATA ({columns}) VALUES ({placeholders})"
                                Series_EQ=True
                            else:
                                insert_sql = f"INSERT INTO NSE_LIVEDATA_Oth ({columns}) VALUES ({placeholders})"
                                Series_EQ=False
                            
                            cursor.execute(insert_sql, tuple(row))
                        except sqlite3.Error as e:
                            print(f"Error inserting row: {e}")
                    conn.commit()
                    cursor.execute("INSERT INTO NSEData_date (Date1) VALUES (?)", (file_date,))
                    conn.commit()
                    print(f"Data inserted successfully from : {CSV_file_path}.")
                else:
                        print(f"Data present in database of Date  {file_date}.")
                        file_date_present=True
            except Exception as e:
                print(f"Error processing file: {e}")
    else:
        if csv_content != None:
            data_df = pd.read_csv(io.StringIO(csv_content))
            data_df.columns = data_df.columns.str.strip()
  
            data_df.columns = data_df.columns.str.strip()
            data_df['DATE1'] = data_df['DATE1'].apply(lambda x: convert_date_format(x.strip()))
            data_df['Ratio'] = data_df['TTL_TRD_QNTY'] / data_df['NO_OF_TRADES']
            file_date = data_df['DATE1'].iloc[0]
            if not date_exists(file_date):
                file_date_present=False
                columns = ', '.join(data_df.columns)
                placeholders = ', '.join(['?'] * len(data_df.columns))

                for _, row in data_df.iterrows():
                    try:
                        if row['SERIES'].strip() == 'EQ':
                            insert_sql = f"INSERT INTO NSE_LIVEDATA ({columns}) VALUES ({placeholders})"
                            Series_EQ=True
                        else:
                            insert_sql = f"INSERT INTO NSE_LIVEDATA_Oth ({columns}) VALUES ({placeholders})"
                            Series_EQ=False
                        
                        cursor.execute(insert_sql, tuple(row))
                    except sqlite3.Error as e:
                        print(f"Error inserting row: {e}")
                conn.commit()
                cursor.execute("INSERT INTO NSEData_date (Date1) VALUES (?)", (file_date,))
                conn.commit()
                print(f"Data inserted successfully from : {CSV_file_path}.")
            else:
                    print(f"Data present in database of Date  {file_date}.")
                    file_date_present=True
        else:
            print(f"Online data of {CSV_file_path} csv data is none")

    cursor.close()
    conn.close()

    if file_date_present==False and Series_EQ ==True:
        perform_calculations_and_store(db_path, Cur_date)
        print("All data has been successfully loaded and calculations performed.")

def perform_calculations_and_store(db_path, Cur_date):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    start_date = Cur_date - timedelta(days=60)
    Prev_date = Cur_date - timedelta(days=1)
    start_date_str = start_date.strftime('%Y-%m-%d')
    Prev_date_str = Prev_date.strftime('%Y-%m-%d')
    Cur_date_str = Cur_date.strftime('%Y-%m-%d')

    sql_query = '''
    WITH RankedData AS (
        SELECT
            SYMBOL,
            SERIES,
            DATE1,
            CLOSE_PRICE,
            PREV_CLOSE,
            AVG_PRICE,
            DELIV_PER,
            Ratio,
            TTL_TRD_QNTY,
            TURNOVER_LACS,
            (CLOSE_PRICE - PREV_CLOSE) AS PRICE_CHANGE,
            ((CLOSE_PRICE - PREV_CLOSE) / PREV_CLOSE) * 100 AS PriceChange_Per,
            ROW_NUMBER() OVER (PARTITION BY SYMBOL ORDER BY DATE1 DESC) AS rn
        FROM NSE_LIVEDATA
        WHERE DATE1 BETWEEN ? AND ? and TRIM(SERIES) = "EQ"
    ),
    AggregatedData AS (
        SELECT
            SYMBOL,
            SERIES,
            DATE1,
            CLOSE_PRICE,
            PREV_CLOSE,
            AVG_PRICE,
            DELIV_PER,
            Ratio,
            TTL_TRD_QNTY,
            TURNOVER_LACS,
            PRICE_CHANGE,
            PriceChange_Per,
            
            AVG(CLOSE_PRICE) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS Avg_Close_1_Days,
            AVG(CLOSE_PRICE) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS Avg_Close_2_Days,
            AVG(CLOSE_PRICE) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS Avg_Close_3_Days,
            AVG(CLOSE_PRICE) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 12 PRECEDING AND CURRENT ROW) AS Avg_Close_4_Days,

            AVG(AVG_PRICE) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS Avg_AvgPri_1_Days,
            AVG(AVG_PRICE) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS Avg_AvgPri_2_Days,
            AVG(AVG_PRICE) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS Avg_AvgPri_3_Days,
            AVG(AVG_PRICE) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 12 PRECEDING AND CURRENT ROW) AS Avg_AvgPri_4_Days,

            AVG(DELIV_PER) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS Avg_Deliv_1_Days,
            AVG(DELIV_PER) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS Avg_Deliv_2_Days,
            AVG(DELIV_PER) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS Avg_Deliv_3_Days,
            AVG(DELIV_PER) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 12 PRECEDING AND CURRENT ROW) AS Avg_Deliv_4_Days,
            AVG(DELIV_PER) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 21 PRECEDING AND CURRENT ROW) AS Avg_Deliv_5_Days,
            AVG(DELIV_PER) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 43 PRECEDING AND CURRENT ROW) AS Avg_Deliv_6_Days,
            AVG(DELIV_PER) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS Avg_Deliv_7_Days,

            AVG(Ratio) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS Avg_Ratio_1_Days,
            AVG(Ratio) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS Avg_Ratio_2_Days,
            AVG(Ratio) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS Avg_Ratio_3_Days,
            AVG(Ratio) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 12 PRECEDING AND CURRENT ROW) AS Avg_Ratio_4_Days,
            AVG(Ratio) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 21 PRECEDING AND CURRENT ROW) AS Avg_Ratio_5_Days,
            AVG(Ratio) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 43 PRECEDING AND CURRENT ROW) AS Avg_Ratio_6_Days,
            AVG(Ratio) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS Avg_Ratio_7_Days,

            AVG(TURNOVER_LACS) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS Avg_vol_1_Days,
            AVG(TURNOVER_LACS) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS Avg_vol_2_Days,
            AVG(TURNOVER_LACS) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS Avg_vol_3_Days,
            AVG(TURNOVER_LACS) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 12 PRECEDING AND CURRENT ROW) AS Avg_vol_4_Days,

            AVG(PRICE_CHANGE) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS Avg_PriChange_1_Days,
            AVG(PRICE_CHANGE) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS Avg_PriChange_2_Days,
            AVG(PRICE_CHANGE) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS Avg_PriChange_3_Days,
            AVG(PRICE_CHANGE) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 12 PRECEDING AND CURRENT ROW) AS Avg_PriChange_4_Days,

            AVG(PriceChange_Per) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS Avg_PriceChange_Per_1_Days,
            AVG(PriceChange_Per) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS Avg_PriceChange_Per_2_Days,
            AVG(PriceChange_Per) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS Avg_PriceChange_Per_3_Days,
            AVG(PriceChange_Per) OVER (PARTITION BY SYMBOL ORDER BY DATE1 ROWS BETWEEN 12 PRECEDING AND CURRENT ROW) AS Avg_PriceChange_Per_4_Days
        FROM RankedData
    )
    SELECT 
        a.*, 
        p.Ratio AS Prev_Ratio
    FROM 
        AggregatedData a
    LEFT JOIN 
        (SELECT SYMBOL, DATE1, Ratio FROM RankedData WHERE DATE1 = ?) p
    ON 
        a.SYMBOL = p.SYMBOL
    WHERE 
        a.DATE1 = ?
    '''

    pd.set_option('future.no_silent_downcasting', True)

    data_df = pd.read_sql_query(sql_query, conn, params=(start_date_str, Cur_date_str, Prev_date_str, Cur_date_str))
    #print(data_df[['DELIV_PER', 'Avg_Deliv_1_Days', 'Avg_Deliv_2_Days', 'Avg_Deliv_3_Days', 'Avg_Deliv_4_Days', 'Avg_Deliv_5_Days', 'Avg_Deliv_6_Days', 'Avg_Deliv_7_Days']])
    
    data_df['CMP_AVG_P_SIGNAL1'] = data_df.apply(lambda row: 1 if row['CLOSE_PRICE'] > row['Avg_Close_1_Days'] else 0, axis=1)
    data_df['CMP_AVG_P_SIGNAL2'] = data_df.apply(lambda row: 1 if row['Avg_Close_1_Days'] > row['Avg_Close_2_Days'] else 0, axis=1)
    data_df['CMP_AVG_P_SIGNAL3'] = data_df.apply(lambda row: 1 if row['Avg_Close_2_Days'] > row['Avg_Close_3_Days'] else 0, axis=1)
    data_df['CMP_AVG_P_SIGNAL4'] = data_df.apply(lambda row: 1 if row['Avg_Close_3_Days'] > row['Avg_Close_4_Days'] else 0, axis=1)

    data_df['AVG_P_SIGNAL1'] = data_df.apply(lambda row: 1 if row['AVG_PRICE'] > row['Avg_AvgPri_1_Days'] else 0, axis=1)
    data_df['AVG_P_SIGNAL2'] = data_df.apply(lambda row: 1 if row['Avg_AvgPri_1_Days'] > row['Avg_AvgPri_2_Days'] else 0, axis=1)
    data_df['AVG_P_SIGNAL3'] = data_df.apply(lambda row: 1 if row['Avg_AvgPri_2_Days'] > row['Avg_AvgPri_3_Days'] else 0, axis=1)
    data_df['AVG_P_SIGNAL4'] = data_df.apply(lambda row: 1 if row['Avg_AvgPri_3_Days'] > row['Avg_AvgPri_4_Days'] else 0, axis=1)

    data_df['PERCENT_DELIVERY_SIGNAL1'] = data_df.apply(lambda row: 1 if row['DELIV_PER'] > row['Avg_Deliv_1_Days'] else 0, axis=1)
    data_df['PERCENT_DELIVERY_SIGNAL2'] = data_df.apply(lambda row: 1 if row['Avg_Deliv_1_Days'] > row['Avg_Deliv_2_Days'] else 0, axis=1)
    data_df['PERCENT_DELIVERY_SIGNAL3'] = data_df.apply(lambda row: 1 if row['Avg_Deliv_2_Days'] > row['Avg_Deliv_3_Days'] else 0, axis=1)
    data_df['PERCENT_DELIVERY_SIGNAL4'] = data_df.apply(lambda row: 1 if row['Avg_Deliv_3_Days'] > row['Avg_Deliv_4_Days'] else 0, axis=1)

    data_df['RATIO_SIGNAL1'] = data_df.apply(lambda row: 1 if row['Ratio'] > row['Avg_Ratio_1_Days'] else 0, axis=1)
    data_df['RATIO_SIGNAL2'] = data_df.apply(lambda row: 1 if row['Avg_Ratio_1_Days'] > row['Avg_Ratio_2_Days'] else 0, axis=1)
    data_df['RATIO_SIGNAL3'] = data_df.apply(lambda row: 1 if row['Avg_Ratio_2_Days'] > row['Avg_Ratio_3_Days'] else 0, axis=1)
    data_df['RATIO_SIGNAL4'] = data_df.apply(lambda row: 1 if row['Avg_Ratio_3_Days'] > row['Avg_Ratio_4_Days'] else 0, axis=1)

    data_df['Prev_Ratio'] = data_df['Prev_Ratio'].fillna(0).infer_objects(copy=False)

    data_df['SIGNAL1'] = data_df.apply(lambda row: 1 if row['Ratio'] > (row['Avg_Ratio_7_Days'] * 2) else 0, axis=1)
    data_df['SIGNAL2'] = data_df.apply(lambda row: 1 if row['Ratio'] > (row['Prev_Ratio'] * 2) else 0, axis=1)
  
    # Only keep the necessary columns for insertion
    columns_to_drop = [
        'CLOSE_PRICE', 'PREV_CLOSE', 'AVG_PRICE', 'DELIV_PER', 'Ratio', 
        'TTL_TRD_QNTY', 'TURNOVER_LACS','Prev_Ratio'
    ]
    data_df_to_insert = data_df.drop(columns=columns_to_drop)

    calc_columns = ', '.join(data_df_to_insert.columns)
    calc_placeholders = ', '.join(['?'] * len(data_df_to_insert.columns))
    insert_calc_sql = f"INSERT INTO CALCULATED_DATA ({calc_columns}) VALUES ({calc_placeholders})"

    for _, row in data_df_to_insert.iterrows():
        cursor.execute(insert_calc_sql, tuple(row))
    
    conn.commit()
    cursor.close()
    conn.close()