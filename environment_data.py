db_config = {
    'user': 'root',
    'password': '',
    'host': 'localhost',
    'database': 'NSEALL',
    'db_type':'sqlit',
    'db_path':'NSEALLDATA.db',
 }
app_config = {
    
    'base_symbol_url':"https://tradetesting-symbol.streamlit.app",
    #'base_symbol_url': 'http://localhost:8502'  # Change this to your Streamlit URL when deploying
    'max_retries': '3',
    'retry_delay':'5',
    'Download_type':'online',  #online = direct load data without downloading ,, offline = download file from web and then store into db
    'folder_path_NSE': '../NSE',
    'folder_path_BSE' : '../BSE'
}

create_NSE_LIVEDATA = '''
    CREATE TABLE IF NOT EXISTS NSE_LIVEDATA (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        SYMBOL TEXT,
        SERIES TEXT,
        DATE1 DATE,
        PREV_CLOSE REAL,
        OPEN_PRICE REAL,
        HIGH_PRICE REAL,
        LOW_PRICE REAL,
        LAST_PRICE REAL,
        CLOSE_PRICE REAL,
        AVG_PRICE REAL,
        TTL_TRD_QNTY REAL,
        TURNOVER_LACS REAL,
        NO_OF_TRADES INTEGER,
        DELIV_QTY INTEGER,
        DELIV_PER REAL,
        Ratio REAL
    );
    '''

create_calculated_table_sql = '''
    CREATE TABLE IF NOT EXISTS CALCULATED_DATA (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        SYMBOL TEXT,
        SERIES TEXT,
        DATE1 DATE,
       
        PRICE_CHANGE REAL,
        PriceChange_Per REAL,
        
        Avg_Close_1_Days REAL,
        Avg_Close_2_Days REAL,
        Avg_Close_3_Days REAL,
        Avg_Close_4_Days REAL,

        Avg_AvgPri_1_Days REAL,
        Avg_AvgPri_2_Days REAL,
        Avg_AvgPri_3_Days REAL,
        Avg_AvgPri_4_Days REAL,

        Avg_Deliv_1_Days REAL,
        Avg_Deliv_2_Days REAL,
        Avg_Deliv_3_Days REAL,
        Avg_Deliv_4_Days REAL,
        Avg_Deliv_5_Days REAL,
        Avg_Deliv_6_Days REAL,
        Avg_Deliv_7_Days REAL,

        Avg_Ratio_1_Days REAL,
        Avg_Ratio_2_Days REAL,
        Avg_Ratio_3_Days REAL,
        Avg_Ratio_4_Days REAL,
        Avg_Ratio_5_Days REAL,
        Avg_Ratio_6_Days REAL,
        Avg_Ratio_7_Days REAL,

        Avg_vol_1_Days REAL,
        Avg_vol_2_Days REAL,
        Avg_vol_3_Days REAL,
        Avg_vol_4_Days REAL,

        Avg_PriChange_1_Days REAL,
        Avg_PriChange_2_Days REAL,
        Avg_PriChange_3_Days REAL,
        Avg_PriChange_4_Days REAL,

        Avg_PriceChange_Per_1_Days REAL,
        Avg_PriceChange_Per_2_Days REAL,
        Avg_PriceChange_Per_3_Days REAL,
        Avg_PriceChange_Per_4_Days REAL,

        CMP_AVG_P_SIGNAL1 INTEGER,
        CMP_AVG_P_SIGNAL2 INTEGER,
        CMP_AVG_P_SIGNAL3 INTEGER,
        CMP_AVG_P_SIGNAL4 INTEGER,

        AVG_P_SIGNAL1 INTEGER,
        AVG_P_SIGNAL2 INTEGER,
        AVG_P_SIGNAL3 INTEGER,
        AVG_P_SIGNAL4 INTEGER,

        PERCENT_DELIVERY_SIGNAL1 INTEGER,
        PERCENT_DELIVERY_SIGNAL2 INTEGER,
        PERCENT_DELIVERY_SIGNAL3 INTEGER,
        PERCENT_DELIVERY_SIGNAL4 INTEGER,

        RATIO_SIGNAL1 INTEGER,
        RATIO_SIGNAL2 INTEGER,
        RATIO_SIGNAL3 INTEGER,
        RATIO_SIGNAL4 INTEGER,
        SIGNAL1 INTEGER,
        SIGNAL2 INTEGER
        );

    '''
create_date_table_sql = '''
    CREATE TABLE IF NOT EXISTS NSEData_date (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Date1 DATE
    );
    '''
create_NSE_LIVEDATA_oth = '''
    CREATE TABLE IF NOT EXISTS NSE_LIVEDATA_Oth (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        SYMBOL TEXT,
        SERIES TEXT,
        DATE1 DATE,
        PREV_CLOSE REAL,
        OPEN_PRICE REAL,
        HIGH_PRICE REAL,
        LOW_PRICE REAL,
        LAST_PRICE REAL,
        CLOSE_PRICE REAL,
        AVG_PRICE REAL,
        TTL_TRD_QNTY REAL,
        TURNOVER_LACS REAL,
        NO_OF_TRADES INTEGER,
        DELIV_QTY INTEGER,
        DELIV_PER REAL,
        Ratio REAL
    );
    '''
create_NSE_FNO = '''
    CREATE TABLE IF NOT EXISTS NSE_FNO (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        SYMBOL TEXT,
        SERIES TEXT);
        '''
