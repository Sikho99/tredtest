import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from environment_data import *
import sqlite3
import numpy as np
from itertools import zip_longest
from GetDataDownloadFromSite import ImportDataFromweb 
from environment_data import *
from GetDataDirectFromSite import import_data_from_web_WO_Dwnlod
st.set_page_config(
    page_title="NSE All",
    page_icon=":chart_with_upwards_trend:",
    layout="wide",
    initial_sidebar_state="expanded",
)
# Functions to load different content based on the button clicked
def load_dashboard():
    # Function to fetch and process data from the SQLite database
    @st.cache_data
    def fetch_data(date_str, db_config):
        st.markdown(
        """
        <style>
            .main-title {
                padding-top: 0 !important;
            }
            .datepicker-width {
                width: 150px !important;
            }
            [data-testid="stHorizontalBlock"] > div > div {
                flex: 1;
                width: 100%;
            }
            [data-testid="stTabs"] > div {
                display: flex;
                flex-direction: column;
                width: 100%;
            }
            .no-top-padding .stTextInput > div > div > input {
                padding-top: 0 !important;
            }
            .custom-height {
                height: 56px; /* Set your desired height */
                display: flex;
                align-items: center; /* Center the content vertically */
            }
            .dataframe-container {
                overflow-x: auto;
            }
            .dataframe {
                width: 100%;
            }
        </style>
        """,
        unsafe_allow_html=True
        )
        
    # Apply custom CSS to the date picker
        st.markdown(
            """
            <style>
            div[data-testid="stDateInput"] > div > div > div > input {
                width: 150px;
            }
            </style>
            """,
            unsafe_allow_html=True
        )

        engine = create_engine(f"sqlite:///{db_config['db_path']}")
        conn = engine.connect()

        query = """
        WITH RankedData AS (
            SELECT
                SYMBOL,
                DATE1,
                Ratio,
                CLOSE_PRICE,
                PREV_CLOSE,
                TURNOVER_LACS,
                TTL_TRD_QNTY,
                DELIV_PER,
                ROW_NUMBER() OVER (PARTITION BY SYMBOL ORDER BY DATE1 DESC) AS rn,
                AVG(TURNOVER_LACS) OVER (
                    PARTITION BY SYMBOL 
                    ORDER BY DATE1 
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS Avg_Turnover_20_days,
                AVG(CLOSE_PRICE) OVER (
                    PARTITION BY SYMBOL 
                    ORDER BY DATE1 
                    ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
                ) AS Avg_Close_2_days,
                AVG(DELIV_PER) OVER (
                    PARTITION BY SYMBOL 
                    ORDER BY DATE1 
                    ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
                ) AS Avg_Delivery_2_days,
                AVG(Ratio) OVER (
                    PARTITION BY SYMBOL 
                    ORDER BY DATE1 
                    ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
                ) AS Avg_Ratio_2_days,
                AVG(TTL_TRD_QNTY) OVER (
                    PARTITION BY SYMBOL 
                    ORDER BY DATE1 
                    ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
                ) AS Avg_TTL_TRD_QNTY_2_days,
                AVG(CLOSE_PRICE) OVER (
                    PARTITION BY SYMBOL 
                    ORDER BY DATE1 
                    ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
                ) AS Avg_Price_2_days
            FROM NSE_LIVEDATA
            WHERE DATE1 BETWEEN DATE(?, '-65 days') AND ?
            AND TRIM(SERIES) = 'EQ'
        ),
        PriceAndRatioData AS (
            SELECT
                SYMBOL,
                MAX(CASE WHEN rn = 1 THEN CLOSE_PRICE END) AS ClosePrice_Today,
                MAX(CASE WHEN rn = 1 THEN Ratio END) AS Ratio_Today,
                MAX(CASE WHEN rn = 2 THEN Ratio END) AS Ratio_PrevDay,
                MAX(CASE WHEN rn = 1 THEN PREV_CLOSE END) AS PrevClose,
                MAX(CASE WHEN rn = 1 THEN TURNOVER_LACS END) AS Turnover_Today,
                MAX(CASE WHEN rn = 1 THEN DELIV_PER END) AS Delivery_Today,
                MAX(CASE WHEN rn = 1 THEN Avg_Turnover_20_days END) AS Avg_Turnover_20_days,
                MAX(CASE WHEN rn = 1 THEN Avg_Close_2_days END) AS Avg_Close_2_days,
                MAX(CASE WHEN rn = 1 THEN Avg_Delivery_2_days END) AS Avg_Delivery_2_days,
                MAX(CASE WHEN rn = 1 THEN Avg_Ratio_2_days END) AS Avg_Ratio_2_days,
                MAX(CASE WHEN rn = 1 THEN Avg_TTL_TRD_QNTY_2_days END) AS Avg_TTL_TRD_QNTY_2_days,
                MAX(CASE WHEN rn = 1 THEN Avg_Price_2_days END) AS Avg_Price_2_days
            FROM RankedData
            GROUP BY SYMBOL
        ),
        CalculatedData AS (
            SELECT
                SYMBOL,
                Ratio_Today,
                Ratio_PrevDay,
                ClosePrice_Today,
                PrevClose,
                Turnover_Today,
                Avg_Turnover_20_days,
                Delivery_Today,
                Avg_Close_2_days,
                Avg_Delivery_2_days,
                Avg_Ratio_2_days,
                Avg_TTL_TRD_QNTY_2_days,
                Avg_Price_2_days,
                CASE WHEN Ratio_PrevDay IS NOT NULL AND Ratio_Today > (Ratio_PrevDay * 3) THEN 'SPT' ELSE '-' END AS Day2_3,
                CASE WHEN Ratio_PrevDay IS NOT NULL AND Ratio_Today > (Ratio_PrevDay * 5) THEN 'SPT' ELSE '-' END AS Day2_5,
                ROUND(ClosePrice_Today, 2) AS Current_Price,
                CASE WHEN PrevClose IS NOT NULL THEN ROUND(((ClosePrice_Today - PrevClose) / PrevClose) * 100, 2) ELSE 'N/A' END AS Change_of_Percent,
                ROUND(Avg_Turnover_20_days, 2) AS Avg_Turnover_20_days,
                ROUND(Turnover_Today, 2) AS Turnover_Today,
                CASE WHEN Turnover_Today > Avg_Turnover_20_days THEN 'UP' ELSE '-' END AS Volume_Signal,
                CASE WHEN Turnover_Today > (Avg_Turnover_20_days * 2) THEN 'High' ELSE '-' END AS HighVol_Signal
            FROM PriceAndRatioData
        )
        SELECT
            SYMBOL,
            Day2_3,
            Day2_5,
            Current_Price,
            Change_of_Percent,
            Avg_Turnover_20_days,
            Turnover_Today,
            Volume_Signal,
            HighVol_Signal,
            ClosePrice_Today AS Close_Price1,
            Avg_Close_2_days AS Close_Price2,
            Delivery_Today AS Delivery1_Percent,
            Avg_Delivery_2_days AS Delivery2_Percent,
            Ratio_Today AS Ratio1,
            Avg_Ratio_2_days AS Ratio2,
            Avg_Price_2_days AS Avg_Price1,
            Avg_Price_2_days AS Avg_Price2,
            Turnover_Today AS Volume1,
            Avg_TTL_TRD_QNTY_2_days AS Volume2
        FROM CalculatedData
        ;
        """
        print(f"Data catch Start date : {date_str}")
        results = pd.read_sql_query(query, conn, params=(date_str,date_str,))
        
        conn.close()

        # Process the results
        results.insert(0, 'Sr.No', range(1, len(results) + 1))
        results = results.rename(columns={
            "SYMBOL": "Symbol",
            "Day2_3": "Day23",
            "Day2_5": "Day25",
            "Current_Price": "Cur Price",
            "Change_of_Percent": "% Change",
            "Avg_Turnover_20_days": "Avg1 Vol",
            "Turnover_Today": "Vol",
            "Volume_Signal": "Vol Sgnl",
            "HighVol_Signal": "HVol Sgnl",
            "Close_Price1": "Cl Pri1",
            "Close_Price2": "Cl Pri2",
            "Delivery1_Percent": "Delvry1 %",
            "Delivery2_Percent": "Delvry2 %",
            "Ratio1": "Ratio1",
            "Ratio2": "Ratio2",
            "Avg_Price1": "Avg Pri1",
            "Avg_Price2": "Avg Pri2",
            "Volume1": "Volume1",
            "Volume2": "Volume2"
        })

        return results
   

    # Streamlit app

    col1, col2 = st.columns([5, 1])
    with col2:
        selected_date = st.date_input("Select a date", value=datetime.now(), key="date_picker_dash")
    date_str = selected_date.strftime('%Y-%m-%d')
    with col1:
        st.markdown('<h1 class="main-title">NSE All Data</h1>', unsafe_allow_html=True)


    # Fetch and process data
  
    def data_available(date_str, db_config):
        conn = sqlite3.connect(db_config['db_path'])
      
        query ="Select * from NSEDATA_Date where Date1= ?"
        results = pd.read_sql_query(query, conn, params=(date_str,))
        
        conn.close()
        if not results.empty:
            return True
        else:
            st.write(f" No Data available in given date : {date_str}")
            return False

    data_avail = data_available(date_str, db_config)
    if data_avail==True :

        data = fetch_data(date_str, db_config)
        base_url = app_config['base_symbol_url']
        data['Symbol'] = data.apply(lambda row: f'<a href="{base_url}/?symbol={row["Symbol"]}&date={date_str}" target="_blank">{row["Symbol"]}</a>', axis=1)

        # Define custom styles for the DataFrame
        def style_df(row):
            styles = []
            for col in row.index:
                if col == 'Day23' and row[col] == 'SPT':
                    styles.append('background-color: orange')
                elif col == 'Day25' and row[col] == 'SPT':
                    styles.append('background-color: green')
                elif col == 'Vol Sgnl' and row[col] == 'UP':
                    styles.append('background-color: violet')
                elif col == 'HVol Sgnl' and row[col] == 'High':
                    styles.append('background-color: green')
                else:
                    styles.append('')
            return styles
        
        st.markdown(
            """
            <style>
            .dataframe td, .dataframe th {
                text-align: right;
            }
            .dataframe th:nth-child(1) {
                min-width: 150px;
            }
            </style>
            """,
            unsafe_allow_html=True
        )
        try:

            data1 = data.apply(lambda x: f"{float(x):.2f}" if isinstance(x, (float)) else x)
           # data1 = data.applymap(lambda x: f"{float(x):.2f}" if isinstance(x, (float)) else x)
            data1.reset_index(drop=True, inplace=True)
            # Apply styles
            styled_df = data1.style.apply(style_df, axis=1) \
                                .set_properties(subset=["Cur Price", "% Change","Avg1 Vol","Vol"], **{'text-align': 'right'}) \
                                .set_properties(subset=["Day23", "Day25","Vol Sgnl","HVol Sgnl"], **{'text-align': 'center'}) \
                                .set_properties(subset=['Symbol'], **{'text-align': 'center', 'color': 'blue', 'text-decoration': 'underline'}) \
                                .set_table_styles([{'selector': 'td.col2', 'props': [('text-align', 'right')]},
                                                    {'selector': 'td.col3', 'props': [('text-align', 'right')]},
                                                    {'selector': 'td.col4', 'props': [('text-align', 'right')]},
                                                    {'selector': 'td.col5', 'props': [('text-align', 'right')]},
                                                    {'selector': 'td.col6', 'props': [('text-align', 'right')]},
                                                    {'selector': 'td.col8', 'props': [('text-align', 'right')]},
                                                    {'selector': 'td.col9', 'props': [('text-align', 'right')]},
                                                    {'selector': 'td.col10', 'props': [('text-align', 'right')]},
                                                    {'selector': 'td.col11', 'props': [('text-align', 'right')]},
                                                    {'selector': 'td.col12', 'props': [('text-align', 'right')]},
                                                    {'selector': 'td.col13', 'props': [('text-align', 'right')]},
                                                    {'selector': 'td.col14', 'props': [('text-align', 'right')]},
                                                    {'selector': 'td.col15', 'props': [('text-align', 'right')]}
                                                ]) \
                                .set_table_styles([ {'selector': 'td', 'props': [('padding', '2px')]},
                                                   {'selector': 'th', 'props': [('padding', '2px')]} ]) \


            # Render the styled DataFrame to HTML and display it in Streamlit
            st.markdown( styled_df.to_html(classes='dataframe'), unsafe_allow_html=True)
        except Exception as e:
            print(f"Exceptions :: {e}")


def load_hot_cake():
    @st.cache_data
    def fetch_data(date_str, db_config):
        engine = create_engine(f"sqlite:///{db_config['db_path']}")
        conn = engine.connect()

        query = """
        WITH RankedData AS (
        SELECT
            SYMBOL,
            DATE1,
            SERIES,
            Ratio,
            CLOSE_PRICE,
            PREV_CLOSE,
            TURNOVER_LACS,
            TTL_TRD_QNTY,
            ROW_NUMBER() OVER (PARTITION BY SYMBOL ORDER BY DATE1 DESC) AS rn,
            AVG(TURNOVER_LACS) OVER (
                PARTITION BY SYMBOL 
                ORDER BY DATE1 
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS Avg_Turnover_20_days
        FROM NSE_LIVEDATA
        WHERE DATE1 BETWEEN DATE(?, '-65 days') AND ?
        AND TRIM(SERIES) = 'EQ'
    ),
    PriceAndRatioData AS (
        SELECT
            SYMBOL,
            SERIES,
            MAX(CASE WHEN rn = 1 THEN DATE1 END) AS Date1,
            MAX(CASE WHEN rn = 1 THEN CLOSE_PRICE END) AS ClosePrice_Today,
            MAX(CASE WHEN rn = 1 THEN Ratio END) AS Ratio_Today,
            MAX(CASE WHEN rn = 2 THEN Ratio END) AS Ratio_PrevDay,
            MAX(CASE WHEN rn = 1 THEN PREV_CLOSE END) AS PrevClose,
            MAX(CASE WHEN rn = 1 THEN TURNOVER_LACS END) AS Turnover_Today,
            MAX(CASE WHEN rn = 1 THEN Avg_Turnover_20_days END) AS Avg_Turnover_20_days
        FROM RankedData
        GROUP BY SYMBOL, SERIES
    ),
    CalculatedData AS (
        SELECT
            p.SYMBOL,
            p.SERIES,
            p.Date1,
            p.Ratio_Today,
            p.Ratio_PrevDay,
            p.ClosePrice_Today,
            p.PrevClose,
            p.Turnover_Today,
            p.Avg_Turnover_20_days,
            CASE WHEN p.Ratio_PrevDay IS NOT NULL AND p.Ratio_Today > (p.Ratio_PrevDay * 3) THEN 'SP' ELSE '-' END AS Day2_3,
            CASE WHEN p.Ratio_PrevDay IS NOT NULL AND p.Ratio_Today > (p.Ratio_PrevDay * 5) THEN 'SP' ELSE '-' END AS Day2_5,
            ROUND(p.ClosePrice_Today, 2) AS Current_Price,
            CASE WHEN p.PrevClose IS NOT NULL THEN ROUND(((p.ClosePrice_Today - p.PrevClose) / p.PrevClose) * 100, 2) ELSE 'N/A' END AS Change_of_Percent,
            ROUND(p.Avg_Turnover_20_days, 2) AS Avg_Turnover_20_days,
            ROUND(p.Turnover_Today, 2) AS Turnover_Today,
            CASE WHEN p.Turnover_Today > p.Avg_Turnover_20_days THEN 'UP' ELSE '-' END AS Volume_Signal,
            CASE WHEN p.Turnover_Today > (p.Avg_Turnover_20_days * 2) THEN 'High' ELSE '-' END AS HighVol_Signal,
            c.CMP_AVG_P_SIGNAL1,
            c.AVG_P_SIGNAL1,
            c.PERCENT_DELIVERY_SIGNAL1,
            c.RATIO_SIGNAL1,
            c.SIGNAL1,
            c.SIGNAL2,
            c.Avg_Deliv_1_Days,
            c.Avg_Deliv_2_Days,
            c.Avg_Deliv_3_Days,
            c.Avg_Deliv_4_Days,
            c.Avg_Deliv_5_Days,
            c.Avg_Deliv_6_Days,
            c.Avg_Deliv_7_Days,
            c.Avg_Ratio_1_Days,
            c.Avg_Ratio_2_Days,
            c.Avg_Ratio_3_Days,
            c.Avg_Ratio_4_Days,
            c.Avg_Ratio_5_Days,
            c.Avg_Ratio_6_Days,
            c.Avg_Ratio_7_Days
        FROM PriceAndRatioData p
        LEFT JOIN CALCULATED_DATA c
        ON p.SYMBOL = c.SYMBOL AND p.SERIES = c.SERIES AND p.Date1 = c.Date1
    )
    SELECT
        SYMBOL,
        Day2_3,
        Day2_5,
        Current_Price,
        Change_of_Percent,
        Avg_Turnover_20_days,
        Turnover_Today,
        Volume_Signal,
        HighVol_Signal,
        CMP_AVG_P_SIGNAL1,
        AVG_P_SIGNAL1,
        PERCENT_DELIVERY_SIGNAL1,
        RATIO_SIGNAL1,
        SIGNAL1,
        SIGNAL2,
        Avg_Deliv_1_Days,
        Avg_Deliv_2_Days,
        Avg_Deliv_3_Days,
        Avg_Deliv_4_Days,
        Avg_Deliv_5_Days,
        Avg_Deliv_6_Days,
        Avg_Deliv_7_Days,
        Avg_Ratio_1_Days,
        Avg_Ratio_2_Days,
        Avg_Ratio_3_Days,
        Avg_Ratio_4_Days,
        Avg_Ratio_5_Days,
        Avg_Ratio_6_Days,
        Avg_Ratio_7_Days
    FROM CalculatedData
    WHERE Day2_3 = 'SP' OR Day2_5 = 'SP'
    ORDER BY
        CASE WHEN SYMBOL GLOB '[A-Za-z]*' THEN 1 ELSE 2 END,
        SYMBOL;
        """

        results = pd.read_sql_query(query, conn, params=(date_str, date_str))
        
        # Check if symbols are in NSE_FNO
        fno_symbols = pd.read_sql_query("SELECT SYMBOL FROM NSE_FNO ORDER BY SYMBOL DESC", conn)
        fno_symbols_set = set(fno_symbols['SYMBOL'])
        results['Is_FNO'] = results['SYMBOL'].apply(lambda x: x in fno_symbols_set)
        conn.close()

        # Process the results and remove the Sr.No and Is_FNO columns
        results = results.rename(columns={
            "SYMBOL": "Symbol",
            "Day2_3": "Day1",
            "Day2_5": "Day2",
            "Current_Price": "Current Price",
            "Change_of_Percent": "Change of %",
            "Avg_Turnover_20_days": "Avg Vol",
            "Turnover_Today": "Volume",
            "Volume_Signal": "Vol Sgnl",
            "HighVol_Signal": "HVol Sgnl",
            "CMP_AVG_P_SIGNAL1": "CMP-AVG P SNGL",
            "AVG_P_SIGNAL1": "AVG P SGNL",
            "PERCENT_DELIVERY_SIGNAL1": "%DEL SGNL",
            "RATIO_SIGNAL1": "RATIO SGN",
            "SIGNAL1": "SGN1",
            "SIGNAL2": "SGN2",
            "Avg_Deliv_1_Days": "Avg Delv 1",
            "Avg_Deliv_2_Days": "Avg Delv 2",
            "Avg_Deliv_3_Days": "Avg Delv 3",
            "Avg_Deliv_4_Days": "Avg Delv 4",
            "Avg_Deliv_5_Days": "Avg Delv 5",
            "Avg_Deliv_6_Days": "Avg Delv 6",
            "Avg_Deliv_7_Days": "Avg Delv 7",
            "Avg_Ratio_1_Days": "Avg Ratio 1",
            "Avg_Ratio_2_Days": "Avg Ratio 2",
            "Avg_Ratio_3_Days": "Avg Ratio 3",
            "Avg_Ratio_4_Days": "Avg Ratio 4",
            "Avg_Ratio_5_Days": "Avg Ratio 5",
            "Avg_Ratio_6_Days": "Avg Ratio 6",
            "Avg_Ratio_7_Days": "Avg Ratio 7"
        })

        # Drop the 'Sr.No' and 'Is_FNO' columns
        #results = results.drop(columns=['Is_FNO'])

        return results
    st.markdown(
        """
        <style>
            .main-title {
                padding-top: 0 !important;
            }
            .datepicker-width {
                width: 150px !important;
            }
            [data-testid="stHorizontalBlock"] > div > div {
                flex: 1;
                width: 100%;
            }
            [data-testid="stTabs"] > div {
                display: flex;
                flex-direction: column;
                width: 100%;
            }
            .no-top-padding .stTextInput > div > div > input {
                padding-top: 0 !important;
            }
            .custom-height {
                height: 56px; /* Set your desired height */
                display: flex;
                align-items: center; /* Center the content vertically */
            }
        </style>
        """,
        unsafe_allow_html=True
    )

    # Streamlit app


    col1, col2 = st.columns([5, 1])
    with col2:
        selected_date = st.date_input("Select a date", value=datetime.now(), key="date_picker_hotcake")
    date_str = selected_date.strftime('%Y-%m-%d')
    with col1:
        st.markdown('<h1 class="main-title">NSE All Symbol Data</h1>', unsafe_allow_html=True)

    # Apply custom CSS to the date picker
    st.markdown(
        """
        <style>
        div[data-testid="stDateInput"] > div > div > div > input {
            width: 150px;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    def data_available(date_str, db_config):
        conn = sqlite3.connect(db_config['db_path'])
      
        query ="Select * from NSEDATA_Date where Date1= ?"
        results = pd.read_sql_query(query, conn, params=(date_str,))
        conn.close()
        if not results.empty:
            return True
        else:
            st.write(f" No Data available in given date : {date_str}")
            return False

    data_avail = data_available(date_str, db_config)
    if data_avail==True :
        data = fetch_data(date_str, db_config)
        #data['Symbol'] = data.apply(lambda row: f'<a href="http://localhost:8502/?symbol={row["Symbol"]}&date={date_str}" target="_blank">{row["Symbol"]}</a>', axis=1)
        base_url = app_config['base_symbol_url']
        data['Symbol'] = data.apply(lambda row: f'<a href="{base_url}/?symbol={row["Symbol"]}&date={date_str}" target="_blank">{row["Symbol"]}</a>', axis=1)
    
        # Define custom styles for the DataFrame
        def style_df(row):
            styles = []
            for col in row.index:
                if col in ["CMP-AVG P SNGL", "AVG P SGNL", "%DEL SGNL", "RATIO SGN"]:
                    styles.append('background-color: green; color: green' if row[col] == 1 else 'background-color: white; color: white')
                elif col in ["SGN1", "SGN2"]:
                    styles.append('background-color: orange; color: orange' if row[col] == 1 else 'background-color: white; color: white')
                elif col == 'Day1' and row[col] == 'SP':
                    styles.append('background-color: orange')
                elif col == 'Day2' and row[col] == 'SP':
                    styles.append('background-color: green')
                elif col == 'Vol Sgnl' and row[col] == 'UP':
                    styles.append('background-color: violet')
                elif col == 'HVol Sgnl' and row[col] == 'High':
                    styles.append('background-color: green')
                elif col == 'Symbol' and row['Is_FNO']:
                    styles.append('background-color: lightgrey')
                else:
                    styles.append('')
            return styles

        st.markdown(
            """
            <style>
            .dataframe td, .dataframe th {
                text-align: left;
                padding: 2px; /* Adjust padding to make the cell size smaller */
            }
            .dataframe th:nth-child(1) {
                min-width: 150px;
            }
            </style>
            """,
            unsafe_allow_html=True
        )

        try:
            data1 = data.apply(lambda x: f"{float(x):.2f}" if isinstance(x, (float)) else x)
            #data1 = data.applymap(lambda x: f"{float(x):.2f}" if isinstance(x, (float)) else x)
            data1.reset_index(drop=True, inplace=True)

            # Apply styles
            styled_df = data1.style.apply(style_df, axis=1) \
                                .set_properties(**{'text-align': 'left', 'padding': '2px'}) \
                                .set_properties(subset=["Day1", "Day2", "Vol Sgnl", "HVol Sgnl", "Symbol"], **{'text-align': 'center'}) \
                                .set_properties(subset=["Current Price", "Change of %", "Avg Vol", "Volume"], **{'text-align': 'right'}) \
                                .set_properties(subset=['Symbol'], **{'color': 'blue', 'text-decoration': 'underline'}) \
                                .set_table_styles([
                                    {'selector': 'td', 'props': [('padding', '2px')]},
                                    {'selector': 'th', 'props': [('padding', '2px')]}
                                ])

            # Render the styled DataFrame to HTML and display it in Streamlit
            st.markdown(styled_df.to_html(), unsafe_allow_html=True)
        except Exception as e:
            st.error(f"Exception: {e}")


def load_direct_symbol_data():
    def fetch_data(symbol, selected_date, db_config):

        conn = sqlite3.connect(db_config['db_path'])
        cursor = conn.cursor()
        query = """
        SELECT
            n.DATE1,
            n.CLOSE_PRICE,
            n.PREV_CLOSE,
            n.DELIV_PER,
            n.Ratio,
            n.TURNOVER_LACS AS Volume,
            n.AVG_PRICE,
            c.DATE1,
            c.PRICE_CHANGE,
            c.PriceChange_Per,
            c.Avg_Close_1_Days,
            c.Avg_Close_2_Days,
            c.Avg_Close_3_Days,
            c.Avg_Close_4_Days,
            c.Avg_AvgPri_1_Days,
            c.Avg_AvgPri_2_Days,
            c.Avg_AvgPri_3_Days,
            c.Avg_AvgPri_4_Days,
            c.Avg_PriChange_1_Days,
            c.Avg_PriChange_2_Days,
            c.Avg_PriChange_3_Days,
            c.Avg_PriChange_4_Days,
            c.Avg_PriceChange_Per_1_Days,
            c.Avg_PriceChange_Per_2_Days,
            c.Avg_PriceChange_Per_3_Days,
            c.Avg_PriceChange_Per_4_Days,
            c.Avg_Deliv_1_Days,
            c.Avg_Deliv_2_Days,
            c.Avg_Deliv_3_Days,
            c.Avg_Deliv_4_Days,
            c.Avg_Ratio_1_Days,
            c.Avg_Ratio_2_Days,
            c.Avg_Ratio_3_Days,
            c.Avg_Ratio_4_Days,
            c.Avg_vol_1_Days,
            c.Avg_vol_2_Days,
            c.Avg_vol_3_Days,
            c.Avg_vol_4_Days,
            c.Avg_Deliv_5_Days,
            c.Avg_Deliv_6_Days,
            c.Avg_Deliv_7_Days,
            c.Avg_Ratio_5_Days,
            c.Avg_Ratio_6_Days,
            c.Avg_Ratio_7_Days,
            c.CMP_AVG_P_SIGNAL1,
            c.CMP_AVG_P_SIGNAL2,
            c.CMP_AVG_P_SIGNAL3,
            c.CMP_AVG_P_SIGNAL4,
            c.AVG_P_SIGNAL1,
            c.AVG_P_SIGNAL2,
            c.AVG_P_SIGNAL3,
            c.AVG_P_SIGNAL4,
            c.PERCENT_DELIVERY_SIGNAL1,
            c.PERCENT_DELIVERY_SIGNAL2,
            c.PERCENT_DELIVERY_SIGNAL3,
            c.PERCENT_DELIVERY_SIGNAL4,
            c.RATIO_SIGNAL1,
            c.RATIO_SIGNAL2,
            c.RATIO_SIGNAL3,
            c.RATIO_SIGNAL4
        FROM
            CALCULATED_DATA c
        JOIN
            NSE_LIVEDATA n on
            c.DATE1 = n.DATE1 AND c.SYMBOL = n.SYMBOL AND c.SERIES = n.SERIES
        WHERE
            c.SYMBOL = ? AND c.DATE1 = ?
        ORDER BY
            c.DATE1 DESC
        LIMIT 1;
        """
        cursor.execute(query, (symbol, selected_date))

        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if result:
            columns = [desc[0] for desc in cursor.description]
            return pd.DataFrame([result], columns=columns)
        else:
            return pd.DataFrame()

    # Function to fetch data for the second table from the SQLite database
    def fetch_signal_data(symbol, start_date, end_date, db_config):
        conn = sqlite3.connect(db_config['db_path'])
        cursor = conn.cursor()

        query = """
        SELECT
            n.DATE1,
            n.PREV_CLOSE,
            n.CLOSE_PRICE,
            n.AVG_PRICE,
            c.PRICE_CHANGE,
            c.PriceChange_Per,
            n.DELIV_PER,
            n.Ratio,
            n.DELIV_QTY,
            n.TURNOVER_LACS AS Volume,
            c.Avg_Ratio_1_Days,
            c.Avg_Ratio_2_Days,
            c.Avg_Ratio_3_Days,
            c.Avg_Ratio_4_Days,
            c.Avg_Ratio_5_Days,
            c.Avg_Ratio_6_Days,
            c.Avg_Ratio_7_Days,
            c.Avg_Deliv_1_Days,
            c.Avg_Deliv_2_Days,
            c.Avg_Deliv_3_Days,
            c.Avg_Deliv_4_Days,
            c.Avg_Deliv_5_Days,
            c.Avg_Deliv_6_Days,
            c.Avg_Deliv_7_Days,
            c.SIGNAL1,
            c.SIGNAL2
        FROM
            CALCULATED_DATA c
        JOIN
            NSE_LIVEDATA n
        ON
            c.DATE1 = n.DATE1 AND c.SYMBOL = n.SYMBOL AND c.SERIES = n.SERIES
        WHERE
            c.SYMBOL = ? AND c.DATE1 BETWEEN ? AND ?
        ORDER BY
            c.DATE1 DESC;
        """
        print(f"Check Symbol : {symbol}, start_date :{start_date} , end_date {end_date}")
        cursor.execute(query, (symbol, start_date, end_date))

        results = cursor.fetchall()
        cursor.close()
        conn.close()

        if results:
            columns = [desc[0] for desc in cursor.description]
            return pd.DataFrame(results, columns=columns)
        else:
            return pd.DataFrame()

    def create_table1(row):
        avg_days = [1, 2, 3, 4, 5]
        period1_days = [1, 2, 3, 4]
        period2_days = [1, 2, 3]
        columns = ['Avg. Days', 'CMP', 'Avg. Price', 'Price Change', '% Change', '% Delivery', 'Ratio', 'Volume', 
                'CMP / AVG.P Signal', 'AVG.P Signal', '% Delivery Signal', 'Ratio Signal', 
                'Period1', 'Delivery1 %', 'Ratio1', 'Period2', 'Delivery2 %', 'Ratio2']
        table1 = pd.DataFrame(columns=columns)

        rows_list = []

        for days, period1_day, period2_day in zip_longest(avg_days, period1_days, period2_days, fillvalue=None):
            row_dict = dict.fromkeys(columns, '')
            row_dict['Avg. Days'] = days

            if days == 1:
                row_dict['CMP'] = row['CLOSE_PRICE']
                row_dict['Avg. Price'] = row['AVG_PRICE']
                row_dict['Price Change'] = row['PRICE_CHANGE']
                row_dict['% Change'] = row['PriceChange_Per']
                row_dict['% Delivery'] = row['DELIV_PER']
                row_dict['Ratio'] = row['Ratio']
                row_dict['Volume'] = row['Volume']
            else:
                row_dict[f'CMP'] = row[f'Avg_Close_{days-1}_Days']
                row_dict[f'Avg. Price'] = row[f'Avg_AvgPri_{days-1}_Days']
                row_dict[f'Price Change'] = row[f'Avg_PriChange_{days-1}_Days']
                row_dict[f'% Change'] = row[f'Avg_PriceChange_Per_{days-1}_Days']
                row_dict[f'% Delivery'] = row[f'Avg_Deliv_{days-1}_Days']
                row_dict[f'Ratio'] = row[f'Avg_Ratio_{days-1}_Days']
                row_dict[f'Volume'] = row[f'Avg_vol_{days-1}_Days']

            if period1_day:
                row_dict['Period1'] = period1_day
                row_dict[f'Delivery1 %'] = row[f'Avg_Deliv_{period1_day}_Days']
                row_dict[f'Ratio1'] = row[f'Avg_Ratio_{period1_day}_Days']

            if period2_day:
                row_dict['Period2'] = period2_day
                row_dict[f'Delivery2 %'] = row[f'Avg_Deliv_{period2_day + 4}_Days']
                row_dict[f'Ratio2'] = row[f'Avg_Ratio_{period2_day + 4}_Days']
            if not days >= 5:
                row_dict['CMP / AVG.P Signal'] = row[f'CMP_AVG_P_SIGNAL{days}']
                row_dict['AVG.P Signal'] = row[f'AVG_P_SIGNAL{days}']
                row_dict['% Delivery Signal'] = row[f'PERCENT_DELIVERY_SIGNAL{days}']
                row_dict['Ratio Signal'] = row[f'RATIO_SIGNAL{days}']

            rows_list.append(row_dict)
        
        table1 = pd.DataFrame(rows_list)
        return table1

    def calculate_signals(row):
        signals = {}
        for i in range(1, 8):
            avg_ratio = row[f'Avg_Ratio_{i}_Days']
            avg_deliv = row[f'Avg_Deliv_{i}_Days']
            ratio = row['Ratio']
            deliv_per = row['DELIV_PER']
            
            if ratio > avg_ratio and deliv_per > avg_deliv:
                signals[f'Signal{i}'] = 'High'
            elif ratio > avg_ratio and deliv_per < avg_deliv:
                signals[f'Signal{i}'] = 'Mid'
            elif ratio < avg_ratio and deliv_per > avg_deliv:
                signals[f'Signal{i}'] = 'Low'
            else:
                signals[f'Signal{i}'] = '-'
        
        return signals

    def display_symbol_data(symbol, selected_date):
        def highlight_top_values(df, columns):
            top_values = {}
            for col in columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                top_values[col] = df[col].nlargest(2).values
            return top_values
        
        def style_df(row):
            styles = []
            for col in row.index:
                if col in ['CMP / AVG.P Signal', 'AVG.P Signal', '% Delivery Signal', 'Ratio Signal']:
                    styles.append('background-color: green; color: green' if row[col] == 1 else 'background-color: white; color: white')
                elif col in ["SGN1", "SGN2"]:
                    styles.append('background-color: orange; color: orange' if row[col] == 1 else 'background-color: white; color: white')
                elif col == 'Day1' and row[col] == 'SP':
                    styles.append('background-color: orange')
                elif col == 'Day2' and row[col] == 'SP':
                    styles.append('background-color: green')
                elif col == 'Vol Sgnl' and row[col] == 'UP':
                    styles.append('background-color: violet')
                elif col == 'HVol Sgnl' and row[col] == 'High':
                    styles.append('background-color: green')
                elif col == 'Symbol' and row['Is_FNO']:
                    styles.append('background-color: lightgrey')
                else:
                    styles.append('')
            return styles

        def detaildata_style_df(row):
            styles = []
            columns_to_highlight = ['% Delivery', 'Ratio', 'Del Qty', 'Volume']
            for col in row.index:
                if col in ['MSignal 1', 'MSignal 2']:
                    styles.append('background-color: green; color: orange' if row[col] == 1 else 'background-color: white; color: white')
                elif col in ["Signal 1", "Signal 2", "Signal 3", "Signal 4", "Signal 5", "Signal 6", "Signal 7"]:
                    if row[col] == "High":
                        styles.append('background-color: blue; color: blue')
                    elif row[col] == "Low":
                        styles.append('background-color: #B0E0E6; color: #B0E0E6')
                    elif row[col] == "Mid":
                        styles.append('background-color: #ADD8E6; color: #ADD8E6')
                    else:
                        styles.append('background-color: white; color: white')
                elif col == 'Day1' and row[col] == 'SP':
                    styles.append('background-color: orange')
                elif col == 'Day2' and row[col] == 'SP':
                    styles.append('background-color: green')
                elif col == 'Vol Sgnl' and row[col] == 'UP':
                    styles.append('background-color: violet')
                elif col == 'HVol Sgnl' and row[col] == 'High':
                    styles.append('background-color: green')
                elif col == 'Symbol' and row['Is_FNO']:
                    styles.append('background-color: lightgrey')
                elif col in columns_to_highlight:
                    # Apply highlighting logic for top 2 values
                    if row[col] in top_values[col]:
                        styles.append('background-color: green')
                    else:
                        styles.append('')
                else:
                    styles.append('')
            return styles


        #selected_symbol = st.selectbox("Select a symbol", symbols, index=symbols.index(symbol))

        #if selected_symbol != symbol:
         #   symbol = selected_symbol    
            

        data_df = fetch_data(symbol, selected_date, db_config)
        if not data_df.empty:
            data_row = data_df.iloc[0]
            summary_data = create_table1(data_row)
            
            # Fetch signal data for the past 90 days
            end_date = selected_date
            start_date = (pd.to_datetime(selected_date) - pd.DateOffset(days=90)).strftime('%Y-%m-%d')
            signal_data_df = fetch_signal_data(symbol, start_date, end_date, db_config)

            # Calculate signals
            signal_data_df = signal_data_df.apply(lambda row: pd.Series(calculate_signals(row)), axis=1).join(signal_data_df)
            # Drop the specified columns from the DataFrame
            columns_to_drop = [
                'Avg_Ratio_1_Days', 'Avg_Ratio_2_Days', 'Avg_Ratio_3_Days', 
                'Avg_Ratio_4_Days', 'Avg_Ratio_5_Days', 'Avg_Ratio_6_Days', 
                'Avg_Ratio_7_Days', 'Avg_Deliv_1_Days', 'Avg_Deliv_2_Days', 
                'Avg_Deliv_3_Days', 'Avg_Deliv_4_Days', 'Avg_Deliv_5_Days', 
                'Avg_Deliv_6_Days', 'Avg_Deliv_7_Days'
            ]
            signal_data_df = signal_data_df.drop(columns=columns_to_drop)
            
            # Rearrange columns in the desired sequence
            desired_columns_order = [
                'DATE1', 'PREV_CLOSE', 'CLOSE_PRICE', 'AVG_PRICE', 'PRICE_CHANGE', 
                'PriceChange_Per', 'DELIV_PER', 'Ratio', 'DELIV_QTY', 'Volume', 
                'Signal1', 'Signal2', 'Signal3', 'Signal4', 'Signal5', 'Signal6', 
                'Signal7', 'SIGNAL1', 'SIGNAL2'
            ]
            signal_data_df = signal_data_df.reindex(columns=desired_columns_order)

            # Rename columns
            signal_data_df = signal_data_df.rename(columns={
                'DATE1': 'Date',
                'PREV_CLOSE': 'Prv Close',
                'CLOSE_PRICE': 'Close Price',
                'AVG_PRICE': 'Avg Price',
                'PRICE_CHANGE': 'Price Change',
                'PriceChange_Per': '% P.Change',
                'DELIV_PER': '% Delivery',
                'Ratio': 'Ratio',
                'DELIV_QTY': 'Del Qty',
                'Volume': 'Volume',
                'Signal1': 'Signal 1',
                'Signal2': 'Signal 2',
                'Signal3': 'Signal 3',
                'Signal4': 'Signal 4',
                'Signal5': 'Signal 5',
                'Signal6': 'Signal 6',
                'Signal7': 'Signal 7',
                'SIGNAL1': 'MSignal 1',
                'SIGNAL2': 'MSignal 2'
            })

            # Apply number formatting

            summary_data = summary_data.apply(lambda x: f"{float(x):.2f}" if isinstance(x, (float)) else x)
            #summary_data = summary_data.applymap(lambda x: f"{float(x):.2f}" if isinstance(x, (float)) else x)
            summary_data.reset_index(drop=True, inplace=True)

            signal_data_df = signal_data_df.apply(lambda x: f"{float(x):.2f}" if isinstance(x, (float)) else x)
            #signal_data_df = signal_data_df.applymap(lambda x: f"{float(x):.2f}" if isinstance(x, (float)) else x)
            signal_data_df.reset_index(drop=True, inplace=True)
            
            columns_to_highlight = ['% Delivery', 'Ratio', 'Del Qty', 'Volume']
            global top_values
            top_values = highlight_top_values(signal_data_df, columns_to_highlight)


            styled_summary_data = summary_data.style.apply(style_df, axis=1) \
                                    .set_properties(subset=["CMP", "Avg. Price", "Price Change", "% Change", "% Delivery", "Ratio", "Volume", "Delivery1 %", "Ratio1", "Delivery2 %", "Ratio2"], 
                                                    **{'text-align': 'right'}) \
                                    .set_properties(subset=["Avg. Days", "Period1", "Period2", "CMP / AVG.P Signal", "AVG.P Signal", "% Delivery", "Ratio"], 
                                                    **{'text-align': 'center'}) \
                                    
            styled_signal_data = signal_data_df.style.apply(detaildata_style_df,axis=1) \
                                                .set_properties(**{'text-align': 'right'}) \
                                                .set_table_styles([{'selector': 'td.col0', 'props': [('width', '130px')]},
                                                                {'selector': 'td.col8', 'props': [('width', '110px')]},
                                                                    {'selector': 'td.col9', 'props': [('width', '110px')]},
                                                                    {'selector': 'td.col10', 'props': [('width', '30px')]},
                                                                    {'selector': 'td.col11', 'props': [('width', '30px')]},
                                                                    {'selector': 'td.col12', 'props': [('width', '30px')]},
                                                                    {'selector': 'td.col13', 'props': [('width', '30px')]},
                                                                    {'selector': 'td.col14', 'props': [('width', '50px')]},
                                                                    {'selector': 'td.col15', 'props': [('width', '50px')]},
                                                                    {'selector': 'td.col16', 'props': [('width', '50px')]},
                                                                    {'selector': 'td.col18', 'props': [('width', '30px')]},
                                                                    {'selector': 'td.col19', 'props': [('width', '30px')]}
                                                                    ])

            st.write(styled_summary_data.to_html(), unsafe_allow_html=True)
            st.write(styled_signal_data.to_html(), unsafe_allow_html=True)
        else:
            st.write("No data available for the selected symbol and date.")


    #query_params = st.query_params
    #selected_symbol = query_params.get('symbol')
    #selected_date=query_params.get('date')                                 
    conn = sqlite3.connect(db_config['db_path'])
    symbols = pd.read_sql("SELECT DISTINCT SYMBOL FROM NSE_LIVEDATA WHERE TRIM(SERIES) = 'EQ'", conn)['SYMBOL'].tolist()
    conn.close()

    
    col1, col2, col3 = st.columns([4, 1, 1])

    with col3:
        selected_date = st.date_input("Select Date")

    with col2:
        selected_symbol = st.selectbox("Select a symbol", symbols, index=0)

    with col1:
        st.markdown(f'<h2>Data for {selected_symbol}</h2>', unsafe_allow_html=True)

  
    display_symbol_data(selected_symbol, selected_date)



def upload_data(start_date, end_date):
    st.write("### Upload Data")
    #st.write(f"Data from {start_date} to {end_date} has been updated.")
    #st.write("Data upload and display functionality goes here.")

# Sidebar with buttons
with st.sidebar:
    st.header("Navigation")
    if st.button("Dashboard"):
        st.session_state['page'] = 'dashboard'
    if st.button("Hot Cake"):
        st.session_state['page'] = 'hot_cake'
    if st.button("Direct Symbol Data"):
        st.session_state['page'] = 'direct_symbol_data'
    if st.button("Upload Data"):
        st.session_state['page'] = 'upload_data'

import os
# Load content based on session state
if 'page' in st.session_state:
    if st.session_state['page'] == 'dashboard':
        load_dashboard()
    elif st.session_state['page'] == 'hot_cake':
        load_hot_cake()
    elif st.session_state['page'] == 'direct_symbol_data':
        load_direct_symbol_data()
    elif st.session_state['page'] == 'upload_data':

        col1, col2 = st.columns([1, 4])
        with col1:
            upload_option = st.selectbox("Select Option:", ["Upto Today", "Custom Date"])
            if upload_option == "Upto Today":
                end_date = datetime.today().date()
                start_date = end_date - timedelta(days=7)
                st.write(f"Date Range: {start_date} to {end_date}")
            elif upload_option == "Custom Date":
                start_date = st.date_input("Start Date", datetime.today() - timedelta(days=7))
                end_date = st.date_input("End Date", datetime.today())
            if st.button("Update Data"):
                upload_data(start_date, end_date)
                from_date = start_date.strftime('%Y-%m-%d')
                to_date=end_date.strftime('%Y-%m-%d')
                st.write("Data Uploading start")

                if app_config['Download_type'] == "online":
                    import_data_from_web_WO_Dwnlod(from_date,to_date,db_config)
                else:
                    os.makedirs(app_config['folder_path_NSE'], exist_ok=True) 
                    ImportDataFromweb(app_config['folder_path_NSE'],from_date,to_date,db_config)

                st.write("Data Uploading Complete")
def Create_check_db():
    print("DB Creation Started")
    db_path = db_config['db_path']
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # Create the necessary tables
    cursor.execute(create_NSE_LIVEDATA)
    cursor.execute(create_calculated_table_sql)
    cursor.execute(create_date_table_sql)
    cursor.execute(create_NSE_LIVEDATA_oth)
    cursor.execute(create_NSE_FNO)
    conn.commit()
    cursor.close()
    conn.close()
    print("DB Created")                

def app():
    print("APP function called")
    

# Load default content if no button is clicked
Create_check_db()

if 'default' not in st.session_state:
    print("DEFAULT SESSION called")
    st.session_state['default'] = True
    load_dashboard()

if __name__ == "__main__":
    app()
   # os.system("streamlit run main.py --server.port=8501 &")
   # os.system("streamlit run symbol_app.py --server.port=8502")
