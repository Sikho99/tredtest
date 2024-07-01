import streamlit as st
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from environment_data import *
# Function to fetch and process data from the SQLite database
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

st.set_page_config(
    page_title="NSE All Symbol Data",
    page_icon=":chart_with_upwards_trend:",
    layout="wide",
    initial_sidebar_state="expanded",
)

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
def main():

    col1, col2 = st.columns([5, 1])
    with col2:
        selected_date = st.date_input("Select a date", value=datetime.now(), key="date_picker_HighPrio")
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

    # Fetch and process data
    data = fetch_data(date_str, db_config)
    data['Symbol'] = data.apply(lambda row: f'<a href="http://localhost:8502/?symbol={row["Symbol"]}&date={date_str}" target="_blank">{row["Symbol"]}</a>', axis=1)


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
        data1['Column1'] = data1['Column1'].apply(lambda x: round(x, 2))

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

if __name__ == "__main__":
    main()
