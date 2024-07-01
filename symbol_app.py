import streamlit as st
import pandas as pd
import sqlite3
from itertools import zip_longest
import datetime
from environment_data import *
# Function to fetch data for the first table from the SQLite database
def fetch_data(symbol, selected_date, db_config):
    conn = sqlite3.connect(db_config['db_path'])
    cursor = conn.cursor()

    query = """
    SELECT
        c.DATE1,
        n.CLOSE_PRICE,
        n.PREV_CLOSE,
        n.DELIV_PER,
        n.Ratio,
        n.TURNOVER_LACS AS Volume,
        n.AVG_PRICE,
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
        NSE_LIVEDATA n
    ON
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


    conn = sqlite3.connect(db_config['db_path'])
    symbols = pd.read_sql("SELECT DISTINCT SYMBOL FROM NSE_LIVEDATA WHERE TRIM(SERIES) = 'EQ'", conn)['SYMBOL'].tolist()
    conn.close()

    
    col1, col2, col3 = st.columns([4, 1, 1])
    default_date = datetime.datetime.strptime(selected_date, "%Y-%m-%d").date()

    # Create a date input widget with the default date
    with col3:
        selected_date = st.date_input("Select Date", value=default_date)

    with col2:
        selected_symbol = st.selectbox("Select a symbol", symbols, index=symbols.index(symbol))

    with col1:
        st.markdown(f'<h2>Data for {symbol}</h2>', unsafe_allow_html=True)

    if selected_symbol != symbol:
        symbol = selected_symbol

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

st.set_page_config(
    page_title="NSE All",
    page_icon=":chart_with_upwards_trend:",
    layout="wide",
    initial_sidebar_state="expanded",
)
def main():
    
    query_params = st.query_params
    selected_symbol = query_params.get('symbol')
    selected_date=query_params.get('date')                                 
    
    if selected_symbol:
        print(f"Selected date : {selected_date} .. Selected Symbol : {selected_symbol}")
        display_symbol_data(selected_symbol, selected_date)
    else:
        st.write("No symbol selected.")

if __name__ == "__main__":
    main()
