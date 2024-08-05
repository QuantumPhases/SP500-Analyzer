# %% [markdown]
# Importing Liberties  

# %%
import yfinance as yf
import pandas as pd  
import requests                                                                     # Importing packages from my Python venv 
from bs4 import BeautifulSoup 
import locale
import sqlite3
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text, Date, Float
import os
import altair as alt
alt.data_transformers.enable('vegafusion')

# %% [markdown]
# Data Scraping 

# %%
sp500_url = 'http://en.wikipedia.org/wiki/List_of_S%26P_500_companies'              # Assigning link to 'sp500_url'
response = requests.get(sp500_url)                                                  # Using request to get access to 'sp500_url'

# %%
if response.status_code ==200:
    print('Request successful')
else:                                                                              # Checking status = 200 code using 'if' & 'else'
    print('Request not successful')

# %%
soup = BeautifulSoup(response.content, 'html.parser')
table = soup.find ('table')                                       # Using Beautifulsoup() to find table within my url in 'response'

# %%
data_table = pd.read_html(str(table))[0]            # Using pandas to read link from table and assigning a value to a variable 'data_table'

# %%
print("Resulting DataFrame:")

print(data_table.head(503))     

# %% [markdown]
# Data Formatting .tolist()

# %%
tickers = data_table ['Symbol'].tolist()                                                              # Assigning 'tickers' to only the 'Symbol' of the 'data_table' and convert into a list()
tickers_list = [ticker.replace('BF.B', 'BF-B').replace('BRK.B', 'BRK-B') for ticker in tickers]       # Replacing two incorrect Value 'tickers' within a variable 'tickers_list'

print('Tickers list')
print(tickers_list) 

# %%
sectors_list = data_table ['GICS Sector'].tolist()       # Convert 'GICS Sector' into a list() within 'data_table' assigned to 'sectors_list'

print('Sectors list')
print(sectors_list)

# %%
company_list = data_table ['Security'].tolist()         # Convert 'Security' into a list() within 'data_table' assigned to 'stocks_list'

print('Stocks list')
print(company_list)

# %%
Industry_list = data_table ['GICS Sub-Industry'].tolist()                     # Convert 'GICS Sub-Industry' into a list() within 'data_table' assigned to 'Industry_list'

print('Industry list')
print(Industry_list)

# %%
Location_list = data_table ['Headquarters Location'].tolist()                                  # Convert 'Headquarters Location' into a list() within 'data_table' assigned to 'Location_list'

print('Headquarters Location list')
print(Location_list)

# %% [markdown]
# DataFrame Creation

# %%

Sp500_columns = pd.DataFrame({
    'Ticker': tickers_list,
    'Company': company_list,                                                  # Adding my variables ('tickers_list','stocks_list, and 'sectors_list') to a new Dataframe 'Sp500_columns' using pandas & {}
    'Sector': sectors_list,
    'Industry': Industry_list,
    'Location': Location_list,}) 

print(Sp500_columns)

# %% [markdown]
# Spiting 'Location' Column Into City & State

# %%
# Split 'Location' column into 'City' and 'State'
split_location = Sp500_columns['Location'].str.split(", ", n=1, expand=True)

if len(split_location.columns) == 2:  # Ensure split results in two columns
    Sp500_columns[['City', 'State']] = split_location
    Sp500_columns = Sp500_columns.drop(columns=['Location'])
else:
    print("Error: Unable to split 'Location' column into 'City' and 'State' properly.")

print(Sp500_columns)


# %% [markdown]
# Importing Market Caps From Each Ticker 

# %%
data = []

# Fetch data for tickers
for ticker in tickers_list:
    try:
        stock = yf.Ticker(ticker)
        info = stock.info

        # Extracting information
        market_cap = info.get('marketCap', None)
        name = info.get('longName', 'N/A')
        sector = info.get('sector', 'N/A')

         # Append data to list
        data.append({
                'Ticker': ticker,
                'Company': name,
                'Sector': sector,
                'Market_Cap': market_cap
            })
       

    except Exception as e:
        print(f"Error retrieving data for {ticker}: {e}")

# Create DataFrame from the data
SP500 = pd.DataFrame(data)

# Sort by market cap
SP500_MCS = SP500.sort_values(by='Market_Cap', ascending=False)

# %%
SP500_MCS.drop('Company', axis=1, inplace=True)
                                                                     # Dropping Unnecessary columns from DataFrame(SP500_MC)
SP500_MCS.drop('Sector', axis=1, inplace=True)

print(SP500_MCS)

# %% [markdown]
# Data Formatting (Market Cap) & Function Creation 

# %%
def format_market_cap(market_cap):
    if isinstance(market_cap, (int, float)):
                                                                   # If it's already a numeric value, format it accordingly
        if market_cap >= 1e12:
            return "${:.2f} T".format(market_cap / 1e12)
        elif market_cap >= 1e9:
            return "${:.2f} B".format(market_cap / 1e9)
        elif market_cap >= 1e6:
            return "${:.2f} M".format(market_cap / 1e6)
        else:
            return "${:.2f}".format(market_cap)
    else:
        try:
                                                                   # Convert the market cap value to a floating-point number
            market_cap = float(market_cap.replace('$', '').replace('B', 'e9').replace('M', 'e6').replace('T', 'e12'))

                                                                   # Check the magnitude of the market cap and append 'T', 'B', or 'M' accordingly
            if market_cap >= 1e12:
                return "${:.2f} T".format(market_cap / 1e12)
            elif market_cap >= 1e9:
                return "${:.2f} B".format(market_cap / 1e9)
            elif market_cap >= 1e6:
                return "${:.2f} M".format(market_cap / 1e6)
            else:
                return "${:.2f}".format(market_cap)
        except Exception as e:
            
            print(f"Error formatting market cap: {e}")
            return market_cap                                       # Return the original value if there's an issue with formatting


SP500_MCS['Market_Cap'] = SP500_MCS['Market_Cap'].apply(format_market_cap)

print(SP500_MCS)

# %% [markdown]
# Data Mapping 

# %%
market_cap_mapping = {
'DAY': '$7.81 B',}

for ticker, market_cap_value in market_cap_mapping.items():                                                                 
   SP500_MCS.loc[SP500_MCS['Ticker'] == ticker, 'Market_Cap'] = market_cap_value 

print(SP500_MCS)

# %% [markdown]
# Data Merging (JOIN) Drop And Inserting Column

# %%
Updated_Sp500_columns = pd.merge(Sp500_columns, SP500_MCS[['Ticker', 'Market_Cap']], left_on='Ticker', right_on='Ticker', how='right')             # pd.merge is used to merge two DataFrames 'JOIN'


Updated_Sp500_columns.drop('Industry', axis=1, inplace=True)                                                                                        # Dropping Unnecessary Columns in merged DataFrames


Updated_Sp500_columns.insert(0, 'id', range(1, len(Updated_Sp500_columns) + 1))

# %% [markdown]
# Downloading Realtime Data & Function Creation (Date, Open, Close  & Weekly Volume) 

# %%
def download_stock_data(ticker, period="3y", interval="1d"):
    try:
        stock_data = yf.download(ticker, period=period, interval=interval)
        return stock_data
    except Exception as e:
        print(f"Error downloading data for {ticker}: {e}")
        return None

def calculate_Date_weekly(stock_data):
    if not isinstance(stock_data.index, pd.DatetimeIndex):
        raise KeyError("DatetimeIndex not found in data.")

    # Resample the data to weekly frequency, considering Friday as the end of the week
    Weekly_Date = stock_data.resample('W-Fri').last()

    most_recent_weekly_date = Weekly_Date.index[-1]
    date_part = most_recent_weekly_date.strftime('%Y-%m-%d')

    return date_part

def calculate_open_weekly_average(stock_data):
    Open_SP500 = stock_data['Open'].resample('W-Fri').last()
    most_recent_weekly_open = Open_SP500.iloc[-1]
    return most_recent_weekly_open

def calculate_close_weekly_average(stock_data):
    Close_SP500 = stock_data['Close'].resample('W-Fri').last()
    most_recent_weekly_close = Close_SP500.iloc[-1]
    return most_recent_weekly_close


def calculate_volume_weekly_average(ticker_weekly_data):                                                                               # Defines a function to calculate the weekly average
    weekly_average = ticker_weekly_data['Volume'].resample('W-Fri').last()                                                    # Calculates the mean of the weekly average volumes for each ticker
    
    most_recent_weekly_volume = weekly_average.iloc[-1]                                                                        # Get the most recent weekly open value
    return most_recent_weekly_volume  


# Apply calculations to each row in the DataFrame
Updated_Sp500_columns['Date'] = Updated_Sp500_columns['Ticker'].apply(
    lambda ticker: calculate_Date_weekly(download_stock_data(ticker))
)

Updated_Sp500_columns['Weekly_Open'] = Updated_Sp500_columns['Ticker'].apply(
    lambda ticker: calculate_open_weekly_average(download_stock_data(ticker))
)

Updated_Sp500_columns['Weekly_Close'] = Updated_Sp500_columns['Ticker'].apply(
    lambda ticker: calculate_close_weekly_average(download_stock_data(ticker))
)

Updated_Sp500_columns['Weekly_Volume'] = Updated_Sp500_columns['Ticker'].apply(
    lambda ticker: calculate_volume_weekly_average(download_stock_data(ticker))
)

# %% [markdown]
# Column Creation Weekly Percentage (Open/Close) 

# %%
Updated_Sp500_columns['Weekly_Percentage'] = (((Updated_Sp500_columns['Weekly_Close'] - Updated_Sp500_columns['Weekly_Open']) / Updated_Sp500_columns['Weekly_Open']) * 100).round(2)

# %% [markdown]
# Reapplying Column (Weekly Volume) to the end 

# %%
Weekly_Volume = Updated_Sp500_columns.pop('Weekly_Volume')

Updated_Sp500_columns['Weekly_Volume'] = Weekly_Volume

# %% [markdown]
# Data Formatting (Open, Close & Volume) 

# %%
locale.setlocale(locale.LC_ALL, '')                                                                                # Using 'locale' to  formatting the 'Average Weekly Open' column in a DataFrame by adding commas as thousand separators to make it more readable.

Updated_Sp500_columns['Weekly_Open'] = Updated_Sp500_columns['Weekly_Open'].apply(lambda x: locale.format_string("%d", x, grouping=True))


locale.setlocale(locale.LC_ALL, '')                                                                                # Using 'locale' to  formatting the 'Average Weekly Close' column in a DataFrame by adding commas as thousand separators to make it more readable.

Updated_Sp500_columns['Weekly_Close'] = Updated_Sp500_columns['Weekly_Close'].apply(lambda x: locale.format_string("%d", x, grouping=True))


locale.setlocale(locale.LC_ALL, '')                                                                                # Using 'locale' to  formatting the 'Weekly Average Volume' column in a DataFrame by adding commas as thousand separators to make it more readable.

Updated_Sp500_columns['Weekly_Volume'] = Updated_Sp500_columns['Weekly_Volume'].apply(lambda x: locale.format_string("%d", x, grouping=True))

# %% [markdown]
# Changing Data Types (int)

# %%
Updated_Sp500_columns['Weekly_Open'] = Updated_Sp500_columns['Weekly_Open'].astype(int)                   

Updated_Sp500_columns['Weekly_Close'] = Updated_Sp500_columns['Weekly_Close'].astype(int)                    # Converting Columns To int()
 
Updated_Sp500_columns['Weekly_Volume'] = Updated_Sp500_columns['Weekly_Volume'].astype(int)                   

# %% [markdown]
# Downloading Historical Data Function Creation (Date, Open, Close, Volume)

# %%
def collect_historical_data(ticker):
    stock_data = download_stock_data(ticker)
    
    if stock_data is not None:
        historical_data = stock_data.reset_index()[['Date', 'Open', 'Close', 'Volume']]
        # Convert 'Date' to datetime format and keep only the date part
        historical_data['Date'] = pd.to_datetime(historical_data['Date']).dt.strftime('%Y-%m-%d')
        return historical_data
    else:
        return None

# Create a list to store DataFrames for each stock
all_historical_data = []

# Create a list of tickers from Updated_Sp500_columns DataFrame 
SP_ticker_list = Updated_Sp500_columns['Ticker'].tolist()

# Iterate through each ticker and collect historical data
for ticker in SP_ticker_list:
    historical_data = collect_historical_data(ticker)
    if historical_data is not None:
        # Add the ticker and sector as columns for identification
        historical_data['Ticker'] = ticker
        historical_data['Sector'] = Updated_Sp500_columns.loc[
            Updated_Sp500_columns['Ticker'] == ticker, 'Sector'
        ].values[0]
        all_historical_data.append(historical_data)

# Concatenate all DataFrames into a single DataFrame
historical_data_df = pd.concat(all_historical_data, ignore_index=True)

# Merge with the original Updated_Sp500_columns DataFrame on the 'Ticker' column
SP500_Historical_Data = pd.merge(Updated_Sp500_columns, historical_data_df, on='Ticker', how='left')

# Drop unnecessary columns in one call
columns_to_drop = ['City','State','Date_x', 'Weekly_Open', 'Weekly_Close', 'Weekly_Volume',
                    'Market_Cap', 'Sector_y', 'Weekly_Percentage']
SP500_Historical_Data = SP500_Historical_Data.drop(columns=columns_to_drop)

# Rename columns
SP500_Historical_Data = SP500_Historical_Data.rename(columns={'Date_y': 'Date', 'Sector_x': 'Sector'})

# Round multiple columns
columns_to_round = ['Open', 'Close']
SP500_Historical_Data = SP500_Historical_Data.round({col: 2 for col in columns_to_round})

# Calculate Percentage Difference
SP500_Historical_Data['Difference'] = (
    ((SP500_Historical_Data['Close'] - SP500_Historical_Data['Open']) / SP500_Historical_Data['Open']) * 100
).round(2)

# %% [markdown]
# Reapplying Column (Volume) to the end 

# %%
Volume = SP500_Historical_Data.pop('Volume')
SP500_Historical_Data['Volume'] = Volume

print("Resulting S&P 500 Historical DataFrame:")

SP500_Historical_Data

# %% [markdown]
# Reapplying Column (Market Cap) to the end 

# %%
Market_Cap = Updated_Sp500_columns.pop('Market_Cap')
Updated_Sp500_columns['Market_Cap'] = Market_Cap

print("Resulting S&P 500 Realtime DataFrame:")

Updated_Sp500_columns

# %% [markdown]
# Checking Data Types

# %%
SP500_Historical_Data.info()

# %%
Updated_Sp500_columns.info()

# %% [markdown]
# Exporting DataFrames to Tableau 

# %%
# Realtime Data CSV
Tableau_path = '/Users/quantumsphere/Desktop/SP500 Analyzer/S&P Tableau Files/Tableau_Realtime.csv'

Updated_Sp500_columns.to_csv(Tableau_path, index=False)

# Historical Data CSV
Tableau_Historical_path = '/Users/quantumsphere/Desktop/SP500 Analyzer/S&P Tableau Files/Tableau_Historical.csv'

SP500_Historical_Data.to_csv(Tableau_Historical_path, index=False)

# %% [markdown]
# Exporting DataFrame to Excel

# %%
folder_path = '/Users/quantumsphere/Desktop/SP500 Analyzer/S&P Excel Files'
file_name = 'SP500_Realtime_Excel.xlsx'

# Get the full path to the Excel file
excel_file_path = os.path.join(folder_path, file_name)

# Make sure the folder exists, create it if not
os.makedirs(folder_path, exist_ok=True)

# Save the DataFrame to Excel
Updated_Sp500_columns.to_excel(excel_file_path, index=False, )

# %% [markdown]
# lowercase Columns with (str.lower)

# %%
# Changing all columns to lowercase 
SP500_Historical_Data.columns = map(str.lower, SP500_Historical_Data.columns)

Updated_Sp500_columns.columns = map(str.lower, Updated_Sp500_columns.columns)

# %% [markdown]
# Python Visualizations (Line Charts)

# %%
SP500_Historical_Data['date'] = pd.to_datetime(SP500_Historical_Data['date'], errors='coerce')

latest_date = SP500_Historical_Data['date'].max()
start_date = latest_date - pd.DateOffset(years=1)
filtered_data = SP500_Historical_Data[SP500_Historical_Data['date'] >= start_date]

filtered_data.set_index('date', inplace=True)
weekly_data = weekly_data = filtered_data.groupby([pd.Grouper(freq='W'), 'sector']).agg({
    'difference': 'sum'
}).reset_index()

# Create the chart
line_chart = alt.Chart(weekly_data).mark_line().encode(
    x=alt.X('date:T', axis=alt.Axis(format='%m/%d/%Y')),
    y='difference:Q',
    color='sector:N',
    tooltip=[
        alt.Tooltip('sector:N'),
        alt.Tooltip('difference:Q'),
        alt.Tooltip('date:T', format='%m/%d/%Y')
        
        
    ]
).facet(
    facet='sector:N',
    columns=3
).properties(
    title='S&P 500 Sector Charts'
)

line_chart.show()

# %% [markdown]
# SQLAlchemy Engine For PostgreSQL Render Realtime Data

# %%
# Load environment variables from .env file
load_dotenv()

# Get the connection string from the environment variables
connection_string = os.getenv("CONNECTION_STRING")

# Check if the connection string is retrieved successfully
if connection_string is None:
    raise ValueError("Connection string not found in environment.")

print("Connection string retrieved from environment.")


# Create the SQLAlchemy engine
engine = create_engine(connection_string)

# Define the table name
table_name = 'sp500_realtime_data'

# Assuming Updated_Sp500_columns is your DataFrame
Updated_Sp500_columns.to_sql(table_name, engine, index=False, if_exists='replace', 
                             dtype={'id': Integer,
                                    'ticker': Text,
                                    'company': Text,
                                    'sector': Text,
                                    'city': Text,
                                    'state': Text,
                                    'date': Date,
                                    'weekly_open': Float,
                                    'weekly_close': Float,
                                    'weekly_percentage': Float,
                                    'weekly_volume': Integer,
                                    'market_cap': Text})

# %% [markdown]
# SQLAlchemy Engine For PostgreSQL Render Historical Data

# %%
# Load environment variables from .env file
load_dotenv()

# Get the connection string from the environment variables
connection_string = os.getenv("CONNECTION_STRING")

# Check if the connection string is retrieved successfully
if connection_string is None:
    raise ValueError("Connection string not found in environment.")

print("Connection string retrieved from environment.")

# Create the SQLAlchemy engine
engine = create_engine(connection_string)

# Assuming SP500_Historical_Data is your DataFrame
SP500_Historical_Data.to_sql('sp500_historical_data', engine, index=False, if_exists='replace', 
                             dtype={'id': Integer,
                                    'ticker': Text,
                                    'company': Text,
                                    'sector': Text,
                                    'date': Date,
                                    'open': Float,
                                    'close': Float,
                                    'difference': Float,
                                    'volume': Integer})


# %% [markdown]
# Exporting DataFrame To SQLite With Primary Key (Realtime Data)  

# %%
project_path = '/Users/quantumsphere/Desktop/SP500 Analyzer'
instance_folder = 'Instance'
SP_Database = 'SP500_Database.db'
SP500_Database_Path = os.path.join(project_path, instance_folder, SP_Database)

# Connect to the SQLite database using the new file path
conn = sqlite3.connect(SP500_Database_Path)

Updated_Sp500_columns.to_sql('SP500_Realtime_Data', conn, index=False, if_exists='replace')   # Adding DataFrame To SQLite Database

cursor = conn.cursor()    # Starting SQL Query 

                          # Creating New Table With Primary Key (Realtime Data)
cursor.execute('''         
    CREATE TABLE IF NOT EXISTS SP500_Columns_Key_New (
        id INTEGER PRIMARY KEY,
        ticker TEXT,
        company TEXT,
        sector TEXT,
        city TEXT,
        state TEXT,
        date DATE,
        weekly_open REAL,
        weekly_close REAL,
        weekly_percentage INTEGER,
        weekly_volume INTEGER,
        market_cap TEXT
    )''')


conn.commit()

                          # Copying Data From Existing Table To The New Table With Primary Key
cursor.execute('''
    INSERT INTO SP500_Columns_Key_New ( ticker, company, sector, city, state, date, weekly_open, weekly_close, weekly_percentage, weekly_volume, market_cap)    
    SELECT ticker, company, sector, city, state, date, weekly_open, weekly_close, weekly_percentage, weekly_volume, market_cap
    FROM SP500_Realtime_Data
''')

conn.commit()

                         # Renaming Both Tables One With Primary Key and One Without
cursor.execute('''
    ALTER TABLE SP500_Realtime_Data RENAME TO SP500_Columns_Key_Old
''')

cursor.execute('''
    ALTER TABLE SP500_Columns_Key_New RENAME TO SP500_Realtime_Data
''')

conn.commit()

                        # Dropping Old Table 
cursor.execute('''
    DROP TABLE IF EXISTS SP500_Columns_Key_Old
''')

conn.close()

# %% [markdown]
# Exporting DataFrame To SQLite With Primary Key (Historical Data)

# %%
# Connect to the SQLite database using the new file path
conn = sqlite3.connect(SP500_Database_Path)

# Adding DataFrame to SQLite Database
SP500_Historical_Data.to_sql('SP500_Historical_Data', conn, index=False, if_exists='replace')

cursor = conn.cursor()    # Starting SQL Query

# Creating a new table (Historical Data)
cursor.execute('''
    CREATE TABLE IF NOT EXISTS SP500_Historical_Data (
        id INTEGER PRIMARY KEY,
        ticker TEXT,
        company TEXT,
        sector TEXT,
        date DATE,
        open REAL,
        close REAL,
        difference INTEGER,
        volume INTEGER
    )''')

conn.commit()

# Copying data from the existing table to the new table with a primary key
cursor.execute('''
    INSERT INTO SP500_Historical_Data (ticker, company, sector, date, open, close, difference, volume)    
    SELECT ticker, company, sector, date, open, close, difference, volume
    FROM SP500_Historical_Data
''')

conn.commit()

conn.close()


