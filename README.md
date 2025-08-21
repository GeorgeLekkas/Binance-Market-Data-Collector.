# Binance-Market-Data-Collector.
This script downloads 5-minute OHLC data for 1,400–1,500 cryptocurrencies, splits them into tables of 100 each, and merges them into two final tables. Each crypto keeps a single time series for efficient market analysis.


📌 Description
This project collects and organizes market data from Binance, creating a clean structure for large-scale analysis.
Fetches historical kline data for all active trading tickers within a specified interval.
Splits tickers into batches of 100, storing each batch in separate MySQL tables (my_table1, my_table2, …).
Cleans and aligns data by removing redundant indexes and synchronizing timestamps.
Merges the batch tables into two consolidated tables (final_table1, final_table2).
Keeps only one time series (closing prices) per cryptocurrency, ensuring a consistent dataset.
Enables efficient analysis of the entire market or targeted subsets over a chosen time window.

⚙️ Requirements
Python 3.8+

Binance API (python-binance)

Pandas

SQLAlchemy

MySQL with pymysql driver

🚀 Usage
Configure your MySQL connection string inside the script.
Set the interval (e.g., 5m, 15m) and time window.
Run the script to download data and generate tables.
Use the final tables (final_table1, final_table2) for further market analysis.

📊 Output
Intermediate tables: my_table1, my_table2, … (each with 100 tickers).
Final merged tables:
final_table1 – first set of tickers
final_table2 – second set of tickers
Each cryptocurrency is represented by a single time series of closing prices, making large-scale and subset analysis much faster.
