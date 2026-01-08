import yfinance as yf
import pandas as pd
import datetime as dt
import os

class DataCollector:
    """
    Docstring for DataCollector
    """

    def __init__(self, tickers: str | list):
        self.tickers = tickers

    def fetch_data(self):
        """
        Fetch historical market data for the given tickers symbol.

        Returns:
            pandas.DataFrame: Historical market data.
        """
        tickers = self.tickers
        
        df_dict = {}

        if isinstance(tickers, list):
            yf_instance = yf.Tickers(tickers=tickers)
            for ticker in tickers: # fazer esse loop para cada ticker e gerar um df separado
                data = yf_instance.history(period='3mo')
                data['Ticker'] = ticker
                df_dict[ticker] = data

    
    def save_data(self, df: pd.DataFrame, filename:str):
        """
        Save the DataFrame to a CSV file.

        Args:
            df (pandas.DataFrame): The DataFrame to save.
            filename (str): The name of the file to save the data to.
        """

        tickers = self.tickers

        for ticker in (tickers if isinstance(tickers, list) else [tickers]):
            df = df.filter(df['Ticker'] == ticker)


        df.to_csv(filename)
    
if __name__ == '__main__':
    collector = DataCollector(tickers="PETR4.SA")
    data = collector.fetch_data()
    filename = 'data/raw/data.csv'
    collector.save_data(data, filename)
    