import yfinance as yf
import polars as pl
import datetime as dt

class DataCollector:
    """
    Docstring for DataCollector
    """

    def __init__(self, ticker: str | None = None):
        self.ticker = ticker

    def fetch_data(self):
        """
        Fetch historical market data for the given ticker symbol.

        Returns:
            polars.DataFrame: Historical market data.
        """
        ticker = self.ticker
        if not ticker:
            raise ValueError("Ticker symbol must be provided.")
        data_atual = dt.date.today()
        period = data_atual - dt.timedelta(days=30, seconds=0, microseconds=0)
        data = yf.Ticker(ticker)
        historical_data = data.history(period=period)
        df = pl.DataFrame(historical_data)

        return df
    
    def save_data(self, df: pl.DataFrame, filename:str):
        """
        Save the DataFrame to a CSV file.

        Args:
            df (polars.DataFrame): The DataFrame to save.
            filename (str): The name of the file to save the data to.
        """
        df.write_csv(filename)
    
if __name__ == '__main__':
    collector = DataCollector(ticker='PETR4.SA')
    data = collector.fetch_data()
    filename = 'data/raw/data.csv'
    collector.save_data(data, filename)
    