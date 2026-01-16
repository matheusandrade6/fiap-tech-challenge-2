import yfinance as yf
import pandas as pd
import boto3
from io import BytesIO
from botocore.exceptions import ClientError

class DataCollector:
    """
    Coletor de dados do yfinance e salvamento em parquet no S3 com partição por data
    """

    def __init__(self, tickers: str | list, bucket_name: str, s3_prefix: str = 'raw-data/'):
        self.tickers = tickers if isinstance(tickers, list) else [tickers]
        self.bucket_name = bucket_name
        self.s3_prefix = s3_prefix
        self.s3_client = boto3.client('s3')

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

        df = pd.concat(df_dict.values()).reset_index()
        df = pd.DataFrame(df)
        return df #pd.concat(df_dict.values()).reset_index()

    
    def save_data(self, df: pd.DataFrame, path:str):
        """
        Save the DataFrame to a parquet file.

        Args:
            df (pandas.DataFrame): The DataFrame to save.
            filename (str): The name of the file to save the data to.
        """

        tickers = self.tickers

        for ticker in (tickers if isinstance(tickers, list) else [tickers]):
            df = df[df['Ticker'] == ticker]
            PATH = os.path.join(path, ticker, f"{ticker}_data.parquet")

            df.to_parquet(PATH)

if __name__ == '__main__':
    collector = DataCollector(tickers=["PETR4.SA", "VALE3.SA"])
    data = collector.fetch_data()
    print(data.head())
    collector.save_data(data, 'data/raw/')
    