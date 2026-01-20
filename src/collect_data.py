import yfinance as yf
import pandas as pd
import datetime as dt
import boto3
from io import BytesIO
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv('.env')

class DataCollector:
    """
    Coletor de dados do yfinance e salvamento em parquet no S3 com partição por df
    """

    def __init__(self, tickers: str | list, bucket_name: str, s3_prefix: str):
        self.tickers = tickers if isinstance(tickers, list) else [tickers]
        self.bucket_name = bucket_name
        self.s3_prefix = s3_prefix
        self.s3_client = boto3.client('s3')

    def fetch_data(self, ticker: str) -> pd.DataFrame:
        """
        Coleta dos dados a partir do ticker.

        Returns:
            pandas.DataFrame: Dados do mercado de ações.
        """
        try:
            yf_instance = yf.Ticker(ticker=ticker)
            df = yf_instance.history(period='1d')
            df['Ticker'] = ticker
            print(f'Dados coletados para: {ticker} (Registros: {len(df)})')
            return df
        except Exception as e:
            print(f'Erro ao coletar dados para {self.tickers}: {e}')
            return pd.DataFrame()

            
    def save_to_s3(self, df: pd.DataFrame, ticker: str):
        """
        Salvar os dataframes em arquivos .parquet no S3.

        Args:
            df (pandas.DataFrame): The DataFrame to save.
        """

        if df.empty:
            return print(f'Nenhum dado para salvar para: {ticker}')
        pass
            
        buffer = BytesIO()
        df.reset_index(inplace=True)
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        date = df['Date'].iloc[0]
        year = date.year
        month = date.month
        day = date.day

        s3_key = f"{self.s3_prefix}ticker={ticker}/year={year}/month={month}/day={day}/{ticker}.parquet"

        try:
            self.s3_client.upload_fileobj(buffer, self.bucket_name, s3_key)
            print(f'Arquivo salvo no S3: s3://{self.bucket_name}/{s3_key}')
        except ClientError as e:
            print(f'Erro ao salvar arquivo no S3 para {ticker}: {e}')
        
    def run(self):
        """
        Executa o pipeline completo: coleta e salva
        """
        for ticker in self.tickers:
            df = self.fetch_data(ticker)
            self.save_to_s3(df=df, ticker=ticker)
        
if __name__ == '__main__':
    BUCKET_NAME = 'mlet-financial-df-matheus'
    TICKERS = ['HGLG11.SA', 'PETR4.SA']
    S3_PREFIX = 'raw/'

    collector = DataCollector(tickers=TICKERS, bucket_name=BUCKET_NAME, s3_prefix=S3_PREFIX)
    df = collector.fetch_data(ticker='HGLG11.SA')
    print(df.head())
    