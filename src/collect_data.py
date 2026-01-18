import yfinance as yf
import pandas as pd
import datetime as dt
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
        Coleta dos dados a partir do ticker.

        Returns:
            pandas.DataFrame: Dados do mercado de ações.
        """
        df_dict = {}
            
        yf_instance = yf.Tickers(tickers=self.tickers)
        
        for ticker in self.tickers:
            try:
                data = yf_instance.history(period='1d')
                data['Ticker'] = ticker
                df_dict[ticker] = data
                print(f'Dados coletados para: {ticker} (Registros: {len(data)})')
            except Exception as e:
                print(f'Erro ao coletar dados para {ticker}: {e}')

        if not df_dict:
            print("Nenhum dado foi coletado.")
        
        df = pd.concat(df_dict.values()).reset_index()
        return df

    
    def save_to_s3(self, df: pd.DataFrame):
        """
        Salvar os dataframes em arquivos .parquet no S3.

        Args:
            df (pandas.DataFrame): The DataFrame to save.
        """

        data_atual = dt.datetime.now().strftime('%Y-%m-%d')

        for ticker in self.tickers:
            df_ticker = df[df['Ticker'] == ticker]
            if df_ticker.empty:
                print(f'Nenhum dado para salvar para: {ticker}')
                continue

            buffer = BytesIO()
            df_ticker.to_parquet(buffer, index=False)
            buffer.seek(0)

            s3_key = f"{self.s3_prefix}date={data_atual}/{ticker}/{ticker}.parquet"

            try:
                self.s3_client.upload_fileobj(buffer, self.bucket_name, s3_key)
                print(f'Arquivo salvo no S3: s3://{self.bucket_name}/{s3_key}')
            except ClientError as e:
                print(f'Erro ao salvar arquivo no S3 para {ticker}: {e}')
        
    def run(self):
        """
        Executa o pipeline completo: coleta e salva
        """

        df = self.fetch_data()
        self.save_to_s3(df)
        
if __name__ == '__main__':
    BUCKET_NAME = 'mlet-financial-data-matheus'
    TICKERS = ['HGLG11.SA', 'PETR4.SA']

    collector = DataCollector(tickers=TICKERS, bucket_name=BUCKET_NAME, s3_prefix='raw-data/')

    collector.run()
    