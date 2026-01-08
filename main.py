import yfinance as yf
import polars as pl
import pandas as pd

ticker = yf.Ticker("PETR4.SA")
data = yf.download(["PETR4.SA", "HGLG11.SA"], period="5d")  # últimos 5 dias
df = pd.DataFrame(data)

print(data)
print(df.head())

