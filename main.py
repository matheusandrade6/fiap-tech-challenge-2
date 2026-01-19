from src.collect_data import DataCollector
from src.config import S3_PREFIX, TICKERS, BUCKET_NAME

collector = DataCollector(
    tickers=TICKERS,
    bucket_name=BUCKET_NAME,
    s3_prefix=S3_PREFIX
)

collector.run()
