from src.collect_data import DataCollector
from src.config import S3_BUCKET_NAME, TICKERS, S3_PREFIX, AWS_REGION

def lambda_handler(event, context):
    """
    Docstring for lambda_handler
    
    :param event: Description
    :param context: Description
    """

    collector = DataCollector(tickers=TICKERS, bucket_name=S3_BUCKET_NAME, s3_prefix=S3_PREFIX, aws_region=AWS_REGION)
    collector.run()