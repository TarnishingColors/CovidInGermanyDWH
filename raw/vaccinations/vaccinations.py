import requests
from pipeline.load import S3Load, Table
from pipeline.utils import Level

from raw import add_date_column

load_table = Table(schema='default', table_name='vaccinations')

url = 'https://api.corona-zahlen.org/vaccinations/history/15'

response = add_date_column(requests.get(url).json())

s3 = S3Load(df=response, table=load_table, level=Level.raw)
s3.upload_data_as_json(days_delta=-1)
