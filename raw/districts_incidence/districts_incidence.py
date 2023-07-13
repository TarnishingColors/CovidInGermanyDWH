import requests
from pipeline.load import S3Load, Table
from pipeline.utils import Level


load_table = Table(schema='default', table_name='districts_incidence')

url = 'https://api.corona-zahlen.org/districts/history/incidence/1'

response = requests.get(url).json()

s3 = S3Load(df=response, table=load_table, level=Level.raw)
s3.upload_data_as_json(days_delta=-1)
