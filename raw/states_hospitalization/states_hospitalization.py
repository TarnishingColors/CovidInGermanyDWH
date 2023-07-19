import requests
from pipeline.load import S3Load, Table
from pipeline.utils import Level

from raw import data_column_to_list, add_date_column

load_table = Table(schema='default', table_name='states_hospitalization')

url = 'https://api.corona-zahlen.org/states/history/hospitalization/3'

response = data_column_to_list(add_date_column(requests.get(url).json()))

s3 = S3Load(df=response, table=load_table, level=Level.raw)
s3.upload_data_as_json()
