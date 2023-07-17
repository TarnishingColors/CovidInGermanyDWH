import requests
from pipeline.load import S3Load, Table
from pipeline.utils import Level

from raw import data_column_to_list, add_date_column

load_table = Table(schema='default', table_name='districts_cases')

# for some reason with days=1 does not show data, so it's a workaround
url = 'https://api.corona-zahlen.org/districts/history/cases/2'

response = data_column_to_list(add_date_column(requests.get(url).json()))

s3 = S3Load(df=response, table=load_table, level=Level.raw)
s3.upload_data_as_json(days_delta=-1)
