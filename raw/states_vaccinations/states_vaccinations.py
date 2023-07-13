import requests
from pipeline.load import S3Load, Table
from pipeline.utils import Level


load_table = Table(schema='default', table_name='states_hospitalization')

url = 'https://api.corona-zahlen.org/vaccinations/states'

response = requests.get(url).json()

s3 = S3Load(df=response, table=load_table, level=Level.raw)
s3.upload_data_as_json(days_delta=-1)
