from pipeline.extract import S3Extract
from pipeline.load import HiveLoad, Table
from pipeline.utils import Level
from pipeline.utils.df_func import union_multiple_dfs
from pyspark.sql import functions as F
from datetime import datetime, timedelta


previous_date = str((datetime.today() - timedelta(1)).date())
ext = S3Extract("raw_districts_recovered")
df = ext.extract(file_dir=f"raw/default/districts_recovered_{previous_date}.json")
