from pipeline.extract import S3Extract
from pipeline.load import HiveLoad, Table
from pipeline.utils import Level
from datetime import datetime, timedelta


previous_date = str((datetime.today() - timedelta(1)).date())
ext = S3Extract("ods_districts_incidence")
df = ext.extract(file_dir=f"raw/default/districts_incidence_{previous_date}.json")

result_df = (
    df.selectExpr(
        "EXPLODE(data) AS district_data",
        "meta.source AS source",
        "meta.info AS info",
        "TO_DATE(meta.lastUpdate) AS last_update_date",
    )
    .selectExpr(
        "*",
        "EXPLODE(district_data.history) AS incidence_data",
        "district_data.ags AS district_ags",
        "district_data.name AS district_name",
        "district_data.code AS district_code"
    )
    .selectExpr(
        "*",
        "incidence_data.weekIncidence AS district_week_incidence",
        "TO_DATE(incidence_data.date) AS district_incidence_date"
    )
    .drop("district_data", "incidence_data")
)

ext.spark.sql("drop table ods_districts_incidence")

load_table = Table(schema='default', table_name='districts_incidence', periodic_column='district_incidence_date')
hl = HiveLoad(level=Level.ods, df=result_df, table=load_table, spark=ext.spark)
hl.load_by_period()
