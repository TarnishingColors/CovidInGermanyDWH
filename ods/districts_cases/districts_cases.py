from pipeline.extract import S3Extract
from pipeline.load import HiveLoad, Table
from pipeline.utils import Level
from pipeline.utils.date import get_yesterday_date


previous_date = get_yesterday_date()
ext = S3Extract("ods_districts_cases")
df = ext.extract(file_dir=f"raw/default/districts_cases_{previous_date}.json")

result_df = (
    df.selectExpr(
        "EXPLODE(data) AS district_data",
        "meta.source AS source",
        "meta.info AS info",
        "TO_DATE(meta.lastUpdate) AS last_update_date",
    )
    .selectExpr(
        "*",
        "EXPLODE(district_data.history) AS cases_data",
        "district_data.ags AS district_ags",
        "district_data.name AS district_name",
        "district_data.code AS district_code"
    )
    .selectExpr(
        "*",
        "cases_data.cases AS district_cases_amount",
        "TO_DATE(cases_data.date) AS district_cases_date"
    )
    .drop("district_data", "cases_data")
)

load_table = Table(schema='default', table_name='districts_cases', periodic_column='district_cases_date')
hl = HiveLoad(level=Level.ods, df=result_df, table=load_table, spark=ext.spark)
hl.load_by_period()
