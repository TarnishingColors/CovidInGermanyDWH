from pipeline.extract import S3Extract
from pipeline.load import HiveLoad, Table
from pipeline.utils import Level
from pipeline.utils.date import get_yesterday_date
from pyspark.sql import functions as F


previous_date = get_yesterday_date()
ext = S3Extract("ods_districts_info")
df = ext.extract(file_dir=f"raw/default/districts_info_{previous_date}.json")

result_df = (
    df.selectExpr(
        "EXPLODE(data) AS district_data",
        "meta.source AS source",
        "meta.info AS info",
        "TO_DATE(meta.lastUpdate) AS last_update_date",
    )
    .selectExpr(
        "*",
        "district_data.ags AS district_ags",
        "district_data.name AS district_name",
        "district_data.state AS district_state",
        "district_data.population AS district_population",
        "district_data.cases AS district_cases",
        "district_data.deaths AS district_deaths",
        "district_data.casesPerWeek AS district_cases_per_week",
        "district_data.deathsPerWeek AS district_deaths_per_week",
        "district_data.stateAbbreviation AS state_abbreviation",
        "district_data.recovered AS district_recovered",
        "district_data.weekIncidence AS district_week_incidence",
        "district_data.casesPer100k AS district_cases_per_100k",
        "district_data.code AS district_code",
    )
    .withColumn('district_info_date', F.to_date(F.lit(previous_date)))
    .drop("district_data")
)

load_table = Table(schema='default', table_name='districts_info', periodic_column='district_info_date')
hl = HiveLoad(level=Level.ods, df=result_df, table=load_table, spark=ext.spark)
hl.load_by_period()
