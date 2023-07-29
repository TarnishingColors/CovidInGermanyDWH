from datetime import datetime

from pipeline.extract import S3Extract
from pipeline.load import HiveLoad, Table
from pipeline.utils import Level
from pipeline.utils.date import get_yesterday_date
from pipeline.utils.df_func import union_multiple_dfs
from pyspark import StorageLevel
from pyspark.sql import functions as F

previous_date = get_yesterday_date()
ext = S3Extract("ods_districts_age_groups")
df = ext.extract(file_dir=f"raw/default/districts_age_groups_{previous_date}.json")

raw_df = (
    df.selectExpr(
        "EXPLODE(data) AS district_data",
        "meta.source AS source",
        "meta.info AS info",
        "TO_DATE(meta.lastUpdate) AS last_update_date",
    )
    .withColumn('district_age_groups_date', F.to_date(F.lit(previous_date)))
).persist(StorageLevel.DISK_ONLY)

first_df = (
    raw_df
    .selectExpr(
        "*",
        "district_data.`A00-A04`.casesMale AS cases_male",
        "district_data.`A00-A04`.casesFemale AS cases_female",
        "district_data.`A00-A04`.deathsMale AS deaths_male",
        "district_data.`A00-A04`.deathsFemale AS deaths_female",
        "district_data.`A00-A04`.casesMalePer100k AS cases_male_per_100k",
        "district_data.`A00-A04`.casesFemalePer100k AS cases_female_per_100k",
        "district_data.`A00-A04`.deathsMalePer100k AS deaths_male_per_100k",
        "district_data.`A00-A04`.deathsFemalePer100k AS deaths_female_per_100k",
        "district_data.code AS district_code",
    )
    .withColumn('age_group', F.lit('A00-A04'))
    .drop("district_data")
)

second_df = (
    raw_df
    .selectExpr(
        "*",
        "district_data.`A05-A14`.casesMale AS cases_male",
        "district_data.`A05-A14`.casesFemale AS cases_female",
        "district_data.`A05-A14`.deathsMale AS deaths_male",
        "district_data.`A05-A14`.deathsFemale AS deaths_female",
        "district_data.`A05-A14`.casesMalePer100k AS cases_male_per_100k",
        "district_data.`A05-A14`.casesFemalePer100k AS cases_female_per_100k",
        "district_data.`A05-A14`.deathsMalePer100k AS deaths_male_per_100k",
        "district_data.`A05-A14`.deathsFemalePer100k AS deaths_female_per_100k",
        "district_data.code AS district_code",
    )
    .withColumn('age_group', F.lit('A05-A14'))
    .drop("district_data")
)

third_df = (
    raw_df
    .selectExpr(
        "*",
        "district_data.`A15-A34`.casesMale AS cases_male",
        "district_data.`A15-A34`.casesFemale AS cases_female",
        "district_data.`A15-A34`.deathsMale AS deaths_male",
        "district_data.`A15-A34`.deathsFemale AS deaths_female",
        "district_data.`A15-A34`.casesMalePer100k AS cases_male_per_100k",
        "district_data.`A15-A34`.casesFemalePer100k AS cases_female_per_100k",
        "district_data.`A15-A34`.deathsMalePer100k AS deaths_male_per_100k",
        "district_data.`A15-A34`.deathsFemalePer100k AS deaths_female_per_100k",
        "district_data.code AS district_code",
    )
    .withColumn('age_group', F.lit('A15-A34'))
    .drop("district_data")
)

fourth_df = (
    raw_df
    .selectExpr(
        "*",
        "district_data.`A35-A59`.casesMale AS cases_male",
        "district_data.`A35-A59`.casesFemale AS cases_female",
        "district_data.`A35-A59`.deathsMale AS deaths_male",
        "district_data.`A35-A59`.deathsFemale AS deaths_female",
        "district_data.`A35-A59`.casesMalePer100k AS cases_male_per_100k",
        "district_data.`A35-A59`.casesFemalePer100k AS cases_female_per_100k",
        "district_data.`A35-A59`.deathsMalePer100k AS deaths_male_per_100k",
        "district_data.`A35-A59`.deathsFemalePer100k AS deaths_female_per_100k",
        "district_data.code AS district_code",
    )
    .withColumn('age_group', F.lit('A35-A59'))
    .drop("district_data")
)

fifth_df = (
    raw_df
    .selectExpr(
        "*",
        "district_data.`A60-A79`.casesMale AS cases_male",
        "district_data.`A60-A79`.casesFemale AS cases_female",
        "district_data.`A60-A79`.deathsMale AS deaths_male",
        "district_data.`A60-A79`.deathsFemale AS deaths_female",
        "district_data.`A60-A79`.casesMalePer100k AS cases_male_per_100k",
        "district_data.`A60-A79`.casesFemalePer100k AS cases_female_per_100k",
        "district_data.`A60-A79`.deathsMalePer100k AS deaths_male_per_100k",
        "district_data.`A60-A79`.deathsFemalePer100k AS deaths_female_per_100k",
        "district_data.code AS district_code",
    )
    .withColumn('age_group', F.lit('A60-A79'))
    .drop("district_data")
)

sixth_df = (
    raw_df
    .selectExpr(
        "*",
        "district_data.`A80+`.casesMale AS cases_male",
        "district_data.`A80+`.casesFemale AS cases_female",
        "district_data.`A80+`.deathsMale AS deaths_male",
        "district_data.`A80+`.deathsFemale AS deaths_female",
        "district_data.`A80+`.casesMalePer100k AS cases_male_per_100k",
        "district_data.`A80+`.casesFemalePer100k AS cases_female_per_100k",
        "district_data.`A80+`.deathsMalePer100k AS deaths_male_per_100k",
        "district_data.`A80+`.deathsFemalePer100k AS deaths_female_per_100k",
        "district_data.code AS district_code",
    )
    .withColumn('age_group', F.lit('A80+'))
    .drop("district_data")
)

result_df = union_multiple_dfs([
    first_df, second_df, third_df, fourth_df, fifth_df, sixth_df
])

# action to evaluate the df before unpersisting the root one
result_df.show(truncate=False)
raw_df.unpersist()

load_table = Table(schema='default', table_name='districts_age_groups', periodic_column='district_age_groups_date')
hl = HiveLoad(level=Level.ods, df=result_df, table=load_table, spark=ext.spark)
hl.load_by_period()
