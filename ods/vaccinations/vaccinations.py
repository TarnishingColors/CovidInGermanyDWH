from pipeline.extract import S3Extract
from pipeline.load import Table, HiveLoad
from pipeline.utils import Level
from pipeline.utils.date import get_yesterday_date


previous_date = get_yesterday_date()
ext = S3Extract("ods_vaccinations")
df = ext.extract(file_dir=f"raw/default/vaccinations_{previous_date}.json")

result_df = (
    df.selectExpr(
        "EXPLODE(data.history) AS vaccinations_by_day",
        "meta.source AS source",
        "meta.info AS info",
        "TO_DATE(meta.lastUpdate) AS last_update_date"
    )
    .selectExpr(
        "*",
        "TO_DATE(vaccinations_by_day.date) AS vaccination_date",
        "CAST(vaccinations_by_day.vaccinated AS INT) AS vaccinated",
        "CAST(vaccinations_by_day.firstVaccination AS INT) AS first_vaccination",
        "CAST(vaccinations_by_day.secondVaccination AS INT) AS second_vaccination",
        "CAST(vaccinations_by_day.firstBoosterVaccination AS INT) AS first_booster_vaccination",
        "CAST(vaccinations_by_day.secondBoosterVaccination AS INT) AS second_booster_vaccination",
        "CAST(vaccinations_by_day.thirdBoosterVaccination AS INT) AS third_booster_vaccination",
        "CAST(vaccinations_by_day.fourthBoosterVaccination AS INT) AS fourth_booster_vaccination",
        "CAST(vaccinations_by_day.totalVacciantionOfTheDay AS INT) AS total_vacciantion_by_day",
    )
    .drop("vaccinations_by_day")
).fillna(0)

result_df.show(truncate=False)

load_table = Table(schema='default', table_name='vaccinations', periodic_column='vaccination_date')
hl = HiveLoad(level=Level.ods, df=result_df, table=load_table, spark=ext.spark)
hl.load_by_period()
