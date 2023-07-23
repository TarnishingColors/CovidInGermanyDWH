from pipeline.extract import S3Extract
from pipeline.load import HiveLoad, Table
from pipeline.utils import Level
from pipeline.utils.date import get_yesterday_date
from pipeline.utils.df_func import union_multiple_dfs
from pyspark.sql import functions as F


previous_date = get_yesterday_date()
ext = S3Extract("ods_states_vaccinations")
df = ext.extract(file_dir=f"raw/default/states_vaccinations_{previous_date}.json")

parsed_data = (
    df.select(
        F.explode(F.col("data")).alias("state_data"),
        F.col("meta.source").alias("source"),
        F.col("meta.info").alias("info_link"),
        F.to_date(F.col("meta.lastUpdate")).alias("last_update_date"),
        F.to_date(F.col("load_date")).alias("load_date")
    ).withColumn("state_code", F.col("state_data.code"))
    .withColumn("state_name", F.col("state_data.name"))
    .withColumn("state_administered_vaccinations", F.col("state_data.administeredVaccinations"))
)

states_first_vaccinations = parsed_data.select(
    "state_code",
    "state_name",
    "last_update_date",
    "load_date",
    "state_data.vaccinated",
    "state_data.delta",
    "state_data.vaccination.astraZeneca",
    "state_data.vaccination.biontech",
    "state_data.vaccination.janssen",
    "state_data.vaccination.moderna",
    "state_data.vaccination.novavax",
    "state_data.vaccination.valneva",
    "state_data.vaccination.biontechBivalent",
    "state_data.vaccination.modernaBivalent",
    "state_data.vaccination.biontechInfant"
).withColumn("vaccination_type", F.lit("first")).fillna(0)

states_second_vaccinations = parsed_data.select(
    "state_code",
    "state_name",
    "last_update_date",
    "load_date",
    "state_data.secondVaccination.vaccinated",
    "state_data.secondVaccination.delta",
    "state_data.secondVaccination.vaccination.astraZeneca",
    "state_data.secondVaccination.vaccination.biontech",
    "state_data.secondVaccination.vaccination.janssen",
    "state_data.secondVaccination.vaccination.moderna",
    "state_data.secondVaccination.vaccination.novavax",
    "state_data.secondVaccination.vaccination.valneva",
    "state_data.secondVaccination.vaccination.biontechBivalent",
    "state_data.secondVaccination.vaccination.modernaBivalent",
    "state_data.secondVaccination.vaccination.biontechInfant"
).withColumn("vaccination_type", F.lit("second")).fillna(0)

states_booster_vaccinations = parsed_data.select(
    "state_code",
    "state_name",
    "last_update_date",
    "load_date",
    "state_data.boosterVaccination.vaccinated",
    "state_data.boosterVaccination.delta",
    "state_data.boosterVaccination.vaccination.astraZeneca",
    "state_data.boosterVaccination.vaccination.biontech",
    "state_data.boosterVaccination.vaccination.janssen",
    "state_data.boosterVaccination.vaccination.moderna",
    "state_data.boosterVaccination.vaccination.novavax",
    "state_data.boosterVaccination.vaccination.valneva",
    "state_data.boosterVaccination.vaccination.biontechBivalent",
    "state_data.boosterVaccination.vaccination.modernaBivalent",
    "state_data.boosterVaccination.vaccination.biontechInfant"
).withColumn("vaccination_type", F.lit("booster")).fillna(0)

states_second_booster_vaccinations = parsed_data.select(
    "state_code",
    "state_name",
    "last_update_date",
    "load_date",
    "state_data.2ndBoosterVaccination.vaccinated",
    "state_data.2ndBoosterVaccination.delta",
    "state_data.2ndBoosterVaccination.vaccination.astraZeneca",
    "state_data.2ndBoosterVaccination.vaccination.biontech",
    "state_data.2ndBoosterVaccination.vaccination.janssen",
    "state_data.2ndBoosterVaccination.vaccination.moderna",
    "state_data.2ndBoosterVaccination.vaccination.novavax",
    "state_data.2ndBoosterVaccination.vaccination.valneva",
    "state_data.2ndBoosterVaccination.vaccination.biontechBivalent",
    "state_data.2ndBoosterVaccination.vaccination.modernaBivalent",
    "state_data.2ndBoosterVaccination.vaccination.biontechInfant"
).withColumn("vaccination_type", F.lit("second_booster")).fillna(0)

states_third_booster_vaccinations = parsed_data.select(
    "state_code",
    "state_name",
    "last_update_date",
    "load_date",
    "state_data.3rdBoosterVaccination.vaccinated",
    "state_data.3rdBoosterVaccination.delta",
    "state_data.3rdBoosterVaccination.vaccination.astraZeneca",
    "state_data.3rdBoosterVaccination.vaccination.biontech",
    "state_data.3rdBoosterVaccination.vaccination.janssen",
    "state_data.3rdBoosterVaccination.vaccination.moderna",
    "state_data.3rdBoosterVaccination.vaccination.novavax",
    "state_data.3rdBoosterVaccination.vaccination.valneva",
    "state_data.3rdBoosterVaccination.vaccination.biontechBivalent",
    "state_data.3rdBoosterVaccination.vaccination.modernaBivalent",
    "state_data.3rdBoosterVaccination.vaccination.biontechInfant"
).withColumn("vaccination_type", F.lit("third_booster")).fillna(0)

states_fourth_booster_vaccinations = parsed_data.select(
    "state_code",
    "state_name",
    "last_update_date",
    "load_date",
    "state_data.4thBoosterVaccination.vaccinated",
    "state_data.4thBoosterVaccination.delta",
    "state_data.4thBoosterVaccination.vaccination.astraZeneca",
    "state_data.4thBoosterVaccination.vaccination.biontech",
    "state_data.4thBoosterVaccination.vaccination.janssen",
    "state_data.4thBoosterVaccination.vaccination.moderna",
    "state_data.4thBoosterVaccination.vaccination.novavax",
    "state_data.4thBoosterVaccination.vaccination.valneva",
    "state_data.4thBoosterVaccination.vaccination.biontechBivalent",
    "state_data.4thBoosterVaccination.vaccination.modernaBivalent",
    "state_data.4thBoosterVaccination.vaccination.biontechInfant"
).withColumn("vaccination_type", F.lit("fourth_booster")).fillna(0)

result_df = union_multiple_dfs([
    states_first_vaccinations,
    states_second_vaccinations,
    states_booster_vaccinations,
    states_second_booster_vaccinations,
    states_third_booster_vaccinations,
    states_fourth_booster_vaccinations
])

load_table = Table(schema='default', table_name='states_vaccinations', periodic_column='load_date')
hl = HiveLoad(level=Level.ods, df=result_df, table=load_table, spark=ext.spark)
hl.load_by_period()
