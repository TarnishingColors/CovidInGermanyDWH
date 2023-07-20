from pipeline.extract import S3Extract
from pipeline.load import HiveLoad, Table
from pipeline.utils import Level
from datetime import datetime


current_date = str(datetime.today().date())
ext = S3Extract("ods_states_hospitalization")
df = ext.extract(file_dir=f"raw/default/states_hospitalization_{current_date}.json")

result_df = (
    df.selectExpr(
        "EXPLODE(data) AS state_data",
        "meta.source AS source",
        "meta.info AS info",
        "TO_DATE(meta.lastUpdate) AS last_update_date"
    )
    .selectExpr(
        "*",
        "EXPLODE(state_data.history) AS hospitalization_data",
        "state_data.code AS state_code",
        "state_data.id AS state_id",
        "state_data.name AS state_name"
    )
    .selectExpr(
        "*",
        "hospitalization_data.cases7Days",
        "hospitalization_data.incidence7Days",
        "TO_TIMESTAMP(hospitalization_data.date) AS hospitalization_date"
    )
    .drop("state_data", "hospitalization_data")
)

load_table = Table(schema='default', table_name='states_hospitalization', periodic_column='hospitalization_date')
hl = HiveLoad(level=Level.ods, df=result_df, table=load_table, spark=ext.spark)
hl.load_by_period()
