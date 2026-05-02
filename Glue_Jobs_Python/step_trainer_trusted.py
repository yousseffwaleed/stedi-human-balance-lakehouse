import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    spark = glueContext.spark_session
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = "stedi-human-balance-ye"

# ✅ Landing source from S3 (JSON)
step_trainer_landing = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [f"s3://{BUCKET}/landing/step_trainer_landing/"],
        "recurse": True
    },
    transformation_ctx="step_trainer_landing"
)

# Curated customer set (Catalog) — if you don’t have customer_curated, swap to customer_trusted
customer_curated = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="customer_curated"
)

query = """
SELECT s.serialnumber, s.sensorreadingtime, s.distancefromobject
FROM s
INNER JOIN c
ON s.serialnumber = c.serialnumber
"""

step_trainer_trusted = sparkSqlQuery(
    glueContext,
    query=query,
    mapping={"s": step_trainer_landing, "c": customer_curated},
    transformation_ctx="step_trainer_trusted"
)

sink = glueContext.getSink(
    path=f"s3://{BUCKET}/trusted/step_trainer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_sink"
)
sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="step_trainer_trusted")
sink.setFormat("parquet")
sink.writeFrame(step_trainer_trusted)

job.commit()