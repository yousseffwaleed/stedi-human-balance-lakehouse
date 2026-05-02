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

step_trainer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted"
)

accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted"
)

query = """
SELECT
  s.serialnumber,
  s.sensorreadingtime,
  s.distancefromobject,
  a.user,
  a.x, a.y, a.z
FROM a
INNER JOIN s
ON a.timestamp = s.sensorreadingtime
"""
machine_learning_curated = sparkSqlQuery(
    glueContext,
    query=query,
    mapping={"a": accelerometer_trusted, "s": step_trainer_trusted},
    transformation_ctx="machine_learning_curated"
)

sink = glueContext.getSink(
    path="s3://stedi-human-balance-ye/curated/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="ml_curated_sink"
)
sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="machine_learning_curated")
sink.setFormat("parquet")
sink.writeFrame(machine_learning_curated)

job.commit()