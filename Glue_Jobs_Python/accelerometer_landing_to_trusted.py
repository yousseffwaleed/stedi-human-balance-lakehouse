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

accelerometer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing"
)

customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted"
)

query = """
SELECT a.user, a.timestamp, a.x, a.y, a.z
FROM a
INNER JOIN c
ON a.user = c.email
"""
accelerometer_trusted = sparkSqlQuery(
    glueContext,
    query=query,
    mapping={"a": accelerometer_landing, "c": customer_trusted},
    transformation_ctx="accelerometer_trusted"
)

sink = glueContext.getSink(
    path="s3://stedi-human-balance-ye/trusted/accelerometer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trusted_sink"
)
sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="accelerometer_trusted")
sink.setFormat("parquet")
sink.writeFrame(accelerometer_trusted)

job.commit()